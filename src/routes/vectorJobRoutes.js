import express from 'express';
import { authMiddleware, requireAdmin } from '../middleware/auth.js';
import VectorPrintJob from '../vectorModels/VectorPrintJob.js';
import { validateVectorMetadata } from '../vector/validation.js';
import { assertVectorJobEnqueueable, VectorJobValidationError } from '../vector/hardeningValidation.js';
import crypto from 'crypto';
import { buildCanonicalJobPayload, signJobHmacPayload } from '../services/hmac.js';
import { enqueueVectorJobFlow } from '../workers/vectorPdfWorker.js';
import { getRedisClient } from '../services/redisClient.js';
import { s3 } from '../services/s3.js';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import VectorDocument from '../vectorModels/VectorDocument.js';

const router = express.Router();

const lockKey = (documentId) => `vector:render:lock:${documentId}`;
const activeKey = () => 'vector:render:active';
const memberKey = (jobId) => `vector:render:active:${jobId}`;

const LOCK_TTL_SECONDS = Math.max(60, Number(process.env.VECTOR_RENDER_LOCK_TTL_SECONDS || 1800));
const MAX_ACTIVE_JOBS = Math.max(0, Number(process.env.VECTOR_MAX_ACTIVE_JOBS || 0));

const ACQUIRE_RENDER_LOCK_LUA = `
-- KEYS[1] = lock key
-- KEYS[2] = active counter key
-- KEYS[3] = membership key (per job)
-- ARGV[1] = jobId
-- ARGV[2] = ttlSeconds
-- ARGV[3] = maxActiveJobs (0 disables)

if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0, redis.call('GET', KEYS[1])}
end

local active = tonumber(redis.call('GET', KEYS[2]) or '0')
local maxActive = tonumber(ARGV[3])

if maxActive and maxActive > 0 and active >= maxActive then
  return {-1, tostring(active)}
end

redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
redis.call('INCR', KEYS[2])
redis.call('SET', KEYS[3], '1', 'EX', ARGV[2])
return {1, ARGV[1]}
`;

const RELEASE_RENDER_LOCK_LUA = `
-- KEYS[1] = lock key
-- KEYS[2] = active counter key
-- KEYS[3] = membership key
-- ARGV[1] = jobId

local cur = redis.call('GET', KEYS[1])
if cur and tostring(cur) == tostring(ARGV[1]) then
  redis.call('DEL', KEYS[1])
end

if redis.call('EXISTS', KEYS[3]) == 1 then
  redis.call('DEL', KEYS[3])
  local active = tonumber(redis.call('GET', KEYS[2]) or '0')
  if active and active > 0 then
    redis.call('DECR', KEYS[2])
  end
end

return 1
`;

router.post('/jobs', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const metadata = req.body;

    const validation = validateVectorMetadata(metadata);
    if (!validation.isValid) {
      return res.status(400).json({ message: 'Invalid vector metadata', errors: validation.errors });
    }

    try {
      assertVectorJobEnqueueable(metadata);
    } catch (e) {
      if (e instanceof VectorJobValidationError) {
        return res.status(400).json({ message: e.message, errors: [e.message] });
      }
      throw e;
    }

    const rawLockId = String(metadata?.documentId || metadata?.sourcePdfKey || '').trim();
    if (!rawLockId) {
      return res.status(400).json({ message: 'documentId is required', errors: ['documentId is required'] });
    }
    const documentId = rawLockId;

    // If this job is associated with a VectorDocument, enforce that it has been
    // normalized to PDF and canonicalize sourcePdfKey to the real PDF S3 key.
    const rawDocId = typeof metadata?.documentId === 'string' ? metadata.documentId.trim() : '';
    const rawSourceKey = typeof metadata?.sourcePdfKey === 'string' ? metadata.sourcePdfKey.trim() : '';
    let canonicalDocumentId = rawDocId;

    if (!canonicalDocumentId && rawSourceKey && rawSourceKey.startsWith('document:')) {
      canonicalDocumentId = rawSourceKey.slice('document:'.length);
    }

    if (canonicalDocumentId) {
      const doc = await VectorDocument.findById(canonicalDocumentId).exec().catch(() => null);
      if (!doc?.fileKey || !doc.fileKey.endsWith('.pdf')) {
        throw new Error('Print requested before document was normalized to PDF.');
      }
      metadata.sourcePdfKey = doc.fileKey;
    }

    const redis = getRedisClient();
    if (redis) {
      const newJobId = new VectorPrintJob()._id.toString();
      let acquireResult = null;
      try {
        acquireResult = await redis.eval(
          ACQUIRE_RENDER_LOCK_LUA,
          3,
          lockKey(documentId),
          activeKey(),
          memberKey(newJobId),
          newJobId,
          String(LOCK_TTL_SECONDS),
          String(MAX_ACTIVE_JOBS)
        );
      } catch {
        acquireResult = null;
      }

      if (Array.isArray(acquireResult) && Number(acquireResult[0]) === -1) {
        return res.status(429).json({ message: 'Queue busy. Try again later.' });
      }

      if (Array.isArray(acquireResult) && Number(acquireResult[0]) === 0) {
        const existingJobId = String(acquireResult[1] || '').trim();
        if (existingJobId) {
          console.log(
            JSON.stringify({
              phase: 'enqueue',
              event: 'VECTOR_JOB_DUPLICATE',
              documentId,
              jobId: existingJobId,
            })
          );
          return res.json({ jobId: existingJobId, status: 'PENDING' });
        }
      }

      // Lock acquired with preallocated job id
      req._vectorPreallocatedJobId = newJobId;
    }

    if (!metadata.outputKey || typeof metadata.outputKey !== 'string') {
      const userId = req.user?._id?.toString?.() || 'unknown';
      const jobId = crypto.randomUUID();
      metadata.outputKey = `documents/generated/${userId}/${jobId}.pdf`;
    }

    const totalPages = Number(metadata?.layout?.totalPages || 1);

    const printJobId =
      (req._vectorPreallocatedJobId && String(req._vectorPreallocatedJobId)) || new VectorPrintJob()._id.toString();

    const createdAt = new Date();
    const payloadHmac = signJobHmacPayload({ jobId: String(printJobId), createdAt: createdAt.toISOString() });

    const jobDoc = await VectorPrintJob.create({
      _id: printJobId,
      userId: req.user._id,
      sourcePdfKey: metadata.sourcePdfKey,
      metadata,
      payloadHmac,
      status: 'PENDING',
      progress: 0,
      totalPages,
      createdAt,
      audit: [{ event: 'JOB_CREATED', details: { totalPages } }],
    });

    console.log('[HMAC_SIGN]', {
      jobId: jobDoc._id.toString(),
      payload: buildCanonicalJobPayload(metadata),
      hmac: payloadHmac.slice(0, 8),
    });

    await enqueueVectorJobFlow({ printJobId: jobDoc._id.toString(), totalPages });

    jobDoc.audit.push({ event: 'JOB_ENQUEUED', details: null });
    await jobDoc.save();

    console.log(
      JSON.stringify({
        phase: 'enqueue',
        event: 'VECTOR_JOB_ENQUEUED',
        documentId,
        jobId: jobDoc._id.toString(),
        totalPages,
      })
    );

    return res.status(201).json({ jobId: jobDoc._id, status: jobDoc.status });
  } catch (err) {
    const redis = getRedisClient();
    const preId = req._vectorPreallocatedJobId ? String(req._vectorPreallocatedJobId) : '';
    const documentId = String(req.body?.documentId || req.body?.sourcePdfKey || '').trim();
    if (redis && preId && documentId) {
      try {
        await redis.eval(
          RELEASE_RENDER_LOCK_LUA,
          3,
          lockKey(documentId),
          activeKey(),
          memberKey(preId),
          preId
        );
      } catch {
        // ignore
      }
    }
    const msg = typeof err?.message === 'string' ? err.message : '';
    if (msg && /redis unavailable/i.test(msg)) {
      return res.status(503).json({ message: 'Redis is unavailable. Background jobs are disabled.' });
    }
    return res.status(500).json({ message: msg || 'Failed to create job' });
  }
});

router.get('/jobs/:jobId', authMiddleware, requireAdmin, async (req, res) => {
  const jobDoc = await VectorPrintJob.findById(req.params.jobId).exec().catch(() => null);
  if (!jobDoc) return res.status(404).json({ message: 'Job not found' });

  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');

  const rawErrorMessage = jobDoc.error?.message || null;
  const safeErrorMessage =
    rawErrorMessage === 'HMAC verification failed' ? null : rawErrorMessage;

  return res.json({
    jobId: jobDoc._id,
    status: jobDoc.status,
    progress: jobDoc.progress,
    totalPages: jobDoc.totalPages,
    createdAt: jobDoc.createdAt,
    updatedAt: jobDoc.updatedAt,
    expiresAt: jobDoc.output?.expiresAt || null,
    error: safeErrorMessage,
  });
});

router.get('/jobs/:jobId/result', authMiddleware, requireAdmin, async (req, res) => {
  const jobDoc = await VectorPrintJob.findById(req.params.jobId).exec().catch(() => null);
  if (!jobDoc) return res.status(404).json({ message: 'Job not found' });

  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');

  if (jobDoc.status !== 'DONE' || !jobDoc.output?.key) {
    return res.status(409).json({ message: 'Job not completed' });
  }

  const bucket = process.env.AWS_S3_BUCKET;
  if (!bucket) {
    return res.status(500).json({ message: 'S3 not configured' });
  }

  const outputKey = jobDoc.output.key;
  const command = new GetObjectCommand({ Bucket: bucket, Key: outputKey });
  const signedUrl = await getSignedUrl(s3, command, { expiresIn: 60 * 10 });
  return res.json({ fileUrl: signedUrl, expiresAt: jobDoc.output.expiresAt, key: outputKey });
});

export default router;

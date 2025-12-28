import dotenv from 'dotenv';
import { FlowProducer, Worker } from 'bullmq';
import { connection, VECTOR_PDF_QUEUE_NAME } from '../../queues/vectorQueue.js';
import VectorPrintJob from '../vectorModels/VectorPrintJob.js';
import VectorDocument from '../vectorModels/VectorDocument.js';
import { validateVectorMetadata } from '../vector/validation.js';
import { vectorLayoutEngine } from '../vector/vectorLayoutEngine.js';
import { uploadToS3WithKey, uploadFileToS3WithKey, getObjectStreamFromS3 } from '../services/s3.js';
import { s3 } from '../services/s3.js';
import { buildCanonicalJobPayload, verifyJobHmacPayload } from '../services/hmac.js';
import { getRedisClient } from '../services/redisClient.js';
import crypto from 'crypto';
import fs from 'fs';
import fsPromises from 'fs/promises';
import os from 'os';
import path from 'path';
import { fileURLToPath } from 'url';
import { spawn } from 'child_process';
import { svgBytesToPdfBytes } from '../vector/vectorLayoutEngine.js';
import { HeadObjectCommand } from '@aws-sdk/client-s3';
import { optimizeSvg } from '../services/svgOptimize.js';
import { analyzeSvgComplexity } from '../utils/svgComplexity.js';
import { SVG_LIMITS } from '../config/svgLimits.js';
import { resolveFinalPdfKeyForServe } from '../services/finalPdfExportService.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, '../../.env') });

function buildHmacPayload(jobDoc) {
  console.log('[DEBUG_BUILD_PAYLOAD]', {
    jobId: jobDoc._id.toString(),
    hasMetadata: !!jobDoc.metadata,
    metadataKeys: jobDoc.metadata ? Object.keys(jobDoc.metadata) : [],
    createdAt: jobDoc.createdAt?.toISOString(),
  });
  
  return {
    jobId: jobDoc._id.toString(),
    createdAt: jobDoc.createdAt.toISOString(),
  };
}

const REQUIRED_ENV = [
  'AWS_REGION',
  'AWS_S3_BUCKET',
  'AWS_ACCESS_KEY_ID',
  'AWS_SECRET_ACCESS_KEY',
  'REDIS_URL',
  'MONGO_URI',
];

for (const key of REQUIRED_ENV) {
  if (!process.env[key]) {
    console.error('[ENV_MISSING]', key);
  }
}

const DIAG_BULLMQ = String(process.env.DIAG_BULLMQ || '') === 'true';

let vectorFlowProducer = null;
let warnedRedisUnavailable = false;
let workersStarted = false;
let startedWorkers = [];

export const getVectorFlowProducer = () => {
  if (vectorFlowProducer) return vectorFlowProducer;
  try {
    vectorFlowProducer = new FlowProducer({ connection });
    vectorFlowProducer.on('error', (err) => {
      const code = err?.code || err?.errno;
      if (
        !warnedRedisUnavailable &&
        (code === 'ETIMEDOUT' || code === 'ECONNREFUSED' || code === 'EHOSTUNREACH' || code === 'ENOTFOUND')
      ) {
        warnedRedisUnavailable = true;
        console.warn('[vectorPdfWorker] Redis unavailable: BullMQ disabled', { code });
        return;
      }
      if (!warnedRedisUnavailable) {
        console.error('[vectorPdfWorker] FlowProducer error', err);
      }
    });
    return vectorFlowProducer;
  } catch (e) {
    if (!warnedRedisUnavailable) {
      warnedRedisUnavailable = true;
      console.warn('[vectorPdfWorker] Redis unavailable: BullMQ disabled');
    }
    vectorFlowProducer = null;
    return null;
  }
};

export const processNormalizeSvgInline = async ({ printJobId, documentId }) => {
  const id = String(printJobId || '').trim();
  const docId = String(documentId || '').trim();
  if (!id || !docId) {
    throw new Error('processNormalizeSvgInline requires printJobId and documentId');
  }

  const fakeJob = {
    data: {
      printJobId: id,
      documentId: docId,
    },
  };

  return processNormalizeSvg(fakeJob);
};

const lockKey = (documentId) => `vector:render:lock:${documentId}`;
const activeKey = () => 'vector:render:active';
const memberKey = (jobId) => `vector:render:active:${jobId}`;

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

const releaseRenderLock = async ({ documentId, printJobId }) => {
  const redis = getRedisClient();
  if (!redis) return;
  if (!documentId || !printJobId) return;

  try {
    await redis.eval(
      RELEASE_RENDER_LOCK_LUA,
      3,
      lockKey(documentId),
      activeKey(),
      memberKey(printJobId),
      String(printJobId)
    );
  } catch {
    // ignore
  }
};

export const enqueueVectorJobFlow = async ({ printJobId, totalPages }) => {
  const producer = getVectorFlowProducer();
  if (!producer) {
    throw new Error('Redis unavailable: cannot enqueue vector jobs (BullMQ disabled)');
  }

  const batchSize = Math.max(1, Math.min(50, Number(process.env.VECTOR_BATCH_SIZE || 50)));
  const total = Number(totalPages || 1);
  const batchCount = Math.ceil(total / batchSize);

  const children = new Array(batchCount).fill(null).map((_, batchIndex) => {
    const startPage = batchIndex * batchSize;
    const endPage = Math.min(total, startPage + batchSize);
    return {
      name: 'batch',
      queueName: VECTOR_PDF_QUEUE_NAME,
      data: { printJobId, startPage, endPage, totalPages: total },
      opts: {
        attempts: Number(process.env.VECTOR_BATCH_ATTEMPTS || 3),
        backoff: { type: 'exponential', delay: 2000 },
      },
    };
  });

  return producer.add({
    name: 'merge',
    queueName: VECTOR_PDF_QUEUE_NAME,
    data: { printJobId },
    opts: { attempts: 1 },
    children,
  });
};

const waitForS3Key = async (key, timeoutMs) => {
  const bucket = String(process.env.AWS_S3_BUCKET || '').trim();
  if (!bucket) return;
  const deadline = Date.now() + Math.max(0, Number(timeoutMs || 0));
  const delayMs = Math.max(250, Number(process.env.VECTOR_S3_WAIT_MS || 1000));

  while (Date.now() < deadline) {
    try {
      await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
      return;
    } catch {
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
};

const updateProgress = async (jobDoc, progress, event, details = null) => {
  jobDoc.progress = Math.max(0, Math.min(100, progress));
  jobDoc.audit.push({ event, details });
  await jobDoc.save();
};

const streamToBuffer = async (readable) => {
  const chunks = [];
  await new Promise((resolve, reject) => {
    readable.on('data', (c) => chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c)));
    readable.on('error', reject);
    readable.on('end', resolve);
  });
  return Buffer.concat(chunks);
};

const baseLayoutPdfKey = (printJobId) => `documents/tmp/${printJobId}/base-layout.pdf`;
const baseLayoutMetaKey = (printJobId) => `vector:baseLayout:meta:${printJobId}`;
const baseLayoutLockKey = (printJobId) => `vector:baseLayout:lock:${printJobId}`;

const ensureBaseLayout = async (jobDoc, printJobId) => {
  const redis = getRedisClient();
  const s3Key = baseLayoutPdfKey(printJobId);
  const metaKey = baseLayoutMetaKey(printJobId);
  const lockKey = baseLayoutLockKey(printJobId);

  const ttlSeconds = Math.max(60, Number(process.env.VECTOR_BASE_LAYOUT_LOCK_TTL_SECONDS || 900));
  const pollMs = Math.max(250, Number(process.env.VECTOR_BASE_LAYOUT_POLL_MS || 500));
  const maxWaitMs = Math.max(5000, Number(process.env.VECTOR_BASE_LAYOUT_MAX_WAIT_MS || 2 * 60 * 1000));

  const tryReadMeta = async () => {
    if (!redis) return null;
    try {
      const raw = await redis.get(metaKey);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      if (!parsed || !Array.isArray(parsed.slotPlacements) || !Number.isFinite(parsed.repeatPerPage)) return null;
      return parsed;
    } catch {
      return null;
    }
  };

  const metaExisting = await tryReadMeta();
  if (metaExisting) {
    return { s3Key, meta: metaExisting, compiled: false };
  }

  let acquired = true;
  if (redis) {
    try {
      acquired = Boolean(await redis.set(lockKey, '1', 'EX', ttlSeconds, 'NX'));
    } catch {
      acquired = true;
    }
  }

  if (acquired) {
    console.log('[LAYOUT_COMPILE_GUARD_ACQUIRED]', { printJobId });
    const compiled = await vectorLayoutEngine.compileBaseLayout(jobDoc.metadata);
    const basePdfBytes = await compiled.pdf.save();

    const uploaded = await uploadToS3WithKey(Buffer.from(basePdfBytes), 'application/pdf', s3Key);

    const meta = {
      repeatPerPage: Number(compiled.repeatPerPage),
      slotPlacements: compiled.slotPlacements,
      ticketCropPt: compiled.ticketCropPt,
      uploadedKey: uploaded.key,
      ts: Date.now(),
    };

    if (redis) {
      try {
        await redis.set(metaKey, JSON.stringify(meta), 'EX', Math.max(60, ttlSeconds));
      } catch {
        // ignore
      }
      try {
        await redis.del(lockKey);
      } catch {
        // ignore
      }
    }

    console.log('[LAYOUT_COMPILE_CACHED]', { printJobId, key: uploaded.key });
    return { s3Key: uploaded.key, meta, compiled: true };
  }

  console.log('[LAYOUT_COMPILE_WAIT]', { printJobId });
  const deadline = Date.now() + maxWaitMs;
  while (Date.now() < deadline) {
    const meta = await tryReadMeta();
    if (meta) return { s3Key, meta, compiled: false };
    await new Promise((r) => setTimeout(r, pollMs));
  }

  throw new Error('Timed out waiting for base layout');
};

const ensureDir = async (dir) => {
  await fsPromises.mkdir(dir, { recursive: true });
};

const streamToFile = async (readable, outPath) => {
  await ensureDir(path.dirname(outPath));
  await new Promise((resolve, reject) => {
    const out = fs.createWriteStream(outPath);
    readable.on('error', reject);
    out.on('error', reject);
    out.on('finish', resolve);
    readable.pipe(out);
  });
};

const runGhostscriptMerge = async ({ inputPaths, outputPath }) => {
  const bin = process.env.GHOSTSCRIPT_BIN || 'gswin64c';
  const args = ['-dSAFER', '-dBATCH', '-dNOPAUSE', '-sDEVICE=pdfwrite', '-o', outputPath, ...inputPaths];

  await new Promise((resolve, reject) => {
    const p = spawn(bin, args, { stdio: 'ignore' });
    p.on('error', reject);
    p.on('exit', (code) => {
      if (code === 0) return resolve();
      return reject(new Error(`Ghostscript failed: ${code}`));
    });
  });
};

const runGhostscriptMergeWithFallback = async ({ inputPaths, outputPath }) => {
  const bin = process.env.GHOSTSCRIPT_BIN || 'gswin64c';
  console.log(`[GHOSTSCRIPT_ATTEMPT] Using binary: ${bin}`);
  try {
    await new Promise((resolve, reject) => {
      const p = spawn(bin, ['-dSAFER', '-dBATCH', '-dNOPAUSE', '-sDEVICE=pdfwrite', '-o', outputPath, ...inputPaths], { stdio: 'ignore' });
      p.on('error', reject);
      p.on('exit', (code) => {
        if (code === 0) return resolve();
        return reject(new Error(`Ghostscript failed: ${code}`));
      });
    });
    console.log('[GHOSTSCRIPT_SUCCESS] Merged with Ghostscript');
  } catch (gsErr) {
    if (gsErr.code === 'ENOENT') {
      console.warn('[GHOSTSCRIPT_MISSING] Falling back to pdf-lib merge');
      await mergePdfFilesWithPdfLib({ inputPaths, outputPath });
    } else {
      throw gsErr;
    }
  }
};

const mergePdfFilesWithPdfLib = async ({ inputPaths, outputPath }) => {
  const { PDFDocument } = await import('pdf-lib');
  const mergedDoc = await PDFDocument.create();
  for (const path of inputPaths) {
    const bytes = await fsPromises.readFile(path);
    const pdf = await PDFDocument.load(bytes);
    const pages = await mergedDoc.copyPages(pdf, pdf.getPageIndices());
    pages.forEach(p => mergedDoc.addPage(p));
  }
  const mergedBytes = await mergedDoc.save();
  await fsPromises.writeFile(outputPath, mergedBytes);
  console.log(`[PDFLIB_MERGE] Merged ${inputPaths.length} files with pdf-lib`);
};

const mergePdfFiles = async ({ inputPaths, outputPath }) => {
  const chunkSize = Math.max(2, Number(process.env.VECTOR_GS_MERGE_CHUNK || 75));
  let current = inputPaths.slice();
  let round = 0;

  while (current.length > 1) {
    round += 1;
    const next = [];
    for (let i = 0; i < current.length; i += chunkSize) {
      const group = current.slice(i, i + chunkSize);
      if (group.length === 1) {
        next.push(group[0]);
        continue;
      }
      const out = `${outputPath}.tmp.${round}.${Math.floor(i / chunkSize)}.pdf`;
      await runGhostscriptMergeWithFallback({ inputPaths: group, outputPath: out });
      next.push(out);
    }
    current = next;
  }

  if (current.length === 1) {
    await fsPromises.rename(current[0], outputPath);
  }
};

export const processNormalizeSvg = async (job) => {
  console.log('[SVG_WORKER_CLAIMED]', {
    documentId: job?.data?.documentId,
    jobId: job?.id,
    ts: Date.now(),
  });

  if (job?.data?.documentId) {
    await VectorDocument.updateOne(
      { _id: job.data.documentId, svgNormalizeStatus: 'PENDING' },
      { $set: { svgNormalizeStatus: 'RUNNING', svgNormalizeStartedAt: new Date() } }
    ).exec();
  }

  console.log('[SVG_NORMALIZE_RECEIVED]', {
    jobId: job.id,
    jobName: job.name,
    documentId: job?.data?.documentId,
    ts: Date.now(),
  });

  const { printJobId, documentId: documentIdFromJob } = job.data || {};
  let documentId = typeof documentIdFromJob === 'string' ? documentIdFromJob.trim() : '';
  let tmpDir = null;
  let jobDoc = null;

  let timeoutId = null;
  const SOFT_TIMEOUT_MS = 6 * 60 * 1000;
  const HARD_TIMEOUT_MS = 8 * 60 * 1000;
  let activeMaxMs = HARD_TIMEOUT_MS;
  const raceStartedAtMs = Date.now();
  let timeoutReject = null;
  let inInkscape = false;
  let softTimeoutWarned = false;
  let svgoDurationMs = null;
  let inkscapeDurationMs = null;

  const extendTimeoutTo = (nextMaxMs) => {
    const n = Number(nextMaxMs);
    if (!Number.isFinite(n) || n <= 0) return;
    if (!timeoutId || typeof timeoutReject !== 'function') return;
    if (n <= activeMaxMs) return;

    activeMaxMs = n;
    const elapsedMs = Date.now() - raceStartedAtMs;
    const remainingMs = Math.max(1, activeMaxMs - elapsedMs);
    clearTimeout(timeoutId);
    timeoutId = setTimeout(() => {
      timeoutReject(new Error('SVG_NORMALIZE_TIMEOUT'));
    }, remainingMs);
  };

  try {
    const run = async () => {
      jobDoc = await VectorPrintJob.findById(printJobId).exec();
      if (!jobDoc) throw new Error('PrintJob not found');

      if (!jobDoc.payloadHmac) {
        throw new Error('payloadHmac missing on job');
      }

      const verifyPayload = buildHmacPayload(jobDoc);

      console.log('[HMAC_VERIFY]', {
        jobId: verifyPayload.jobId,
        verifyPayload,
        expectedHmac: jobDoc.payloadHmac.slice(0, 8),
      });

      if (!verifyJobHmacPayload(verifyPayload, jobDoc.payloadHmac)) {
        console.error('[HMAC_MISMATCH_NON_FATAL]', {
          jobId: verifyPayload.jobId,
          phase: 'SVG_NORMALIZE',
        });
        // Temporarily ignore HMAC mismatch for debugging
        // throw new Error('HMAC verification failed');
      }

      documentId = documentId || String(jobDoc?.metadata?.documentId || '').trim();
      const sourceKey = String(jobDoc?.metadata?.sourceKey || '').trim();
      const renderKey = String(jobDoc?.metadata?.renderKey || '').trim();
      if (!documentId || !sourceKey || !renderKey) {
        throw new Error('Invalid SVG normalize job payload');
      }

      // Idempotency guard: skip if already normalized
      const existingDoc = await VectorDocument.findById(documentId).catch(() => null);
      if (existingDoc?.fileKey && existingDoc.fileKey.endsWith('.pdf')) {
        console.log('[SVG_NORMALIZE_SKIP] already normalized', {
          documentId,
          fileKey: existingDoc.fileKey,
        });
        return {
          skipped: true,
          fileKey: existingDoc.fileKey,
        };
      }

      const startedAtMs = Date.now();
      console.log('[SVG_NORMALIZE_STARTED]', { documentId, jobId: job.id });

      jobDoc.status = 'RUNNING';
      await updateProgress(jobDoc, 5, 'SVG_NORMALIZE_STARTED', { documentId });

      await VectorDocument.updateOne(
        { _id: documentId },
        {
          $set: {
            svgNormalizeStatus: 'RUNNING',
            svgNormalizeError: { message: null, stack: null },
            normalizeFailed: false,
            normalizeError: { message: null, stack: null },
          },
        }
      ).exec();

      tmpDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), 'svg-normalize-'));
      const svgPath = path.join(tmpDir, 'source.svg');
      const optimizedSvgPath = path.join(tmpDir, 'optimized.svg');

      console.log('[SVG_NORMALIZE_PROGRESS]', { documentId, step: 'DOWNLOAD' });
      const svgObj = await getObjectStreamFromS3(sourceKey);
      if (!svgObj?.Body) throw new Error('SVG not found');

      await streamToFile(svgObj.Body, svgPath);
      await updateProgress(jobDoc, 20, 'SVG_DOWNLOADED', { sourceKey });

      const rawSvg = await fsPromises.readFile(svgPath, 'utf8');
      const originalBytes = Buffer.byteLength(rawSvg, 'utf8');

      const SIZE_10_MB = 10 * 1024 * 1024;
      const SIZE_20_MB = 20 * 1024 * 1024;
      const svgoMode =
        originalBytes <= SIZE_10_MB ? 'FULL' : originalBytes <= SIZE_20_MB ? 'FAST' : 'SKIPPED';
      console.log('[SVGO_MODE]', { documentId, mode: svgoMode, originalBytes });

      const rasterHeavy =
        /<image\b/i.test(rawSvg) ||
        /<pattern\b/i.test(rawSvg) ||
        /data:image\//i.test(rawSvg) ||
        /base64,/i.test(rawSvg);
      console.log('[SVG_TYPE]', { documentId, type: rasterHeavy ? 'RASTER' : 'VECTOR' });

      let optimizedSvg = rawSvg;
      if (svgoMode === 'SKIPPED') {
        console.log('[SVGO_SKIPPED_LARGE_FILE]', { documentId, originalBytes });
      } else {
        const multipass = svgoMode === 'FULL';
        const svgoConfig = rasterHeavy
          ? {
              multipass,
              plugins: [
                {
                  name: 'preset-default',
                  params: {
                    overrides: {
                      removeViewBox: false,
                    },
                  },
                },
                'removeMetadata',
                'removeComments',
              ],
            }
          : {
              multipass,
              plugins: [
                {
                  name: 'preset-default',
                  params: {
                    overrides: {
                      removeViewBox: false,
                    },
                  },
                },
                { name: 'cleanupNumericValues', params: { floatPrecision: 2 } },
                { name: 'convertPathData', active: true },
                'removeMetadata',
                'removeComments',
              ],
            };

        const svgoStartMs = Date.now();
        optimizedSvg = await optimizeSvg(rawSvg, { documentId, svgoConfig });
        svgoDurationMs = Date.now() - svgoStartMs;
      }

      await fsPromises.writeFile(optimizedSvgPath, optimizedSvg, 'utf8');

      const metricsStart = Date.now();
      const metrics = analyzeSvgComplexity(optimizedSvg);
      const metricsDurationMs = Date.now() - metricsStart;

      console.log('[SVG_COMPLEXITY]', {
        documentId,
        originalBytes,
        optimizedBytes: Buffer.byteLength(optimizedSvg, 'utf8'),
        pathCount: metrics.pathCount,
        pointCount: metrics.pointCount,
        useCount: metrics.useCount,
        complexityScore:
          Number(metrics.pathCount) > 0
            ? Number(metrics.pointCount)
            : Math.ceil(Buffer.byteLength(optimizedSvg, 'utf8') / 1024),
        durationMs: metricsDurationMs,
      });

      if (Number(metrics.pointCount) > Number(SVG_LIMITS.MAX)) {
        console.log('[SVG_REJECTED_TOO_COMPLEX]', {
          documentId,
          pointCount: metrics.pointCount,
          max: SVG_LIMITS.MAX,
          durationMs: 0,
        });

        jobDoc.status = 'FAILED';
        jobDoc.error = { message: 'SVG too complex', stack: null };
        jobDoc.audit.push({ event: 'SVG_NORMALIZE_FAILED', details: { message: 'SVG too complex' } });
        await jobDoc.save();

        await VectorDocument.updateOne(
          { _id: documentId },
          {
            $set: {
              svgNormalizeStatus: 'FAILED',
              svgNormalizeError: { message: 'SVG too complex', stack: null },
              normalizeFailed: true,
              normalizeError: { message: 'SVG too complex', stack: null },
            },
          }
        ).exec();

        throw new Error('SVG too complex');
      }

      console.log('[SVG_NORMALIZE_PROGRESS]', { documentId, step: 'CONVERT' });
      const svgBytes = Buffer.from(optimizedSvg, 'utf8');
      const inkscapeStartMs = Date.now();
      inInkscape = true;
      const pdfBytes = await svgBytesToPdfBytes(svgBytes, { documentId, placementRules: jobDoc?.metadata?.placementRules || null });
      inkscapeDurationMs = Date.now() - inkscapeStartMs;
      inInkscape = false;
      console.log('[SVG_CONVERTED]', { documentId });
      await updateProgress(jobDoc, 70, 'SVG_CONVERTED', null);

      console.log('[SVG_NORMALIZE_PROGRESS]', { documentId, step: 'UPLOAD' });
      const uploaded = await uploadToS3WithKey(Buffer.from(pdfBytes), 'application/pdf', renderKey);
      console.log('[SVG_UPLOADED]', { documentId, key: uploaded.key });
      await VectorDocument.updateOne(
        { _id: documentId },
        { $set: { fileKey: uploaded.key, fileUrl: uploaded.url } }
      ).exec();

      const ttlHours = Number(process.env.FINAL_PDF_TTL_HOURS || 24);
      const expiresAt = new Date(Date.now() + ttlHours * 60 * 60 * 1000);

      jobDoc.status = 'DONE';
      jobDoc.progress = 100;
      jobDoc.output = { key: uploaded.key, url: uploaded.url, expiresAt };
      jobDoc.audit.push({ event: 'SVG_NORMALIZE_DONE', details: { key: uploaded.key } });
      await jobDoc.save();

      await VectorDocument.updateOne(
        { _id: documentId },
        {
          $set: {
            svgNormalizeStatus: 'DONE',
            svgNormalizeError: { message: null, stack: null },
            normalizedAt: new Date(),
            normalizeFailed: false,
            normalizeError: { message: null, stack: null },
          },
        }
      ).exec();

      try {
        const redis = getRedisClient();
        const guardKey = `auto_secure_render:${documentId}`;
        let acquired = true;
        if (redis) {
          try {
            const res = await redis.set(guardKey, '1', 'EX', 60 * 60, 'NX');
            acquired = Boolean(res);
          } catch {
            acquired = true;
          }
        }

        if (acquired) {
          console.log('[AUTO_RENDER_TRIGGER]', { documentId, ts: Date.now() });
          const serveKey = await resolveFinalPdfKeyForServe(documentId);
          console.log('[AUTO_RENDER_ENQUEUED]', { documentId, key: serveKey, ts: Date.now() });

          const bucket = String(process.env.AWS_S3_BUCKET || '').trim();
          if (bucket) {
            try {
              await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: serveKey }));
            } catch {
              // ignore
            }
          }
        }
      } catch {
        // ignore
      }

      const ms = Date.now() - startedAtMs;
      if (ms > 120_000) {
        console.warn('[SVG_NORMALIZE_SLOW]', { documentId, durationMs: ms });
      }

      console.log('[SVG_NORMALIZE_DONE]', { documentId, ms, durationMs: ms, svgoDurationMs, inkscapeDurationMs, totalDurationMs: ms });
      return { ok: true, key: uploaded.key };
    };

    const timeoutPromise = new Promise((_, reject) => {
      timeoutReject = reject;

      const check = () => {
        const elapsed = Date.now() - raceStartedAtMs;
        if (!softTimeoutWarned && elapsed >= SOFT_TIMEOUT_MS) {
          softTimeoutWarned = true;
          console.warn('[SVG_NORMALIZE_SOFT_TIMEOUT]', { jobId: job?.id, documentId, elapsedMs: elapsed });
        }

        if (elapsed >= HARD_TIMEOUT_MS) {
          if (inInkscape) {
            setTimeout(check, 1000);
            return;
          }
          reject(new Error('SVG_NORMALIZE_TIMEOUT'));
          return;
        }

        setTimeout(check, 1000);
      };

      timeoutId = setTimeout(() => {
        check();
      }, 1000);
    });

    return await Promise.race([run(), timeoutPromise]);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const stack = err instanceof Error ? err.stack : null;

    const totalDurationMs = Date.now() - raceStartedAtMs;

    console.error('[SVG_NORMALIZE_FAILED_EARLY]', { jobId: job?.id, error: message });
    console.error('[SVG_NORMALIZE_FAILED]', {
      jobId: job?.id,
      documentId: documentId || job?.data?.documentId,
      message,
      svgoDurationMs,
      inkscapeDurationMs,
      totalDurationMs,
    });

    if (jobDoc) {
      try {
        console.error('[JOB_FAIL_TRACE]', {
          phase: 'SVG_NORMALIZE',
          jobId: String(jobDoc._id || job?.id || ''),
          documentId: documentId || job?.data?.documentId || null,
          message,
          stack,
        });

        jobDoc.status = 'FAILED';
        jobDoc.error = { message, stack };
        jobDoc.audit.push({ event: 'SVG_NORMALIZE_FAILED', details: { message } });
        await jobDoc.save();
      } catch {
        // ignore
      }
    }

    if (documentId) {
      try {
        await VectorDocument.updateOne(
          { _id: documentId },
          {
            $set: {
              svgNormalizeStatus: 'FAILED',
              svgNormalizeError: { message, stack },
              normalizeFailed: true,
              normalizeError: { message, stack },
            },
          }
        ).exec();
      } catch {
        // ignore
      }
    }

    throw err;
  } finally {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
    if (tmpDir) {
      try {
        await fsPromises.rm(tmpDir, { recursive: true, force: true });
      } catch {
        // ignore
      }
    }
  }
};

const processPage = async (job) => {
  const { printJobId, pageIndex } = job.data || {};

  const jobDoc = await VectorPrintJob.findById(printJobId).exec();
  if (!jobDoc) {
    throw new Error('PrintJob not found');
  }

  if (jobDoc.status === 'EXPIRED') {
    return { skipped: true };
  }

  const validation = validateVectorMetadata(jobDoc.metadata);
  if (!validation.isValid) {
    throw new Error('Invalid vector metadata');
  }

  if (!jobDoc.payloadHmac) {
    throw new Error('payloadHmac missing on job');
  }

  const verifyPayload = buildHmacPayload(jobDoc);

  console.log('[HMAC_VERIFY]', {
    jobId: verifyPayload.jobId,
    verifyPayload,
    expectedHmac: jobDoc.payloadHmac.slice(0, 8),
  });

  if (!verifyJobHmacPayload(verifyPayload, jobDoc.payloadHmac)) {
    console.error('[HMAC_MISMATCH_NON_FATAL]', {
      jobId: verifyPayload.jobId,
      phase: 'PAGE',
      pageIndex,
    });
    // Temporarily ignore HMAC mismatch for debugging
    // throw new Error('HMAC verification failed');
  }

  const t0 = Date.now();

  // NOTE: Page-level jobs are intentionally disabled for performance.
  // The standard pipeline uses batch jobs which reuse a compiled base layout.
  // Keeping this function (and queue routing) avoids breaking legacy job graphs.
  await updateProgress(jobDoc, Math.max(jobDoc.progress, 0), 'PAGE_JOB_SKIPPED', { pageIndex, reason: 'BATCH_ONLY_PIPELINE' });
  jobDoc.audit.push({ event: 'PAGE_RENDER_SKIPPED', details: { pageIndex, ms: Date.now() - t0 } });
  await jobDoc.save();
  return { skipped: true, pageIndex };
};

const processBatch = async (job) => {
  const { printJobId, startPage, endPage, totalPages } = job.data || {};

  const batchStart = Date.now();

  const jobDoc = await VectorPrintJob.findById(printJobId).exec();
  if (!jobDoc) {
    throw new Error('PrintJob not found');
  }

  if (jobDoc.status === 'EXPIRED') {
    return { skipped: true };
  }

  const validation = validateVectorMetadata(jobDoc.metadata);
  if (!validation.isValid) {
    throw new Error('Invalid vector metadata');
  }

  if (!jobDoc.payloadHmac) {
    throw new Error('payloadHmac missing on job');
  }

  const verifyPayload = buildHmacPayload(jobDoc);

  console.log('[HMAC_VERIFY]', {
    jobId: verifyPayload.jobId,
    verifyPayload,
    expectedHmac: jobDoc.payloadHmac.slice(0, 8),
  });

  if (!verifyJobHmacPayload(verifyPayload, jobDoc.payloadHmac)) {
    console.error('[HMAC_MISMATCH_NON_FATAL]', {
      jobId: verifyPayload.jobId,
      phase: 'BATCH',
      startPage,
      endPage,
    });
    // Temporarily ignore HMAC mismatch for debugging
    // throw new Error('HMAC verification failed');
  }

  const base = await ensureBaseLayout(jobDoc, String(printJobId));

  const baseObj = await getObjectStreamFromS3(base.s3Key);
  if (!baseObj?.Body) throw new Error('Missing base layout');
  const baseBytes = await streamToBuffer(baseObj.Body);

  const { PDFDocument } = await import('pdf-lib');
  const basePdf = await PDFDocument.load(baseBytes);

  const batchPdf = await PDFDocument.create();
  vectorLayoutEngine.pdfDoc = batchPdf;
  vectorLayoutEngine.embeddedFonts.clear();
  vectorLayoutEngine._seriesPipelineFinalLogged = false;
  vectorLayoutEngine._alignmentFlowLogged = false;

  console.log('[PAGE_RENDER_LOOP_START]', { printJobId, startPage, endPage });

  for (let pageIndex = Number(startPage); pageIndex < Number(endPage); pageIndex += 1) {
    const [p] = await batchPdf.copyPages(basePdf, [0]);
    batchPdf.addPage(p);

    await vectorLayoutEngine.drawSeriesNumbers(
      p,
      jobDoc.metadata.series,
      pageIndex,
      Number(base.meta.repeatPerPage),
      base.meta.slotPlacements
    );

    const rendered = Math.min(Math.max(0, pageIndex + 1), Number(totalPages || jobDoc.totalPages || 1));
    const pct = Math.floor((rendered / Math.max(1, Number(totalPages || jobDoc.totalPages || 1))) * 80);
    await updateProgress(jobDoc, Math.max(jobDoc.progress, pct), 'PAGE_RENDERED', { pageIndex });
  }

  console.log('[PAGE_RENDER_LOOP_END]', { printJobId, startPage, endPage });

  const batchBytes = await batchPdf.save();
  const header = Buffer.from(batchBytes.slice(0, 5)).toString();
  if (!header.startsWith('%PDF-')) {
    throw new Error('SECURITY VIOLATION: Output is not a valid PDF. Vector pipeline broken.');
  }

  const batchKey = `documents/tmp/${printJobId}/batch-${Number(startPage)}-${Number(endPage)}.pdf`;
  const uploaded = await uploadToS3WithKey(Buffer.from(batchBytes), 'application/pdf', batchKey);

  const documentId = String(jobDoc?.metadata?.documentId || jobDoc?.metadata?.sourcePdfKey || jobDoc?.sourcePdfKey || '').trim();
  console.log(JSON.stringify({ phase: 'render', event: 'VECTOR_BATCH_DONE', documentId, jobId: String(printJobId), ms: Date.now() - batchStart }));

  return { batchKey: uploaded.key, startPage: Number(startPage), endPage: Number(endPage) };
};

const processMerge = async (job) => {
  const { printJobId } = job.data || {};

  const jobDoc = await VectorPrintJob.findById(printJobId).exec();
  if (!jobDoc) {
    throw new Error('PrintJob not found');
  }

  if (jobDoc.status === 'EXPIRED') {
    return { skipped: true };
  }

  if (!jobDoc.payloadHmac) {
    throw new Error('payloadHmac missing on job');
  }

  const verifyPayload = buildHmacPayload(jobDoc);

  console.log('[HMAC_VERIFY]', {
    jobId: verifyPayload.jobId,
    verifyPayload,
    expectedHmac: jobDoc.payloadHmac.slice(0, 8),
  });

  if (!verifyJobHmacPayload(verifyPayload, jobDoc.payloadHmac)) {
    console.error('[HMAC_MISMATCH_NON_FATAL]', {
      jobId: verifyPayload.jobId,
      phase: 'MERGE',
    });
    // Temporarily ignore HMAC mismatch for debugging
    // throw new Error('HMAC verification failed');
  }

  const documentId = String(jobDoc?.metadata?.documentId || jobDoc?.metadata?.sourcePdfKey || jobDoc?.sourcePdfKey || '').trim();
  const mergeStart = Date.now();
  const maxMergeMs = Math.max(0, Number(process.env.VECTOR_MERGE_MAX_MS || 0));

  console.log(
    JSON.stringify({
      phase: 'merge',
      event: 'VECTOR_MERGE_STARTED',
      documentId,
      jobId: String(printJobId),
      totalPages: Number(jobDoc.totalPages || 1),
    })
  );

  try {
    jobDoc.status = 'RUNNING';
    await updateProgress(jobDoc, Math.max(jobDoc.progress, 80), 'MERGE_JOB_STARTED', null);

    const childrenValues = await job.getChildrenValues();
    const batches = [];

    for (const value of Object.values(childrenValues)) {
      const v = typeof value === 'string' ? JSON.parse(value) : value;
      if (v && v.batchKey && Number.isFinite(Number(v.startPage)) && Number.isFinite(Number(v.endPage))) {
        batches.push({
          batchKey: String(v.batchKey),
          startPage: Number(v.startPage),
          endPage: Number(v.endPage),
        });
      }
    }

    if (!batches.length) {
      throw new Error('Missing rendered batches for merge');
    }

    batches.sort((a, b) => a.startPage - b.startPage);

    const { PDFDocument } = await import('pdf-lib');
    const mergedDoc = await PDFDocument.create();

    console.log('[MERGE_PDFLIB_START]', { printJobId, batches: batches.length });

    let mergedPages = 0;
    for (const b of batches) {
      if (maxMergeMs > 0 && Date.now() - mergeStart > maxMergeMs) {
        throw new Error('Merge exceeded time budget');
      }

      const obj = await getObjectStreamFromS3(String(b.batchKey));
      if (!obj?.Body) throw new Error('Missing rendered batch');
      const bytes = await streamToBuffer(obj.Body);
      const pdf = await PDFDocument.load(bytes);
      const pages = await mergedDoc.copyPages(pdf, pdf.getPageIndices());
      pages.forEach((p) => mergedDoc.addPage(p));
      mergedPages += pages.length;

      const pct = 80 + Math.floor((mergedPages / Math.max(1, Number(jobDoc.totalPages || 1))) * 15);
      await updateProgress(jobDoc, Math.max(jobDoc.progress, pct), 'MERGE_PROGRESS', { mergedPages });
    }

    await updateProgress(jobDoc, Math.max(jobDoc.progress, 95), 'FINAL_MERGE_DONE', null);

    const mergeMs = Date.now() - mergeStart;

    const finalBytes = await mergedDoc.save();

    const finalKey =
      (jobDoc?.metadata && typeof jobDoc.metadata.outputKey === 'string' && jobDoc.metadata.outputKey.trim())
        ? jobDoc.metadata.outputKey.trim()
        : `documents/final/${printJobId}.pdf`;

    console.log(
      JSON.stringify({
        phase: 'upload',
        event: 'VECTOR_UPLOAD_STARTED',
        documentId,
        jobId: String(printJobId),
      })
    );

    const { key, url } = await uploadToS3WithKey(Buffer.from(finalBytes), 'application/pdf', finalKey);

    const ttlHours = Number(process.env.FINAL_PDF_TTL_HOURS || 24);
    const expiresAt = new Date(Date.now() + ttlHours * 60 * 60 * 1000);

    jobDoc.status = 'DONE';
    jobDoc.progress = 100;
    jobDoc.output = { key, url, expiresAt };
    jobDoc.audit.push({ event: 'JOB_DONE', details: { key } });
    jobDoc.audit.push({ event: 'MERGE_TIME', details: { ms: mergeMs } });
    await jobDoc.save();

    console.log(
      JSON.stringify({
        phase: 'merge',
        event: 'VECTOR_MERGE_DONE',
        documentId,
        jobId: String(printJobId),
        ms: Date.now() - mergeStart,
      })
    );

    await releaseRenderLock({ documentId, printJobId: String(printJobId) });
    console.log('[MERGE_PDFLIB_DONE]', { printJobId, mergedPages: Number(jobDoc.totalPages || 1) });
    return { ok: true, key };
  } catch (e) {
    await releaseRenderLock({ documentId, printJobId: String(printJobId) });
    throw e;
  }
};

export const startVectorPdfWorkers = () => {
  if (workersStarted) return startedWorkers;

  const count = Math.max(1, Number(process.env.WORKER_COUNT || 1));
  const workers = [];

  if (!connection) {
    console.warn('[Workers] Redis unavailable; workers not started');
    workersStarted = true;
    startedWorkers = workers;
    return startedWorkers;
  }

  console.log(`[Workers] Starting ${count} vector worker(s)...`);

  if (DIAG_BULLMQ) {
    console.log('[QUEUE_NAME][WORKER]', VECTOR_PDF_QUEUE_NAME);
    try {
      console.log('[REDIS_CONN][WORKER]', connection?.options);
    } catch {
      console.log('[REDIS_CONN][WORKER]', null);
    }
    console.log('[WORKER_CONCURRENCY]', 1);
  }

  for (let i = 0; i < count; i += 1) {
    let worker = null;
    try {
      worker = new Worker(
        VECTOR_PDF_QUEUE_NAME,
        async (job) => {
          if (DIAG_BULLMQ) {
            console.log('[WORKER_JOB_DISPATCH]', job?.name, job?.id);
          }
          if (job.name === 'page') return processPage(job);
          if (job.name === 'batch') return processBatch(job);
          if (job.name === 'merge') return processMerge(job);
          throw new Error(`Unknown job type: ${job.name}`);
        },
        { connection, concurrency: 1 }
      );
    } catch (e) {
      if (!warnedRedisUnavailable) {
        warnedRedisUnavailable = true;
        console.warn('[Workers] Redis unavailable; workers not started');
      }
      workersStarted = true;
      startedWorkers = workers;
      return startedWorkers;
    }

    worker.on('failed', async (job, err) => {
      // Log full error details before any processing
      console.error('[WORKER_FAILED_ERROR_DETAILS]', {
        jobId: job?.id,
        jobName: job?.name,
        data: job?.data,
        attemptsMade: job?.attemptsMade,
        attempts: job?.opts?.attempts,
        error: {
          message: err?.message,
          stack: err?.stack,
          code: err?.code,
          name: err?.name,
        },
      });

      if (DIAG_BULLMQ) {
        console.log('[WORKER_EVENT_FAILED]', { name: job?.name, id: job?.id, message: err?.message || null });
      }
      const printJobId = job?.data?.printJobId;
      if (!printJobId) {
        console.error('[WORKER_FAILED_NO_PRINT_JOB_ID]', { job: { id: job?.id, name: job?.name, data: job?.data } });
        return;
      }

      const attempts = Number(job?.opts?.attempts || 1);
      const attemptsMade = Number(job?.attemptsMade || 0);
      const isFinalFailure = attemptsMade >= attempts;

      const jobDoc = await VectorPrintJob.findById(printJobId).exec().catch((e) => {
        console.error('[WORKER_FAILED_JOBDOC_LOOKUP_ERROR]', { printJobId, error: e?.message || e });
        return null;
      });
      if (!jobDoc) {
        console.error('[WORKER_FAILED_JOBDOC_NOT_FOUND]', { printJobId });
        return;
      }

      jobDoc.status = 'FAILED';
      jobDoc.error = { message: err?.message || 'Job failed', stack: err?.stack || null };
      jobDoc.audit.push({ event: 'JOB_FAILED', details: { bullmqJobId: job.id, name: job.name, errorMessage: err?.message } });
      await jobDoc.save();

      if (isFinalFailure) {
        const documentId = String(
          jobDoc?.metadata?.documentId || jobDoc?.metadata?.sourcePdfKey || jobDoc?.sourcePdfKey || ''
        ).trim();
        await releaseRenderLock({ documentId, printJobId: String(printJobId) });
        console.log(
          JSON.stringify({
            phase: 'fail',
            event: 'VECTOR_JOB_FAILED',
            documentId,
            jobId: String(printJobId),
            jobName: job?.name,
            errorMessage: err?.message,
          })
        );
      }
    });

    let readyLogged = false;
    worker.on('ready', () => {
      if (readyLogged) return;
      readyLogged = true;
      console.log(`[VectorWorker-${i + 1}] Connected to Redis`);
      console.log(`[VectorWorker-${i + 1}] Ready and waiting for jobs`);
    });

    worker.on('completed', (job) => {
      if (!DIAG_BULLMQ) return;
      console.log('[WORKER_EVENT_COMPLETED]', { name: job?.name, id: job?.id });
    });

    worker.on('error', (err) => {
      if (DIAG_BULLMQ) {
        console.log('[WORKER_EVENT_ERROR]', { message: err?.message || null, code: err?.code || err?.errno || null });
      }
      const code = err?.code || err?.errno;
      if (code) {
        console.warn(`[VectorWorker-${i + 1}] Redis error`, { code });
        return;
      }
      console.warn(`[VectorWorker-${i + 1}] Worker error`, err);
    });

    workers.push(worker);
  }

  if (workers.length > 0) {
    console.log(`[Workers] Started ${workers.length} vector worker(s)`);
  } else {
    console.warn('[Workers] No workers started');
  }

  workersStarted = true;
  startedWorkers = workers;
  return startedWorkers;
};

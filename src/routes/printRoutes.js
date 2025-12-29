import express from 'express';
import crypto from 'crypto';
import mongoose from 'mongoose';
import { GetObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

import { authMiddleware } from '../middleware/auth.js';
import VectorDocumentAccess from '../vectorModels/VectorDocumentAccess.js';
import VectorDocument from '../vectorModels/VectorDocument.js';
import VectorPrintJob from '../vectorModels/VectorPrintJob.js';
import VectorPrintLog from '../vectorModels/VectorPrintLog.js';

import { assertAndConsumePrintQuota } from '../services/printQuotaService.js';
import { resolveFinalPdfKeyForServe } from '../services/finalPdfExportService.js';
import { deleteFromS3, s3 } from '../services/s3.js';

const router = express.Router();

const isVirtualPrinter = (name) => {
  const n = String(name || '').toLowerCase();
  return /microsoft print to pdf|save as pdf|pdf|xps|onenote|fax/i.test(n);
};

const computeRemaining = (access) => {
  const quota =
    Number.isFinite(access?.printQuota) && access.printQuota !== null
      ? Number(access.printQuota)
      : Number(access?.assignedQuota || 0);
  const used = Math.max(
    Number.isFinite(access?.printsUsed) ? Number(access.printsUsed) : 0,
    Number.isFinite(access?.usedPrints) ? Number(access.usedPrints) : 0
  );
  return { maxPrints: quota, remainingPrints: Math.max(0, quota - used) };
};

router.get('/print-agent/download', async (req, res) => {
  try {
    const bucket = typeof process.env.AWS_S3_BUCKET === 'string' ? process.env.AWS_S3_BUCKET.trim() : '';
    if (!bucket) {
      return res.status(500).json({ message: 'S3 not configured' });
    }

    const installerKey =
      typeof process.env.PRINT_AGENT_S3_KEY === 'string' && process.env.PRINT_AGENT_S3_KEY.trim().length > 0
        ? process.env.PRINT_AGENT_S3_KEY.trim()
        : 'securepdf/print-agent/SecurePrintHub-Setup-1.0.0.exe';

    const filenameRaw =
      typeof process.env.PRINT_AGENT_FILENAME === 'string' && process.env.PRINT_AGENT_FILENAME.trim().length > 0
        ? process.env.PRINT_AGENT_FILENAME.trim()
        : 'SecurePrintHub-Setup-1.0.0.exe';

    const filename = filenameRaw.replace(/[\\/\n\r\t"]/g, '_');

    let head;
    try {
      head = await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: installerKey }));
    } catch (err) {
      const statusCode = err?.$metadata?.httpStatusCode;
      const errName = err?.name;
      if (statusCode === 404 || errName === 'NotFound' || errName === 'NoSuchKey') {
        return res.status(404).json({
          message: 'Print agent installer not found. Please upload it to S3 and set PRINT_AGENT_S3_KEY correctly.',
        });
      }
      throw err;
    }

    const size = Number(head?.ContentLength ?? 0);
    if (!Number.isFinite(size) || size < 1024 * 1024) {
      return res.status(500).json({
        message: 'Print agent installer is missing or corrupted. Please re-upload a valid installer build.',
      });
    }

    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: installerKey,
      ResponseContentDisposition: `attachment; filename="${filename}"`,
      ResponseContentType: 'application/octet-stream',
    });

    const signedUrl = await getSignedUrl(s3, command, { expiresIn: 60 * 5 });

    const accept = String(req.headers.accept || '').toLowerCase();
    const wantsJson =
      (typeof req.query?.format === 'string' && req.query.format.toLowerCase() === 'json') ||
      accept.includes('application/json');

    if (wantsJson) {
      return res.json({ url: signedUrl, filename, size });
    }

    return res.redirect(signedUrl);
  } catch (err) {
    console.error('Print agent download error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.post('/print/fetch', authMiddleware, async (req, res) => {
  try {
    const printId = typeof req.body?.printId === 'string' ? req.body.printId.trim() : '';
    const token = typeof req.body?.token === 'string' ? req.body.token.trim() : '';
    const deviceId = typeof req.headers['x-device-id'] === 'string' ? String(req.headers['x-device-id']).trim() : '';

    if (!printId || !token || !deviceId) {
      return res.status(400).json({ message: 'printId, token, and X-Device-Id are required' });
    }

    const job = await VectorPrintJob.findOne({ _id: printId, userId: req.user._id }).exec();
    if (!job) {
      return res.status(404).json({ message: 'Print job not found' });
    }

    if (job.status !== 'RUNNING') {
      return res.status(409).json({ message: 'Print job not active' });
    }

    if (job.metadata?.deviceId && job.metadata.deviceId !== deviceId) {
      return res.status(403).json({ message: 'Device mismatch' });
    }

    if (job.metadata?.fetchToken !== token) {
      return res.status(403).json({ message: 'Invalid token' });
    }

    if (job.metadata?.fetchedAt) {
      return res.status(409).json({ message: 'PDF already fetched' });
    }

    const expiresAt = job.output?.expiresAt ? new Date(job.output.expiresAt) : null;
    if (expiresAt && expiresAt.getTime() <= Date.now()) {
      if (job.output?.key) {
        await deleteFromS3(job.output.key).catch(() => null);
      }
      job.status = 'EXPIRED';
      job.output = { key: null, url: null, expiresAt: null };
      job.audit.push({ event: 'FETCH_DENIED_EXPIRED_AND_OUTPUT_DELETED', details: null });
      await job.save();
      return res.status(410).json({ message: 'Expired' });
    }

    const sourceKey = String(job.sourcePdfKey || '').trim();
    if (!sourceKey) {
      return res.status(410).json({ message: 'PDF not available' });
    }

    const bucket = String(process.env.AWS_S3_BUCKET || '').trim();
    if (!bucket) {
      return res.status(500).json({ message: 'S3 not configured' });
    }

    try {
      await s3.send(
        new HeadObjectCommand({
          Bucket: bucket,
          Key: sourceKey,
        })
      );
    } catch (err) {
      console.error('[PRINT_BLOCKED] PDF missing in S3:', sourceKey);
      return res.status(409).json({
        message: 'PDF not available for printing. Please regenerate.',
      });
    }

    const command = new GetObjectCommand({ Bucket: bucket, Key: sourceKey });
    const obj = await s3.send(command);
    if (!obj?.Body) {
      return res.status(410).json({ message: 'PDF not available' });
    }

    job.output = { key: null, url: null, expiresAt: null };
    job.metadata.fetchedAt = new Date().toISOString();
    job.markModified('metadata');
    job.audit.push({
      event: 'FETCHED_ONCE_AND_OUTPUT_DELETED',
      details: { deviceId, previousExpiresAt: expiresAt ? expiresAt.toISOString() : null },
    });
    await job.save();

    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Cache-Control', 'no-store');
    if (obj.ContentLength !== undefined && obj.ContentLength !== null) {
      res.setHeader('Content-Length', String(obj.ContentLength));
    }

    obj.Body.on('error', () => {
      try {
        res.end();
      } catch {
        // ignore
      }
    });

    return obj.Body.pipe(res);
  } catch (err) {
    console.error('Print fetch error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.get('/assignments', authMiddleware, async (req, res) => {
  try {
    const accesses = await VectorDocumentAccess.find({ userId: req.user._id, revoked: false })
      .populate({ path: 'documentId', model: 'VectorDocument' })
      .sort({ createdAt: -1 })
      .exec();

    const out = accesses.map((access) => {
      const doc = access.documentId;
      const { maxPrints, remainingPrints } = computeRemaining(access);
      return {
        assignmentId: access._id.toString(),
        documentId: doc?._id?.toString?.() || null,
        title: doc?.title || 'Document',
        remainingPrints,
        maxPrints,
      };
    });

    return res.json(out);
  } catch (err) {
    console.error('Assignments list error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.post('/print/request', authMiddleware, async (req, res) => {
  try {
    const assignmentId = typeof req.body?.assignmentId === 'string' ? req.body.assignmentId.trim() : '';
    const printerName = typeof req.body?.printerName === 'string' ? req.body.printerName.trim() : '';
    const deviceId = typeof req.headers['x-device-id'] === 'string' ? String(req.headers['x-device-id']).trim() : '';

    if (!assignmentId || !printerName || !deviceId) {
      return res.status(400).json({ message: 'assignmentId, printerName, and X-Device-Id are required' });
    }

    if (isVirtualPrinter(printerName)) {
      return res.status(400).json({ message: 'Virtual printers are blocked' });
    }

    const access = await VectorDocumentAccess.findOne({ _id: assignmentId, userId: req.user._id, revoked: false })
      .select('documentId printQuota assignedQuota printsUsed usedPrints')
      .exec();

    if (!access) {
      return res.status(404).json({ message: 'Assignment not found' });
    }

    const docId = access.documentId?.toString?.() || '';
    if (!docId) {
      return res.status(404).json({ message: 'Document not found' });
    }

    const { remainingPrints } = computeRemaining(access);
    if (remainingPrints <= 0) {
      return res.status(403).json({ message: 'Print limit exceeded' });
    }

    // Prevent concurrent job spam from bypassing remainingPrints check.
    const runningCount = await VectorPrintJob.countDocuments({
      userId: req.user._id,
      status: 'RUNNING',
      'metadata.assignmentId': assignmentId,
    }).exec();
    if (runningCount >= remainingPrints) {
      return res.status(403).json({ message: 'Print limit exceeded' });
    }

    const requestId = crypto.randomUUID();

    const doc = await VectorDocument.findById(docId).select('title').exec();

    const sourceKey = await resolveFinalPdfKeyForServe(docId);

    const issuedAtIso = new Date().toISOString();
    const serial = crypto.randomUUID();

    const printId = new mongoose.Types.ObjectId();
    const printIdStr = printId.toString();

    const expiresIn = Number(process.env.PRINT_URL_TTL_SECONDS || 60);
    const expiresAt = new Date(Date.now() + expiresIn * 1000);

    const secret = process.env.PRINT_PAYLOAD_SECRET || process.env.JWT_SECRET;
    if (!secret) {
      return res.status(500).json({ message: 'Server print signing not configured' });
    }
    const payloadHmac = crypto
      .createHmac('sha256', secret)
      .update(`${printIdStr}:${docId}:${req.user._id.toString()}`)
      .digest('hex');

    const fetchToken = crypto.randomBytes(32).toString('hex');

    await VectorPrintJob.create({
      _id: printId,
      userId: req.user._id,
      sourcePdfKey: sourceKey,
      metadata: {
        documentId: docId,
        assignmentId,
        deviceId,
        printerName,
        issuedAt: issuedAtIso,
        serial,
        title: doc?.title || 'Document',
        requestId,
        fetchToken,
        fetchedAt: null,
      },
      payloadHmac,
      status: 'RUNNING',
      progress: 0,
      totalPages: 1,
      output: {
        key: null,
        url: null,
        expiresAt,
      },
      audit: [
        { event: 'PRINT_REQUESTED', details: { assignmentId, printerName, deviceId, requestId } },
      ],
    });

    return res.json({
      printId: printIdStr,
      fetchToken,
      expiresAt: expiresAt.toISOString(),
    });
  } catch (err) {
    console.error('Print request error', err);
    if (err && (err.code === 'LIMIT' || /print limit exceeded/i.test(String(err.message || '')))) {
      return res.status(403).json({ message: 'Print limit exceeded' });
    }
    return res.status(500).json({ message: err?.message || 'Internal server error' });
  }
});

router.post('/print/confirm', authMiddleware, async (req, res) => {
  try {
    const printId = typeof req.body?.printId === 'string' ? req.body.printId.trim() : '';
    const printerName = typeof req.body?.printerName === 'string' ? req.body.printerName.trim() : '';
    const deviceId = typeof req.headers['x-device-id'] === 'string' ? String(req.headers['x-device-id']).trim() : '';

    if (!printId) {
      return res.status(400).json({ message: 'printId is required' });
    }

    const job = await VectorPrintJob.findOne({ _id: printId, userId: req.user._id }).exec();
    if (!job) {
      return res.status(404).json({ message: 'Print job not found' });
    }

    if (job.status !== 'RUNNING') {
      return res.status(409).json({ message: 'Print job already finalized' });
    }

    // Consume quota ONLY on confirmation (per master prompt).
    const docIdForQuota = job.metadata?.documentId;
    const requestIdForQuota = job.metadata?.requestId;
    if (docIdForQuota && requestIdForQuota) {
      await assertAndConsumePrintQuota(String(docIdForQuota), req.user._id.toString(), String(requestIdForQuota));
    }

    const key = job.output?.key;
    if (key) {
      await deleteFromS3(key).catch(() => null);
    }

    job.status = 'DONE';
    job.output = { key: null, url: null, expiresAt: null };
    job.audit.push({ event: 'PRINT_CONFIRMED_AND_OUTPUT_DELETED', details: { printerName, deviceId } });
    await job.save();

    const docId = job.metadata?.documentId;
    if (docId) {
      await VectorPrintLog.create({
        userId: req.user._id,
        documentId: docId,
        count: 1,
        meta: {
          printId,
          deviceId,
          printerName,
          result: 'SUCCESS',
          serial: job.metadata?.serial || null,
        },
      }).catch(() => null);
    }

    return res.json({ ok: true });
  } catch (err) {
    console.error('Print confirm error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.post('/print/fail', authMiddleware, async (req, res) => {
  try {
    const printId = typeof req.body?.printId === 'string' ? req.body.printId.trim() : '';
    const printerName = typeof req.body?.printerName === 'string' ? req.body.printerName.trim() : '';
    const reason = typeof req.body?.reason === 'string' ? req.body.reason.trim() : '';
    const deviceId = typeof req.headers['x-device-id'] === 'string' ? String(req.headers['x-device-id']).trim() : '';

    if (!printId) {
      return res.status(400).json({ message: 'printId is required' });
    }

    const job = await VectorPrintJob.findOne({ _id: printId, userId: req.user._id }).exec();
    if (!job) {
      return res.status(404).json({ message: 'Print job not found' });
    }

    if (job.status !== 'RUNNING') {
      return res.status(409).json({ message: 'Print job already finalized' });
    }

    const key = job.output?.key;
    if (key) {
      await deleteFromS3(key).catch(() => null);
    }

    job.status = 'FAILED';
    job.output = { key: null, url: null, expiresAt: null };
    job.error = { message: reason || 'Print failed', stack: null };
    job.audit.push({ event: 'PRINT_FAILED_AND_OUTPUT_DELETED', details: { printerName, deviceId, reason } });
    await job.save();

    const docId = job.metadata?.documentId;
    if (docId) {
      await VectorPrintLog.create({
        userId: req.user._id,
        documentId: docId,
        count: 0,
        meta: {
          printId,
          deviceId,
          printerName,
          result: 'FAILED',
          reason,
          serial: job.metadata?.serial || null,
        },
      }).catch(() => null);
    }

    return res.json({ ok: true });
  } catch (err) {
    console.error('Print fail error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

export default router;

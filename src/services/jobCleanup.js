import VectorPrintJob from '../vectorModels/VectorPrintJob.js';
import { deleteFromS3 } from './s3.js';

export const runJobCleanupOnce = async () => {
  const now = new Date();

  const runningExpired = await VectorPrintJob.find({
    status: 'RUNNING',
    'output.key': { $ne: null },
    'output.expiresAt': { $ne: null, $lte: now },
  }).exec();

  for (const job of runningExpired) {
    if (
      job.output?.key &&
      (String(job.output.key).startsWith('documents/final/') ||
        String(job.output.key).startsWith('documents/print/'))
    ) {
      await deleteFromS3(job.output.key).catch(() => null);
    }

    job.status = 'EXPIRED';
    job.output = { key: null, url: null, expiresAt: null };
    job.audit.push({ event: 'RUNNING_JOB_EXPIRED_AND_OUTPUT_DELETED', details: null });
    await job.save();
  }

  const staleMs = Number(process.env.PRINT_JOB_STALE_MS || 15 * 60 * 1000);
  const staleBefore = new Date(now.getTime() - staleMs);
  const staleRunning = await VectorPrintJob.find({
    status: 'RUNNING',
    'output.key': null,
    updatedAt: { $lte: staleBefore },
  }).exec();

  for (const job of staleRunning) {
    job.status = 'EXPIRED';
    job.output = { key: null, url: null, expiresAt: null };
    job.audit.push({ event: 'STALE_RUNNING_JOB_EXPIRED', details: { staleMs } });
    await job.save();
  }

  const expired = await VectorPrintJob.find({
    status: 'DONE',
    'output.expiresAt': { $ne: null, $lte: now },
  }).exec();

  for (const job of expired) {
    if (
      job.output?.key &&
      (String(job.output.key).startsWith('documents/final/') ||
        String(job.output.key).startsWith('documents/print/'))
    ) {
      await deleteFromS3(job.output.key).catch(() => null);
    }

    job.status = 'EXPIRED';
    job.output = { key: null, url: null, expiresAt: null };
    job.audit.push({ event: 'JOB_EXPIRED_AND_OUTPUT_DELETED', details: null });
    await job.save();
  }

  const failed = await VectorPrintJob.find({
    status: 'FAILED',
    updatedAt: { $lte: new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000) },
  }).exec();

  for (const job of failed) {
    job.status = 'EXPIRED';
    job.audit.push({ event: 'FAILED_JOB_ARCHIVED', details: null });
    await job.save();
  }
};

export const startJobCleanupLoop = () => {
  const intervalMs = Number(process.env.JOB_CLEANUP_INTERVAL_MS || 5 * 60 * 1000);

  const tick = async () => {
    try {
      await runJobCleanupOnce();
    } catch (_) {
      // ignore
    }
  };

  tick();
  const timer = setInterval(tick, intervalMs);
  timer.unref?.();

  return () => clearInterval(timer);
};

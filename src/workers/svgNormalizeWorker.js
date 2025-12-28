import { Worker } from 'bullmq';
import { connection } from '../../queues/vectorQueue.js';
import { VECTOR_NORMALIZE_QUEUE_NAME } from '../../queues/vectorNormalizeQueue.js';
import { processNormalizeSvg } from './vectorPdfWorker.js';
import os from 'os';

let workerStarted = false;
let startedWorker = null;

export const startSvgNormalizeWorker = () => {
  if (workerStarted) return startedWorker;

  if (!connection) {
    console.warn('[SVG_WORKER_STARTED]', { ok: false, reason: 'Redis unavailable', ts: Date.now() });
    return null;
  }

  const cpuCount = Array.isArray(os.cpus?.()) ? os.cpus().length : 1;
  const count = Math.max(1, Math.min(4, Number(cpuCount) || 1));
  const workers = [];

  for (let i = 0; i < count; i += 1) {
    const w = new Worker(
      VECTOR_NORMALIZE_QUEUE_NAME,
      async (job) => {
        console.log('[SVG_WORKER_JOB_RECEIVED]', { jobId: job?.id, jobName: job?.name, ts: Date.now() });
        if (job?.name !== 'normalizeSvg') {
          console.error('[SVG_WORKER_UNEXPECTED_JOB]', { jobId: job?.id, jobName: job?.name });
          throw new Error(`Unknown job type: ${job?.name}`);
        }
        return processNormalizeSvg(job);
      },
      { connection, concurrency: 1, lockDuration: 10 * 60 * 1000 }
    );

    w.on('ready', () => {
      console.log('[SVG_WORKER_STARTED]', { ok: true, queue: VECTOR_NORMALIZE_QUEUE_NAME, ts: Date.now() });
    });

    w.on('failed', (job, err) => {
      console.error('[SVG_NORMALIZE_FAILED]', { jobId: job?.id, message: err?.message || null });
    });

    w.on('error', (err) => {
      const code = err?.code || err?.errno;
      if (code) {
        console.warn('[SVG_WORKER_ERROR]', { code });
        return;
      }
      console.warn('[SVG_WORKER_ERROR]', { message: err?.message || 'unknown' });
    });

    workers.push(w);
  }

  workerStarted = true;
  startedWorker = workers;
  return startedWorker;
};

import { Queue } from 'bullmq';

export let connection = null;

// Use a versioned queue name to avoid collisions with legacy BullMQ jobs
// that may use different job names or payload formats.
export const VECTOR_PDF_QUEUE_NAME = 'vectorPdfQueueV2';

let vectorPdfQueue = null;
let warnedRedisUnavailable = false;

export const setRedisConnection = (redisConnection) => {
  connection = redisConnection || null;
  vectorPdfQueue = null;
  warnedRedisUnavailable = false;
};

export const getVectorPdfQueue = () => {
  if (vectorPdfQueue) return vectorPdfQueue;
  if (!connection) {
    if (!warnedRedisUnavailable) {
      warnedRedisUnavailable = true;
      console.warn('[queues/vectorQueue] Redis unavailable: BullMQ disabled');
    }
    return null;
  }
  const diag = String(process.env.DIAG_BULLMQ || '') === 'true';
  if (diag) {
    console.log('[QUEUE_NAME][ENQUEUE]', VECTOR_PDF_QUEUE_NAME);
    try {
      console.log('[REDIS_CONN][ENQUEUE]', connection?.options);
    } catch {
      console.log('[REDIS_CONN][ENQUEUE]', null);
    }
  }
  try {
    const autoPrune = String(process.env.BULLMQ_AUTO_PRUNE || '') === 'true';
    const keepCompleted = Math.max(0, Number(process.env.BULLMQ_KEEP_COMPLETED || 200));
    const keepFailed = Math.max(0, Number(process.env.BULLMQ_KEEP_FAILED || 200));

    vectorPdfQueue = new Queue(VECTOR_PDF_QUEUE_NAME, {
      connection,
      ...(autoPrune
        ? {
            defaultJobOptions: {
              removeOnComplete: keepCompleted > 0 ? { count: keepCompleted } : true,
              removeOnFail: keepFailed > 0 ? { count: keepFailed } : false,
            },
          }
        : {}),
    });
    vectorPdfQueue.on('error', (err) => {
      const code = err?.code || err?.errno;
      if (
        !warnedRedisUnavailable &&
        (code === 'ETIMEDOUT' || code === 'ECONNREFUSED' || code === 'EHOSTUNREACH' || code === 'ENOTFOUND' || code === 'ECONNRESET')
      ) {
        warnedRedisUnavailable = true;
        console.warn('[queues/vectorQueue] Redis unavailable: BullMQ disabled', { code });
        return;
      }
      if (!warnedRedisUnavailable) {
        console.error('[queues/vectorQueue] Queue error', err);
      }
    });
    console.log('[queues/vectorQueue] Vector queue initialized');
    return vectorPdfQueue;
  } catch (err) {
    if (!warnedRedisUnavailable) {
      warnedRedisUnavailable = true;
      console.warn('[queues/vectorQueue] Redis unavailable: BullMQ disabled');
    }
    vectorPdfQueue = null;
    return null;
  }
};

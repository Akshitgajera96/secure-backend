import { Queue } from 'bullmq';
import { connection } from './vectorQueue.js';

export const VECTOR_NORMALIZE_QUEUE_NAME = 'vectorNormalizeQueue';

let vectorNormalizeQueue = null;
let warnedRedisUnavailable = false;

export const getVectorNormalizeQueue = () => {
  if (vectorNormalizeQueue) return vectorNormalizeQueue;
  if (!connection) {
    if (!warnedRedisUnavailable) {
      warnedRedisUnavailable = true;
      console.warn('[queues/vectorNormalizeQueue] Redis unavailable: BullMQ disabled');
    }
    return null;
  }

  try {
    vectorNormalizeQueue = new Queue(VECTOR_NORMALIZE_QUEUE_NAME, {
      connection,
      defaultJobOptions: {
        removeOnComplete: true,
        removeOnFail: true,
      },
    });

    vectorNormalizeQueue.on('error', (err) => {
      const code = err?.code || err?.errno;
      if (
        !warnedRedisUnavailable &&
        (code === 'ETIMEDOUT' || code === 'ECONNREFUSED' || code === 'EHOSTUNREACH' || code === 'ENOTFOUND' || code === 'ECONNRESET')
      ) {
        warnedRedisUnavailable = true;
        console.warn('[queues/vectorNormalizeQueue] Redis unavailable: BullMQ disabled', { code });
        return;
      }
      if (!warnedRedisUnavailable) {
        console.error('[queues/vectorNormalizeQueue] Queue error', err);
      }
    });

    console.log('[queues/vectorNormalizeQueue] Normalize queue initialized');
    return vectorNormalizeQueue;
  } catch {
    if (!warnedRedisUnavailable) {
      warnedRedisUnavailable = true;
      console.warn('[queues/vectorNormalizeQueue] Redis unavailable: BullMQ disabled');
    }
    vectorNormalizeQueue = null;
    return null;
  }
};

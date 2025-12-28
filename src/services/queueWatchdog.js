import { getVectorPdfQueue } from '../../queues/vectorQueue.js';
import VectorDocument from '../vectorModels/VectorDocument.js';
import VectorPrintJob from '../vectorModels/VectorPrintJob.js';

export const startVectorQueueWatchdog = () => {
  if (String(process.env.ENABLE_QUEUE_WATCHDOG || '') !== 'true') {
    return () => {};
  }

  const intervalMs = Math.max(5_000, Number(process.env.QUEUE_WATCHDOG_INTERVAL_MS || 30_000));
  const oldestWaitingWarnMs = Math.max(1_000, Number(process.env.QUEUE_OLDEST_WAITING_WARN_MS || 30_000));
  const failedPoisonThreshold = Math.max(0, Number(process.env.QUEUE_FAILED_POISON_THRESHOLD || 20));
  const waitingChildrenPoisonThreshold = Math.max(0, Number(process.env.QUEUE_WAITING_CHILDREN_POISON_THRESHOLD || 20));

  const pendingDocWarnMs = Math.max(1_000, Number(process.env.SVG_PENDING_WARN_MS || 60_000));
  const pendingDocFailMs = Math.max(pendingDocWarnMs, Number(process.env.SVG_PENDING_FAIL_MS || 10 * 60_000));

  const autoFail = false;

  let running = false;

  const tick = async () => {
    if (running) return;
    running = true;

    try {
      const q = getVectorPdfQueue();
      if (!q) return;

      const counts = await q.getJobCounts().catch(() => null);
      if (counts) {
        const failed = Number(counts.failed || 0);
        const waitingChildren = Number(counts['waiting-children'] || 0);

        if (failed > failedPoisonThreshold || waitingChildren > waitingChildrenPoisonThreshold) {
          console.error('[QUEUE_POISONED]', {
            queue: q.name,
            counts,
            threshold: { failedPoisonThreshold, waitingChildrenPoisonThreshold },
          });
        }
      }

      const waiting = await q.getJobs(['waiting'], 0, 0, true).catch(() => []);
      if (waiting && waiting.length > 0) {
        const oldest = waiting[0];
        const ageMs = Date.now() - Number(oldest.timestamp || 0);
        if (Number.isFinite(ageMs) && ageMs > oldestWaitingWarnMs) {
          console.error('[QUEUE_STARVATION]', {
            queue: q.name,
            oldestWaitingAgeMs: ageMs,
            oldestJob: { id: oldest.id, name: oldest.name },
            counts: counts || null,
          });
        }
      }

      const warnBefore = new Date(Date.now() - pendingDocWarnMs);
      const stalePendingCount = await VectorDocument.countDocuments({
        svgNormalizeStatus: 'PENDING',
        createdAt: { $lte: warnBefore },
      }).catch(() => 0);

      if (stalePendingCount > 0) {
        console.error('[SVG_PENDING_STUCK]', {
          svgNormalizeStatus: 'PENDING',
          staleCount: stalePendingCount,
          olderThanMs: pendingDocWarnMs,
          counts: counts || null,
        });
      }
    } finally {
      running = false;
    }
  };

  tick();
  const timer = setInterval(() => {
    tick().catch(() => null);
  }, intervalMs);
  timer.unref?.();

  return () => clearInterval(timer);
};

import VectorDocument from '../vectorModels/VectorDocument.js';

export const startSvgNormalizeReconciler = () => {
  if (String(process.env.ENABLE_SVG_RECONCILER || '') !== 'true') {
    return () => {};
  }

  const intervalMs = 10_000;
  const maxWaitMs = Math.max(1_000, Number(process.env.SVG_NORMALIZE_MAX_WAIT_MS || 60_000));

  let running = false;

  const tick = async () => {
    if (running) return;
    running = true;

    try {
      const cutoff = new Date(Date.now() - maxWaitMs);

      const candidates = await VectorDocument.find({
        svgNormalizeStatus: 'PENDING',
        svgNormalizeEnqueuedAt: { $lte: cutoff },
        $or: [{ svgNormalizeStartedAt: null }, { svgNormalizeStartedAt: { $exists: false } }],
      })
        .select('_id svgNormalizeJobId svgNormalizeEnqueuedAt')
        .limit(50)
        .exec()
        .catch(() => []);

      for (const doc of candidates) {
        const enqAt = doc?.svgNormalizeEnqueuedAt instanceof Date ? doc.svgNormalizeEnqueuedAt : null;
        const waitedMs = enqAt ? Date.now() - enqAt.getTime() : null;

        const res = await VectorDocument.updateOne(
          {
            _id: doc._id,
            svgNormalizeStatus: 'PENDING',
            $or: [{ svgNormalizeStartedAt: null }, { svgNormalizeStartedAt: { $exists: false } }],
          },
          {
            $set: {
              svgNormalizeStatus: 'FAILED',
              svgNormalizeError: { message: 'WORKER_NOT_CONSUMED', stack: null },
              normalizeFailed: true,
              normalizeError: {
                message: 'WORKER_NOT_CONSUMED',
                stack: null,
                reason: 'WORKER_NOT_CONSUMED',
                at: new Date(),
              },
            },
          }
        )
          .exec()
          .catch(() => null);

        const modified = Number(res?.modifiedCount || res?.nModified || 0);
        if (modified > 0) {
          console.error('[SVG_RECONCILED_TO_FAILED]', {
            documentId: String(doc._id),
            jobId: doc?.svgNormalizeJobId || null,
            waitedMs: waitedMs === null ? null : waitedMs,
          });
        }
      }
    } finally {
      running = false;
    }
  };

  tick().catch(() => null);
  const timer = setInterval(() => {
    tick().catch(() => null);
  }, intervalMs);
  timer.unref?.();

  return () => clearInterval(timer);
};

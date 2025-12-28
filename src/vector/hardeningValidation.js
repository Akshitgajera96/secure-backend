export class VectorJobValidationError extends Error {
  constructor(message, details = null) {
    super(message);
    this.name = 'VectorJobValidationError';
    this.code = 'BAD_VECTOR_JOB';
    this.details = details;
  }
}

export function assertVectorJobEnqueueable(metadata) {
  const maxPages = Math.max(1, Number(process.env.VECTOR_MAX_PAGES || 700));
  const totalPages = Number(metadata?.layout?.totalPages || 1);

  if (!Number.isFinite(totalPages) || totalPages < 1) {
    throw new VectorJobValidationError('Invalid totalPages', { field: 'layout.totalPages' });
  }

  if (totalPages > maxPages) {
    throw new VectorJobValidationError('Job exceeds max pages', { totalPages, maxPages });
  }

  const repeatPerPage = Number(metadata?.layout?.repeatPerPage || 4);
  if (!Number.isFinite(repeatPerPage) || repeatPerPage < 1) {
    throw new VectorJobValidationError('Invalid repeatPerPage', { field: 'layout.repeatPerPage' });
  }

  const totalItems = totalPages * repeatPerPage;
  if (!Number.isFinite(totalItems) || totalItems < 1 || totalItems > Number.MAX_SAFE_INTEGER) {
    throw new VectorJobValidationError('Invalid total items', { totalPages, repeatPerPage });
  }

  const mode = metadata?.colorMode;
  if (mode !== undefined && mode !== null && mode !== 'RGB' && mode !== 'CMYK') {
    throw new VectorJobValidationError('Invalid colorMode', { colorMode: mode });
  }

  const maxSeriesEnd = Math.max(1, Number(process.env.VECTOR_MAX_SERIES_END || 1000000000));
  if (Array.isArray(metadata?.series)) {
    for (const s of metadata.series) {
      if (!s) continue;
      const start = Number(s.start);
      const step = Number(s.step);
      if (Number.isFinite(start) && Number.isFinite(step) && step > 0) {
        const end = start + (Math.max(0, totalItems - 1) * step);
        if (!Number.isFinite(end) || end > maxSeriesEnd) {
          throw new VectorJobValidationError('Invalid series range', {
            start,
            step,
            totalItems,
            computedEnd: end,
            maxSeriesEnd,
          });
        }
      }
    }
  }
}

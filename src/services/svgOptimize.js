import { optimize } from 'svgo';

export async function optimizeSvg(svgContent, options = {}) {
  const documentId = options && typeof options.documentId === 'string' ? options.documentId : null;
  const raw = typeof svgContent === 'string' ? svgContent : String(svgContent || '');
  const svgoConfig = options && typeof options.svgoConfig === 'object' && options.svgoConfig ? options.svgoConfig : null;

  const originalBytes = Buffer.byteLength(raw, 'utf8');
  const startedAtMs = Date.now();

  console.log('[SVGO_START]', { documentId, originalBytes, durationMs: 0, ts: startedAtMs });
  try {
    const result = optimize(raw, {
      multipass: typeof svgoConfig?.multipass === 'boolean' ? svgoConfig.multipass : true,
      plugins: Array.isArray(svgoConfig?.plugins)
        ? svgoConfig.plugins
        : [
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
    });

    const optimized = typeof result?.data === 'string' ? result.data : raw;
    const optimizedBytes = Buffer.byteLength(optimized, 'utf8');
    const durationMs = Date.now() - startedAtMs;
    const reductionPercent = originalBytes > 0 ? Math.round(((originalBytes - optimizedBytes) / originalBytes) * 10000) / 100 : 0;

    console.log('[SVGO_DONE]', {
      documentId,
      originalBytes,
      optimizedBytes,
      reductionPercent,
      durationMs,
    });

    return optimized;
  } catch (err) {
    const durationMs = Date.now() - startedAtMs;
    console.log('[SVGO_DONE]', {
      documentId,
      originalBytes,
      optimizedBytes: originalBytes,
      reductionPercent: 0,
      durationMs,
      error: err instanceof Error ? err.message : String(err),
    });

    return raw;
  }
}

const readPositiveInt = (key, fallback) => {
  const raw = Number(process.env[key]);
  const v = Number.isFinite(raw) ? Math.floor(raw) : NaN;
  return v > 0 ? v : fallback;
};

export const SVG_LIMITS = {
  FAST: readPositiveInt('SVG_LIMIT_FAST', 200_000),
  SLOW: readPositiveInt('SVG_LIMIT_SLOW', 500_000),
  MAX: readPositiveInt('SVG_LIMIT_MAX', 800_000),
};

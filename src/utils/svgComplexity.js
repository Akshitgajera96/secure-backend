const countMatches = (re, s) => {
  if (!s) return 0;
  const r = new RegExp(re.source, re.flags.includes('g') ? re.flags : `${re.flags}g`);
  let count = 0;
  while (r.exec(s)) count += 1;
  return count;
};

const countNumbersInString = (s) => {
  if (!s) return 0;
  const re = /[-+]?\d*\.?\d+(?:e[-+]?\d+)?/gi;
  let count = 0;
  while (re.exec(s)) count += 1;
  return count;
};

export function analyzeSvgComplexity(svgContent) {
  const raw = typeof svgContent === 'string' ? svgContent : String(svgContent || '');

  const pathCount = countMatches(/<path\b/gi, raw);
  const useCount = countMatches(/<use\b/gi, raw);

  let pointCount = 0;
  const dAttrRe = /\bd\s*=\s*(['"])([\s\S]*?)\1/gi;
  let m;
  while ((m = dAttrRe.exec(raw))) {
    const d = m[2];
    pointCount += countNumbersInString(d);
  }

  const complexityScore = pointCount;

  return {
    pathCount,
    pointCount,
    useCount,
    complexityScore,
  };
}

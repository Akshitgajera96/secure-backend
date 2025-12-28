// SVG matrix locking for vector rendering
import crypto from 'crypto';

export class SvgRenderer {
  constructor() {
    this.svgCache = new Map();
  }

  // Parse and cache SVG paths
  parseSvg(svgContent) {
    const cacheKey = this.hashSvg(svgContent);
    
    if (!this.svgCache.has(cacheKey)) {
      const parsed = this.parseSvgContent(svgContent);
      this.svgCache.set(cacheKey, parsed);
    }
    
    return this.svgCache.get(cacheKey);
  }

  parseSvgContent(svgContent) {
    // Extract viewBox and paths from SVG
    const viewBoxMatch = svgContent.match(/viewBox\s*=\s*(['"])([^'"]+)\1/i);
    if (!viewBoxMatch) {
      throw new Error('SVG must include a viewBox');
    }

    const viewBoxParts = viewBoxMatch[2]
      .trim()
      .split(/[ ,]+/)
      .map((v) => Number(v));

    if (viewBoxParts.length !== 4 || viewBoxParts.some((v) => !Number.isFinite(v))) {
      throw new Error('SVG viewBox must have 4 finite numbers');
    }

    const [vbX, vbY, vbW, vbH] = viewBoxParts;
    if (vbW <= 0 || vbH <= 0) {
      throw new Error('SVG viewBox width/height must be positive');
    }

    const parseStyleAttr = (styleRaw) => {
      const style = typeof styleRaw === 'string' ? styleRaw : '';
      const out = {};
      style
        .split(';')
        .map((s) => s.trim())
        .filter(Boolean)
        .forEach((pair) => {
          const idx = pair.indexOf(':');
          if (idx <= 0) return;
          const key = pair.slice(0, idx).trim().toLowerCase();
          const val = pair.slice(idx + 1).trim();
          const allowed = new Set([
            'fill',
            'fill-opacity',
            'stroke',
            'stroke-opacity',
            'stroke-width',
            'stroke-linecap',
            'stroke-linejoin',
            'stroke-dasharray',
            'stroke-dashoffset',
            'opacity',
          ]);
          if (!allowed.has(key)) return;
          if (/url\s*\(/i.test(val)) return;
          out[key] = val;
        });
      return out;
    };

    const pickAttr = (tag, name) => {
      const re = new RegExp(`\\b${name}\\s*=\\s*(['"])([^'"]*)\\1`, 'i');
      const m = tag.match(re);
      return m ? m[2] : null;
    };

    const pathTags = svgContent.match(/<path[^>]*\sd\s*=\s*(?:"[^"]+"|'[^']+')[^>]*>/gis) || [];

    const pathEntries = pathTags
      .map((tag) => {
        const d = pickAttr(tag, 'd');
        if (!d) return null;

        const styleFromAttr = parseStyleAttr(pickAttr(tag, 'style'));

        const attrs = {
          fill: pickAttr(tag, 'fill') ?? styleFromAttr['fill'] ?? null,
          fillOpacity: pickAttr(tag, 'fill-opacity') ?? styleFromAttr['fill-opacity'] ?? null,
          stroke: pickAttr(tag, 'stroke') ?? styleFromAttr['stroke'] ?? null,
          strokeOpacity: pickAttr(tag, 'stroke-opacity') ?? styleFromAttr['stroke-opacity'] ?? null,
          strokeWidth: pickAttr(tag, 'stroke-width') ?? styleFromAttr['stroke-width'] ?? null,
          strokeLinecap: pickAttr(tag, 'stroke-linecap') ?? styleFromAttr['stroke-linecap'] ?? null,
          strokeLinejoin: pickAttr(tag, 'stroke-linejoin') ?? styleFromAttr['stroke-linejoin'] ?? null,
          strokeDasharray: pickAttr(tag, 'stroke-dasharray') ?? styleFromAttr['stroke-dasharray'] ?? null,
          strokeDashoffset: pickAttr(tag, 'stroke-dashoffset') ?? styleFromAttr['stroke-dashoffset'] ?? null,
          opacity: pickAttr(tag, 'opacity') ?? styleFromAttr['opacity'] ?? null,
        };

        return { d, attrs };
      })
      .filter(Boolean);

    const paths = pathEntries.map((p) => p.d).filter((d) => typeof d === 'string' && d.length > 0);

    if (paths.length === 0) {
      throw new Error('SVG must contain at least one <path d="...">');
    }

    return {
      viewBox: { x: vbX, y: vbY, width: vbW, height: vbH },
      paths,
      pathEntries,
      originalContent: svgContent
    };
  }

  // Apply explicit transform matrix (no cumulative transforms)
  createTransform(x, y, scaleX = 1, scaleY = 1, rotation = 0) {
    const cos = Math.cos(rotation);
    const sin = Math.sin(rotation);

    return [
      scaleX * cos, -scaleX * sin, x,
      scaleY * sin, scaleY * cos, y,
      0, 0, 1
    ];
  }

  // Sanitize SVG (PATH ONLY - no scripts, no external refs)
  sanitizeSvg(svgContent) {
    const raw = typeof svgContent === 'string' ? svgContent : '';

    const styleBlocks = Array.from(raw.matchAll(/<style\b[^>]*>([\s\S]*?)<\/style>/gi)).map((m) => m?.[1]).filter(Boolean);

    const cleaned = raw
      .replace(/<\?xml[^>]*\?>/gi, '')
      .replace(/<!DOCTYPE[^>]*>/gi, '')
      .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, '')
      .replace(/<defs\b[^>]*>[\s\S]*?<\/defs>/gi, '')
      .replace(/<metadata\b[^>]*>[\s\S]*?<\/metadata>/gi, '');

    // Hard-fail on known dangerous constructs and external refs
    const forbiddenPatterns = [
      /<script\b/i,
      /<foreignObject\b/i,
      /<(?:image|use)\b/i,
      /\bon\w+\s*=/i,
      /\b(?:href|xlink:href)\s*=/i,
      /javascript:/i,
      /data:/i,
      /url\s*\(/i,
    ];

    for (const pattern of forbiddenPatterns) {
      if (pattern.test(cleaned)) {
        throw new Error('SVG contains forbidden elements/attributes');
      }
    }

    let viewBoxMatch = cleaned.match(/viewBox\s*=\s*(['"])([^'"]+)\1/i);
    if (!viewBoxMatch) {
      const widthMatch = cleaned.match(/\bwidth\s*=\s*(['"])([^'"]+)\1/i);
      const heightMatch = cleaned.match(/\bheight\s*=\s*(['"])([^'"]+)\1/i);
      const widthRaw = widthMatch?.[2];
      const heightRaw = heightMatch?.[2];
      const width = widthRaw ? Number(String(widthRaw).replace(/px$/i, '').trim()) : NaN;
      const height = heightRaw ? Number(String(heightRaw).replace(/px$/i, '').trim()) : NaN;
      if (Number.isFinite(width) && Number.isFinite(height) && width > 0 && height > 0) {
        viewBoxMatch = ['viewBox', '"', `0 0 ${width} ${height}`];
      }
    }
    if (!viewBoxMatch) {
      throw new Error('SVG must include a viewBox');
    }

    const originalViewBox = String(viewBoxMatch[2] || '').trim();

    const parseStyleAttr = (styleRaw) => {
      const style = typeof styleRaw === 'string' ? styleRaw : '';
      const out = {};
      style
        .split(';')
        .map((s) => s.trim())
        .filter(Boolean)
        .forEach((pair) => {
          const idx = pair.indexOf(':');
          if (idx <= 0) return;
          const key = pair.slice(0, idx).trim().toLowerCase();
          const val = pair.slice(idx + 1).trim();
          const allowed = new Set([
            'fill',
            'fill-opacity',
            'stroke',
            'stroke-opacity',
            'stroke-width',
            'stroke-linecap',
            'stroke-linejoin',
            'stroke-dasharray',
            'stroke-dashoffset',
            'opacity',
          ]);
          if (!allowed.has(key)) return;
          if (/url\s*\(/i.test(val)) return;
          out[key] = val;
        });
      return out;
    };

    const pickAttr = (tag, name) => {
      const re = new RegExp(`\\b${name}\\s*=\\s*(['"])([^'"]*)\\1`, 'i');
      const m = tag.match(re);
      return m ? m[2] : null;
    };

    const parseCssClassStyles = (cssText) => {
      const text = typeof cssText === 'string' ? cssText : '';
      const classMap = new Map();
      const ruleRe = /([^{}]+)\{([^}]*)\}/g;
      let m;
      while ((m = ruleRe.exec(text))) {
        const selectorsRaw = String(m[1] || '').trim();
        const bodyRaw = String(m[2] || '');
        if (!selectorsRaw) continue;
        if (/url\s*\(/i.test(bodyRaw)) continue;

        const decls = parseStyleAttr(bodyRaw);
        if (!decls || Object.keys(decls).length === 0) continue;

        const selectors = selectorsRaw
          .split(',')
          .map((s) => s.trim())
          .filter(Boolean);

        for (const sel of selectors) {
          if (!sel.startsWith('.')) continue;
          const className = sel.slice(1).trim();
          if (!/^[a-zA-Z0-9_-]+$/.test(className)) continue;

          const existing = classMap.get(className) || {};
          classMap.set(className, { ...existing, ...decls });
        }
      }
      return classMap;
    };

    const cssClassStyles = new Map();
    for (const block of styleBlocks) {
      const parsed = parseCssClassStyles(block);
      for (const [k, v] of parsed.entries()) {
        const existing = cssClassStyles.get(k) || {};
        cssClassStyles.set(k, { ...existing, ...v });
      }
    }

    const pickNumber = (tag, name) => {
      const rawVal = pickAttr(tag, name);
      if (rawVal == null) return null;
      const n = Number(String(rawVal).replace(/px$/i, '').trim());
      return Number.isFinite(n) ? n : null;
    };

    const parsePoints = (rawPoints) => {
      const s = typeof rawPoints === 'string' ? rawPoints : '';
      if (!s) return [];
      const nums = s
        .match(/[-+]?(?:\d*\.\d+|\d+)(?:[eE][-+]?\d+)?/g)
        ?.map((v) => Number(v))
        .filter((n) => Number.isFinite(n));
      if (!nums || nums.length < 4) return [];
      const pts = [];
      for (let i = 0; i + 1 < nums.length; i += 2) {
        pts.push([nums[i], nums[i + 1]]);
      }
      return pts;
    };

    const rectToPath = (tag) => {
      const x = pickNumber(tag, 'x') ?? 0;
      const y = pickNumber(tag, 'y') ?? 0;
      const w = pickNumber(tag, 'width');
      const h = pickNumber(tag, 'height');
      if (!Number.isFinite(w) || !Number.isFinite(h) || w <= 0 || h <= 0) return null;
      const x2 = x + w;
      const y2 = y + h;
      return `M ${x} ${y} L ${x2} ${y} L ${x2} ${y2} L ${x} ${y2} Z`;
    };

    const lineToPath = (tag) => {
      const x1 = pickNumber(tag, 'x1');
      const y1 = pickNumber(tag, 'y1');
      const x2 = pickNumber(tag, 'x2');
      const y2 = pickNumber(tag, 'y2');
      if ([x1, y1, x2, y2].some((n) => !Number.isFinite(n))) return null;
      return `M ${x1} ${y1} L ${x2} ${y2}`;
    };

    const polyToPath = (tag, close) => {
      const pts = parsePoints(pickAttr(tag, 'points'));
      if (pts.length < 2) return null;
      const [x0, y0] = pts[0];
      const segs = [`M ${x0} ${y0}`];
      for (let i = 1; i < pts.length; i++) {
        const [x, y] = pts[i];
        segs.push(`L ${x} ${y}`);
      }
      if (close) segs.push('Z');
      return segs.join(' ');
    };

    const circleToPath = (tag) => {
      const cx = pickNumber(tag, 'cx');
      const cy = pickNumber(tag, 'cy');
      const r = pickNumber(tag, 'r');
      if ([cx, cy, r].some((n) => !Number.isFinite(n)) || r <= 0) return null;
      const x1 = cx + r;
      const x2 = cx - r;
      return `M ${x1} ${cy} A ${r} ${r} 0 1 0 ${x2} ${cy} A ${r} ${r} 0 1 0 ${x1} ${cy} Z`;
    };

    const ellipseToPath = (tag) => {
      const cx = pickNumber(tag, 'cx');
      const cy = pickNumber(tag, 'cy');
      const rx = pickNumber(tag, 'rx');
      const ry = pickNumber(tag, 'ry');
      if ([cx, cy, rx, ry].some((n) => !Number.isFinite(n)) || rx <= 0 || ry <= 0) return null;
      const x1 = cx + rx;
      const x2 = cx - rx;
      return `M ${x1} ${cy} A ${rx} ${ry} 0 1 0 ${x2} ${cy} A ${rx} ${ry} 0 1 0 ${x1} ${cy} Z`;
    };

    const pathTags = cleaned.match(/<path[^>]*\sd\s*=\s*(?:"[^"]+"|'[^']+')[^>]*>/gis) || [];
    const rectTags = cleaned.match(/<rect\b[^>]*>/gis) || [];
    const circleTags = cleaned.match(/<circle\b[^>]*>/gis) || [];
    const ellipseTags = cleaned.match(/<ellipse\b[^>]*>/gis) || [];
    const lineTags = cleaned.match(/<line\b[^>]*>/gis) || [];
    const polylineTags = cleaned.match(/<polyline\b[^>]*>/gis) || [];
    const polygonTags = cleaned.match(/<polygon\b[^>]*>/gis) || [];

    const drawableEntries = [];
    for (const tag of pathTags) {
      const d = pickAttr(tag, 'd');
      if (d) drawableEntries.push({ tag, d });
    }
    for (const tag of rectTags) {
      const d = rectToPath(tag);
      if (d) drawableEntries.push({ tag, d });
    }
    for (const tag of circleTags) {
      const d = circleToPath(tag);
      if (d) drawableEntries.push({ tag, d });
    }
    for (const tag of ellipseTags) {
      const d = ellipseToPath(tag);
      if (d) drawableEntries.push({ tag, d });
    }
    for (const tag of lineTags) {
      const d = lineToPath(tag);
      if (d) drawableEntries.push({ tag, d });
    }
    for (const tag of polylineTags) {
      const d = polyToPath(tag, false);
      if (d) drawableEntries.push({ tag, d });
    }
    for (const tag of polygonTags) {
      const d = polyToPath(tag, true);
      if (d) drawableEntries.push({ tag, d });
    }

    if (drawableEntries.length === 0) {
      throw new Error('SVG must contain at least one drawable element');
    }

    const expandBoundsFromPath = (d, bounds) => {
      const str = typeof d === 'string' ? d : '';
      if (!str) return;

      const tokens = [];
      const re = /([a-zA-Z])|([-+]?(?:\d*\.\d+|\d+)(?:[eE][-+]?\d+)?)/g;
      let m;
      while ((m = re.exec(str))) {
        if (m[1]) tokens.push(m[1]);
        else tokens.push(Number(m[2]));
      }

      const upd = (x, y) => {
        if (!Number.isFinite(x) || !Number.isFinite(y)) return;
        bounds.minX = Math.min(bounds.minX, x);
        bounds.minY = Math.min(bounds.minY, y);
        bounds.maxX = Math.max(bounds.maxX, x);
        bounds.maxY = Math.max(bounds.maxY, y);
      };

      let i = 0;
      let cmd = null;
      let cx = 0;
      let cy = 0;
      let sx = 0;
      let sy = 0;

      const nextNum = () => (typeof tokens[i] === 'number' ? tokens[i++] : NaN);
      const hasNum = () => typeof tokens[i] === 'number';

      while (i < tokens.length) {
        if (typeof tokens[i] === 'string') {
          cmd = tokens[i++];
        }
        if (!cmd) break;

        const lower = String(cmd).toLowerCase();
        const isRel = String(cmd) === lower;

        if (lower === 'z') {
          cx = sx;
          cy = sy;
          upd(cx, cy);
          continue;
        }

        if (lower === 'm') {
          const x = nextNum();
          const y = nextNum();
          if (!Number.isFinite(x) || !Number.isFinite(y)) break;
          cx = isRel ? cx + x : x;
          cy = isRel ? cy + y : y;
          sx = cx;
          sy = cy;
          upd(cx, cy);

          while (hasNum()) {
            const lx = nextNum();
            const ly = nextNum();
            if (!Number.isFinite(lx) || !Number.isFinite(ly)) break;
            cx = isRel ? cx + lx : lx;
            cy = isRel ? cy + ly : ly;
            upd(cx, cy);
          }
          continue;
        }

        if (lower === 'l') {
          while (hasNum()) {
            const x = nextNum();
            const y = nextNum();
            if (!Number.isFinite(x) || !Number.isFinite(y)) break;
            cx = isRel ? cx + x : x;
            cy = isRel ? cy + y : y;
            upd(cx, cy);
          }
          continue;
        }

        if (lower === 'h') {
          while (hasNum()) {
            const x = nextNum();
            if (!Number.isFinite(x)) break;
            cx = isRel ? cx + x : x;
            upd(cx, cy);
          }
          continue;
        }

        if (lower === 'v') {
          while (hasNum()) {
            const y = nextNum();
            if (!Number.isFinite(y)) break;
            cy = isRel ? cy + y : y;
            upd(cx, cy);
          }
          continue;
        }

        if (lower === 'c') {
          while (hasNum()) {
            const x1 = nextNum();
            const y1 = nextNum();
            const x2 = nextNum();
            const y2 = nextNum();
            const x = nextNum();
            const y = nextNum();
            if ([x1, y1, x2, y2, x, y].some((n) => !Number.isFinite(n))) break;
            const ax1 = isRel ? cx + x1 : x1;
            const ay1 = isRel ? cy + y1 : y1;
            const ax2 = isRel ? cx + x2 : x2;
            const ay2 = isRel ? cy + y2 : y2;
            cx = isRel ? cx + x : x;
            cy = isRel ? cy + y : y;
            upd(ax1, ay1);
            upd(ax2, ay2);
            upd(cx, cy);
          }
          continue;
        }

        if (lower === 's') {
          while (hasNum()) {
            const x2 = nextNum();
            const y2 = nextNum();
            const x = nextNum();
            const y = nextNum();
            if ([x2, y2, x, y].some((n) => !Number.isFinite(n))) break;
            const ax2 = isRel ? cx + x2 : x2;
            const ay2 = isRel ? cy + y2 : y2;
            cx = isRel ? cx + x : x;
            cy = isRel ? cy + y : y;
            upd(ax2, ay2);
            upd(cx, cy);
          }
          continue;
        }

        if (lower === 'q') {
          while (hasNum()) {
            const x1 = nextNum();
            const y1 = nextNum();
            const x = nextNum();
            const y = nextNum();
            if ([x1, y1, x, y].some((n) => !Number.isFinite(n))) break;
            const ax1 = isRel ? cx + x1 : x1;
            const ay1 = isRel ? cy + y1 : y1;
            cx = isRel ? cx + x : x;
            cy = isRel ? cy + y : y;
            upd(ax1, ay1);
            upd(cx, cy);
          }
          continue;
        }

        if (lower === 't') {
          while (hasNum()) {
            const x = nextNum();
            const y = nextNum();
            if (!Number.isFinite(x) || !Number.isFinite(y)) break;
            cx = isRel ? cx + x : x;
            cy = isRel ? cy + y : y;
            upd(cx, cy);
          }
          continue;
        }

        if (lower === 'a') {
          while (hasNum()) {
            const rx = nextNum();
            const ry = nextNum();
            const rot = nextNum();
            const laf = nextNum();
            const sf = nextNum();
            const x = nextNum();
            const y = nextNum();
            if ([rx, ry, rot, laf, sf, x, y].some((n) => !Number.isFinite(n))) break;
            // Conservative arc bounds: arcs can bulge beyond end points.
            // Without solving the ellipse center, include a generous envelope around
            // both start/end points using rx/ry.
            const absRx = Math.abs(rx);
            const absRy = Math.abs(ry);
            const sx0 = cx;
            const sy0 = cy;

            const ex = isRel ? cx + x : x;
            const ey = isRel ? cy + y : y;

            // Start/end points
            upd(sx0, sy0);
            upd(ex, ey);

            // Envelope around both points
            if (Number.isFinite(absRx) && Number.isFinite(absRy) && (absRx > 0 || absRy > 0)) {
              upd(sx0 - absRx, sy0 - absRy);
              upd(sx0 + absRx, sy0 + absRy);
              upd(ex - absRx, ey - absRy);
              upd(ex + absRx, ey + absRy);
            }

            cx = ex;
            cy = ey;
          }
          continue;
        }

        // Unknown command: stop.
        break;
      }
    };

    const vbParts = viewBoxMatch[2]
      .trim()
      .split(/[ ,]+/)
      .map((v) => Number(v));

    const [vbX, vbY, vbW, vbH] = vbParts.length === 4 ? vbParts : [0, 0, 0, 0];

    const bounds = { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity };

    const sanitizedPaths = drawableEntries
      .map((entry) => {
        const tag = entry.tag;
        const d = entry.d;
        if (!d) return '';

        expandBoundsFromPath(d, bounds);

        const classRaw = pickAttr(tag, 'class') || '';
        const classNames = classRaw
          .split(/\s+/)
          .map((s) => s.trim())
          .filter(Boolean);
        const styleFromCss = {};
        for (const cn of classNames) {
          const found = cssClassStyles.get(cn);
          if (!found) continue;
          Object.assign(styleFromCss, found);
        }

        const styleFromAttr = parseStyleAttr(pickAttr(tag, 'style'));
        const attrs = {
          fill: pickAttr(tag, 'fill') ?? styleFromAttr['fill'] ?? styleFromCss['fill'] ?? null,
          fillOpacity: pickAttr(tag, 'fill-opacity') ?? styleFromAttr['fill-opacity'] ?? styleFromCss['fill-opacity'] ?? null,
          stroke: pickAttr(tag, 'stroke') ?? styleFromAttr['stroke'] ?? styleFromCss['stroke'] ?? null,
          strokeOpacity: pickAttr(tag, 'stroke-opacity') ?? styleFromAttr['stroke-opacity'] ?? styleFromCss['stroke-opacity'] ?? null,
          strokeWidth: pickAttr(tag, 'stroke-width') ?? styleFromAttr['stroke-width'] ?? styleFromCss['stroke-width'] ?? null,
          strokeLinecap: pickAttr(tag, 'stroke-linecap') ?? styleFromAttr['stroke-linecap'] ?? styleFromCss['stroke-linecap'] ?? null,
          strokeLinejoin: pickAttr(tag, 'stroke-linejoin') ?? styleFromAttr['stroke-linejoin'] ?? styleFromCss['stroke-linejoin'] ?? null,
          strokeDasharray: pickAttr(tag, 'stroke-dasharray') ?? styleFromAttr['stroke-dasharray'] ?? styleFromCss['stroke-dasharray'] ?? null,
          strokeDashoffset: pickAttr(tag, 'stroke-dashoffset') ?? styleFromAttr['stroke-dashoffset'] ?? styleFromCss['stroke-dashoffset'] ?? null,
          opacity: pickAttr(tag, 'opacity') ?? styleFromAttr['opacity'] ?? styleFromCss['opacity'] ?? null,
        };

        const parts = [`d="${d}"`];
        const safe = (k, v) => {
          if (!v) return;
          const s = String(v).trim();
          if (!s) return;
          if (/url\s*\(/i.test(s)) return;
          parts.push(`${k}="${s}"`);
        };

        safe('fill', attrs.fill);
        safe('fill-opacity', attrs.fillOpacity);
        safe('stroke', attrs.stroke);
        safe('stroke-opacity', attrs.strokeOpacity);
        safe('stroke-width', attrs.strokeWidth);
        safe('stroke-linecap', attrs.strokeLinecap);
        safe('stroke-linejoin', attrs.strokeLinejoin);
        safe('stroke-dasharray', attrs.strokeDasharray);
        safe('stroke-dashoffset', attrs.strokeDashoffset);
        safe('opacity', attrs.opacity);

        return `<path ${parts.join(' ')}/>`;
      })
      .filter((p) => p.length > 0)
      .join('');

    return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="${originalViewBox}">${sanitizedPaths}</svg>`;
  }

  hashSvg(svgContent) {
    // Simple hash for caching
    return crypto.createHash('sha256').update(svgContent).digest('hex');
  }

  // Clear cache
  clear() {
    this.svgCache.clear();
  }
}

// Singleton instance
export const svgRenderer = new SvgRenderer();

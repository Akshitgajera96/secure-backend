// Vector-only layout engine - NO RASTERIZATION
import PDFLib from 'pdf-lib';
import fs from 'fs/promises';
import os from 'os';
import path from 'path';
import { spawn } from 'child_process';
import { A4_WIDTH, A4_HEIGHT, SAFE_MARGIN, snap } from './constants.js';
import { coordinateConverter } from './coordinateUtils.js';
import { fontMetricsCache } from './fontMetrics.js';
import { svgRenderer } from './svgRenderer.js';
import { downloadFromS3 } from '../services/s3.js';
import VectorDocument from '../vectorModels/VectorDocument.js';

const { PDFDocument, rgb, StandardFonts, pushGraphicsState, popGraphicsState, concatTransformationMatrix, degrees, rect, clip, endPath } = PDFLib;

// Color parsing utilities for preserving original colors
const parseColor = (colorValue) => {
  if (!colorValue) return rgb(0, 0, 0); // Default to black if no color specified
  
  // Handle hex colors (#RRGGBB or #RGB)
  if (typeof colorValue === 'string' && colorValue.startsWith('#')) {
    const hex = colorValue.slice(1);
    if (hex.length === 3) {
      // Short hex #RGB -> #RRGGBB
      const r = parseInt(hex[0] + hex[0], 16) / 255;
      const g = parseInt(hex[1] + hex[1], 16) / 255;
      const b = parseInt(hex[2] + hex[2], 16) / 255;
      return rgb(r, g, b);
    } else if (hex.length === 6) {
      const r = parseInt(hex.slice(0, 2), 16) / 255;
      const g = parseInt(hex.slice(2, 4), 16) / 255;
      const b = parseInt(hex.slice(4, 6), 16) / 255;
      return rgb(r, g, b);
    }

  }

  // Handle rgb(r, g, b) format
  if (typeof colorValue === 'string' && colorValue.startsWith('rgb(')) {
    const matches = colorValue.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (matches) {
      const r = parseInt(matches[1]) / 255;
      const g = parseInt(matches[2]) / 255;
      const b = parseInt(matches[3]) / 255;
      return rgb(r, g, b);
    }
  }
  
  // Handle array format [r, g, b] where values are 0-255
  if (Array.isArray(colorValue) && colorValue.length === 3) {
    const r = colorValue[0] / 255;
    const g = colorValue[1] / 255;
    const b = colorValue[2] / 255;
    return rgb(r, g, b);
  }
  
  // Handle object format {r: 0-1, g: 0-1, b: 0-1}
  if (typeof colorValue === 'object' && 
      typeof colorValue.r === 'number' && 
      typeof colorValue.g === 'number' && 
      typeof colorValue.b === 'number') {
    return rgb(colorValue.r, colorValue.g, colorValue.b);
  }
  
  // Default to black
  return rgb(0, 0, 0);
};

const parseNumberList = (raw) => {
  const s = typeof raw === 'string' ? raw.trim() : '';
  if (!s || s.toLowerCase() === 'none') return null;
  const parts = s.split(/[ ,]+/).map((p) => Number(p)).filter((n) => Number.isFinite(n));
  return parts.length ? parts : null;
};

const extractViewBoxFromSvg = (svgContent) => {
  const raw = typeof svgContent === 'string' ? svgContent : '';
  const viewBoxMatch = raw.match(/viewBox\s*=\s*(['"])([^'"]+)\1/i);
  if (!viewBoxMatch) return null;
  const parts = String(viewBoxMatch[2] || '')
    .trim()
    .split(/[ ,]+/)
    .map((v) => Number(v));
  if (parts.length !== 4 || parts.some((v) => !Number.isFinite(v))) return null;
  const [x, y, width, height] = parts;
  if (width <= 0 || height <= 0) return null;
  return { x, y, width, height };
};

const extractWidthHeightFallbackViewBoxFromSvg = (svgContent) => {
  const raw = typeof svgContent === 'string' ? svgContent : '';
  if (!raw) return null;

  const svgOpenMatch = raw.match(/<svg\b[^>]*>/i);
  if (!svgOpenMatch) return null;
  const openTag = svgOpenMatch[0];

  const pick = (name) => {
    const m = openTag.match(new RegExp(`\\b${name}\\s*=\\s*(['"])([^'"]+)\\1`, 'i'));
    return m ? String(m[2] || '').trim() : null;
  };

  const parseLen = (value) => {
    const s = typeof value === 'string' ? value.trim() : '';
    if (!s) return null;
    // Only accept raw numbers or pt. Anything else is not canonical for this pipeline.
    const m = s.match(/^([+-]?(?:\d+\.?\d*|\d*\.?\d+))(pt)?$/i);
    if (!m) return null;
    const n = Number(m[1]);
    if (!Number.isFinite(n) || n <= 0) return null;
    return n;
  };

  const w = parseLen(pick('width'));
  const h = parseLen(pick('height'));
  if (!Number.isFinite(w) || !Number.isFinite(h)) return null;
  return { x: 0, y: 0, width: w, height: h };
};

const injectNonScalingStroke = (svgContent) => {
  const raw = typeof svgContent === 'string' ? svgContent : '';
  if (!raw) return raw;

  // Hard-remove scripts to prevent execution during conversion.
  const withoutScripts = raw.replace(/<script[\s\S]*?<\/script>/gi, '');

  const styleTag = '<style>*{vector-effect:non-scaling-stroke;}</style>';
  if (/<style[\s\S]*vector-effect\s*:\s*non-scaling-stroke[\s\S]*?<\/style>/i.test(withoutScripts)) {
    return withoutScripts;
  }

  const svgOpenIdx = withoutScripts.search(/<svg\b[^>]*>/i);
  if (svgOpenIdx < 0) return withoutScripts;
  const openTagMatch = withoutScripts.match(/<svg\b[^>]*>/i);
  if (!openTagMatch) return withoutScripts;
  const openTag = openTagMatch[0];
  const insertAt = svgOpenIdx + openTag.length;
  return `${withoutScripts.slice(0, insertAt)}${styleTag}${withoutScripts.slice(insertAt)}`;
};

const normalizeSvgToA4 = (rawSvg, placementRules = null) => {
  const raw = String(rawSvg || '');
  const vb = extractViewBoxFromSvg(raw) || extractWidthHeightFallbackViewBoxFromSvg(raw);
  if (!vb || vb.width <= 0 || vb.height <= 0) {
    throw new Error('SVG is missing a valid viewBox');
  }

  const a4W = Number(A4_WIDTH);
  const a4H = Number(A4_HEIGHT);
  if (!Number.isFinite(a4W) || !Number.isFinite(a4H) || a4W <= 0 || a4H <= 0) {
    throw new Error('SVG normalization failed: invalid A4 constants');
  }

  const scale = Math.min(a4W / vb.width, a4H / vb.height);
  let tx = -vb.x * scale + (a4W - vb.width * scale) / 2;
  let ty = -vb.y * scale + (a4H - vb.height * scale) / 2;

  const placement =
    placementRules && typeof placementRules === 'object'
      ? placementRules.seriesPlacement && typeof placementRules.seriesPlacement === 'object'
        ? placementRules.seriesPlacement
        : placementRules
      : null;

  if (placement) {
    const anchorRaw = typeof placement.anchor === 'string' ? placement.anchor.trim().toUpperCase() : '';
    const offsetX = Number(placement?.offset?.x);
    const offsetY = Number(placement?.offset?.y);

    if (anchorRaw && Number.isFinite(offsetX) && Number.isFinite(offsetY)) {
      const contentW = vb.width * scale;
      const contentH = vb.height * scale;

      const curTL = { x: (a4W - contentW) / 2, y: (a4H - contentH) / 2 };
      const curTR = { x: curTL.x + contentW, y: curTL.y };
      const curBL = { x: curTL.x, y: curTL.y + contentH };
      const curBR = { x: curTL.x + contentW, y: curTL.y + contentH };

      const chooseCorner = (name) => {
        if (/_TOP_RIGHT$/.test(name)) return { corner: 'TR', pt: curTR };
        if (/_BOTTOM_LEFT$/.test(name)) return { corner: 'BL', pt: curBL };
        if (/_BOTTOM_RIGHT$/.test(name)) return { corner: 'BR', pt: curBR };
        return { corner: 'TL', pt: curTL };
      };

      const { pt: currentRef } = chooseCorner(anchorRaw);

      let base = null;
      if (anchorRaw.startsWith('PAGE_')) {
        if (/_TOP_RIGHT$/.test(anchorRaw)) base = { x: a4W, y: 0 };
        else if (/_BOTTOM_LEFT$/.test(anchorRaw)) base = { x: 0, y: a4H };
        else if (/_BOTTOM_RIGHT$/.test(anchorRaw)) base = { x: a4W, y: a4H };
        else base = { x: 0, y: 0 };
      } else if (anchorRaw.startsWith('MARGIN_')) {
        if (/_TOP_RIGHT$/.test(anchorRaw)) base = { x: a4W - SAFE_MARGIN, y: SAFE_MARGIN };
        else if (/_BOTTOM_LEFT$/.test(anchorRaw)) base = { x: SAFE_MARGIN, y: a4H - SAFE_MARGIN };
        else if (/_BOTTOM_RIGHT$/.test(anchorRaw)) base = { x: a4W - SAFE_MARGIN, y: a4H - SAFE_MARGIN };
        else base = { x: SAFE_MARGIN, y: SAFE_MARGIN };
      } else if (anchorRaw.startsWith('CONTENT_')) {
        base = currentRef;
      }

      if (base) {
        const desired = { x: base.x + offsetX, y: base.y + offsetY };
        const dx = desired.x - currentRef.x;
        const dy = desired.y - currentRef.y;
        if (Number.isFinite(dx) && Number.isFinite(dy)) {
          tx += dx;
          ty += dy;
        }
      }
    }
  }

  console.log('[BACKEND:SVG_NORMALIZE]', {
    originalViewBox: { x: vb.x, y: vb.y, width: vb.width, height: vb.height, unit: 'viewBox' },
    finalViewBox: { x: 0, y: 0, width: a4W, height: a4H, unit: 'pt' },
    scaleApplied: scale,
    translate: { tx, ty, unit: 'pt' },
  });

  const svgOpenMatch = raw.match(/<svg\b[^>]*>/i);
  if (!svgOpenMatch) return raw;
  const openTag = svgOpenMatch[0];

  // Canonical rewrite of the <svg ...> open tag.
  // Keep unrelated attributes, but force these:
  // - viewBox="0 0 595.28 841.89"
  // - width/height in pt
  // - xmlns present
  const existingXmlnsMatch = openTag.match(/\bxmlns\s*=\s*(['"])([^'"]+)\1/i);
  const xmlns = existingXmlnsMatch ? String(existingXmlnsMatch[2] || '').trim() : 'http://www.w3.org/2000/svg';
  const attrsBody = openTag
    .replace(/^<svg\b/i, '')
    .replace(/>\s*$/i, '')
    .replace(/\bviewBox\s*=\s*(['"])([^'"]*)\1/gi, '')
    .replace(/\bwidth\s*=\s*(['"])([^'"]*)\1/gi, '')
    .replace(/\bheight\s*=\s*(['"])([^'"]*)\1/gi, '')
    .replace(/\bpreserveAspectRatio\s*=\s*(['"])([^'"]*)\1/gi, '')
    .replace(/\bxmlns\s*=\s*(['"])([^'"]*)\1/gi, '')
    .trim();
  const nextOpenTag = `<svg xmlns="${xmlns}" viewBox="0 0 ${a4W} ${a4H}" width="${a4W}pt" height="${a4H}pt"${attrsBody ? ` ${attrsBody}` : ''}>`;

  const openIdx = raw.toLowerCase().indexOf(openTag.toLowerCase());
  const afterOpenIdx = openIdx + openTag.length;
  const closeIdx = raw.toLowerCase().lastIndexOf('</svg>');
  if (openIdx < 0 || closeIdx < 0 || closeIdx <= afterOpenIdx) return raw;

  const inner = raw.slice(afterOpenIdx, closeIdx);
  const wrapped = `<g id="A4_NORMALIZED_ROOT" transform="translate(${tx} ${ty}) scale(${scale})">${inner}</g>`;

  const rewritten = `${raw.slice(0, openIdx)}${nextOpenTag}${wrapped}</svg>`;

  // POST-NORMALIZATION ASSERTION (MUST CRASH ON FAIL)
  const finalVb = extractViewBoxFromSvg(rewritten);
  if (!finalVb || finalVb.width !== a4W || finalVb.height !== a4H) {
    throw new Error(
      `SVG normalization assertion failed: expected viewBox ${a4W}x${a4H}, got ${finalVb ? `${finalVb.width}x${finalVb.height}` : 'none'}`
    );
  }

  return rewritten;
};

let INKSCAPE_AVAILABLE = null;
let INKSCAPE_PROBED = false;
let INKSCAPE_BIN_RESOLVED = null;

const resolveInkscapeBin = () => {
  return process.env.INKSCAPE_PATH || process.env.INKSCAPE_BIN || 'inkscape';
};

export const inkscapeAvailabilityState = () => {
  return INKSCAPE_AVAILABLE;
};

export const probeInkscape = async () => {
  if (INKSCAPE_PROBED) return INKSCAPE_AVAILABLE === true;
  INKSCAPE_PROBED = true;

  const bin = resolveInkscapeBin();
  INKSCAPE_BIN_RESOLVED = bin;

  try {
    await new Promise((resolve, reject) => {
      const p = spawn(bin, ['--version'], { stdio: 'ignore' });
      p.on('error', (err) => reject(err));
      p.on('exit', (code) => {
        if (code === 0) return resolve();
        return reject(new Error(`Inkscape probe failed: ${code}`));
      });
    });
    INKSCAPE_AVAILABLE = true;
    return true;
  } catch {
    INKSCAPE_AVAILABLE = false;
    return false;
  }
};

const inkscapeSvgToPdfBytes = async (svgContent, options = null) => {
  if (!await probeInkscape()) {
    throw new Error('INKSCAPE_UNAVAILABLE: Inkscape is unavailable. SVG→PDF rendering is disabled.');
  }

  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vector-svg-'));
  const inputSvgPath = path.join(tmpDir, 'input.svg');
  const plainSvgPath = path.join(tmpDir, 'plain.svg');
  const outputPdfPath = path.join(tmpDir, 'output.pdf');

  try {
    const documentId = options && typeof options.documentId === 'string' ? options.documentId : null;
    const inputBytes = Buffer.byteLength(String(svgContent || ''), 'utf8');

    await fs.writeFile(inputSvgPath, svgContent, 'utf8');

    if (INKSCAPE_PROBED && INKSCAPE_AVAILABLE === false) {
      throw new Error(
        `INKSCAPE_UNAVAILABLE: Inkscape is unavailable. SVG→PDF rendering is disabled. (resolved bin: "${INKSCAPE_BIN_RESOLVED || resolveInkscapeBin()}")`
      );
    }

    const bin = resolveInkscapeBin();

    const runInkscape = async (args) => {
      await new Promise((resolve, reject) => {
        const p = spawn(bin, args, { stdio: 'ignore' });
        p.on('error', (err) => {
          if (err && err.code === 'ENOENT') {
            INKSCAPE_AVAILABLE = false;
            INKSCAPE_PROBED = true;
            INKSCAPE_BIN_RESOLVED = bin;
            return reject(
              new Error(
                `INKSCAPE_NOT_FOUND: Failed to spawn inkscape binary "${bin}". Install Inkscape and ensure it is in PATH, or set INKSCAPE_PATH (preferred) / INKSCAPE_BIN to the full inkscape executable path.`
              )
            );
          }
          return reject(err);
        });
        p.on('exit', (code) => {
          if (code === 0) {
            INKSCAPE_AVAILABLE = true;
            INKSCAPE_PROBED = true;
            INKSCAPE_BIN_RESOLVED = bin;
            return resolve();
          }
          return reject(new Error(`Inkscape SVG→PDF failed: ${code}`));
        });
      });
    };

    await runInkscape([
      inputSvgPath,
      '--export-type=svg',
      '--export-plain-svg',
      '--export-area-page',
      '--export-dpi=300',
      '--export-text-to-path=false',
      `--export-filename=${plainSvgPath}`,
    ]);

    const t0 = Date.now();
    console.log('[INKSCAPE_START]', { documentId, inputBytes, durationMs: 0, ts: t0 });

    await runInkscape([
      plainSvgPath,
      '--export-type=pdf',
      '--export-area-page',
      '--export-dpi=300',
      '--export-text-to-path=false',
      '--export-plain-svg',
      `--export-filename=${outputPdfPath}`,
    ]);

    const durationMs = Date.now() - t0;
    const pdfBytes = await fs.readFile(outputPdfPath);
    console.log('[INKSCAPE_DONE]', { documentId, inputBytes, outputBytes: pdfBytes.length, durationMs });
    return pdfBytes;
  } finally {
    try {
      await fs.rm(tmpDir, { recursive: true, force: true });
    } catch {
      // ignore
    }
  }
};

export const svgBytesToPdfBytes = async (bytes, options = null) => {
  const raw = Buffer.isBuffer(bytes) ? bytes.toString('utf8') : Buffer.from(bytes).toString('utf8');
  const normalized = normalizeSvgToA4(raw, options?.placementRules || null);
  const injected = injectNonScalingStroke(normalized);
  return inkscapeSvgToPdfBytes(injected, options);
};


export class VectorLayoutEngine {
  constructor(options = {}) {
    this.calibration = options.calibration || { dx: 0, dy: 0 };
    this.embeddedFonts = new Map();
    this._seriesPipelineFinalLogged = false;
    this._alignmentFlowLogged = false;
  }

  mmToPt(mm) {
    const n = Number(mm);
    if (!Number.isFinite(n)) return NaN;
    return (n * 72) / 25.4;
  }

  _finiteOrNull(v) {
    if (v === null || v === undefined) return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }

  _boolOrNull(v) {
    if (v === null || v === undefined) return null;
    if (typeof v === 'boolean') return v;
    return null;
  }

  _stringOrNull(v) {
    if (v === null || v === undefined) return null;
    if (typeof v !== 'string') return null;
    const s = v.trim();
    return s ? s : null;
  }

  _assertFiniteIfPresent(v, name) {
    if (v === null || v === undefined) return;
    const n = Number(v);
    if (!Number.isFinite(n)) {
      throw new Error(`MM_LAYOUT_INVALID: ${name} must be a finite number or null`);
    }
  }

  buildSlotLayoutPlan(repeatPerPage, slotSpacingPt = 0) {
    const slotsPerPage = Math.max(1, Number(repeatPerPage || 1));
    const usableHeight = A4_HEIGHT - 2 * SAFE_MARGIN;

    const rawGap = Number(slotSpacingPt);
    const gap = Number.isFinite(rawGap) ? Math.max(0, rawGap) : 0;

    const totalGaps = gap * Math.max(0, slotsPerPage - 1);
    const heightWithGap = usableHeight - totalGaps;

    const effectiveGap = heightWithGap > 0 ? gap : 0;
    const slotHeight = (usableHeight - effectiveGap * Math.max(0, slotsPerPage - 1)) / slotsPerPage;

    return new Array(slotsPerPage).fill(null).map((_, index) => {
      return {
        index,
        x: SAFE_MARGIN,
        y: SAFE_MARGIN + index * (slotHeight + effectiveGap),
        width: A4_WIDTH - 2 * SAFE_MARGIN,
        height: slotHeight,
      };
    });
  }

  /**
   * GOLDEN_RENDER_PIPELINE
   *
   * This rendering logic is visually locked.
   * Any change in output appearance is a regression.
   *
   * Allowed in Phase-2:
   * - Guards
   * - Hash checks
   * - Tests
   *
   * Forbidden:
   * - Render math changes
   * - Visual changes
   */
  async compileBaseLayout(metadata) {
    const { sourcePdfKey, ticketCrop, layout, watermarks } = metadata;

    if (!ticketCrop || typeof ticketCrop !== 'object') {
      throw new Error('Missing ticketCrop');
    }

    if (!Number.isFinite(ticketCrop.widthRatio)) {
      throw new Error('Missing ticketCrop.widthRatio');
    }

    this.validateMetadata(metadata);

    const t0 = Date.now();
    console.log('[LAYOUT_COMPILE_START]', { sourcePdfKey, totalPages: Number(layout?.totalPages || 1) });

    const sourcePdf = await this.loadSourcePdf(sourcePdfKey);

    const srcPage = sourcePdf.getPage(ticketCrop.pageIndex);
    const srcW = srcPage.getWidth();
    const srcH = srcPage.getHeight();

    const cropX = ticketCrop.xRatio * srcW;
    const cropY = ticketCrop.yRatio * srcH;
    const cropW = ticketCrop.widthRatio * srcW;
    const cropH = ticketCrop.heightRatio * srcH;

    const ticketCropPt = {
      pageIndex: ticketCrop.pageIndex,
      x: cropX,
      y: cropY,
      width: cropW,
      height: cropH,
      xMm: ticketCrop.xMm ?? null,
      yMm: ticketCrop.yMm ?? null,
      widthMm: ticketCrop.widthMm ?? null,
      heightMm: ticketCrop.heightMm ?? null,
      cutMarginMm: ticketCrop.cutMarginMm ?? null,
      rotationDeg: ticketCrop.rotationDeg ?? null,
      keepProportions: ticketCrop.keepProportions ?? null,
      alignment: ticketCrop.alignment ?? null,
    };

    const pdf = await PDFDocument.create();
    this.pdfDoc = pdf;
    this.embeddedFonts.clear();
    this._seriesPipelineFinalLogged = false;
    this._alignmentFlowLogged = false;

    const page = pdf.addPage([A4_WIDTH, A4_HEIGHT]);
    const repeatPerPage = Math.max(1, Math.min(16, Number(layout?.repeatPerPage || 4)));

    const slotPlacements = await this.drawSourceFragment(
      page,
      sourcePdf,
      ticketCropPt,
      0,
      repeatPerPage,
      layout?.slotSpacingPt || 0
    );

    await this.drawSvgWatermarks(page, watermarks.filter((w) => w.type === 'svg'), ticketCropPt, slotPlacements);
    await this.drawTextWatermarks(page, watermarks.filter((w) => w.type === 'text'), ticketCropPt, slotPlacements);

    const ms = Date.now() - t0;
    console.log('[LAYOUT_COMPILE_DONE]', { ms, repeatPerPage, slotCount: Array.isArray(slotPlacements) ? slotPlacements.length : 0 });

    return { pdf, ticketCropPt, slotPlacements, repeatPerPage };
  }

  async createSinglePage(metadata, pageIdx = 0) {
    const { sourcePdfKey, ticketCrop, layout, series, watermarks } = metadata;

    if (!ticketCrop || typeof ticketCrop !== 'object') {
      throw new Error('Missing ticketCrop');
    }

    if (!Number.isFinite(ticketCrop.widthRatio)) {
      throw new Error('Missing ticketCrop.widthRatio');
    }

    this.validateMetadata(metadata);

    const sourcePdf = await this.loadSourcePdf(sourcePdfKey);

    // Convert ratio crop → SOURCE PDF points
    const srcPage = sourcePdf.getPage(ticketCrop.pageIndex);
    const srcW = srcPage.getWidth();
    const srcH = srcPage.getHeight();

    const cropX = ticketCrop.xRatio * srcW;
    const cropY = ticketCrop.yRatio * srcH;
    const cropW = ticketCrop.widthRatio * srcW;
    const cropH = ticketCrop.heightRatio * srcH;

    const ticketCropPt = {
      pageIndex: ticketCrop.pageIndex,
      x: cropX,
      y: cropY,
      width: cropW,
      height: cropH,
      xMm: ticketCrop.xMm ?? null,
      yMm: ticketCrop.yMm ?? null,
      widthMm: ticketCrop.widthMm ?? null,
      heightMm: ticketCrop.heightMm ?? null,
      cutMarginMm: ticketCrop.cutMarginMm ?? null,
      rotationDeg: ticketCrop.rotationDeg ?? null,
      keepProportions: ticketCrop.keepProportions ?? null,
      alignment: ticketCrop.alignment ?? null,
    };

    const pdf = await PDFDocument.create();
    this.pdfDoc = pdf;
    this.embeddedFonts.clear();
    this._seriesPipelineFinalLogged = false;
    this._alignmentFlowLogged = false;

    const page = pdf.addPage([A4_WIDTH, A4_HEIGHT]);

    const repeatPerPage = Math.max(1, Math.min(16, Number(layout?.repeatPerPage || 4)));

    const slotPlacements = await this.drawSourceFragment(page, sourcePdf, ticketCropPt, pageIdx, repeatPerPage, layout.slotSpacingPt || 0);

    await this.drawSvgWatermarks(page, watermarks.filter((w) => w.type === 'svg'), ticketCropPt, slotPlacements);
    await this.drawTextWatermarks(page, watermarks.filter((w) => w.type === 'text'), ticketCropPt, slotPlacements);
    await this.drawSeriesNumbers(page, series, pageIdx, repeatPerPage, slotPlacements);

    return pdf;
  }

  /**
   * GOLDEN_RENDER_PIPELINE
   *
   * This rendering logic is visually locked.
   * Any change in output appearance is a regression.
   *
   * Allowed in Phase-2:
   * - Guards
   * - Hash checks
   * - Tests
   *
   * Forbidden:
   * - Render math changes
   * - Visual changes
   */
  // Create A4 page with vector-only content
  async createPage(metadata) {
    const { sourcePdfKey, ticketCrop, layout, series, watermarks } = metadata;

    if (!ticketCrop || typeof ticketCrop !== 'object') {
      throw new Error('Missing ticketCrop');
    }

    if (!Number.isFinite(ticketCrop.widthRatio)) {
      throw new Error('Missing ticketCrop.widthRatio');
    }

    // Validate A4 bounds
    this.validateMetadata(metadata);
    
    // Load source PDF (vector-safe)
    const sourcePdf = await this.loadSourcePdf(sourcePdfKey);

    // Convert ratio crop → SOURCE PDF points
    const srcPage = sourcePdf.getPage(ticketCrop.pageIndex);
    const srcW = srcPage.getWidth();
    const srcH = srcPage.getHeight();

    const cropX = ticketCrop.xRatio * srcW;
    const cropY = ticketCrop.yRatio * srcH;
    const cropW = ticketCrop.widthRatio * srcW;
    const cropH = ticketCrop.heightRatio * srcH;

    const ticketCropPt = {
      pageIndex: ticketCrop.pageIndex,
      x: cropX,
      y: cropY,
      width: cropW,
      height: cropH,
      xMm: ticketCrop.xMm ?? null,
      yMm: ticketCrop.yMm ?? null,
      widthMm: ticketCrop.widthMm ?? null,
      heightMm: ticketCrop.heightMm ?? null,
      cutMarginMm: ticketCrop.cutMarginMm ?? null,
      rotationDeg: ticketCrop.rotationDeg ?? null,
      keepProportions: ticketCrop.keepProportions ?? null,
      alignment: ticketCrop.alignment ?? null,
    };
    
    // Create new A4 PDF
    const pdf = await PDFDocument.create();
    this.pdfDoc = pdf;
    this.embeddedFonts.clear();
    this._seriesPipelineFinalLogged = false;
    this._alignmentFlowLogged = false;
    
    // Process each page
    const totalPages = Number(layout.totalPages || 1);

    const repeatPerPage = Math.max(1, Math.min(16, Number(layout?.repeatPerPage || 4)));
    
    for (let pageIdx = 0; pageIdx < totalPages; pageIdx++) {
      const page = pdf.addPage([A4_WIDTH, A4_HEIGHT]);
      
      // Draw order: Source PDF → SVG watermarks → Text watermarks → Series numbers
      const slotPlacements = await this.drawSourceFragment(page, sourcePdf, ticketCropPt, pageIdx, repeatPerPage, layout.slotSpacingPt || 0);

      await this.drawSvgWatermarks(page, watermarks.filter(w => w.type === 'svg'), ticketCropPt, slotPlacements);
      await this.drawTextWatermarks(page, watermarks.filter(w => w.type === 'text'), ticketCropPt, slotPlacements);
      await this.drawSeriesNumbers(page, series, pageIdx, repeatPerPage, slotPlacements);
    }
    
    return pdf;
  }

  async loadSourcePdf(sourcePdfKey) {
    const normalized = typeof sourcePdfKey === 'string' ? sourcePdfKey : '';
    const keyOrRef = normalized.startsWith('s3://') ? normalized.slice('s3://'.length) : normalized;

    if (keyOrRef.startsWith('document:')) {
      const documentId = keyOrRef.slice('document:'.length);
      const doc = await VectorDocument.findById(documentId).catch(() => null);
      if (!doc || !doc.fileKey) {
        throw new Error('Invalid document reference for sourcePdfKey');
      }
      const bytes = await downloadFromS3(doc.fileKey);
      const header = Buffer.from(bytes.slice(0, 5)).toString();
      if (!header.startsWith('%PDF-')) {
        throw new Error(
          'Invariant violation: vector print pipeline received non-PDF source. SVG must be normalized to PDF via svgNormalizeWorker before printing.'
        );
      }
      return PDFDocument.load(bytes);
    }

    const bytes = await downloadFromS3(keyOrRef);
    const header = Buffer.from(bytes.slice(0, 5)).toString();
    if (!header.startsWith('%PDF-')) {
      throw new Error(
        'Invariant violation: vector print pipeline received non-PDF source. SVG must be normalized to PDF via svgNormalizeWorker before printing.'
      );
    }
    return PDFDocument.load(bytes);
  }

  async drawSourceFragment(page, sourcePdf, ticketCrop, _pageIdx, repeatPerPage, slotSpacingPt = 0) {
    // 1) Original PDF page is copied into the output PDF
    const [srcPage] = await this.pdfDoc.copyPages(sourcePdf, [ticketCrop.pageIndex]);

    const srcWidth = snap(srcPage.getWidth());
    const srcHeight = snap(srcPage.getHeight());

    const cropLeft = snap(ticketCrop.x);
    const cropWidth = snap(ticketCrop.width);
    const cropHeight = snap(ticketCrop.height);
    const cropBottom = snap(srcHeight - ticketCrop.y - ticketCrop.height);

    // Embed ONLY the cropped region of the source page (vector-safe)
    // This avoids overwriting other slots when the source page has background artwork.
    const clamp = (n, min, max) => Math.min(max, Math.max(min, n));

    let left = Number(cropLeft);
    let bottom = Number(cropBottom);
    let right = Number(snap(cropLeft + cropWidth));
    let top = Number(snap(cropBottom + cropHeight));

    if (!Number.isFinite(left)) left = 0;
    if (!Number.isFinite(bottom)) bottom = 0;
    if (!Number.isFinite(right)) right = srcWidth;
    if (!Number.isFinite(top)) top = srcHeight;

    left = clamp(left, 0, srcWidth);
    right = clamp(right, 0, srcWidth);
    bottom = clamp(bottom, 0, srcHeight);
    top = clamp(top, 0, srcHeight);

    // If crop collapses/out-of-bounds (can happen due to rounding/unit mismatch), fall back to full page.
    const cropValid = right - left > 0.5 && top - bottom > 0.5;
    if (!cropValid) {
      left = 0;
      bottom = 0;
      right = srcWidth;
      top = srcHeight;
    }

    const embedBox = { left: snap(left), bottom: snap(bottom), right: snap(right), top: snap(top) };

    const embedded = await this.pdfDoc.embedPage(srcPage, embedBox);

    const renderBBox = {
      x: 0,
      y: 0,
      width: snap(right - left),
      height: snap(top - bottom),
    };

    const safeNumber = (v, fallback) => (Number.isFinite(Number(v)) ? Number(v) : fallback);
    const clampPage = (v, min, max) => Math.min(max, Math.max(min, v));
    const safeOrigin = (origin) => {
      const x = safeNumber(origin?.x, 0);
      const y = safeNumber(origin?.y, 0);
      return {
        x: snap(clampPage(x, -A4_WIDTH * 2, A4_WIDTH * 2)),
        y: snap(clampPage(y, -A4_HEIGHT * 2, A4_HEIGHT * 2)),
      };
    };
    const safeScale = (s) => {
      const n = safeNumber(s, 1);
      // Prevent invisible draw due to near-zero scale; cap huge scales too.
      return snap(clampPage(n, 0.0005, 1000));
    };

    const widthMm = this._finiteOrNull(ticketCrop?.widthMm);
    const heightMm = this._finiteOrNull(ticketCrop?.heightMm);
    const xMm = this._finiteOrNull(ticketCrop?.xMm);
    const yMm = this._finiteOrNull(ticketCrop?.yMm);
    const cutMarginMm = this._finiteOrNull(ticketCrop?.cutMarginMm);
    const rotationDeg = this._finiteOrNull(ticketCrop?.rotationDeg);
    const keepProportionsRaw = this._boolOrNull(ticketCrop?.keepProportions);
    const receivedAlignment = ticketCrop?.alignment;
    const alignment =
      receivedAlignment === 'left' || receivedAlignment === 'center' || receivedAlignment === 'right'
        ? receivedAlignment
        : 'left';

    const isUserMmMode =
      ticketCrop &&
      typeof ticketCrop.widthMm === 'number' &&
      typeof ticketCrop.heightMm === 'number' &&
      ticketCrop.widthMm > 0 &&
      ticketCrop.heightMm > 0;

    if (isUserMmMode) {
      this._assertFiniteIfPresent(ticketCrop?.widthMm, 'ticketCrop.widthMm');
      this._assertFiniteIfPresent(ticketCrop?.heightMm, 'ticketCrop.heightMm');
      this._assertFiniteIfPresent(ticketCrop?.xMm, 'ticketCrop.xMm');
      this._assertFiniteIfPresent(ticketCrop?.yMm, 'ticketCrop.yMm');
      this._assertFiniteIfPresent(ticketCrop?.cutMarginMm, 'ticketCrop.cutMarginMm');
      this._assertFiniteIfPresent(ticketCrop?.rotationDeg, 'ticketCrop.rotationDeg');

      if (xMm !== null && xMm < 0) {
        throw new Error('MM_LAYOUT_INVALID: xMm must be >= 0');
      }
      if (yMm !== null && yMm < 0) {
        throw new Error('MM_LAYOUT_INVALID: yMm must be >= 0');
      }
      if (cutMarginMm !== null && cutMarginMm < 0) {
        throw new Error('MM_LAYOUT_INVALID: cutMarginMm must be >= 0');
      }
    }

    const slotPlacements = [];

    const slotsPerPage = Math.max(1, Math.min(16, Number(repeatPerPage || 1)));

    if (isUserMmMode) {
      const objectWidthMm = Number(widthMm);
      const objectHeightMm = Number(heightMm);
      const startXmm = xMm !== null ? Number(xMm) : 0;
      const startYmm = yMm !== null ? Number(yMm) : 0;
      const cutGapMm = cutMarginMm !== null ? Number(cutMarginMm) : 0;

      // FINAL object size in points (never altered)
      const objectWidthPt = snap(this.mmToPt(widthMm));
      const objectHeightPt = snap(this.mmToPt(heightMm));

      const sourceWidthPt = snap(Number(renderBBox.width));
      const sourceHeightPt = snap(Number(renderBBox.height));

      // SCALING LOGIC (FINAL)
      let scaleX = snap(objectWidthPt / Math.max(0.0001, sourceWidthPt));
      let scaleY = snap(objectHeightPt / Math.max(0.0001, sourceHeightPt));
      const initialScaleX = scaleX;
      const initialScaleY = scaleY;

      // Apply Keep Proportions if requested
      if (keepProportionsRaw === true) {
        const uniformScale = snap(Math.min(scaleX, scaleY));
        scaleX = scaleY = uniformScale;
        console.log('[KEEP_PROPORTIONS]', {
          initialScaleX,
          initialScaleY,
          uniformScale,
        });
      }

      const effectiveWidthPt = snap(sourceWidthPt * scaleX);
      const effectiveHeightPt = snap(sourceHeightPt * scaleY);
      console.log('[MM_PRINT_ASSERT]', {
        requestedWidthPt: objectWidthPt,
        requestedHeightPt: objectHeightPt,
        finalWidthPt: effectiveWidthPt,
        finalHeightPt: effectiveHeightPt,
        diffWidthPt: Math.abs(effectiveWidthPt - objectWidthPt),
        diffHeightPt: Math.abs(effectiveHeightPt - objectHeightPt),
        keepProportions: keepProportionsRaw === true,
      });

      // cutMarginMm = gap between consecutive objects
      // NOT page margin, NOT object padding, NOT slot spacing
      const verticalStepMm = objectHeightMm + cutGapMm;

      const A4_HEIGHT_MM = (A4_HEIGHT * 25.4) / 72;
      const lastObjectBottomMm = startYmm + (slotsPerPage - 1) * verticalStepMm + objectHeightMm;
      if (lastObjectBottomMm > A4_HEIGHT_MM) {
        throw new Error('Objects do not fit on page');
      }

      console.log('[OBJECT_REPEAT_FIX]', {
        objectHeightMm,
        cutMarginMm: cutGapMm,
        verticalStepMm,
        objectsPerPage: slotsPerPage,
        lastObjectBottomMm,
      });

      for (let i = 0; i < slotsPerPage; i += 1) {
        const objXmm = startXmm;
        const objYmm = startYmm + i * verticalStepMm;

        const slotScaleX = scaleX;
        const slotScaleY = scaleY;

        const finalWidthPt = effectiveWidthPt;
        const finalHeightPt = effectiveHeightPt;

        const pageWidthPt = A4_WIDTH;
        const cutMarginPt = this.mmToPt(cutGapMm);

        let x = snap(this.mmToPt(objXmm));
        if (xMm === null) {
          if (alignment === 'right') {
            x = pageWidthPt - finalWidthPt - (cutMarginPt || 0);
          } else if (alignment === 'center') {
            x = (pageWidthPt - finalWidthPt) / 2;
          } else {
            x = (cutMarginPt || 0); // left
          }
        }

        if (!this._alignmentFlowLogged) {
          console.log('[ALIGNMENT_FLOW]', {
            received: receivedAlignment,
            applied: alignment,
            xPos: x,
          });
          this._alignmentFlowLogged = true;
        }

        const drawX = snap(x);
        const drawY = snap(A4_HEIGHT - this.mmToPt(objYmm) - finalHeightPt);

        const calibratedOrigin = safeOrigin(coordinateConverter.applyCalibration(drawX, drawY, this.calibration));

        const rot = Number.isFinite(rotationDeg) ? Number(rotationDeg) : 0;
        if (rot) {
          const theta = (rot * Math.PI) / 180;
          const cos = Math.cos(theta);
          const sin = Math.sin(theta);

          const a = snap(cos * slotScaleX);
          const b = snap(sin * slotScaleX);
          const c = snap(-sin * slotScaleY);
          const d = snap(cos * slotScaleY);

          const centerX = snap(Number(calibratedOrigin.x) + finalWidthPt / 2);
          const centerY = snap(Number(calibratedOrigin.y) + finalHeightPt / 2);

          const e = snap(centerX + a * (-Number(renderBBox.width) / 2) + c * (-Number(renderBBox.height) / 2));
          const f = snap(centerY + b * (-Number(renderBBox.width) / 2) + d * (-Number(renderBBox.height) / 2));

          page.pushOperators(pushGraphicsState());
          page.pushOperators(concatTransformationMatrix(a, b, c, d, e, f));
          page.drawPage(embedded, {
            x: 0,
            y: 0,
            xScale: 1,
            yScale: 1,
          });
          page.pushOperators(popGraphicsState());
        } else {
          page.drawPage(embedded, {
            x: snap(calibratedOrigin.x),
            y: snap(calibratedOrigin.y),
            xScale: safeScale(slotScaleX),
            yScale: safeScale(slotScaleY),
          });
        }

        slotPlacements.push({
          index: i,
          slotLeft: drawX,
          slotBottom: drawY,
          slotWidth: objectWidthPt,
          slotHeight: objectHeightPt,
          contentLeft: drawX,
          contentBottom: drawY,
          contentWidth: objectWidthPt,
          contentHeight: objectHeightPt,
          objectBBoxPt: {
            width: snap(Number(renderBBox.width)),
            height: snap(Number(renderBBox.height)),
          },
          slotScale: scaleY,
          slotScaleX: scaleX,
          slotScaleY: scaleY,
        });
      }

      return slotPlacements;
    }

    const slotLayoutPlan = this.buildSlotLayoutPlan(slotsPerPage, slotSpacingPt);

    for (const layoutSlot of slotLayoutPlan) {
      const slotScale = snap(Math.min(
        Number(layoutSlot.width) / Math.max(0.0001, Number(renderBBox.width)),
        Number(layoutSlot.height) / Math.max(0.0001, Number(renderBBox.height))
      ));

      if (_pageIdx === 0) {
        console.log('[BACKEND:SLOT_LAYOUT]', {
          slotIndex: layoutSlot.index,
          slotBoxPt: {
            left: Number(layoutSlot.x),
            bottom: Number(layoutSlot.y),
            width: Number(layoutSlot.width),
            height: Number(layoutSlot.height),
          },
          slotScale,
          unit: 'pt',
        });
      }

      const objectWidthPt = snap(Number(renderBBox.width) * slotScale);
      const slotWidthPt = snap(Number(layoutSlot.width));
      const slotLeftPt = snap(Number(layoutSlot.x));
      const cutMarginPt = this.mmToPt(cutMarginMm !== null ? cutMarginMm : 0);

      let x = snap(Number(layoutSlot.x) - Number(renderBBox.x) * slotScale);
      if (!(objectWidthPt > 0) || !(slotWidthPt > 0) || objectWidthPt >= slotWidthPt) {
        x = slotLeftPt + (cutMarginPt || 0);
      } else if (alignment === 'right') {
        x = slotLeftPt + slotWidthPt - objectWidthPt - (cutMarginPt || 0);
      } else if (alignment === 'center') {
        x = slotLeftPt + (slotWidthPt - objectWidthPt) / 2;
      } else {
        x = slotLeftPt + (cutMarginPt || 0); // left
      }

      if (!this._alignmentFlowLogged) {
        console.log('[ALIGNMENT_FLOW]', {
          received: receivedAlignment,
          applied: alignment,
          xPos: x,
        });
        this._alignmentFlowLogged = true;
      }

      const drawX = snap(x);
      const drawY = snap(
        Number(layoutSlot.y) +
          (Number(layoutSlot.height) - Number(renderBBox.height) * slotScale) -
          Number(renderBBox.y) * slotScale
      );

      const calibratedOrigin = coordinateConverter.applyCalibration(drawX, drawY, this.calibration);
      page.drawPage(embedded, {
        x: snap(safeOrigin(calibratedOrigin).x),
        y: snap(safeOrigin(calibratedOrigin).y),
        xScale: safeScale(slotScale),
        yScale: safeScale(slotScale),
      });

      if (_pageIdx === 0) {
        console.log('[BACKEND:OBJECT_RENDERED_IN_SLOT]', {
          slotIndex: layoutSlot.index,
          objectBBoxPt: {
            width: snap(Number(renderBBox.width)),
            height: snap(Number(renderBBox.height)),
          },
          note: 'This is the REAL object size used for series math',
        });
      }

      slotPlacements.push({
        index: layoutSlot.index,
        slotLeft: layoutSlot.x,
        slotBottom: layoutSlot.y,
        slotWidth: layoutSlot.width,
        slotHeight: layoutSlot.height,
        contentLeft: drawX,
        contentBottom: drawY,
        contentWidth: snap(Number(renderBBox.width) * slotScale),
        contentHeight: snap(Number(renderBBox.height) * slotScale),
        objectBBoxPt: {
          width: snap(Number(renderBBox.width)),
          height: snap(Number(renderBBox.height)),
        },
        slotScale,
        slotScaleX: slotScale,
        slotScaleY: slotScale,
      });
    }

    return slotPlacements;
  }

  async drawSvgWatermarks(page, svgWatermarks, ticketCrop, slotPlacements) {
    for (const watermark of svgWatermarks) {
      const sanitized = svgRenderer.sanitizeSvg(watermark.svgPath);
      const parsed = svgRenderer.parseSvg(sanitized);

      const relativeToObject = watermark?.relativeTo === 'object';
      const targets = relativeToObject
        ? (Array.isArray(slotPlacements) ? slotPlacements : [])
        : [null];

      for (const placement of targets) {
        const baseX = Number(placement?.contentLeft ?? placement?.slotLeft ?? 0);
        const baseY = Number(placement?.contentBottom ?? placement?.slotBottom ?? 0);
        const baseW = Number(placement?.contentWidth ?? placement?.slotWidth ?? 0);
        const baseH = Number(placement?.contentHeight ?? placement?.slotHeight ?? 0);

        const posX = relativeToObject
          ? baseX + (Number(watermark.position.x) * baseW)
          : Number(watermark.position.x);
        const posY = relativeToObject
          ? baseY + ((1 - Number(watermark.position.y)) * baseH)
          : Number(watermark.position.y);

        const calibrated = coordinateConverter.applyCalibration(posX, posY, this.calibration);

        const rotation = (watermark.rotate || 0) * (Math.PI / 180);
        const s = (watermark.scale || 1);
        const a = snap(Math.cos(rotation) * s);
        const b = snap(Math.sin(rotation) * s);
        const c = snap(-Math.sin(rotation) * s);
        const d = snap(Math.cos(rotation) * s);
        const e = snap(calibrated.x);
        const f = snap(calibrated.y);

        const viewBoxX = snap(parsed?.viewBox?.x || 0);
        const viewBoxY = snap(parsed?.viewBox?.y || 0);

        page.pushOperators(pushGraphicsState());
        page.pushOperators(concatTransformationMatrix(a, b, c, d, e, f));
        const entries = Array.isArray(parsed?.pathEntries) && parsed.pathEntries.length
          ? parsed.pathEntries
          : (Array.isArray(parsed?.paths) ? parsed.paths.map((d) => ({ d, attrs: {} })) : []);
        for (const entry of entries) {
          const path = entry?.d;
          if (!path) continue;

          const attrs = entry?.attrs || {};
          const fill = parseColor(attrs.fill);
          const stroke = parseColor(attrs.stroke);
          const opacity = Number.isFinite(Number(attrs.opacity)) ? Number(attrs.opacity) : (watermark.opacity || 1);
          const fillOpacity = Number.isFinite(Number(attrs.fillOpacity)) ? Number(attrs.fillOpacity) : undefined;
          const strokeOpacity = Number.isFinite(Number(attrs.strokeOpacity)) ? Number(attrs.strokeOpacity) : undefined;

          const swRaw = Number(attrs.strokeWidth);
          const strokeWidth = Number.isFinite(swRaw) && swRaw > 0 ? swRaw : undefined;
          const dash = parseNumberList(attrs.strokeDasharray);
          const dashOffsetRaw = Number(attrs.strokeDashoffset);
          const dashOffset = Number.isFinite(dashOffsetRaw) ? dashOffsetRaw : undefined;

          const options = {
            x: snap(-viewBoxX),
            y: snap(-viewBoxY),
            scale: 1,
            rotate: degrees(0),
            opacity,
          };

          if (fill) {
            options.color = fill;
            if (Number.isFinite(fillOpacity)) {
              options.opacity = Number.isFinite(opacity) ? Math.min(opacity, fillOpacity) : fillOpacity;
            }
          }
          if (stroke) {
            options.borderColor = stroke;
            if (Number.isFinite(strokeOpacity)) {
              options.borderOpacity = Number.isFinite(opacity) ? Math.min(opacity, strokeOpacity) : strokeOpacity;
            }
            if (strokeWidth) {
              options.borderWidth = strokeWidth;
            }
            if (dash) {
              options.borderDashArray = dash;
            }
            if (Number.isFinite(dashOffset)) {
              options.borderDashPhase = dashOffset;
            }
            if (typeof attrs.strokeLinecap === 'string') {
              const lc = attrs.strokeLinecap.toLowerCase();
              options.borderLineCap = lc === 'round' ? 'Round' : lc === 'square' ? 'ProjectingSquare' : 'Butt';
            }
          }

          page.drawSvgPath(path, options);
        }
        page.pushOperators(popGraphicsState());
      }
    }
  }

  async drawTextWatermarks(page, textWatermarks, ticketCrop, slotPlacements) {
    for (const watermark of textWatermarks) {
      const font = await this.embedFont(watermark.fontFamily || 'Helvetica');

      const relativeToObject = watermark?.relativeTo === 'object';
      const targets = relativeToObject
        ? (Array.isArray(slotPlacements) ? slotPlacements : [])
        : [null];

      for (const placement of targets) {
        const baseX = Number(placement?.contentLeft ?? placement?.slotLeft ?? 0);
        const baseY = Number(placement?.contentBottom ?? placement?.slotBottom ?? 0);
        const baseW = Number(placement?.contentWidth ?? placement?.slotWidth ?? 0);
        const baseH = Number(placement?.contentHeight ?? placement?.slotHeight ?? 0);

        const posX = relativeToObject ? baseX + (Number(watermark.position.x) * baseW) : Number(watermark.position.x);
        const posY = relativeToObject ? baseY + ((1 - Number(watermark.position.y)) * baseH) : Number(watermark.position.y);

        const fontSize = (watermark.fontSize || 12);

        // Baseline-aware positioning
        const baselineY = fontMetricsCache.visualToBaseline(
          posY,
          watermark.fontFamily || 'Arial',
          fontSize,
          font
        );

        // Apply calibration and snap
        const calibrated = coordinateConverter.applyCalibration(
          posX, baselineY, this.calibration
        );

        // Draw text as vector with original color preservation
        const originalColor = watermark.color || '#000000'; // Default to black if not specified
        page.drawText(watermark.value, {
          x: snap(calibrated.x),
          y: snap(calibrated.y),
          size: fontSize,
          font,
          color: parseColor(originalColor),
          opacity: watermark.opacity || 1 // Preserve original opacity
        });
      }
    }
  }

  async drawSeriesNumbers(page, series, pageIdx, repeatPerPage, slotPlacements) {
    for (const seriesConfig of series) {
      const font = await this.embedFont(seriesConfig.font || 'Helvetica');
      
      const placements = Array.isArray(slotPlacements) ? slotPlacements : [];
      const slotDefs = Array.isArray(seriesConfig.slots) ? seriesConfig.slots : [];
      const maxSlots = Math.min(placements.length, Number(repeatPerPage) || placements.length);

      for (let slotIdx = 0; slotIdx < maxSlots; slotIdx++) {
        const slot = slotDefs.length === 1 ? slotDefs[0] : slotDefs[slotIdx];
        const placement = placements[slotIdx];

        if (!slot) continue;
        
        // O(1) arithmetic progression
        const globalIdx = pageIdx * repeatPerPage + slotIdx;
        const seriesNumber = seriesConfig.start + (globalIdx * seriesConfig.step);
        const padLength = Number(seriesConfig.padLength || 0);
        const prefix = typeof seriesConfig.prefix === 'string' ? seriesConfig.prefix : '';
        const seriesValue = padLength > 0
          ? `${prefix}${String(seriesNumber).padStart(padLength, '0')}`
          : `${prefix}${String(seriesNumber)}`;
        
        const xRatio = Number(slot.xRatio);
        const yRatio = Number(slot.yRatio);

        const objectBBoxPt = {
          width: Number(placement?.objectBBoxPt?.width ?? 0),
          height: Number(placement?.objectBBoxPt?.height ?? 0),
        };
        if (!Number.isFinite(objectBBoxPt.width) || !Number.isFinite(objectBBoxPt.height) || objectBBoxPt.width <= 0 || objectBBoxPt.height <= 0) {
          throw new Error('Invalid objectBBoxPt for series placement');
        }

        const textTopLeftPt = {
          x: xRatio * objectBBoxPt.width,
          y: yRatio * objectBBoxPt.height,
        };

        const fontSizePt = Number(seriesConfig.fontSize);
        const slotScaleX = Number(placement?.slotScaleX ?? placement?.slotScale ?? 0);
        const slotScaleY = Number(placement?.slotScaleY ?? placement?.slotScale ?? 0);
        if (!Number.isFinite(slotScaleX) || slotScaleX <= 0) {
          throw new Error('Invalid slotScaleX for series placement');
        }
        if (!Number.isFinite(slotScaleY) || slotScaleY <= 0) {
          throw new Error('Invalid slotScaleY for series placement');
        }
        const finalFontSizePt = fontSizePt * slotScaleY;

        const metrics = fontMetricsCache.getMetrics(
          seriesConfig.font || 'Helvetica',
          fontSizePt,
          font
        );

        const baselineY = textTopLeftPt.y + metrics.ascent;

        const objectLeft = Number(placement?.contentLeft ?? placement?.slotLeft ?? 0);
        const objectBottom = Number(placement?.contentBottom ?? placement?.slotBottom ?? 0);

        const objectTopY = objectBottom + (objectBBoxPt.height * slotScaleY);
        const seriesPageX = objectLeft + (textTopLeftPt.x * slotScaleX);
        const seriesPageY = objectTopY - (baselineY * slotScaleY);

        const calibratedSeries = coordinateConverter.applyCalibration(seriesPageX, seriesPageY, this.calibration);
        const drawX = calibratedSeries.x;
        const drawY = calibratedSeries.y;

        if (!this._seriesPipelineFinalLogged && slotIdx === 0) {
          console.log('[SERIES_PIPELINE_FINAL]', {
            receivedRatio: { xRatio, yRatio },
            objectBBoxPt,
            textTopLeftPt,
            fontMetrics: {
              ascent: metrics.ascent,
              descent: metrics.descent,
              height: metrics.height,
            },
            baselineY,
            slotScaleX,
            slotScaleY,
            finalDrawPt: { x: drawX, y: drawY },
          });
          this._seriesPipelineFinalLogged = true;
        }
        
        const letterFontSizes = Array.isArray(seriesConfig.letterFontSizes)
          ? seriesConfig.letterFontSizes
          : null;
        const letterOffsets = Array.isArray(seriesConfig.letterOffsets)
          ? seriesConfig.letterOffsets
          : null;

        if (letterFontSizes && letterFontSizes.length > 0) {
          let cursorX = drawX;
          for (let li = 0; li < seriesValue.length; li += 1) {
            const ch = seriesValue[li];
            const size = Number(letterFontSizes[li] || fontSizePt) * slotScaleY;
            const offsetY = Number(letterOffsets?.[li] || 0);

            const baseline = drawY + offsetY;

            // Preserve original series color
            const seriesColor = parseColor(seriesConfig.color || '#000000'); // Default to black if not specified
            page.drawText(ch, {
              x: cursorX,
              y: baseline,
              size,
              font,
              color: seriesColor,
            });

            cursorX = cursorX + font.widthOfTextAtSize(ch, size);
          }
        } else {
          // Draw series number as vector with original color preservation
          const seriesColor = parseColor(seriesConfig.color || '#000000'); // Default to black if not specified
          page.drawText(seriesValue, {
            x: drawX,
            y: drawY,
            size: finalFontSizePt,
            font,
            color: seriesColor
          });
        }
      }
    }
  }

  async embedFont(fontFamily) {
    if (!this.embeddedFonts.has(fontFamily)) {
      const font = await this.loadFont(fontFamily);
      this.embeddedFonts.set(fontFamily, font);
    }
    
    return this.embeddedFonts.get(fontFamily);
  }

  async loadFont(fontFamily) {
    if (!this.pdfDoc) {
      throw new Error('PDFDocument not initialized');
    }

    const name = (fontFamily || '').toLowerCase();
    if (name.includes('times')) return this.pdfDoc.embedFont(StandardFonts.TimesRoman);
    if (name.includes('courier')) return this.pdfDoc.embedFont(StandardFonts.Courier);
    return this.pdfDoc.embedFont(StandardFonts.Helvetica);
  }

  validateMetadata(metadata) {
    const { ticketCrop, layout, series, watermarks } = metadata;

    // ticketCrop is defined in SOURCE PDF coordinate space.
    // It must NOT be validated against A4 bounds.
    // A4 constraints are enforced only during layout in drawSourceFragment().
    
    // Validate series slots are now object-relative (no page bounds validation needed)
    // Series slots should be relative to object bbox, not page coordinates
    for (const s of series) {
      for (const slot of s.slots) {
        if (typeof slot.xRatio !== 'number' || typeof slot.yRatio !== 'number') {
          throw new Error('Series slot xRatio and yRatio must be numbers');
        }
        // Object-relative coordinates can be positive or negative
        // No A4 bounds validation for object-relative series slots
      }
    }
  }
}

// Singleton instance
export const vectorLayoutEngine = new VectorLayoutEngine();

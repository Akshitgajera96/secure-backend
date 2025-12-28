// Vector pipeline end-to-end test
import { validateVectorMetadata } from './src/vector/validation.js';
import { coordinateConverter } from './src/vector/coordinateUtils.js';
import { A4_WIDTH, A4_HEIGHT } from './src/vector/constants.js';
import { svgRenderer } from './src/vector/svgRenderer.js';

const MAX_DRIFT_PT = 0.001;

const canonicalCanvasToPdf = (canvasX, canvasY, canvasWidth = 0, canvasHeight = 0, scale = 1.0) => {
  return {
    pdfX: canvasX / scale,
    pdfY: A4_HEIGHT - (canvasY + canvasHeight) / scale,
  };
};

// Test vector metadata validation
const testMetadata = {
  sourcePdfKey: "s3://documents/original/test.pdf",
  ticketCrop: {
    pageIndex: 0,
    x: 50,
    y: 50,
    width: 200,
    height: 100
  },
  layout: {
    pageSize: "A4",
    repeatPerPage: 4,
    totalPages: 2
  },
  series: [{
    id: "series-1",
    prefix: "A",
    start: 1,
    step: 1,
    font: "Arial",
    fontSize: 24,
    slots: [
      { x: 60, y: 780 },
      { x: 60, y: 570 },
      { x: 60, y: 360 },
      { x: 60, y: 150 }
    ]
  }],
  watermarks: [{
    id: "watermark-1",
    type: "text",
    value: "CONFIDENTIAL",
    opacity: 0.3,
    rotate: 45,
    position: { x: 400, y: 100 }
  }]
};

console.log('🧪 Testing Vector Pipeline...');

// Test 1: Validation
console.log('\n1️⃣ Testing validation...');
const validation = validateVectorMetadata(testMetadata);
if (validation.isValid) {
  console.log('✅ Validation passed');
} else {
  console.log('❌ Validation failed:', validation.errors);
}

// Test 2: Coordinate conversion
console.log('\n2️⃣ Testing coordinate conversion...');
const canvasCoords = { x: 100, y: 200, width: 50, height: 30 };
const pdfCoords = coordinateConverter.canvasToPdf(
  canvasCoords.x, 
  canvasCoords.y, 
  canvasCoords.width, 
  canvasCoords.height
);

const expectedCoords = canonicalCanvasToPdf(
  canvasCoords.x,
  canvasCoords.y,
  canvasCoords.width,
  canvasCoords.height,
  1.0
);

const driftX = Math.abs(pdfCoords.pdfX - expectedCoords.pdfX);
const driftY = Math.abs(pdfCoords.pdfY - expectedCoords.pdfY);
const maxDrift = Math.max(driftX, driftY);

console.log('Canvas → PDF:', canvasCoords, '→', pdfCoords);
console.log('Expected → PDF:', expectedCoords, 'max drift:', maxDrift);

if (maxDrift > MAX_DRIFT_PT) {
  console.error(`❌ Parity drift exceeds ${MAX_DRIFT_PT}pt:`, { driftX, driftY });
  process.exit(1);
} else {
  console.log(`✅ Parity drift within ${MAX_DRIFT_PT}pt`);
}

// Test 3: A4 bounds validation
console.log('\n3️⃣ Testing A4 bounds...');
const withinBounds = pdfCoords.pdfX >= 0 && 
                      pdfCoords.pdfX <= A4_WIDTH &&
                      pdfCoords.pdfY >= 0 && 
                      pdfCoords.pdfY <= A4_HEIGHT;

console.log('Within A4 bounds:', withinBounds ? '✅' : '❌');

// Test 4: Snapping
console.log('\n4️⃣ Testing coordinate snapping...');
const snapped = coordinateConverter.snap(123.456789);
console.log('Snapped 123.456789 →', snapped);

// Test 5: SVG sanitization & determinism
console.log('\n5️⃣ Testing SVG sanitization & determinism...');

const okSvg = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="10 20 100 100"><path d="M10 10 L20 20"/></svg>';
const sanitizedOk = svgRenderer.sanitizeSvg(okSvg);
const parsedOk = svgRenderer.parseSvg(sanitizedOk);
console.log('Sanitized SVG (ok) length:', sanitizedOk.length);
console.log('Parsed viewBox:', parsedOk.viewBox);

if (!parsedOk?.viewBox || parsedOk.viewBox.x !== 10 || parsedOk.viewBox.y !== 20) {
  console.error('❌ viewBox preservation/parsing failed');
  process.exit(1);
} else {
  console.log('✅ viewBox preserved & parsed');
}

try {
  svgRenderer.sanitizeSvg('<svg viewBox="0 0 10 10"><image href="x"/></svg>');
  console.error('❌ Forbidden <image> was not rejected');
  process.exit(1);
} catch (_err) {
  console.log('✅ Forbidden <image> rejected');
}

try {
  svgRenderer.sanitizeSvg('<svg viewBox="0 0 10 10"><use href="#x"/></svg>');
  console.error('❌ Forbidden <use> was not rejected');
  process.exit(1);
} catch (_err) {
  console.log('✅ Forbidden <use> rejected');
}

const svgA = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 10 10"><path d="M0 0 L10 10"/></svg>';
const svgB = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 10 10"><path d="M0 10 L10 0"/></svg>';
const hashA = svgRenderer.hashSvg(svgA);
const hashB = svgRenderer.hashSvg(svgB);

if (hashA === hashB) {
  console.error('❌ SVG SHA-256 cache keys collided');
  process.exit(1);
}
if (typeof hashA !== 'string' || hashA.length !== 64 || typeof hashB !== 'string' || hashB.length !== 64) {
  console.error('❌ SVG cache key is not a SHA-256 hex digest');
  process.exit(1);
}
console.log('✅ SHA-256 cache keys look correct');

console.log('\n🎯 Vector pipeline test complete!');
console.log('\n📋 Next steps:');
console.log('1. Start vector worker: node workers/vectorWorker.js');
console.log('2. Enqueue via admin API: POST /api/admin/assign-job');
console.log('3. Verify output PDF is vector-only (no raster artifacts)');

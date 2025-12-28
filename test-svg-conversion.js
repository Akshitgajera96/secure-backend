import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { svgBytesToPdfBytes } from './src/vector/vectorLayoutEngine.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function main() {
  const inputSvgPath = path.join(__dirname, 'test.svg');
  const outputPdfPath = path.join(__dirname, 'out-test.pdf');

  if (!fs.existsSync(inputSvgPath)) {
    throw new Error(`Missing ${inputSvgPath}. Create it first (test.svg).`);
  }

  const svgBytes = fs.readFileSync(inputSvgPath);
  const pdfBytes = await svgBytesToPdfBytes(svgBytes);

  fs.writeFileSync(outputPdfPath, Buffer.from(pdfBytes));

  const header = Buffer.from(pdfBytes.slice(0, 5)).toString('utf8');
  if (!header.startsWith('%PDF-')) {
    throw new Error('Output is not a valid PDF');
  }

  console.log(`[OK] SVG->PDF done: ${outputPdfPath} (${pdfBytes.length} bytes)`);
}

main().catch((e) => {
  console.error('[FAIL] SVG->PDF failed:', e?.message || e);
  process.exit(1);
});

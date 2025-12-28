import mongoose from 'mongoose';
import { getVectorPdfQueue } from './queues/vectorQueue.js';
import VectorDocument from './src/vectorModels/VectorDocument.js';
import VectorPrintJob from './src/vectorModels/VectorPrintJob.js';
import { signJobPayload } from './src/services/hmac.js';
import dotenv from 'dotenv';

dotenv.config();

async function testSvgNormalize() {
  try {
    // Connect to MongoDB
    const mongoUri = process.env.MONGO_URI || 'mongodb://localhost:27017/secure-print';
    await mongoose.connect(mongoUri);
    console.log('Connected to MongoDB');

    // Create a test SVG document
    const doc = await VectorDocument.create({
      title: 'Test SVG',
      fileKey: 'test/test.svg',
      fileUrl: 'http://localhost:8000/files/test/test.svg',
      sourceFileKey: 'test/test.svg',
      sourceMimeType: 'image/svg+xml',
      totalPrints: 1,
      createdBy: new mongoose.Types.ObjectId(), // dummy user ID
      svgNormalizeStatus: 'PENDING',
      svgNormalizeError: { message: null, stack: null },
      normalizeFailed: false,
      normalizeError: { message: null, stack: null },
    });

    console.log('Created test document:', doc._id);

    const payload = {
      documentId: doc._id.toString(),
      sourceKey: doc.sourceFileKey,
      renderKey: doc.fileKey,
    };

    const payloadHmac = signJobPayload(payload);

    const jobDoc = await VectorPrintJob.create({
      userId: doc.createdBy,
      sourcePdfKey: doc.fileKey,
      metadata: payload,
      payloadHmac,
      status: 'PENDING',
      progress: 0,
      totalPages: 1,
      audit: [{ event: 'SVG_NORMALIZE_JOB_CREATED', details: { documentId: doc._id.toString() } }],
    });

    // Enqueue the SVG normalization job
    const q = getVectorPdfQueue();
    if (!q) {
      console.error('Redis unavailable - cannot enqueue job');
      return;
    }

    await q.add('normalizeSvg', { printJobId: jobDoc._id.toString(), documentId: doc._id.toString() });
    console.log('SVG normalize job enqueued successfully');

    console.log('Check worker terminal for [SVG_NORMALIZE_STARTED] and [SVG_NORMALIZE_DONE] logs');
    
  } catch (error) {
    console.error('Test failed:', error);
  } finally {
    await mongoose.disconnect();
  }
}

testSvgNormalize();

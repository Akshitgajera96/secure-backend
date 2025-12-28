import fs from 'fs/promises';
import os from 'os';
import path from 'path';
import { spawn } from 'child_process';
import { HeadObjectCommand } from '@aws-sdk/client-s3';

import VectorDocument from '../vectorModels/VectorDocument.js';
import { getRedisClient } from './redisClient.js';
import { downloadFromS3, uploadToS3WithKey, s3 } from './s3.js';

const cacheKey = (documentId, exportVersion, colorMode) =>
  `final_pdf:${documentId}:${exportVersion}:${colorMode}`;

const runGhostscriptCmyk = async ({ inputPdfPath, outputPdfPath, iccPath }) => {
  const bin = process.env.GHOSTSCRIPT_BIN || 'gswin64c';

  const args = [
    '-dSAFER',
    '-dBATCH',
    '-dNOPAUSE',
    '-sDEVICE=pdfwrite',
    '-sColorConversionStrategy=CMYK',
    '-sProcessColorModel=DeviceCMYK',
    `-sOutputICCProfile=${iccPath}`,
    '-o',
    outputPdfPath,
    inputPdfPath,
  ];

  await new Promise((resolve, reject) => {
    const p = spawn(bin, args, { stdio: 'ignore' });
    p.on('error', reject);
    p.on('exit', (code) => {
      if (code === 0) return resolve();
      return reject(new Error(`Ghostscript failed: ${code}`));
    });
  });
};

const exportObjectKey = (documentId, exportVersion, colorMode) =>
  `documents/export/${documentId}/${exportVersion}/${colorMode}.pdf`;

export async function resolveFinalPdfKeyForServe(documentId) {
  const doc = await VectorDocument.findById(documentId)
    .select('fileKey colorMode exportVersion mimeType')
    .exec();

  if (!doc) {
    const err = new Error('Document not found');
    err.code = 'NOT_FOUND';
    throw err;
  }

  const colorMode = doc.colorMode === 'CMYK' ? 'CMYK' : 'RGB';
  const exportVersion = Number(doc.exportVersion || 0);

  const redis = getRedisClient();
  const key = cacheKey(doc._id.toString(), exportVersion, colorMode);

  if (redis) {
    try {
      const cached = await redis.get(key);
      if (cached) return cached;
    } catch {
      // ignore
    }
  }

  if (colorMode === 'RGB') {
    if (redis) {
      try {
        await redis.set(key, doc.fileKey);
      } catch {
        // ignore
      }
    }
    return doc.fileKey;
  }

  // CMYK is admin-materialized only
  const err = new Error('CMYK export not materialized');
  err.code = 'EXPORT_NOT_READY';
  throw err;
}

export async function materializeFinalPdfExportKey(documentId) {
  const doc = await VectorDocument.findById(documentId)
    .select('fileKey colorMode exportVersion mimeType')
    .exec();

  if (!doc) {
    const err = new Error('Document not found');
    err.code = 'NOT_FOUND';
    throw err;
  }

  const colorMode = doc.colorMode === 'CMYK' ? 'CMYK' : 'RGB';
  const exportVersion = Number(doc.exportVersion || 0);

  const redis = getRedisClient();
  const key = cacheKey(doc._id.toString(), exportVersion, colorMode);

  if (redis) {
    try {
      const cached = await redis.get(key);
      if (cached) return cached;
    } catch {
      // ignore
    }
  }

  if (colorMode === 'RGB') {
    if (redis) {
      try {
        await redis.set(key, doc.fileKey);
      } catch {
        // ignore
      }
    }
    return doc.fileKey;
  }

  if (String(doc.mimeType || '').toLowerCase() !== 'application/pdf') {
    const err = new Error('CMYK export requires PDF source');
    err.code = 'BAD_SOURCE';
    throw err;
  }

  const iccPath = process.env.CMYK_ICC_PATH || '';
  if (!iccPath) {
    const err = new Error('CMYK_ICC_PATH not configured');
    err.code = 'NO_ICC';
    throw err;
  }

  const exportKey = exportObjectKey(doc._id.toString(), exportVersion, 'CMYK');

  // Idempotent regeneration: if object exists, reuse
  try {
    await s3.send(new HeadObjectCommand({ Bucket: process.env.AWS_S3_BUCKET, Key: exportKey }));
    if (redis) {
      try {
        await redis.set(key, exportKey);
      } catch {
        // ignore
      }
    }
    return exportKey;
  } catch {
    // continue
  }

  const inputBytes = await downloadFromS3(doc.fileKey);
  const header = Buffer.from(inputBytes.slice(0, 5)).toString();
  if (!header.startsWith('%PDF-')) {
    const err = new Error('CMYK export requires PDF bytes');
    err.code = 'BAD_SOURCE';
    throw err;
  }

  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'cmyk-export-'));
  const inPath = path.join(tmpDir, 'input.pdf');
  const outPath = path.join(tmpDir, 'output.pdf');

  try {
    await fs.writeFile(inPath, inputBytes);
    await runGhostscriptCmyk({ inputPdfPath: inPath, outputPdfPath: outPath, iccPath });
    const outBytes = await fs.readFile(outPath);

    const uploaded = await uploadToS3WithKey(outBytes, 'application/pdf', exportKey);

    if (redis) {
      try {
        await redis.set(key, uploaded.key);
      } catch {
        // ignore
      }
    }

    return uploaded.key;
  } finally {
    try {
      await fs.rm(tmpDir, { recursive: true, force: true });
    } catch {
      // ignore
    }
  }
}

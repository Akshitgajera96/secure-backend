import express from 'express';
import { s3 as awsS3 } from '../services/s3.js';
import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3';

const router = express.Router();

// GET /api/download/enacle-app - Stream Enacle-app.exe from S3
router.get('/enacle-app', async (req, res) => {
  try {
    const railwayBucket = String(process.env.RAILWAY_S3_BUCKET || '').trim();
    const railwayKeyId = String(process.env.RAILWAY_S3_KEY || '').trim();
    const railwaySecret = String(process.env.RAILWAY_S3_SECRET || '').trim();
    const railwayEndpoint = String(process.env.RAILWAY_S3_ENDPOINT || 'https://storage.railway.app').trim();

    const awsBucket = String(process.env.AWS_S3_BUCKET || '').trim();
    const downloadBucketOverride = String(process.env.DOWNLOAD_APP_BUCKET || '').trim();
    const key = String(process.env.DOWNLOAD_APP_KEY || 'Enacle-app.exe').trim();

    const useRailway = Boolean(railwayBucket && railwayKeyId && railwaySecret);
    const useAws = Boolean(!useRailway && awsBucket);

    if (!useRailway && !useAws) {
      return res.status(500).json({
        message:
          'S3 not configured: set AWS_S3_BUCKET (AWS) or RAILWAY_S3_BUCKET/RAILWAY_S3_KEY/RAILWAY_S3_SECRET (Railway Storage)',
      });
    }

    const client = useRailway
      ? new S3Client({
          region: 'auto',
          endpoint: railwayEndpoint,
          forcePathStyle: true,
          credentials: { accessKeyId: railwayKeyId, secretAccessKey: railwaySecret },
        })
      : awsS3;

    const bucket =
      downloadBucketOverride ||
      (useRailway ? railwayBucket : awsBucket);

    const command = new GetObjectCommand({ Bucket: bucket, Key: key });
    const response = await client.send(command);

    if (!response?.Body) {
      return res.status(404).json({ message: 'File not found on S3' });
    }

    res.setHeader('Content-Type', 'application/octet-stream');
    res.setHeader('Content-Disposition', 'attachment; filename="Enacle-app.exe"');
    res.setHeader('Cache-Control', 'no-store');

    if (response.ContentLength !== undefined && response.ContentLength !== null) {
      res.setHeader('Content-Length', String(response.ContentLength));
    }

    response.Body.on('error', (err) => {
      console.error('S3 stream error:', err);
      if (!res.headersSent) {
        res.status(500).json({ message: 'Failed to stream file' });
      }
    });

    return response.Body.pipe(res);
  } catch (err) {
    console.error('Download error:', err);
    const code = err && typeof err === 'object' && 'name' in err ? String(err.name) : '';
    const msg = err && typeof err === 'object' && 'message' in err ? String(err.message) : '';
    if (code === 'NoSuchKey' || /NoSuchKey/i.test(msg)) {
      return res.status(404).json({ message: 'File not found on S3' });
    }
    if (code === 'AccessDenied' || /AccessDenied/i.test(msg)) {
      return res.status(403).json({ message: 'Access denied to S3 object (check bucket policy/IAM credentials)' });
    }
    return res.status(500).json({ message: 'Failed to download file' });
  }
});

export default router;

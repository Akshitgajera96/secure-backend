import express from 'express';
import { authMiddleware } from '../middleware/auth.js';

const router = express.Router();

// Legacy endpoints removed: raster/HTML-to-PDF generation is forbidden.
router.post('/generate-output-pdf', authMiddleware, async (_req, res) => {
  return res.status(410).json({
    message: 'generate-output-pdf has been removed. Use /api/vector/generate for vector-only output.',
  });
});

router.post('/series/generate', authMiddleware, async (_req, res) => {
  return res.status(410).json({
    message: 'series/generate has been removed. Use the vector pipeline for numbered output.',
  });
});

export default router;

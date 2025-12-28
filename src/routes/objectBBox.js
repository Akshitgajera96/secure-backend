import express from 'express';

const router = express.Router();

router.get('/:slotIndex', (_req, res) => {
  res.status(410).json({
    error: 'Gone',
    message: 'object-bbox endpoint is deprecated and must not be used.',
  });
});

export default router;

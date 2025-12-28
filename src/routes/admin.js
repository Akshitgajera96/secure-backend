import express from 'express';
import multer from 'multer';
import bcrypt from 'bcryptjs';
import crypto from 'crypto';
import User from '../vectorModels/VectorUser.js';
import Document from '../vectorModels/VectorDocument.js';
import DocumentAccess from '../vectorModels/VectorDocumentAccess.js';
import DocumentJobs from '../vectorModels/VectorDocumentJobs.js';
import { uploadToS3 } from '../services/s3.js';
import { getVectorPdfQueue } from '../../queues/vectorQueue.js';
import { authMiddleware, requireAdmin } from '../middleware/auth.js';
import { materializeFinalPdfExportKey } from '../services/finalPdfExportService.js';
import Session from '../vectorModels/VectorSession.js';
import BlockedIp from '../vectorModels/VectorBlockedIp.js';

const router = express.Router();
const upload = multer();

// Upload a single base64 ticket image to S3 and return its key
router.post('/upload-ticket-image', authMiddleware, requireAdmin, async (_req, res) => {
  return res.status(410).json({
    message: 'upload-ticket-image has been removed. Vector pipeline forbids raster ticket images.',
  });
});

// Upload document (PDF/SVG) and create Document record
router.post('/documents', authMiddleware, requireAdmin, upload.single('file'), async (req, res) => {
  try {
    const { title, totalPrints } = req.body;
    const file = req.file;

    if (!file) {
      return res.status(400).json({ message: 'File is required' });
    }

    if (!title || !totalPrints) {
      return res.status(400).json({ message: 'Title and totalPrints are required' });
    }

    const parsedTotal = Number(totalPrints);
    if (Number.isNaN(parsedTotal) || parsedTotal <= 0) {
      return res.status(400).json({ message: 'totalPrints must be a positive number' });
    }

    const { key, url } = await uploadToS3(file.buffer, file.mimetype);

    const doc = await Document.create({
      title,
      fileKey: key,
      fileUrl: url,
      totalPrints: parsedTotal,
      createdBy: req.user._id,
    });

    return res.status(201).json(doc);
  } catch (err) {
    console.error('Upload document error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Create background assignment job instead of synchronous PDF generation
router.post('/assign-job', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { email, assignedQuota, vectorMetadata } = req.body || {};

    if (!email || !assignedQuota || !vectorMetadata) {
      return res.status(400).json({ message: 'email, assignedQuota and vectorMetadata are required' });
    }

    const pagesNum = Number(assignedQuota);
    if (Number.isNaN(pagesNum) || pagesNum <= 0) {
      return res.status(400).json({ message: 'assignedQuota must be a positive number' });
    }

    const { validateVectorMetadata } = await import('../vector/validation.js');
    const validation = validateVectorMetadata(vectorMetadata);
    if (!validation.isValid) {
      return res.status(400).json({ message: 'Invalid vectorMetadata', errors: validation.errors });
    }

    const user = await User.findOne({ email: email.toLowerCase() });
    if (!user) {
      return res.status(404).json({ message: 'User with this email not found' });
    }

    const totalPages = Number(vectorMetadata?.layout?.totalPages || 1);

    const jobDoc = await DocumentJobs.create({
      email: email.toLowerCase(),
      assignedQuota: pagesNum,
      // Optional lightweight meta; we do not store full layout in Mongo
      layoutPages: [],
      status: 'processing',
      stage: 'vector-rendering',
      totalPages,
      completedPages: 0,
      outputDocumentId: null,
      userId: user._id,
      createdBy: req.user._id,
    });

    const baseJobId = jobDoc._id.toString();

    const vectorPdfQueue = getVectorPdfQueue();
    if (!vectorPdfQueue) {
      return res.status(503).json({
        message: 'Redis is unavailable. Background jobs are disabled. Use synchronous vector generation endpoint instead.',
      });
    }

    await vectorPdfQueue.add(
      'renderVector',
      {
        email: email.toLowerCase(),
        assignedQuota: pagesNum,
        vectorMetadata,
        adminUserId: req.user._id,
        jobId: baseJobId,
      },
      {
        jobId: `${baseJobId}:vector`,
      }
    );

    return res.status(201).json({
      success: true,
      message: 'Assignment job created',
      jobId: jobDoc._id.toString(),
    });
  } catch (err) {
    console.error('Create assign job error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.get('/users/:userId/sessions', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { userId } = req.params;

    const sessions = await Session.find({ userId }).sort({ createdAt: -1 });

    return res.json({ sessions });
  } catch (err) {
    console.error('List user sessions error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.post('/sessions/:sessionId/logout', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { sessionId } = req.params;

    await Session.deleteOne({ _id: sessionId });

    return res.json({ success: true });
  } catch (err) {
    console.error('Logout session error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.post('/sessions/:sessionId/block-ip', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { sessionId } = req.params;
    const { reason } = req.body || {};

    const session = await Session.findById(sessionId);
    if (!session) {
      return res.status(404).json({ message: 'Session not found' });
    }

    const ip = session.ip;

    await BlockedIp.findOneAndUpdate(
      { ip },
      {
        ip,
        reason: reason || 'Blocked from admin panel',
        blockedBy: req.user._id,
        createdAt: new Date(),
      },
      { upsert: true, new: true }
    );

    await Session.deleteMany({ ip });

    return res.json({ success: true });
  } catch (err) {
    console.error('Block IP error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.post('/logout-all', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { userId } = req.body;

    if (!userId) {
      return res.status(400).json({ message: 'userId is required' });
    }

    await Session.deleteMany({ userId });

    return res.json({ success: true });
  } catch (err) {
    console.error('Logout all devices error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Get all documents created by admin
router.get('/documents', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const docs = await Document.find({ createdBy: req.user._id }).sort({ createdAt: -1 });
    return res.json(docs);
  } catch (err) {
    console.error('List documents error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.post('/documents/:id/color-mode', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const { colorMode } = req.body || {};

    if (!['RGB', 'CMYK'].includes(colorMode)) {
      return res.status(400).json({ message: 'colorMode must be RGB or CMYK' });
    }

    const doc = await Document.findById(id).exec();
    if (!doc) {
      return res.status(404).json({ message: 'Document not found' });
    }

    if ((doc.colorMode || 'RGB') !== colorMode) {
      doc.colorMode = colorMode;
      doc.exportVersion = Number(doc.exportVersion || 0) + 1;
      await doc.save();
    }

    if (colorMode === 'CMYK') {
      await materializeFinalPdfExportKey(doc._id.toString());
    }

    return res.json({
      success: true,
      documentId: doc._id,
      colorMode: doc.colorMode || 'RGB',
      exportVersion: Number(doc.exportVersion || 0),
    });
  } catch (err) {
    console.error('Set color mode error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Assign or update quota for a user on a document (by userId)
router.post('/documents/:id/assign', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const { userId, assignedQuota } = req.body;

    if (!userId || !assignedQuota) {
      return res.status(400).json({ message: 'userId and assignedQuota are required' });
    }

    const parsedQuota = Number(assignedQuota);
    if (Number.isNaN(parsedQuota) || parsedQuota <= 0) {
      return res.status(400).json({ message: 'assignedQuota must be a positive number' });
    }

    const access = await DocumentAccess.findOneAndUpdate(
      { userId, documentId: id },
      { userId, documentId: id, assignedQuota: parsedQuota, printQuota: parsedQuota, printsUsed: 0, revoked: false },
      { upsert: true, new: true }
    );

    return res.json(access);
  } catch (err) {
    console.error('Assign quota error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Assign or update quota for a user on a document, using user email
router.post('/documents/:id/assign-by-email', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const { email, assignedQuota } = req.body;

    if (!email || !assignedQuota) {
      return res.status(400).json({ message: 'email and assignedQuota are required' });
    }

    const parsedQuota = Number(assignedQuota);
    if (Number.isNaN(parsedQuota) || parsedQuota <= 0) {
      return res.status(400).json({ message: 'assignedQuota must be a positive number' });
    }

    const user = await User.findOne({ email: email.toLowerCase() });
    if (!user) {
      return res.status(404).json({ message: 'User with this email not found' });
    }

    const access = await DocumentAccess.findOneAndUpdate(
      { userId: user._id, documentId: id },
      { userId: user._id, documentId: id, assignedQuota: parsedQuota, printQuota: parsedQuota, printsUsed: 0, revoked: false },
      { upsert: true, new: true }
    );

    if (!access.sessionToken) {
      access.sessionToken = crypto.randomBytes(32).toString('hex');
      await access.save();
    }

    return res.json(access);
  } catch (err) {
    console.error('Assign quota by email error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Create a new user (admin only)
router.post('/users', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { email, password, role = 'user' } = req.body;

    if (!email || !password) {
      return res.status(400).json({ message: 'Email and password are required' });
    }

    if (!['admin', 'user'].includes(role)) {
      return res.status(400).json({ message: 'Invalid role. Must be either "admin" or "user"' });
    }

    const existingUser = await User.findOne({ email: email.toLowerCase() });
    if (existingUser) {
      return res.status(400).json({ message: 'Email already registered' });
    }

    const passwordHash = await bcrypt.hash(password, 10);

    const user = await User.create({
      email: email.toLowerCase(),
      passwordHash,
      role,
    });

    return res.status(201).json({
      user: { id: user._id, email: user.email, role: user.role },
    });
  } catch (err) {
    console.error('Create user error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Admin password change
router.put('/change-password', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { currentPassword, newPassword } = req.body;

    if (!currentPassword || !newPassword) {
      return res.status(400).json({ message: 'Current password and new password are required' });
    }

    if (newPassword.length < 6) {
      return res.status(400).json({ message: 'New password must be at least 6 characters long' });
    }

    // Get current admin user
    const admin = await User.findById(req.user._id);
    if (!admin) {
      return res.status(404).json({ message: 'Admin user not found' });
    }

    // Verify current password
    const isCurrentPasswordValid = await bcrypt.compare(currentPassword, admin.passwordHash);
    if (!isCurrentPasswordValid) {
      return res.status(400).json({ message: 'Current password is incorrect' });
    }

    // Hash new password
    const newPasswordHash = await bcrypt.hash(newPassword, 10);

    // Update password
    await User.findByIdAndUpdate(admin._id, { passwordHash: newPasswordHash });

    return res.json({ success: true, message: 'Password changed successfully' });
  } catch (err) {
    console.error('Admin change password error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Create a new admin (admin only)
router.post('/admins', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      return res.status(400).json({ message: 'Email and password are required' });
    }

    if (password.length < 6) {
      return res.status(400).json({ message: 'Password must be at least 6 characters long' });
    }

    const existingUser = await User.findOne({ email: email.toLowerCase() });
    if (existingUser) {
      return res.status(400).json({ message: 'Email already registered' });
    }

    const passwordHash = await bcrypt.hash(password, 10);

    const admin = await User.create({
      email: email.toLowerCase(),
      passwordHash,
      role: 'admin',
    });

    return res.status(201).json({
      admin: { id: admin._id, email: admin.email, role: admin.role },
      message: 'Admin created successfully'
    });
  } catch (err) {
    console.error('Create admin error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Get all admins (admin only)
router.get('/admins', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const admins = await User.find({ role: 'admin' })
      .select('email role createdAt')
      .sort({ createdAt: -1 });

    return res.json({ admins });
  } catch (err) {
    console.error('List admins error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

export default router;

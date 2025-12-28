import express from 'express';
import bcrypt from 'bcryptjs';
import mongoose from 'mongoose';
import User from '../vectorModels/VectorUser.js';
import Session from '../vectorModels/VectorSession.js';
import BlockedIp from '../vectorModels/VectorBlockedIp.js';
import { authMiddleware, requireAdmin } from '../middleware/auth.js';

const router = express.Router();

router.get('/users', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { email } = req.query;

    const filter = {};
    if (typeof email === 'string' && email.trim() !== '') {
      filter.email = { $regex: new RegExp(email.trim(), 'i') };
    }

    const users = await User.find(filter)
      .sort({ createdAt: -1 })
      .select('email role createdAt');

    return res.json({ users });
  } catch (err) {
    console.error('Admin list users error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.get('/users/:userId/ip-overview', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { userId } = req.params;

    if (!mongoose.Types.ObjectId.isValid(userId)) {
      return res.status(400).json({ message: 'Invalid userId' });
    }

    const pipeline = [
      {
        $match: {
          userId: new mongoose.Types.ObjectId(userId),
        },
      },
      {
        $group: {
          _id: '$ip',
          sessionCount: { $sum: 1 },
          lastSeen: { $max: '$lastActivity' },
        },
      },
      { $sort: { lastSeen: -1 } },
    ];

    const sessionsByIp = await Session.aggregate(pipeline);
    const ips = sessionsByIp.map((s) => s._id);

    const blockedDocs = await BlockedIp.find({ ip: { $in: ips } }).lean();
    const blockedMap = new Map(blockedDocs.map((b) => [b.ip, b]));

    const overview = sessionsByIp.map((row) => {
      const blocked = blockedMap.get(row._id);
      const isBlocked = !!blocked && blocked.isActive;
      return {
        ip: row._id,
        sessionCount: row.sessionCount,
        lastSeen: row.lastSeen,
        isBlocked,
        blockedReason: blocked?.reason || null,
      };
    });

    return res.json({ ips: overview });
  } catch (err) {
    console.error('Admin user IP overview error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.post('/users/:userId/block-other-ips', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { userId } = req.params;
    const { reason, keepIp } = req.body || {};

    if (!mongoose.Types.ObjectId.isValid(userId)) {
      return res.status(400).json({ message: 'Invalid userId' });
    }

    const now = new Date();
    const activeSessions = await Session.find({
      userId,
      isActive: true,
      expiresAt: { $gt: now },
    })
      .sort({ lastActivity: -1, createdAt: -1 })
      .lean();

    if (!activeSessions.length) {
      return res.status(400).json({ message: 'No active sessions for this user' });
    }

    const currentIp = keepIp || activeSessions[0].ip;
    const otherIps = Array.from(
      new Set(activeSessions.map((s) => s.ip).filter((ip) => ip !== currentIp))
    );

    if (!otherIps.length) {
      return res.json({ success: true, keptIp: currentIp, blockedIps: [] });
    }

    const blockReason =
      reason || `Blocked other IPs for user ${userId} via admin panel`;

    await Promise.all(
      otherIps.map((ip) =>
        BlockedIp.findOneAndUpdate(
          { ip },
          {
            reason: blockReason,
            blockedBy: req.user._id,
            expiresAt: new Date(Date.now() + 24 * 60 * 60 * 1000),
            isActive: true,
          },
          { upsert: true, new: true }
        )
      )
    );

    await Session.updateMany(
      { userId, ip: { $in: otherIps }, isActive: true },
      {
        $set: {
          isActive: false,
          expiresAt: now,
        },
      }
    );

    return res.json({ success: true, keptIp: currentIp, blockedIps: otherIps });
  } catch (err) {
    console.error('Admin block other IPs error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.get('/blocked-ips', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const blocked = await BlockedIp.find({})
      .populate({ path: 'blockedBy', select: 'email', model: 'VectorUser' })
      .sort({ createdAt: -1 })
      .lean();

    return res.json({ ips: blocked });
  } catch (err) {
    console.error('Admin list blocked IPs error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.post('/block-ip', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { ip, reason, expiresInHours = 24 } = req.body || {};

    if (!ip) {
      return res.status(400).json({ message: 'IP is required' });
    }

    const ipRegex = /^(\d{1,3}\.){3}\d{1,3}(?:\/\d{1,2})?$/;
    if (!ipRegex.test(ip)) {
      return res.status(400).json({ message: 'Invalid IP address format' });
    }

    const doc = await BlockedIp.findOneAndUpdate(
      { ip },
      {
        reason: reason || 'Blocked from admin panel',
        blockedBy: req.user._id,
        expiresAt: new Date(Date.now() + expiresInHours * 60 * 60 * 1000),
        isActive: true,
      },
      { upsert: true, new: true }
    );

    return res.json({ success: true, ip: doc });
  } catch (err) {
    console.error('Admin block IP error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.post('/unblock-ip', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { ip } = req.body || {};

    if (!ip) {
      return res.status(400).json({ message: 'IP is required' });
    }

    const updated = await BlockedIp.findOneAndUpdate(
      { ip, isActive: true },
      {
        isActive: false,
        expiresAt: new Date(),
      },
      { new: true }
    );

    if (!updated) {
      return res.status(404).json({ message: 'IP not found or already unblocked' });
    }

    return res.json({ success: true, ip: updated });
  } catch (err) {
    console.error('Admin unblock IP error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.get('/users/active-sessions', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const now = new Date();

    const pipeline = [
      {
        $match: {
          role: { $ne: 'admin' },
        },
      },
      {
        $lookup: {
          from: 'vector_sessions',
          localField: '_id',
          foreignField: 'userId',
          as: 'sessions',
        },
      },
      {
        $addFields: {
          activeSessions: {
            $filter: {
              input: '$sessions',
              as: 's',
              cond: {
                $and: [
                  { $eq: ['$$s.isActive', true] },
                  { $gt: ['$$s.expiresAt', now] },
                ],
              },
            },
          },
        },
      },
      {
        $project: {
          _id: 1,
          email: 1,
          role: 1,
          createdAt: 1,
          sessionCount: { $size: '$activeSessions' },
          distinctIpCount: {
            $size: {
              $setUnion: [
                {
                  $map: {
                    input: '$activeSessions',
                    as: 's',
                    in: '$$s.ip',
                  },
                },
                [],
              ],
            },
          },
        },
      },
      { $sort: { createdAt: -1 } },
    ];

    const users = await User.aggregate(pipeline);

    return res.json({ users });
  } catch (err) {
    console.error('Admin active sessions users error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Admin creates a new user with email + password
router.post('/users', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      return res.status(400).json({ message: 'Email and password are required' });
    }

    const existing = await User.findOne({ email: email.toLowerCase() });
    if (existing) {
      return res.status(400).json({ message: 'User with this email already exists' });
    }

    const passwordHash = await bcrypt.hash(password, 10);

    const user = await User.create({
      email: email.toLowerCase(),
      passwordHash,
      role: 'user',
    });

    return res.status(201).json({
      user: {
        id: user._id,
        email: user.email,
        role: user.role,
      },
    });
  } catch (err) {
    console.error('Admin create user error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

export default router;

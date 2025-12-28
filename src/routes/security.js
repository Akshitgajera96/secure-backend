import express from 'express';
import { authMiddleware, requireAdmin } from '../middleware/auth.js';
import VectorBlockedIp from '../vectorModels/VectorBlockedIp.js';
import VectorUser from '../vectorModels/VectorUser.js';

const router = express.Router();

router.use(authMiddleware);

router.get('/settings', async (req, res) => {
  try {
    const user = await VectorUser.findById(req.user._id, {
      security: 1,
      allowedIPs: 1,
      lastLoginIP: 1,
      loginHistory: { $slice: -10 },
    });

    if (!user) {
      return res.status(404).json({ success: false, message: 'User not found' });
    }

    return res.json({
      success: true,
      data: {
        security: user.security,
        allowedIPs: user.allowedIPs || [],
        lastLoginIP: user.lastLoginIP,
        loginHistory: user.loginHistory || [],
      },
    });
  } catch (err) {
    console.error('Get security settings error', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});

router.post('/ip-whitelist', async (req, res) => {
  try {
    const { ip, action = 'add' } = req.body || {};

    if (!ip) {
      return res.status(400).json({ success: false, message: 'IP address is required' });
    }

    const ipRegex = /^(\d{1,3}\.){3}\d{1,3}(?:\/\d{1,2})?$/;
    if (!ipRegex.test(ip)) {
      return res.status(400).json({ success: false, message: 'Invalid IP address format' });
    }

    if (action === 'add') {
      const existing = await VectorUser.findOne({
        _id: req.user._id,
        'allowedIPs.ip': ip,
      });

      if (existing) {
        return res
          .status(400)
          .json({ success: false, message: 'IP already in whitelist' });
      }

      await VectorUser.findByIdAndUpdate(req.user._id, {
        $addToSet: {
          allowedIPs: {
            ip,
            isActive: true,
            lastUsed: new Date(),
          },
        },
      });
    } else if (action === 'remove') {
      await VectorUser.findByIdAndUpdate(req.user._id, {
        $pull: { allowedIPs: { ip } },
      });
    } else {
      return res
        .status(400)
        .json({ success: false, message: 'Invalid action. Use "add" or "remove"' });
    }

    return res.json({
      success: true,
      message: `IP ${action === 'add' ? 'added to' : 'removed from'} whitelist`,
    });
  } catch (err) {
    console.error('Update IP whitelist error', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});

router.post('/toggle-ip-whitelist', async (req, res) => {
  try {
    const { enabled } = req.body || {};

    if (typeof enabled !== 'boolean') {
      return res
        .status(400)
        .json({ success: false, message: 'Enabled flag must be a boolean' });
    }

    if (enabled) {
      const user = await VectorUser.findById(req.user._id);
      if (!user || !user.allowedIPs || user.allowedIPs.length === 0) {
        return res.status(400).json({
          success: false,
          message: 'Please add at least one IP to your whitelist before enabling',
        });
      }
    }

    await VectorUser.findByIdAndUpdate(req.user._id, {
      'security.requireIPWhitelist': enabled,
    });

    return res.json({
      success: true,
      data: { requireIPWhitelist: enabled },
    });
  } catch (err) {
    console.error('Toggle IP whitelist error', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});

router.get('/login-history', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit, 10) || 20;
    const page = parseInt(req.query.page, 10) || 1;
    const skip = (page - 1) * limit;

    const user = await VectorUser.findById(
      req.user._id,
      { loginHistory: { $slice: [skip, limit] } }
    ).select('loginHistory -_id');

    if (!user) {
      return res.status(404).json({ success: false, message: 'User not found' });
    }

    const countResult = await VectorUser.aggregate([
      { $match: { _id: req.user._id } },
      { $project: { count: { $size: '$loginHistory' } } },
    ]);

    const total = (countResult[0] && countResult[0].count) || 0;

    return res.json({
      success: true,
      data: user.loginHistory || [],
      pagination: {
        total,
        page,
        limit,
        pages: Math.ceil(total / limit),
      },
    });
  } catch (err) {
    console.error('Get login history error', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});

router.post('/block-ip', requireAdmin, async (req, res) => {
  try {
    const { ip, reason, expiresInHours = 24 } = req.body || {};

    if (!ip || !reason) {
      return res.status(400).json({ success: false, message: 'IP and reason are required' });
    }

    const ipRegex = /^(\d{1,3}\.){3}\d{1,3}(?:\/\d{1,2})?$/;
    if (!ipRegex.test(ip)) {
      return res.status(400).json({ success: false, message: 'Invalid IP address format' });
    }

    const blocked = await VectorBlockedIp.findOneAndUpdate(
      { ip },
      {
        reason,
        blockedBy: req.user._id,
        expiresAt: new Date(Date.now() + expiresInHours * 60 * 60 * 1000),
        isActive: true,
      },
      { upsert: true, new: true }
    ).populate({ path: 'blockedBy', select: 'email', model: 'VectorUser' });

    return res.json({ success: true, data: blocked });
  } catch (err) {
    console.error('Block IP error', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});

router.get('/blocked-ips', requireAdmin, async (req, res) => {
  try {
    const limit = parseInt(req.query.limit, 10) || 20;
    const page = parseInt(req.query.page, 10) || 1;
    const active = req.query.active;
    const skip = (page - 1) * limit;

    const query = {};

    if (active === 'true') {
      query.isActive = true;
      query.$or = [
        { expiresAt: { $exists: false } },
        { expiresAt: { $gt: new Date() } },
      ];
    } else if (active === 'false') {
      query.$or = [
        { isActive: false },
        { expiresAt: { $lt: new Date() } },
      ];
    }

    const [items, total] = await Promise.all([
      VectorBlockedIp.find(query)
        .populate({ path: 'blockedBy', select: 'email', model: 'VectorUser' })
        .sort({ createdAt: -1 })
        .skip(skip)
        .limit(limit)
        .lean(),
      VectorBlockedIp.countDocuments(query),
    ]);

    return res.json({
      success: true,
      data: items,
      pagination: {
        total,
        page,
        limit,
        pages: Math.ceil(total / limit),
      },
    });
  } catch (err) {
    console.error('List blocked IPs error', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});

router.post('/unblock-ip/:ip', requireAdmin, async (req, res) => {
  try {
    const { ip } = req.params;

    if (!ip) {
      return res.status(400).json({ success: false, message: 'IP address is required' });
    }

    const updated = await VectorBlockedIp.findOneAndUpdate(
      { ip, isActive: true },
      {
        isActive: false,
        expiresAt: new Date(),
      },
      { new: true }
    );

    if (!updated) {
      return res
        .status(404)
        .json({ success: false, message: 'No active block found for this IP' });
    }

    return res.json({ success: true, data: updated });
  } catch (err) {
    console.error('Unblock IP error', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});

export default router;

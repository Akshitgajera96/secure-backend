import express from 'express';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import VectorUser from '../vectorModels/VectorUser.js';
import VectorSession from '../vectorModels/VectorSession.js';

const router = express.Router();

// Register regular user
router.post('/register', async (req, res) => {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      return res.status(400).json({ message: 'Email and password are required' });
    }

    const existing = await VectorUser.findOne({ email: email.toLowerCase() });
    if (existing) {
      return res.status(400).json({ message: 'Email already registered' });
    }

    const passwordHash = await bcrypt.hash(password, 10);

    const user = await VectorUser.create({
      email: email.toLowerCase(),
      passwordHash,
      role: 'user',
    });

    return res.status(201).json({
      user: { id: user._id, email: user.email, role: user.role },
    });
  } catch (err) {
    console.error('Register error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Login (admin + user)
router.post('/login', async (req, res) => {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      return res.status(400).json({ message: 'Email and password are required' });
    }

    const user = await VectorUser.findOne({ email: email.toLowerCase() });
    if (!user) {
      return res.status(401).json({ message: 'Invalid email or password' });
    }

    if (user.isAccountLocked && user.isAccountLocked()) {
      const retryAfter = Math.max(
        0,
        Math.ceil((user.security.lockUntil - new Date()) / 1000)
      );

      return res.status(429).json({
        message: 'Account temporarily locked. Please try again later.',
        code: 'ACCOUNT_LOCKED',
        retryAfter,
      });
    }

    const isMatch = await bcrypt.compare(password, user.passwordHash);
    if (!isMatch) {
      if (user.incrementLoginAttempts) {
        await user.incrementLoginAttempts();
      }

      await VectorUser.findByIdAndUpdate(user._id, {
        $push: {
          loginHistory: {
            ip:
              req.ip ||
              req.headers['x-forwarded-for'] ||
              (req.connection && req.connection.remoteAddress) ||
              '',
            userAgent: req.headers['user-agent'] || '',
            status: 'failed',
            reason: 'invalid_password',
          },
        },
      });

      return res.status(401).json({ message: 'Invalid email or password' });
    }

    const jwtSecret = process.env.JWT_SECRET;
    if (!jwtSecret) {
      return res.status(500).json({ message: 'Server auth not configured (JWT_SECRET missing)' });
    }

    const token = jwt.sign(
      {
        userId: user._id.toString(),
        email: user.email,
        role: user.role,
      },
      jwtSecret,
      { expiresIn: '12h' }
    );

    const ip =
      req.ip ||
      req.headers['x-forwarded-for'] ||
      (req.connection && req.connection.remoteAddress) ||
      '';
    const userAgent = req.headers['user-agent'] || '';

    await VectorSession.deleteMany({ userId: user._id });

    await VectorSession.create({
      userId: user._id,
      token,
      ip,
      userAgent,
    });

    if (user.resetLoginAttempts) {
      await user.resetLoginAttempts();
    }

    await VectorUser.findByIdAndUpdate(user._id, {
      $set: {
        lastLoginIP: ip,
      },
      $push: {
        loginHistory: {
          ip,
          userAgent,
          status: 'success',
        },
      },
    });

    return res.json({
      token,
      user: {
        id: user._id,
        email: user.email,
        role: user.role,
      },
    });
  } catch (err) {
    console.error('Login error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

export default router;

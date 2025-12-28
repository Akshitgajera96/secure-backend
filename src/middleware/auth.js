import jwt from 'jsonwebtoken';
import VectorUser from '../vectorModels/VectorUser.js';
import VectorSession from '../vectorModels/VectorSession.js';

export const authMiddleware = async (req, res, next) => {
  try {
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;

    if (!token) {
      return res.status(401).json({ logout: true, message: 'Unauthorized' });
    }

    const jwtSecret = process.env.JWT_SECRET;
    if (!jwtSecret) {
      return res.status(500).json({ message: 'Auth not configured' });
    }

    const originalUrl = req.originalUrl || req.url || '';

    const payload = jwt.verify(token, jwtSecret);

    const session = await VectorSession.findOne({ token });

    if (!session) {
      return res
        .status(401)
        .json({ logout: true, message: 'Session expired or invalid' });
    }

    const user = await VectorUser.findById(payload.userId);
    if (!user) {
      return res.status(401).json({ logout: true, message: 'User not found' });
    }

    req.user = user;
    req.session = session;
    next();
  } catch (err) {
    console.error('Auth error', err);
    return res.status(401).json({ logout: true, message: 'Invalid token' });
  }
};

export const requireAdmin = (req, res, next) => {
  if (!req.user || req.user.role !== 'admin') {
    return res.status(403).json({ message: 'Admin access required' });
  }
  next();
};

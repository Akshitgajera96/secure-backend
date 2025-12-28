import VectorBlockedIp from '../vectorModels/VectorBlockedIp.js';
import VectorUser from '../vectorModels/VectorUser.js';

const getClientIP = (req) => {
  const forwarded = req.headers['x-forwarded-for'];
  if (typeof forwarded === 'string' && forwarded.length > 0) {
    return forwarded.split(',')[0].trim();
  }

  return (
    req.ip ||
    (req.connection && req.connection.remoteAddress) ||
    ''
  );
};

export const ipSecurity = async (req, res, next) => {
  try {
    const clientIP = getClientIP(req);

    const originalUrl = req.originalUrl || req.url || '';

    if (
      originalUrl === '/api/health' ||
      originalUrl.startsWith('/api/auth') ||
      originalUrl.startsWith('/api/admin')
    ) {
      req.clientIP = clientIP;
      return next();
    }

    const isBlocked = await VectorBlockedIp.isBlocked(clientIP);

    if (isBlocked) {
      return res.status(403).json({
        success: false,
        message: 'Access from this IP is blocked',
      });
    }

    req.clientIP = clientIP;
    return next();
  } catch (err) {
    console.error('IP security error', err);
    return next(err);
  }
};

export const checkLoginAttempts = async (req, res, next) => {
  if (!req.path.includes('login') || req.method !== 'POST') {
    return next();
  }

  try {
    const { email } = req.body || {};
    if (!email) {
      return next();
    }

    const user = await VectorUser.findOne({ email: email.toLowerCase() });
    if (!user) {
      return next();
    }

    if (user.isAccountLocked && user.isAccountLocked()) {
      const retryAfter = Math.max(
        0,
        Math.ceil((user.security.lockUntil - new Date()) / 1000)
      );

      return res.status(429).json({
        success: false,
        message: 'Account temporarily locked. Please try again later.',
        code: 'ACCOUNT_LOCKED',
        retryAfter,
      });
    }

    req.userSecurity = user.security || {};
    return next();
  } catch (err) {
    console.error('Login attempt check error', err);
    return next();
  }
};

export const checkIPWhitelist = async (req, res, next) => {
  if (!req.user) {
    return next();
  }

  try {
    const user = await User.findById(req.user._id, {
      security: 1,
      allowedIPs: 1,
    });

    if (!user || !user.security || !user.security.requireIPWhitelist) {
      return next();
    }

    const clientIP = req.clientIP || getClientIP(req);

    const allowedIP = (user.allowedIPs || []).find(
      (item) => item.ip === clientIP && item.isActive !== false
    );

    if (!allowedIP) {
      await User.findByIdAndUpdate(user._id, {
        $push: {
          loginHistory: {
            ip: clientIP,
            userAgent: req.headers['user-agent'] || '',
            status: 'failed',
            reason: 'IP not whitelisted',
          },
        },
      });

      return res.status(403).json({
        success: false,
        message: 'Access denied: IP not whitelisted',
        code: 'IP_NOT_WHITELISTED',
      });
    }

    allowedIP.lastUsed = new Date();
    await user.save();

    return next();
  } catch (err) {
    console.error('IP whitelist check error', err);
    return next(err);
  }
};

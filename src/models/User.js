import mongoose from 'mongoose';

const userSchema = new mongoose.Schema(
  {
    email: {
      type: String,
      required: true,
      unique: true,
      lowercase: true,
      trim: true,
    },
    passwordHash: {
      type: String,
      required: true,
    },
    role: {
      type: String,
      enum: ['admin', 'user'],
      default: 'user',
      required: true,
    },
    lastLoginIP: {
      type: String,
    },
    allowedIPs: [
      {
        ip: {
          type: String,
          required: true,
          trim: true,
        },
        isActive: {
          type: Boolean,
          default: true,
        },
        lastUsed: {
          type: Date,
        },
        createdAt: {
          type: Date,
          default: Date.now,
        },
      },
    ],
    loginHistory: [
      {
        ip: {
          type: String,
        },
        userAgent: {
          type: String,
        },
        timestamp: {
          type: Date,
          default: Date.now,
        },
        status: {
          type: String,
          enum: ['success', 'failed'],
        },
        reason: {
          type: String,
        },
      },
    ],
    security: {
      requireIPWhitelist: {
        type: Boolean,
        default: false,
      },
      maxFailedAttempts: {
        type: Number,
        default: 5,
      },
      failedLoginAttempts: {
        type: Number,
        default: 0,
      },
      lastFailedAttempt: {
        type: Date,
      },
      isLocked: {
        type: Boolean,
        default: false,
      },
      lockUntil: {
        type: Date,
      },
    },
  },
  {
    timestamps: true,
  }
);

userSchema.index({ 'allowedIPs.ip': 1 });
userSchema.index({ 'security.isLocked': 1, 'security.lockUntil': 1 });

userSchema.methods.isAccountLocked = function () {
  return (
    this.security &&
    this.security.isLocked &&
    this.security.lockUntil &&
    this.security.lockUntil > new Date()
  );
};

userSchema.methods.incrementLoginAttempts = async function () {
  const updates = {
    $inc: { 'security.failedLoginAttempts': 1 },
    $set: { 'security.lastFailedAttempt': new Date() },
  };

  const currentFailed = this.security?.failedLoginAttempts || 0;
  const maxAttempts = this.security?.maxFailedAttempts || 5;

  if (currentFailed + 1 >= maxAttempts) {
    const lockTime = 30 * 60 * 1000;
    updates.$set['security.isLocked'] = true;
    updates.$set['security.lockUntil'] = new Date(Date.now() + lockTime);
  }

  return this.updateOne(updates);
};

userSchema.methods.resetLoginAttempts = async function () {
  return this.updateOne({
    $set: {
      'security.failedLoginAttempts': 0,
      'security.isLocked': false,
      'security.lockUntil': null,
    },
  });
};

const User = mongoose.models.User || mongoose.model('User', userSchema);

export default User;

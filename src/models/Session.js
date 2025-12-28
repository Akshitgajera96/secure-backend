import mongoose from 'mongoose';

const sessionSchema = new mongoose.Schema(
  {
    userId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'User',
      required: true,
      index: true,
    },
    token: {
      type: String,
      required: true,
      unique: true,
      index: true,
    },
    ip: {
      type: String,
      required: true,
      trim: true,
    },
    userAgent: {
      type: String,
      default: '',
      trim: true,
    },
    lastActivity: {
      type: Date,
      default: Date.now,
      index: true,
    },
    expiresAt: {
      type: Date,
      required: true,
      default: () => new Date(Date.now() + 24 * 60 * 60 * 1000),
    },
    isActive: {
      type: Boolean,
      default: true,
      index: true,
    },
    deviceInfo: {
      browser: String,
      os: String,
      device: String,
      platform: String,
    },
    createdAt: {
      type: Date,
      default: Date.now,
    },
  },
  {
    timestamps: false,
    collection: 'sessions',
  }
);

sessionSchema.index({ userId: 1, isActive: 1 });
sessionSchema.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });

sessionSchema.methods.updateActivity = function () {
  this.lastActivity = new Date();
  return this.save();
};

sessionSchema.methods.invalidate = function () {
  this.isActive = false;
  this.expiresAt = new Date();
  return this.save();
};

sessionSchema.statics.invalidateAllForUser = async function (userId) {
  return this.updateMany(
    { userId, isActive: true },
    {
      $set: {
        isActive: false,
        expiresAt: new Date(),
      },
    }
  );
};

const Session =
  mongoose.models.Session || mongoose.model('Session', sessionSchema);

export default Session;

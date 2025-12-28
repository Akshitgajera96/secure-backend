import mongoose from 'mongoose';

const blockedIpSchema = new mongoose.Schema(
  {
    ip: {
      type: String,
      required: true,
      unique: true,
      trim: true,
      index: true,
    },
    reason: {
      type: String,
      default: '',
    },
    blockedBy: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'User',
    },
    expiresAt: {
      type: Date,
      default: () => new Date(Date.now() + 24 * 60 * 60 * 1000),
    },
    isActive: {
      type: Boolean,
      default: true,
    },
    createdAt: {
      type: Date,
      default: Date.now,
    },
  },
  {
    timestamps: false,
    collection: 'blocked_ips',
  }
);

blockedIpSchema.index({ ip: 1, isActive: 1 });
blockedIpSchema.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });

blockedIpSchema.statics.isBlocked = async function (ip) {
  const blocked = await this.findOne({
    ip,
    isActive: true,
    $or: [{ expiresAt: { $exists: false } }, { expiresAt: { $gt: new Date() } }],
  });
  return !!blocked;
};

const BlockedIp =
  mongoose.models.BlockedIp || mongoose.model('BlockedIp', blockedIpSchema);

export default BlockedIp;

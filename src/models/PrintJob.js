import mongoose from 'mongoose';

const auditEntrySchema = new mongoose.Schema(
  {
    at: { type: Date, default: Date.now },
    event: { type: String, required: true },
    details: { type: mongoose.Schema.Types.Mixed, default: null },
  },
  { _id: false }
);

const printJobSchema = new mongoose.Schema(
  {
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },

    sourcePdfKey: { type: String, required: true },

    metadata: { type: mongoose.Schema.Types.Mixed, required: true },

    payloadHmac: { type: String, required: true },

    status: {
      type: String,
      enum: ['PENDING', 'RUNNING', 'DONE', 'FAILED', 'EXPIRED'],
      default: 'PENDING',
      index: true,
    },

    progress: { type: Number, default: 0 },
    totalPages: { type: Number, default: 1 },

    output: {
      key: { type: String, default: null },
      url: { type: String, default: null },
      expiresAt: { type: Date, default: null, index: true },
    },

    error: {
      message: { type: String, default: null },
      stack: { type: String, default: null },
    },

    audit: { type: [auditEntrySchema], default: [] },
  },
  { timestamps: true }
);

const PrintJob = mongoose.models.PrintJob || mongoose.model('PrintJob', printJobSchema);

export default PrintJob;

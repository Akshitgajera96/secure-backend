import mongoose from 'mongoose';

const printLogSchema = new mongoose.Schema(
  {
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    documentId: { type: mongoose.Schema.Types.ObjectId, ref: 'Document', required: true },
    count: { type: Number, default: 1 },
    meta: { type: Object },
  },
  { timestamps: true }
);

const PrintLog = mongoose.models.PrintLog || mongoose.model('PrintLog', printLogSchema);

export default PrintLog;

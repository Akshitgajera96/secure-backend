import mongoose from 'mongoose';

const documentAccessSchema = new mongoose.Schema(
  {
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    documentId: { type: mongoose.Schema.Types.ObjectId, ref: 'Document', required: true },
    assignedQuota: { type: Number, required: true },
    usedPrints: { type: Number, default: 0 },
    sessionToken: { type: String, index: true, unique: true, sparse: true },
  },
  { timestamps: true }
);

const DocumentAccess =
  mongoose.models.DocumentAccess || mongoose.model('DocumentAccess', documentAccessSchema);

export default DocumentAccess;

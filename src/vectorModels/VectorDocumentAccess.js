import mongoose from 'mongoose';

const documentAccessSchema = new mongoose.Schema(
  {
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    documentId: { type: mongoose.Schema.Types.ObjectId, ref: 'Document', required: true },
    assignedQuota: { type: Number, required: true },
    usedPrints: { type: Number, default: 0 },
    printQuota: { type: Number, default: null },
    printsUsed: { type: Number, default: 0 },
    lastPrintAt: { type: Date, default: null },
    revoked: { type: Boolean, default: false },
    sessionToken: { type: String, index: true, unique: true, sparse: true },
  },
  { timestamps: true, collection: 'vector_documentaccesses' }
);

documentAccessSchema.index({ documentId: 1, userId: 1 }, { unique: true });

const VectorDocumentAccess =
  mongoose.models.VectorDocumentAccess || mongoose.model('VectorDocumentAccess', documentAccessSchema);

export default VectorDocumentAccess;

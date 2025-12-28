import mongoose from 'mongoose';

const documentSchema = new mongoose.Schema(
  {
    title: { type: String, required: true },
    fileKey: { type: String, required: true },
    fileUrl: { type: String, required: true },
    totalPrints: { type: Number, required: true },
    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    mimeType: { type: String, default: 'application/pdf' },
    documentType: { type: String, default: 'source' }, // e.g. 'source', 'generated-output'
  },
  { timestamps: true }
);

const Document = mongoose.models.Document || mongoose.model('Document', documentSchema);

export default Document;

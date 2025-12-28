import mongoose from 'mongoose';

const printLogSchema = new mongoose.Schema(
  {
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    documentId: { type: mongoose.Schema.Types.ObjectId, ref: 'Document', required: true },
    count: { type: Number, default: 1 },
    meta: { type: Object },
  },
  { timestamps: true, collection: 'vector_printlogs' }
);

const VectorPrintLog = mongoose.models.VectorPrintLog || mongoose.model('VectorPrintLog', printLogSchema);

export default VectorPrintLog;

import mongoose from 'mongoose';

const printConsumptionSchema = new mongoose.Schema(
  {
    documentId: { type: mongoose.Schema.Types.ObjectId, ref: 'Document', required: true, index: true },
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
    requestId: { type: String, required: true },
    remainingAfter: { type: Number, default: null },
    consumedAt: { type: Date, default: Date.now },
  },
  { timestamps: true, collection: 'vector_print_consumptions' }
);

printConsumptionSchema.index({ documentId: 1, userId: 1, requestId: 1 }, { unique: true });

const VectorPrintConsumption =
  mongoose.models.VectorPrintConsumption || mongoose.model('VectorPrintConsumption', printConsumptionSchema);

export default VectorPrintConsumption;

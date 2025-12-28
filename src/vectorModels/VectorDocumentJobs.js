import mongoose from 'mongoose';

const documentJobSchema = new mongoose.Schema(
  {
    email: { type: String, required: true },
    assignedQuota: { type: Number, required: true },
    // Optional lightweight copy or summary of layout; full pages are kept only in the queue payload
    layoutPages: { type: Array, default: [] },
    totalPages: { type: Number, default: 0 },
    completedPages: { type: Number, default: 0 },
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    outputDocumentId: { type: mongoose.Schema.Types.ObjectId, ref: 'Document', default: null },
    status: {
      type: String,
      enum: ['pending', 'processing', 'completed', 'failed'],
      default: 'pending',
    },
    stage: {
      type: String,
      enum: [
        'pending',
        'rendering',
        'merging',
        'vector-rendering',
        'validation-failed',
        'render-failed',
        'completed',
        'failed',
      ],
      default: 'pending',
    },
    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
  },
  {
    timestamps: true,
    collection: 'vector_documentjobs',
  }
);

const VectorDocumentJobs =
  mongoose.models.VectorDocumentJobs || mongoose.model('VectorDocumentJobs', documentJobSchema);

export default VectorDocumentJobs;

import mongoose from 'mongoose';

const assignedPdfSchema = new mongoose.Schema(
  {
    userEmail: {
      type: String,
      required: true,
      lowercase: true,
      trim: true,
    },
    s3Key: {
      type: String,
      required: true,
      trim: true,
    },
    pdfUrl: {
      type: String,
      required: true,
      trim: true,
    },
    printLimit: {
      type: Number,
      required: true,
      min: 1,
    },
    printsUsed: {
      type: Number,
      default: 0,
      min: 0,
    },
    assignedBy: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'User',
      required: true,
    },
    assignedAt: {
      type: Date,
      default: Date.now,
    },
  },
  {
    timestamps: true,
  }
);

assignedPdfSchema.index({ userEmail: 1 });
assignedPdfSchema.index({ s3Key: 1 });

const AssignedPdf = mongoose.models.AssignedPdf || mongoose.model('AssignedPdf', assignedPdfSchema);

export default AssignedPdf;

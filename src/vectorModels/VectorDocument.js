import mongoose from 'mongoose';

const documentSchema = new mongoose.Schema(
  {
    title: { type: String, required: true },
    fileKey: { type: String, required: true },
    fileUrl: { type: String, required: true },
    sourceFileKey: { type: String, default: null },
    sourceMimeType: { type: String, default: null },
    totalPrints: { type: Number, required: true },
    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    mimeType: { type: String, default: 'application/pdf' },
    documentType: { type: String, default: 'source' }, // e.g. 'source', 'generated-output'

    editorProxy: {
      page: {
        widthPt: { type: Number, default: null },
        heightPt: { type: Number, default: null },
      },
      contentBBox: {
        x: { type: Number, default: null },
        y: { type: Number, default: null },
        width: { type: Number, default: null },
        height: { type: Number, default: null },
      },
      viewBox: {
        x: { type: Number, default: null },
        y: { type: Number, default: null },
        width: { type: Number, default: null },
        height: { type: Number, default: null },
      },
      viewBoxTransform: {
        scale: { type: Number, default: null },
        translateX: { type: Number, default: null },
        translateY: { type: Number, default: null },
      },
    },

    placementRules: {
      seriesPlacement: {
        anchor: { type: String, default: null },
        offset: {
          x: { type: Number, default: null },
          y: { type: Number, default: null },
        },
        rotation: { type: Number, default: 0 },
      },
    },

    svgNormalizeStatus: {
      type: String,
      enum: ['PENDING', 'RUNNING', 'DONE', 'FAILED'],
      default: null,
      index: true,
    },

    svgNormalizeError: {
      message: { type: String, default: null },
      stack: { type: String, default: null },
    },

    svgNormalizeEnqueuedAt: { type: Date, default: null, index: true },
    svgNormalizeJobId: { type: String, default: null },
    svgNormalizeStartedAt: { type: Date, default: null },

    normalizedAt: { type: Date, default: null },
    normalizeFailed: { type: Boolean, default: false, index: true },
    normalizeError: {
      message: { type: String, default: null },
      stack: { type: String, default: null },
      reason: { type: String, default: null },
      at: { type: Date, default: null },
    },

    colorMode: { type: String, enum: ['RGB', 'CMYK'], default: 'RGB' },
    exportVersion: { type: Number, default: 0 },
    
    // Optional layout support fields for deterministic rendering (backward-compatible)
    objectHeight: { type: Number, optional: true }, // Height of detected object for layout calculations
    seriesBaseOffset: { 
      type: { x: Number, y: Number }, 
      optional: true 
    }, // Base offset for series positioning relative to object
    layoutReferenceVersion: { type: String, optional: true }, // Version identifier for layout algorithm
  },
  { timestamps: true, collection: 'vector_documents' }
);

const VectorDocument = mongoose.models.VectorDocument || mongoose.model('VectorDocument', documentSchema);

export default VectorDocument;

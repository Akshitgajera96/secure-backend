// Feature flags for gradual vector migration rollout
export const FEATURE_FLAGS = {
  // Enable vector pipeline vs old raster pipeline
  USE_VECTOR_PIPELINE: process.env.ENABLE_VECTOR_PIPELINE === 'true',
  
  // Enable vector API endpoints
  ENABLE_VECTOR_API: process.env.ENABLE_VECTOR_API === 'true',
  
  // Force vector mode for testing (bypasses feature flag checks)
  FORCE_VECTOR_MODE: process.env.FORCE_VECTOR_MODE === 'true',
  
  // Enable vector worker processing
  ENABLE_VECTOR_WORKER: process.env.ENABLE_VECTOR_WORKER === 'true',
  
  // Keep old raster pipeline for fallback
  KEEP_RASTER_FALLBACK: process.env.KEEP_RASTER_FALLBACK !== 'false',
};

// Helper to check if vector features are enabled
export const isVectorEnabled = (feature = 'pipeline') => {
  switch (feature) {
    case 'pipeline':
      return FEATURE_FLAGS.USE_VECTOR_PIPELINE || FEATURE_FLAGS.FORCE_VECTOR_MODE;
    case 'api':
      return FEATURE_FLAGS.ENABLE_VECTOR_API || FEATURE_FLAGS.FORCE_VECTOR_MODE;
    case 'worker':
      return FEATURE_FLAGS.ENABLE_VECTOR_WORKER || FEATURE_FLAGS.FORCE_VECTOR_MODE;
    default:
      return FEATURE_FLAGS.FORCE_VECTOR_MODE;
  }
};

// Helper to get current mode
export const getProcessingMode = () => {
  if (FEATURE_FLAGS.FORCE_VECTOR_MODE) return 'vector-forced';
  if (FEATURE_FLAGS.USE_VECTOR_PIPELINE) return 'vector';
  return 'raster';
};

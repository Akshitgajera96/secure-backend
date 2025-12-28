import VectorDocument from '../vectorModels/VectorDocument.js';

export async function resolveExportColorMode(documentId) {
  const doc = await VectorDocument.findById(documentId).select('colorMode').exec();
  if (doc && doc.colorMode === 'CMYK') return 'CMYK';
  return 'RGB';
}

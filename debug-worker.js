import { startVectorPdfWorkers } from './src/workers/vectorPdfWorker.js';

console.log('[debug] Starting worker debug...');
startVectorPdfWorkers().then(() => {
  console.log('[debug] Workers started successfully');
}).catch(err => {
  console.error('[debug] Worker startup failed:', err);
  console.error('[debug] Stack:', err.stack);
});

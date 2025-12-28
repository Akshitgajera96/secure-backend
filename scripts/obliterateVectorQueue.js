import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
import Redis from 'ioredis';
import { Queue } from 'bullmq';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config({ path: path.resolve(__dirname, '../.env') });

const QUEUE_NAME = 'vectorPdfQueue';

const main = async () => {
  const yes = process.argv.includes('--yes') || process.argv.includes('--force');
  if (!yes) {
    console.error('[QUEUE_OBLITERATE] Refusing to run without --yes');
    console.error('Usage: node scripts/obliterateVectorQueue.js --yes');
    process.exit(2);
  }

  const redisUrl = typeof process.env.REDIS_URL === 'string' ? process.env.REDIS_URL.trim() : '';
  if (!redisUrl) {
    console.error('[QUEUE_OBLITERATE] REDIS_URL not set');
    process.exit(1);
  }

  const redisTlsEnabled =
    String(process.env.REDIS_TLS || '').toLowerCase() === 'true' ||
    redisUrl.startsWith('rediss://');

  const connection = new Redis(redisUrl, {
    ...(redisTlsEnabled ? { tls: {} } : {}),
    enableReadyCheck: true,
    maxRetriesPerRequest: null,
  });

  const q = new Queue(QUEUE_NAME, { connection });

  try {
    const before = await q.getJobCounts().catch(() => null);
    console.log('[QUEUE_OBLITERATE] before', before);

    await q.obliterate({ force: true });

    const after = await q.getJobCounts().catch(() => null);
    console.log('[QUEUE_OBLITERATE] after', after);

    console.log('[QUEUE_OBLITERATE] done');
  } finally {
    try {
      await q.close();
    } catch {
      // ignore
    }
    try {
      connection.disconnect();
    } catch {
      // ignore
    }
  }
};

main().catch((err) => {
  console.error('[QUEUE_OBLITERATE] failed', { message: err?.message || 'unknown' });
  process.exit(1);
});

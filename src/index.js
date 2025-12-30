import express from 'express';
import mongoose from 'mongoose';
import dotenv from 'dotenv';
import Redis from 'ioredis';
import { setRedisConnection, connection } from '../queues/vectorQueue.js';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, '../.env') });

const app = express();
const PORT = process.env.PORT || 8001;

app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', '*');
  res.setHeader('Access-Control-Allow-Headers', '*');
  res.setHeader('Access-Control-Expose-Headers', '*');

  if (req.method === 'OPTIONS') {
    return res.status(204).end();
  }

  next();
});

const BODY_LIMIT = process.env.BODY_LIMIT || '1gb';

app.use(express.json({ limit: BODY_LIMIT }));
app.use(express.urlencoded({ extended: true, limit: BODY_LIMIT }));

/* -------------------------------- start ---------------------------------- */

async function start() {
  const version = process.env.RAILWAY_GIT_COMMIT_SHA || process.env.GIT_COMMIT_SHA || 'unknown';
  console.log('[Build]', {
    version,
    railwayDeploymentId: process.env.RAILWAY_DEPLOYMENT_ID || null,
    railwayServiceId: process.env.RAILWAY_SERVICE_ID || null,
    railwayEnvironment: process.env.RAILWAY_ENVIRONMENT_NAME || null,
  });

  const { default: bcrypt } = await import('bcryptjs');
  const { default: VectorUser } = await import('./vectorModels/VectorUser.js');
  const { default: authRoutes } = await import('./routes/auth.js');
  const { default: adminRoutes } = await import('./routes/admin.js');
  const { default: adminUsersRoutes } = await import('./routes/adminUsers.js');
  const { default: docsRoutes } = await import('./routes/docs.js');
  const { default: vectorRoutes } = await import('./routes/vectorRoutes.js');
  const { default: vectorJobRoutes } = await import('./routes/vectorJobRoutes.js');
  const { default: printRoutes } = await import('./routes/printRoutes.js');
  const { default: downloadRoutes } = await import('./routes/downloadRoutes.js');
  const { startVectorPdfWorkers, getVectorFlowProducer } = await import('./workers/vectorPdfWorker.js');
  const { startSvgNormalizeWorker } = await import('./workers/svgNormalizeWorker.js');
  const { startJobCleanupLoop } = await import('./services/jobCleanup.js');
  const { inkscapeAvailabilityState, probeInkscape } = await import('./vector/vectorLayoutEngine.js');

  const ensureAdminUser = async () => {
    const email = process.env.ADMIN_SEED_EMAIL?.trim();
    const pass = process.env.ADMIN_SEED_PASSWORD;

    if (!email || !pass) {
      console.warn('[AdminSeed] skipped (env not set)');
      return;
    }

    const count = await mongoose.connection.db
      .collection('users')
      .estimatedDocumentCount()
      .catch(() => 0);

    if (count > 0) return;

    const exists = await VectorUser.findOne({ email: email.toLowerCase() });
    if (exists) return;

    const hash = await bcrypt.hash(pass, 10);
    await VectorUser.create({
      email: email.toLowerCase(),
      passwordHash: hash,
      role: 'admin',
    });
  };

  const mongoUri = process.env.MONGO_URI?.trim();
  if (!mongoUri) {
    console.error('[MongoDB] MONGO_URI not set');
    process.exit(1);
  }

  try {
    await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 5000 });
    console.log('[MongoDB] connected');
    await ensureAdminUser();
  } catch (e) {
    console.error('[MongoDB] connect failed', { message: e?.message || 'unknown' });
    process.exit(1);
  }

  app.get('/api/health', (req, res) => {
    let flowProducer = null;
    try {
      flowProducer = getVectorFlowProducer();
    } catch {}

    res.json({
      status: 'ok',
      backendVersion: process.env.RAILWAY_GIT_COMMIT_SHA || process.env.GIT_COMMIT_SHA || 'unknown',
      workersEnabled: String(process.env.ENABLE_WORKERS || '') === 'true',
      ipSecurityEnabled: false,
      redisAvailable: Boolean(flowProducer),
      inkscapeAvailable: inkscapeAvailabilityState() === true,
    });
  });

  app.use((req, res, next) => {
    const version =
      process.env.RAILWAY_GIT_COMMIT_SHA ||
      process.env.GIT_COMMIT_SHA ||
      'unknown';

    res.setHeader('X-Backend-Version', version);
    next();
  });

  app.use('/api/auth', authRoutes);
  app.use('/api/admin', adminRoutes);
  app.use('/api/admin', adminUsersRoutes);
  app.use('/api/docs', docsRoutes);
  app.use('/api/vector', vectorRoutes);
  app.use('/api/vector', vectorJobRoutes);
  app.use('/api', printRoutes);
  app.use('/api/download', downloadRoutes);

  const redisUrl = typeof process.env.REDIS_URL === 'string' ? process.env.REDIS_URL.trim() : '';
  if (!redisUrl) {
    console.warn('[Redis] REDIS_URL not set (workers/queues disabled)');
    setRedisConnection(null);
  } else {
    try {
      const redisTlsEnabled =
        String(process.env.REDIS_TLS || '').toLowerCase() === 'true' ||
        redisUrl.startsWith('rediss://');

      const redis = new Redis(redisUrl, {
        ...(redisTlsEnabled ? { tls: {} } : {}),
        enableReadyCheck: true,
        maxRetriesPerRequest: null,
      });

      redis.on('ready', () => {
        console.log('[Redis] Connected');
      });

      let warned = false;
      redis.on('error', (err) => {
        if (warned) return;
        warned = true;
        const code = err?.code || err?.errno;
        console.warn('[Redis] Error (non-fatal)', { code: code || 'unknown' });
        setTimeout(() => {
          warned = false;
        }, 10_000);
      });

      setRedisConnection(redis);
    } catch (e) {
      console.warn('[Redis] non-fatal:', e?.message);
      setRedisConnection(null);
    }
  }

  const server = app.listen(PORT, '0.0.0.0', () => {
    console.log(`[Server] Backend listening on port ${PORT}`);

    if (String(process.env.ENABLE_WORKERS || '') === 'true') {
      if (!mongoose.connection.readyState) {
        console.error('[WORKERS_ABORTED] Mongo not connected');
        process.exit(1);
      }

      try {
        startVectorPdfWorkers();
      } catch (e) {
        console.error('[Workers] failed', { message: e?.message || 'unknown' });
        process.exit(1);
      }

      try {
        if (connection?.status === 'ready') {
          startSvgNormalizeWorker();
        } else if (connection && typeof connection.once === 'function') {
          connection.once('ready', () => {
            try {
              startSvgNormalizeWorker();
            } catch {}
          });
        }
      } catch {}
    }
  });

  // Increase timeout for large file uploads
  server.timeout = 100 * 60 * 1000; // 10 minutes
  server.keepAliveTimeout = 6500 * 1000; // 65 seconds
  server.headersTimeout = 6600 * 1000; // 66 seconds

  server.on('clientError', (err, socket) => {
    if (err?.code === 'ECONNRESET' || err?.code === 'EPIPE') {
      try { socket.destroy(); } catch {}
      return;
    }
    socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
  });

  try {
    await probeInkscape();
    inkscapeAvailabilityState()
      ? console.log('[Inkscape] ready')
      : console.warn('[Inkscape] unavailable');
  } catch {}

  try {
    startJobCleanupLoop();
  } catch {}
}

start();

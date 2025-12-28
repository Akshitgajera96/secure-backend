import mongoose from 'mongoose';
import dotenv from 'dotenv';

import VectorUser from '../src/vectorModels/VectorUser.js';
import VectorSession from '../src/vectorModels/VectorSession.js';
import VectorDocument from '../src/vectorModels/VectorDocument.js';
import VectorDocumentAccess from '../src/vectorModels/VectorDocumentAccess.js';
import VectorDocumentJobs from '../src/vectorModels/VectorDocumentJobs.js';
import VectorPrintJob from '../src/vectorModels/VectorPrintJob.js';
import VectorPrintLog from '../src/vectorModels/VectorPrintLog.js';
import VectorBlockedIp from '../src/vectorModels/VectorBlockedIp.js';

dotenv.config();

const parseArg = (name, defaultValue) => {
  const prefix = `--${name}=`;
  const hit = process.argv.find((a) => a.startsWith(prefix));
  if (!hit) return defaultValue;
  return hit.slice(prefix.length);
};

const BATCH_SIZE = Number(parseArg('batchSize', '1000'));
const ENSURE_INDEXES = parseArg('ensureIndexes', 'true') !== 'false';

const COLLECTIONS = [
  {
    name: 'User',
    source: 'users',
    target: 'vector_users',
    vectorModel: VectorUser,
  },
  {
    name: 'Session',
    source: 'sessions',
    target: 'vector_sessions',
    vectorModel: VectorSession,
  },
  {
    name: 'Document',
    source: 'documents',
    target: 'vector_documents',
    vectorModel: VectorDocument,
  },
  {
    name: 'DocumentAccess',
    source: 'documentaccesses',
    target: 'vector_documentaccesses',
    vectorModel: VectorDocumentAccess,
  },
  {
    name: 'DocumentJobs',
    source: 'documentjobs',
    target: 'vector_documentjobs',
    vectorModel: VectorDocumentJobs,
  },
  {
    name: 'PrintJob',
    source: 'printjobs',
    target: 'vector_printjobs',
    vectorModel: VectorPrintJob,
  },
  {
    name: 'PrintLog',
    source: 'printlogs',
    target: 'vector_printlogs',
    vectorModel: VectorPrintLog,
  },
  {
    name: 'BlockedIp',
    source: 'blocked_ips',
    target: 'vector_blocked_ips',
    vectorModel: VectorBlockedIp,
  },
];

const log = (...args) => console.log(new Date().toISOString(), '-', ...args);

const migrateCollection = async ({ name, source, target }) => {
  const db = mongoose.connection.db;
  const sourceCol = db.collection(source);
  const targetCol = db.collection(target);

  const srcCount = await sourceCol.estimatedDocumentCount();
  const tgtCountBefore = await targetCol.estimatedDocumentCount().catch(() => 0);

  log(`[${name}] source=${source} (${srcCount}) -> target=${target} (before=${tgtCountBefore}) batchSize=${BATCH_SIZE}`);

  const cursor = sourceCol.find({}, { batchSize: BATCH_SIZE });

  let processed = 0;
  let upserted = 0;
  let batch = [];

  const flush = async () => {
    if (batch.length === 0) return;

    const ops = batch.map((doc) => {
      const { _id, ...rest } = doc;
      return {
        updateOne: {
          filter: { _id },
          update: { $setOnInsert: rest },
          upsert: true,
        },
      };
    });

    try {
      const res = await targetCol.bulkWrite(ops, { ordered: false });
      upserted += Number(res.upsertedCount || 0);
      processed += batch.length;
    } catch (err) {
      // Idempotency rule: do NOT overwrite. If a unique index causes conflicts
      // (e.g. same email already exists in vector_users), we skip those inserts
      // and continue migration.
      const code = err?.code;
      if (code !== 11000) {
        throw err;
      }

      const result = err?.result;
      const upsertedCount = Number(result?.upsertedCount || 0);
      upserted += upsertedCount;
      processed += batch.length;

      log(`[${name}] WARN duplicate key conflicts (E11000). Continuing. upsertedInBatch=${upsertedCount}`);
    }

    if (processed % (BATCH_SIZE * 10) === 0) {
      log(`[${name}] processed=${processed}/${srcCount} upserted=${upserted}`);
    }

    batch = [];
  };

  for await (const doc of cursor) {
    batch.push(doc);
    if (batch.length >= BATCH_SIZE) {
      await flush();
    }
  }

  await flush();

  const tgtCountAfter = await targetCol.estimatedDocumentCount();
  log(`[${name}] DONE processed=${processed} upserted=${upserted} target(after)=${tgtCountAfter}`);
};

async function main() {
  const mongoUri = typeof process.env.MONGO_URI === 'string' ? process.env.MONGO_URI.trim() : '';
  if (!mongoUri) {
    throw new Error('MONGO_URI is not set');
  }

  await mongoose.connect(mongoUri);
  log('Connected to MongoDB');

  if (ENSURE_INDEXES) {
    log('Ensuring Vector indexes...');
    await VectorUser.createIndexes();
    await VectorSession.createIndexes();
    await VectorDocument.createIndexes();
    await VectorDocumentAccess.createIndexes();
    await VectorDocumentJobs.createIndexes();
    await VectorPrintJob.createIndexes();
    await VectorPrintLog.createIndexes();
    await VectorBlockedIp.createIndexes();
    log('Indexes ensured');
  }

  for (const cfg of COLLECTIONS) {
    await migrateCollection(cfg);
  }

  log('All migrations complete');

  await mongoose.disconnect();
}

main().catch(async (err) => {
  console.error(err);
  try {
    await mongoose.disconnect();
  } catch {}
  process.exit(1);
});

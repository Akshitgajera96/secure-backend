import VectorDocumentAccess from '../vectorModels/VectorDocumentAccess.js';
import { getRedisClient } from './redisClient.js';

const quotaKey = (documentId, userId) => `print_quota:${documentId}:${userId}`;
const reqKey = (documentId, userId, requestId) => `print_req:${documentId}:${userId}:${requestId}`;

const IDEMPOTENCY_TTL_SECONDS = 300;

export const ATOMIC_PRINT_DECREMENT = `
-- KEYS[1] = quota key
local remaining = tonumber(redis.call("HGET", KEYS[1], "remaining"))

if not remaining then
  return -2 -- cache miss
end

if remaining <= 0 then
  return -1 -- quota exceeded
end

redis.call("HINCRBY", KEYS[1], "remaining", -1)
return remaining - 1
`;

const ensureDbQuotaInitialized = async (documentId, userId) => {
  const access = await VectorDocumentAccess.findOne({ documentId, userId }).exec();
  if (!access) {
    const err = new Error('Access not found');
    err.code = 'NO_ACCESS';
    throw err;
  }
  if (access.revoked) {
    const err = new Error('Access revoked');
    err.code = 'REVOKED';
    throw err;
  }

  const printQuota = Number.isFinite(access.printQuota) && access.printQuota !== null
    ? Number(access.printQuota)
    : Number(access.assignedQuota || 0);

  // Ensure DB has printQuota populated (additive) so DB fallback works.
  if (access.printQuota === null || access.printQuota === undefined) {
    await VectorDocumentAccess.updateOne(
      { _id: access._id },
      { $set: { printQuota } }
    ).exec();
  }

  const printsUsed = Number.isFinite(access.printsUsed) ? Number(access.printsUsed) : 0;
  const usedLegacy = Number.isFinite(access.usedPrints) ? Number(access.usedPrints) : 0;
  const used = Math.max(printsUsed, usedLegacy);

  return { accessId: access._id, printQuota, printsUsed: used };
};

const seedRedisRemaining = async (redis, documentId, userId, remaining) => {
  const key = quotaKey(documentId, userId);
  await redis.hset(key, 'remaining', String(Math.max(0, remaining)));
};

const dbOptimisticConsume = async (documentId, userId) => {
  const now = new Date();

  const access = await VectorDocumentAccess.findOne({ documentId, userId }).exec();
  if (!access) {
    const err = new Error('Access not found');
    err.code = 'NO_ACCESS';
    throw err;
  }

  const printQuota = Number.isFinite(access.printQuota) && access.printQuota !== null
    ? Number(access.printQuota)
    : Number(access.assignedQuota || 0);

  // Ensure DB has printQuota populated (additive)
  if (access.printQuota === null || access.printQuota === undefined) {
    await VectorDocumentAccess.updateOne({ _id: access._id }, { $set: { printQuota } }).exec();
  }

  const res = await VectorDocumentAccess.updateOne(
    {
      documentId,
      userId,
      revoked: false,
      printsUsed: { $lt: printQuota },
    },
    {
      $inc: { printsUsed: 1, usedPrints: 1 },
      $set: { lastPrintAt: now },
    }
  ).exec();

  if (!res || res.matchedCount === 0) {
    const err = new Error('Print limit exceeded');
    err.code = 'LIMIT';
    throw err;
  }
};

// REQUIRED SIGNATURE (DO NOT CHANGE)
export async function assertAndConsumePrintQuota(documentId, userId, requestId) {
  const docId = String(documentId);
  const usrId = String(userId);
  const rid = String(requestId || '').trim();
  if (!rid) {
    const err = new Error('requestId is required');
    err.code = 'BAD_REQUEST';
    throw err;
  }

  const redis = getRedisClient();

  // STEP 1 — Idempotency Gate (Redis SET NX EX 300)
  if (redis) {
    try {
      const ok = await redis.set(reqKey(docId, usrId, rid), '1', 'NX', 'EX', IDEMPOTENCY_TTL_SECONDS);
      if (!ok) {
        return;
      }

      // STEP 2 — Redis Atomic Decrement
      let dec = await redis.eval(ATOMIC_PRINT_DECREMENT, 1, quotaKey(docId, usrId));

      // STEP 3 — Cache Miss (-2): seed from DB and retry once
      if (Number(dec) === -2) {
        const { printQuota, printsUsed } = await ensureDbQuotaInitialized(docId, usrId);
        await seedRedisRemaining(redis, docId, usrId, printQuota - printsUsed);
        dec = await redis.eval(ATOMIC_PRINT_DECREMENT, 1, quotaKey(docId, usrId));
      }

      if (Number(dec) === -1) {
        const err = new Error('Print limit exceeded');
        err.code = 'LIMIT';
        throw err;
      }

      // STEP 4 — Write-Behind DB Update (single atomic update)
      const now = new Date();
      const access = await VectorDocumentAccess.findOne({ documentId: docId, userId: usrId }).exec();
      const printQuota = access && Number.isFinite(access.printQuota) && access.printQuota !== null
        ? Number(access.printQuota)
        : Number(access?.assignedQuota || 0);

      if (access && (access.printQuota === null || access.printQuota === undefined)) {
        await VectorDocumentAccess.updateOne({ _id: access._id }, { $set: { printQuota } }).exec();
      }

      await VectorDocumentAccess.updateOne(
        { documentId: docId, userId: usrId, revoked: false },
        { $inc: { printsUsed: 1, usedPrints: 1 }, $set: { lastPrintAt: now } }
      ).exec();

      return;
    } catch (e) {
      // Redis down or any redis path failure -> DB fallback below
    }
  }

  // STEP 5 — DB Fallback (Optimistic Concurrency)
  await dbOptimisticConsume(docId, usrId);
}

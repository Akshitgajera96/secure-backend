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

const loadAndComputeRemainingFromDb = async (documentId, userId) => {
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

  const printQuota =
    Number.isFinite(access.printQuota) && access.printQuota !== null
      ? Number(access.printQuota)
      : Number(access.assignedQuota || 0);

  const printsUsed = Number.isFinite(access.printsUsed) ? Number(access.printsUsed) : 0;
  const usedLegacy = Number.isFinite(access.usedPrints) ? Number(access.usedPrints) : 0;
  const used = Math.max(printsUsed, usedLegacy);

  // Keep additive fields populated for correctness going forward.
  if (access.printQuota === null || access.printQuota === undefined || access.printsUsed !== used) {
    await VectorDocumentAccess.updateOne(
      { _id: access._id },
      {
        $set: {
          printQuota,
          printsUsed: used,
        },
      }
    ).exec();
  }

  return Math.max(0, printQuota - used);
};

const seedRedisRemaining = async (redis, documentId, userId, remaining) => {
  await redis.hset(quotaKey(documentId, userId), 'remaining', String(Math.max(0, remaining)));
};

const writeBehindDbConsume = async (documentId, userId) => {
  const now = new Date();
  await VectorDocumentAccess.updateOne(
    { documentId, userId, revoked: false },
    { $inc: { printsUsed: 1 }, $set: { lastPrintAt: now } }
  ).exec();
};

const dbFallbackOptimisticConsume = async (documentId, userId) => {
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

  const printQuota =
    Number.isFinite(access.printQuota) && access.printQuota !== null
      ? Number(access.printQuota)
      : Number(access.assignedQuota || 0);

  if (access.printQuota === null || access.printQuota === undefined) {
    await VectorDocumentAccess.updateOne({ _id: access._id }, { $set: { printQuota } }).exec();
  }

  const now = new Date();
  const res = await VectorDocumentAccess.updateOne(
    {
      documentId,
      userId,
      revoked: false,
      printsUsed: { $lt: printQuota },
    },
    { $inc: { printsUsed: 1 }, $set: { lastPrintAt: now } }
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
  if (redis) {
    const idempotencyKey = reqKey(docId, usrId, rid);

    // STEP 1 — Idempotency Gate
    let idempotencyOk = null;
    try {
      idempotencyOk = await redis.set(idempotencyKey, '1', 'NX', 'EX', IDEMPOTENCY_TTL_SECONDS);
    } catch {
      idempotencyOk = null;
    }
    if (idempotencyOk === null) {
      // Redis unreachable
      await dbFallbackOptimisticConsume(docId, usrId);
      return;
    }
    if (!idempotencyOk) return;

    // STEP 2 — Redis Atomic Decrement
    let result;
    try {
      result = await redis.eval(ATOMIC_PRINT_DECREMENT, 1, quotaKey(docId, usrId));
    } catch {
      await dbFallbackOptimisticConsume(docId, usrId);
      return;
    }

    // STEP 3 — Cache Miss Handling (-2)
    if (Number(result) === -2) {
      const remaining = await loadAndComputeRemainingFromDb(docId, usrId);
      try {
        await seedRedisRemaining(redis, docId, usrId, remaining);
        result = await redis.eval(ATOMIC_PRINT_DECREMENT, 1, quotaKey(docId, usrId));
      } catch {
        await dbFallbackOptimisticConsume(docId, usrId);
        return;
      }
    }

    if (Number(result) === -1) {
      try {
        await redis.del(idempotencyKey);
      } catch {
        // ignore
      }
      const err = new Error('Print limit exceeded');
      err.code = 'LIMIT';
      throw err;
    }

    // STEP 4 — Write-Behind DB Update
    await writeBehindDbConsume(docId, usrId);
    return;
  }

  // STEP 5 — DB Fallback (Redis Down)
  await dbFallbackOptimisticConsume(docId, usrId);
}

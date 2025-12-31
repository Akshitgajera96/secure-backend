import crypto from 'crypto';

const isHmacDisabled = () => String(process.env.DISABLE_JOB_HMAC || '') === 'true';

const stableStringify = (value) => {
  if (value === null || value === undefined) return JSON.stringify(value);
  if (typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map((v) => stableStringify(v)).join(',')}]`;

  const keys = Object.keys(value)
    .filter((k) => value[k] !== undefined)
    .sort();
  const entries = keys.map((k) => `${JSON.stringify(k)}:${stableStringify(value[k])}`);
  return `{${entries.join(',')}}`;
};

export function canonicalizePayload(payload) {
  return JSON.parse(JSON.stringify(payload));
}

export function buildCanonicalJobPayload(metadata) {
  const m = metadata && typeof metadata === 'object' ? metadata : {};
  return {
    documentId: m.documentId ?? null,
    pages: m.pages ?? null,
    pageSize: m.pageSize ?? null,
    placementRules: m.placementRules ?? null,
    seriesConfig: m.seriesConfig ?? null,
    outputKey: m.outputKey ?? null,
  };
}

export function signJobPayload(metadata) {
  if (isHmacDisabled()) {
    return crypto.randomBytes(32).toString('hex');
  }
  const secret = process.env.JOB_PAYLOAD_HMAC_SECRET;
  if (!secret) {
    throw new Error('JOB_PAYLOAD_HMAC_SECRET not configured');
  }

  const canonicalPayload = buildCanonicalJobPayload(metadata);
  const payloadString = stableStringify(canonicalPayload);

  return crypto.createHmac('sha256', secret).update(payloadString).digest('hex');
}

export function verifyJobPayload(metadata, expectedHmac) {
  if (isHmacDisabled()) return true;
  if (typeof expectedHmac !== 'string' || !expectedHmac) return false;

  const secret = process.env.JOB_PAYLOAD_HMAC_SECRET;
  if (!secret) {
    throw new Error('JOB_PAYLOAD_HMAC_SECRET not configured');
  }

  const canonicalPayload = buildCanonicalJobPayload(metadata);
  const payloadString = stableStringify(canonicalPayload);

  const actualHmac = crypto.createHmac('sha256', secret).update(payloadString).digest('hex');

  const a = Buffer.from(actualHmac, 'hex');
  const b = Buffer.from(expectedHmac, 'hex');
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

// Job-level HMAC helpers (immutable payload: { jobId, outputKey, createdAt })
export function signJobHmacPayload(payload) {
  if (isHmacDisabled()) {
    return crypto.randomBytes(32).toString('hex');
  }
  const secret = process.env.JOB_PAYLOAD_HMAC_SECRET;
  if (!secret) {
    throw new Error('JOB_PAYLOAD_HMAC_SECRET not configured');
  }

  const canonical = {
    jobId: String(payload?.jobId || ''),
    createdAt: String(payload?.createdAt || ''),
  };

  const payloadString = stableStringify(canonical);
  return crypto.createHmac('sha256', secret).update(payloadString).digest('hex');
}

export function verifyJobHmacPayload(payload, expectedHmac) {
  if (isHmacDisabled()) return true;
  if (typeof expectedHmac !== 'string' || !expectedHmac) return false;

  const secret = process.env.JOB_PAYLOAD_HMAC_SECRET;
  if (!secret) {
    throw new Error('JOB_PAYLOAD_HMAC_SECRET not configured');
  }

  const canonical = {
    jobId: String(payload?.jobId || ''),
    createdAt: String(payload?.createdAt || ''),
  };

  const payloadString = stableStringify(canonical);
  const actualHmac = crypto.createHmac('sha256', secret).update(payloadString).digest('hex');

  const a = Buffer.from(actualHmac, 'hex');
  const b = Buffer.from(expectedHmac, 'hex');
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

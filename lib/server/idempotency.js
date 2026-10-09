'use strict';

const { createHash } = require('node:crypto');
const { hashTag } = require('@redkern/node-red-kit/redis');
const clientsWithScripts = new WeakSet();

const CLAIM_LUA = `
local created = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2])
if created then return {1, ARGV[1]} end
return {0, redis.call('GET', KEYS[1]) or ''}
`;
const COMPLETE_LUA = `
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
local ok, marker = pcall(cjson.decode, current)
if not ok or marker.requestId ~= ARGV[1] then return 0 end
local ttl = redis.call('PTTL', KEYS[1])
if ttl < 1 then ttl = tonumber(ARGV[4]) end
if ARGV[5] == 'legacy' then
  redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[4])
else
  redis.call('SET', KEYS[2], ARGV[3], 'PX', ARGV[6])
  redis.call('SET', KEYS[1], ARGV[7], 'PX', ttl)
end
return 1
`;
const COMPLETE_ASYNC_LUA = `
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
local ok, marker = pcall(cjson.decode, current)
if not ok or marker.requestId ~= ARGV[1] then return 0 end
local ttl = redis.call('PTTL', KEYS[1])
if ttl < 1 then ttl = tonumber(ARGV[3]) end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ttl)
return 1
`;
const RELEASE_LUA = `
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
local ok, marker = pcall(cjson.decode, current)
if not ok or marker.requestId ~= ARGV[1] then return 0 end
if marker.consumerId and marker.consumerId ~= ARGV[2] then return 0 end
return redis.call('DEL', KEYS[1])
`;
const TAKEOVER_LUA = `
local current = redis.call('GET', KEYS[1])
if not current then return {0, ''} end
local ok, marker = pcall(cjson.decode, current)
if not ok or marker.requestId ~= ARGV[1] then return {0, current} end
if marker.response ~= nil or marker.status == 'complete' or marker.status == 'error' then return {2, current} end
if marker.consumerId and marker.consumerId ~= ARGV[2] then return {0, current} end
local ttl = redis.call('PTTL', KEYS[1])
if ttl < 1 then ttl = tonumber(ARGV[4]) end
marker.consumerId = ARGV[3]
local encoded = cjson.encode(marker)
redis.call('SET', KEYS[1], encoded, 'PX', ttl)
return {1, encoded}
`;

function digest(value) {
  return createHash('sha256').update(value, 'utf8').digest('hex');
}

function makeIdempotencyKeys({ keyPrefix, protocolVersion, client, operation, podId, idempotencyKey, requestId }) {
  if (typeof keyPrefix !== 'string' || keyPrefix.length === 0) throw new TypeError('keyPrefix is required');
  if (typeof idempotencyKey !== 'string' || idempotencyKey.length === 0) return undefined;
  if (protocolVersion === '1.0') {
    const legacyKey = `${keyPrefix}:idem:${digest(`${podId || 'unknown'}:${idempotencyKey}`)}`;
    return { legacy: true, marker: legacyKey, result: undefined };
  }
  const operationName = String(operation);
  const tag = hashTag(`${keyPrefix}:idem`, operationName);
  return {
    legacy: false,
    marker: `${tag}:${digest(`${client}:${operationName}:${idempotencyKey}`)}`,
    result: `${keyPrefix}:result:{${operationName}}:${requestId}`
  };
}

class RedisIdempotencyStore {
  constructor({ redis, keyPrefix, consumerId, dedupWindowMs = 3600000, resultTtlMs = 120000, maxResultBytes = 262144 }) {
    if (!redis || typeof redis.defineCommand !== 'function') throw new TypeError('Redis scripts client is required');
    this.redis = redis;
    this.keyPrefix = keyPrefix;
    this.consumerId = consumerId;
    this.dedupWindowMs = dedupWindowMs;
    this.resultTtlMs = resultTtlMs;
    this.maxResultBytes = maxResultBytes;
    this.enabled = false;
  }

  _defineScripts() {
    if (clientsWithScripts.has(this.redis)) return;
    this.redis.defineCommand('gatewayIdempotencyClaim', { lua: CLAIM_LUA, numberOfKeys: 1 });
    this.redis.defineCommand('gatewayIdempotencyComplete', { lua: COMPLETE_LUA, numberOfKeys: 2 });
    this.redis.defineCommand('gatewayIdempotencyCompleteAsync', { lua: COMPLETE_ASYNC_LUA, numberOfKeys: 1 });
    this.redis.defineCommand('gatewayIdempotencyRelease', { lua: RELEASE_LUA, numberOfKeys: 1 });
    this.redis.defineCommand('gatewayIdempotencyTakeover', { lua: TAKEOVER_LUA, numberOfKeys: 1 });
    clientsWithScripts.add(this.redis);
    this.enabled = true;
  }

  async claim(request) {
    if (!request.idempotencyKey) return { claimed: true, keys: undefined };
    this._defineScripts();
    const keys = makeIdempotencyKeys({ keyPrefix: this.keyPrefix, ...request });
    const marker = JSON.stringify({ requestId: request.requestId, consumerId: this.consumerId });
    const [claimed, current] = await this.redis.gatewayIdempotencyClaim(keys.marker, marker, this.dedupWindowMs);
    let value;
    try { value = JSON.parse(current); } catch { value = undefined; }
    if (Number(claimed) === 1) return { claimed: true, keys, marker: value };
    if (!keys.legacy && value?.requestId) keys.result = `${this.keyPrefix}:result:{${request.operation}}:${value.requestId}`;
    if (value?.response !== undefined) return { claimed: false, completed: true, response: value.response, keys, marker: value, resultAvailable: true };
    if (!keys.legacy && ['complete', 'error'].includes(value?.status)) {
      const response = await this.getResult({ keys });
      return { claimed: false, completed: true, response, keys, marker: value, resultAvailable: response !== undefined };
    }
    return { claimed: false, completed: false, originalRequestId: value?.requestId, keys, marker: value };
  }

  async complete(claim, request, response) {
    if (!claim?.keys) return true;
    const body = JSON.stringify(response);
    if (Buffer.byteLength(body) > this.maxResultBytes) throw Object.assign(new Error('Idempotency response exceeds configured limit'), { code: 'RESULT_TOO_LARGE' });
    const marker = claim.keys.legacy
      ? JSON.stringify({ requestId: request.requestId, response })
      : JSON.stringify({ requestId: request.requestId, status: response?.ok === false ? 'error' : 'complete', code: response?.error?.code });
    const resultKey = claim.keys.result || claim.keys.marker;
    const bufferedBody = claim.keys.legacy
      ? body
      : JSON.stringify({
          owner: { client: request.client, podId: request.podId, sessionId: request.sessionId },
          response
        });
    return Number(await this.redis.gatewayIdempotencyComplete(
      claim.keys.marker,
      resultKey,
      request.requestId,
      marker,
      bufferedBody,
      this.dedupWindowMs,
      claim.keys.legacy ? 'legacy' : 'new',
      this.resultTtlMs,
      marker
    )) === 1;
  }

  async completeAsync(claim, request, error) {
    if (!claim?.keys) return true;
    const marker = JSON.stringify({
      requestId: request.requestId,
      status: error ? 'error' : 'complete',
      ...(error?.code ? { code: error.code } : {})
    });
    return Number(await this.redis.gatewayIdempotencyCompleteAsync(
      claim.keys.marker, request.requestId, marker, this.dedupWindowMs
    )) === 1;
  }

  async release(claim, requestId) {
    if (!claim?.keys) return false;
    return Number(await this.redis.gatewayIdempotencyRelease(claim.keys.marker, requestId, this.consumerId)) === 1;
  }

  async takeover(task) {
    if (!task?.idempotencyKey) return { acquired: true };
    this._defineScripts();
    const keys = makeIdempotencyKeys({
      keyPrefix: this.keyPrefix,
      protocolVersion: task.protocolVersion,
      client: task.client,
      operation: task.operation,
      podId: task.podId,
      idempotencyKey: task.idempotencyKey,
      requestId: task.requestId
    });
    const [result, current] = await this.redis.gatewayIdempotencyTakeover(
      keys.marker,
      task.requestId,
      task.consumerId || '',
      this.consumerId,
      this.dedupWindowMs
    );
    if (Number(result) === 0 && !current) return { acquired: true, marker: undefined, keys };
    let marker;
    try { marker = JSON.parse(current); } catch {}
    if (Number(result) === 2) {
      if (!keys.legacy && marker?.requestId) keys.result = `${this.keyPrefix}:result:{${task.operation}}:${marker.requestId}`;
      const response = marker?.response ?? await this.getResult({ keys });
      return { acquired: false, completed: true, resultAvailable: response !== undefined, response, marker, keys };
    }
    return { acquired: Number(result) === 1, marker, keys };
  }

  async getResult(claim) {
    if (!claim?.keys) return undefined;
    const key = claim.keys.legacy ? claim.keys.marker : claim.keys.result;
    const encoded = await this.redis.get(key);
    if (!encoded) return undefined;
    try {
      const value = JSON.parse(encoded);
      return claim.keys.legacy ? value.response : value.response;
    } catch { return undefined; }
  }

  async ack(claim) {
    if (!claim?.keys || claim.keys.legacy) return false;
    return (await this.redis.del(claim.keys.result)) > 0;
  }

  async resume({ client, podId, sessionId, operations, requestIds }) {
    const results = [];
    for (const operation of operations) {
      for (const requestId of requestIds) {
        const key = `${this.keyPrefix}:result:{${operation}}:${requestId}`;
        const encoded = await this.redis.get(key);
        if (!encoded) continue;
        try {
          const buffered = JSON.parse(encoded);
          const owner = buffered.owner;
          if (owner?.client === client && owner?.podId === podId && owner?.sessionId === sessionId && buffered.response) {
            results.push({ key, requestId, response: buffered.response });
          }
        } catch {}
      }
    }
    return results;
  }

  async ackForSession({ client, podId, sessionId, operations, requestId }) {
    const results = await this.resume({ client, podId, sessionId, operations, requestIds: [requestId] });
    if (results.length === 0) return false;
    await this.redis.del(...results.map((result) => result.key));
    return true;
  }
}

module.exports = {
  RedisIdempotencyStore,
  makeIdempotencyKeys,
  CLAIM_LUA,
  COMPLETE_LUA,
  COMPLETE_ASYNC_LUA,
  RELEASE_LUA,
  TAKEOVER_LUA
};
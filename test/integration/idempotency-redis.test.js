'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const Redis = require('ioredis');
const { createRedisClient } = require('@redkern/node-red-kit/redis');
const { RedisIdempotencyStore, makeIdempotencyKeys } = require('../../lib/server/idempotency.js');

const redisUrl = process.env.REDIS_GCRA_TEST_URL;

test('legacy 1.0 idempotency preserves original key/value body until dedup TTL', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => {
  const endpoint = new URL(redisUrl);
  const handle = createRedisClient({
    ioredis: Redis,
    domain: 'gateway',
    configId: 'idem-test-01',
    role: 'shared',
    mode: 'standalone',
    host: endpoint.hostname,
    port: Number(endpoint.port || 6379),
    ...(endpoint.password ? { password: decodeURIComponent(endpoint.password) } : {}),
    logger: { error() {} }
  });
  await handle.connect();
  const redis = handle.client;
  const keyPrefix = `gateway-idem-legacy-${process.pid}`;
  const oldStore = new RedisIdempotencyStore({ redis, keyPrefix, consumerId: 'legacy-consumer' });
  const duplicateStore = new RedisIdempotencyStore({ redis, keyPrefix, consumerId: 'next-consumer' });
  const request = { protocolVersion: '1.0', podId: 'pod-a', operation: 'billing/calculate', idempotencyKey: 'same', requestId: 'legacy-1' };
  const keys = makeIdempotencyKeys({ keyPrefix, ...request });
  try {
    await redis.del(keys.marker);
    const claim = await oldStore.claim(request);
    assert.equal(claim.claimed, true);
    assert.equal((await duplicateStore.claim({ ...request, requestId: 'legacy-2' })).claimed, false);
    const response = { type: 'result', requestId: request.requestId, operation: request.operation, ok: true, payload: { total: 42 } };
    assert.equal(await oldStore.complete(claim, request, response), true);
    const duplicate = await duplicateStore.claim({ ...request, requestId: 'legacy-2' });
    assert.equal(duplicate.completed, true);
    assert.deepEqual(duplicate.response, response);
    assert.deepEqual(await redis.pttl(keys.marker) > 0, true);
  } finally {
    await redis.del(keys.marker);
    await handle.close();
  }
});

test('protocol 1.1 separates dedup marker TTL from ACK-removable sync result buffer', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => {
  const endpoint = new URL(redisUrl);
  const handle = createRedisClient({
    ioredis: Redis,
    domain: 'gateway',
    configId: 'idem-test-02',
    role: 'shared',
    mode: 'standalone',
    host: endpoint.hostname,
    port: Number(endpoint.port || 6379),
    ...(endpoint.password ? { password: decodeURIComponent(endpoint.password) } : {}),
    logger: { error() {} }
  });
  await handle.connect();
  const redis = handle.client;
  const keyPrefix = `gateway-idem-v11-${process.pid}`;
  const store = new RedisIdempotencyStore({ redis, keyPrefix, consumerId: 'v11-consumer', dedupWindowMs: 3600000, resultTtlMs: 5000 });
  const request = { protocolVersion: '1.1', client: 'client-a', operation: 'billing/calculate', idempotencyKey: 'same', requestId: 'v11-1' };
  const keys = makeIdempotencyKeys({ keyPrefix, ...request });
  try {
    await redis.del(keys.marker, keys.result);
    const claim = await store.claim(request);
    const response = { type: 'result', requestId: request.requestId, response: { total: 42 } };
    assert.equal(await store.complete(claim, request, response), true);
    assert.ok(await redis.pttl(keys.marker) > 5000);
    assert.ok(await redis.pttl(keys.result) > 0);
    assert.deepEqual(await store.getResult(claim), response);
    assert.equal(await store.ack(claim), true);
    assert.equal(await redis.exists(keys.result), 0);
    assert.ok(await redis.pttl(keys.marker) > 0);
  } finally {
    await redis.del(keys.marker, keys.result);
    await handle.close();
  }
});
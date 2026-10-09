'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const Redis = require('ioredis');
const { createRedisClient } = require('@redkern/node-red-kit/redis');
const { createGcraLimiter } = require('../../lib/server/gcra.js');

const redisUrl = process.env.REDIS_GCRA_TEST_URL;

test('Redis GCRA keeps a blocked request unchanged and reserves every limit without TTL', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => {
  const endpoint = new URL(redisUrl);
  const handle = createRedisClient({
    ioredis: Redis,
    domain: 'gateway',
    configId: 'gcra-test-01',
    role: 'shared',
    mode: 'standalone',
    host: endpoint.hostname,
    port: Number(endpoint.port || 6379),
    ...(endpoint.password ? { password: decodeURIComponent(endpoint.password) } : {}),
    logger: { error() {} }
  });
  await handle.connect();
  const client = handle.client;
  const prefix = `gateway-gcra-test-${process.pid}`;
  const limiter = createGcraLimiter(client, prefix);
  const limits = [
    { name: 'g', rate: 10, burst: 1 },
    { name: 'minute', rate: 2, burst: 1 }
  ];
  const keys = limiter.keys('billing/calculate', limits);
  try {
    await client.del(...keys);
    const first = await limiter.reserve('billing/calculate', limits, 0);
    assert.equal(first.allowed, true);

    const before = await client.hmget(...keys.flatMap((key) => [key, 'tat']));
    const blocked = await limiter.reserve('billing/calculate', limits, 0);
    assert.equal(blocked.allowed, false);
    assert.ok(blocked.waitMs > 0);
    const afterBlocked = await client.hmget(...keys.flatMap((key) => [key, 'tat']));
    assert.deepEqual(afterBlocked, before);

    const reserved = await limiter.reserve('billing/calculate', limits, 2000);
    assert.equal(reserved.allowed, true);
    assert.ok(reserved.waitMs > 0);
    for (const key of keys) {
      assert.notEqual(await client.hget(key, 'tat'), null);
      assert.equal(await client.pttl(key), -1);
    }
  } finally {
    await client.del(...keys);
    await handle.close();
  }
});

test('a rejecting GCRA limit does not consume another available bucket limit', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => {
  const endpoint = new URL(redisUrl);
  const handle = createRedisClient({
    ioredis: Redis,
    domain: 'gateway',
    configId: 'gcra-test-02',
    role: 'shared',
    mode: 'standalone',
    host: endpoint.hostname,
    port: Number(endpoint.port || 6379),
    ...(endpoint.password ? { password: decodeURIComponent(endpoint.password) } : {}),
    logger: { error() {} }
  });
  await handle.connect();
  const client = handle.client;
  const prefix = `gateway-gcra-atomic-${process.pid}`;
  const limiter = createGcraLimiter(client, prefix);
  const fast = { name: 'second', rate: 100, burst: 1 };
  const slow = { name: 'minute', rate: 1, burst: 1 };
  const fastKey = limiter.keys('billing/atomic', [fast])[0];
  const slowKey = limiter.keys('billing/atomic', [slow])[0];
  try {
    await client.del(fastKey, slowKey);
    assert.equal((await limiter.reserve('billing/atomic', [slow], 0)).allowed, true);
    const fastTatBefore = await client.hget(fastKey, 'tat');
    const denied = await limiter.reserve('billing/atomic', [fast, slow], 0);
    assert.equal(denied.allowed, false);
    assert.equal(await client.hget(fastKey, 'tat'), fastTatBefore);
    assert.equal(await client.hget(slowKey, 'tat') !== null, true);
  } finally {
    await client.del(fastKey, slowKey);
    await handle.close();
  }
});

test('capacity query reports remaining/resetAt without changing GCRA state', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => {
  const endpoint = new URL(redisUrl);
  const handle = createRedisClient({
    ioredis: Redis,
    domain: 'gateway',
    configId: 'gcra-capacity-01',
    role: 'shared',
    mode: 'standalone',
    host: endpoint.hostname,
    port: Number(endpoint.port || 6379),
    ...(endpoint.password ? { password: decodeURIComponent(endpoint.password) } : {}),
    logger: { error() {} }
  });
  await handle.connect();
  const client = handle.client;
  const prefix = `gateway-capacity-${process.pid}`;
  const limiter = createGcraLimiter(client, prefix);
  const limits = [{ name: 'g', rate: 5, burst: 2 }];
  const key = limiter.keys('billing/capacity', limits)[0];
  try {
    await client.del(key);
    await limiter.reserve('billing/capacity', limits, 0);
    const before = await client.hget(key, 'tat');
    const snapshot = await limiter.capacity('billing/capacity', limits);
    assert.equal(snapshot.length, 1);
    assert.ok(snapshot[0].remaining >= 0 && snapshot[0].remaining <= 2);
    assert.ok(snapshot[0].resetAt >= snapshot[0].now);
    assert.equal(await client.hget(key, 'tat'), before);
  } finally {
    await client.del(key);
    await handle.close();
  }
});
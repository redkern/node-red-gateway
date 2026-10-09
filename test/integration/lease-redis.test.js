'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const Redis = require('ioredis');
const { createRedisClient } = require('@redkern/node-red-kit/redis');
const { LeaseCoordinator } = require('../../lib/server/lease.js');

const redisUrl = process.env.REDIS_GCRA_TEST_URL;

test('Redis lease serializes owners, renews without TTL, and consumes clean marker on takeover (GW-LIFE-6)', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => {
  const endpoint = new URL(redisUrl);
  const handle = createRedisClient({
    ioredis: Redis,
    domain: 'gateway',
    configId: 'lease-test-01',
    role: 'shared',
    mode: 'standalone',
    host: endpoint.hostname,
    port: Number(endpoint.port || 6379),
    ...(endpoint.password ? { password: decodeURIComponent(endpoint.password) } : {}),
    logger: { error() {} }
  });
  await handle.connect();
  const redis = handle.client;
  const prefix = `gateway-lease-test-${process.pid}`;
  const first = new LeaseCoordinator({ redis, keyPrefix: prefix, consumerId: 'instance:boot-a:g1', leaseTtlMs: 15000, safetyMarginMs: 5000 });
  const second = new LeaseCoordinator({ redis, keyPrefix: prefix, consumerId: 'instance:boot-b:g1', leaseTtlMs: 15000, safetyMarginMs: 5000 });
  try {
    await redis.del(first.keys.lease, first.keys.clean);
    assert.equal((await first.acquire()).acquired, true);
    assert.equal((await second.acquire()).acquired, false);
    assert.equal(await redis.pttl(first.keys.lease), -1);
    assert.equal(first.isFresh(), true);
    assert.equal(await first.renew(), true);
    assert.equal(await redis.pttl(first.keys.lease), -1);
    assert.equal(await first.release({ clean: true }), true);
    assert.equal(await redis.get(first.keys.clean), 'instance:boot-a:g1');
    assert.equal((await second.acquire()).acquired, true);
    assert.equal(await redis.get(first.keys.clean), null);
    assert.equal(await redis.pttl(first.keys.lease), -1);
    await second.release({ clean: false });
  } finally {
    await redis.del(first.keys.lease, first.keys.clean);
    await handle.close();
  }
});
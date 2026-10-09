'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const Redis = require('ioredis');
const { createRedisClient } = require('@redkern/node-red-kit/redis');
const { AsyncResultStore, resultStreamKey, GROUP } = require('../../lib/server/async-results.js');

const redisUrl = process.env.REDIS_GCRA_TEST_URL;

test('async results are isolated by client/operation and deleted only by application ACK', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => {
  const endpoint = new URL(redisUrl);
  const handle = createRedisClient({
    ioredis: Redis,
    domain: 'gateway',
    configId: 'async-result-test',
    role: 'shared',
    mode: 'standalone',
    host: endpoint.hostname,
    port: Number(endpoint.port || 6379),
    ...(endpoint.password ? { password: decodeURIComponent(endpoint.password) } : {}),
    logger: { error() {} }
  });
  await handle.connect();
  const redis = handle.client;
  const prefix = `gateway-results-test-${process.pid}`;
  const store = new AsyncResultStore({ redis, keyPrefix: prefix, consumerId: 'gateway-consumer' });
  const first = resultStreamKey(prefix, 'client-a', 'billing/a');
  const otherOperation = resultStreamKey(prefix, 'client-a', 'billing/b');
  const otherClient = resultStreamKey(prefix, 'client-b', 'billing/a');
  try {
    await redis.del(first, otherOperation, otherClient);
    await store.append('client-a', 'billing/a', { requestId: 'async-1', payload: { value: 42 } });
    const items = await store.read('client-a', 'billing/a');
    assert.equal(items.length, 1);
    assert.deepEqual(items[0].entry, { requestId: 'async-1', payload: { value: 42 } });
    assert.equal(items[0].stream, first);
    assert.equal(await redis.xlen(otherOperation), 0);
    assert.equal(await redis.xlen(otherClient), 0);
    assert.equal(await redis.xpending(first, GROUP).then((value) => value[0]), 1);
    assert.equal(await store.ack(items[0].stream, items[0].streamId), true);
    assert.equal(await redis.xlen(first), 0);
    assert.equal(await redis.xpending(first, GROUP).then((value) => value[0]), 0);
  } finally {
    await redis.del(first, otherOperation, otherClient);
    await handle.close();
  }
});

test('async result PEL can be claimed by another live session after reconnect grace', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => {
  const endpoint = new URL(redisUrl);
  const handle = createRedisClient({
    ioredis: Redis,
    domain: 'gateway',
    configId: 'async-result-claim',
    role: 'shared',
    mode: 'standalone',
    host: endpoint.hostname,
    port: Number(endpoint.port || 6379),
    ...(endpoint.password ? { password: decodeURIComponent(endpoint.password) } : {}),
    logger: { error() {} }
  });
  await handle.connect();
  const redis = handle.client;
  const prefix = `gateway-results-claim-${process.pid}`;
  const key = resultStreamKey(prefix, 'client-a', 'billing/a');
  const store = new AsyncResultStore({ redis, keyPrefix: prefix, consumerId: 'gateway-consumer' });
  try {
    await redis.del(key);
    await store.append('client-a', 'billing/a', { requestId: 'claim-1', response: { value: 1 } });
    assert.equal((await store.read('client-a', 'billing/a', 1, 'pod-old:session-old')).length, 1);
    const claimed = await store.claimConsumer('client-a', 'billing/a', 'pod-old:session-old', 'pod-new:session-new', 0);
    assert.equal(claimed.length, 1);
    assert.equal(claimed[0].entry.requestId, 'claim-1');
    assert.equal((await redis.xpending(key, GROUP, '-', '+', 10, 'pod-old:session-old')).length, 0);
    assert.equal((await redis.xpending(key, GROUP, '-', '+', 10, 'pod-new:session-new')).length, 1);
    await store.ack(key, claimed[0].streamId);
    assert.equal(await redis.xlen(key), 0);
  } finally {
    await redis.del(key);
    await handle.close();
  }
});
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

test('a later session can reclaim pending results when no session was live at grace expiry', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => {
  const endpoint = new URL(redisUrl);
  const handle = createRedisClient({
    ioredis: Redis,
    domain: 'gateway',
    configId: 'async-result-abandoned',
    role: 'shared',
    mode: 'standalone',
    host: endpoint.hostname,
    port: Number(endpoint.port || 6379),
    ...(endpoint.password ? { password: decodeURIComponent(endpoint.password) } : {}),
    logger: { error() {} }
  });
  await handle.connect();
  const redis = handle.client;
  const prefix = `gateway-results-abandoned-${process.pid}`;
  const key = resultStreamKey(prefix, 'client-a', 'billing/a');
  const indexKey = `${prefix}:results:index`;
  const readyKey = `${indexKey}:ready`;
  const store = new AsyncResultStore({ redis, keyPrefix: prefix, consumerId: 'gateway-consumer' });
  try {
    await redis.del(key, indexKey, readyKey);
    await store.append('client-a', 'billing/a', { requestId: 'abandoned-1', response: { ok: true } });
    assert.equal((await store.read('client-a', 'billing/a', 1, 'pod-old:session-old')).length, 1);

    const claimed = await store.reclaimAbandoned(
      'client-a', 'billing/a', 'pod-new:session-new', new Set(['pod-new:session-new']), 0, 10
    );

    assert.equal(claimed.length, 1);
    assert.equal(claimed[0].entry.requestId, 'abandoned-1');
    assert.equal((await redis.xpending(key, GROUP, '-', '+', 10, 'pod-new:session-new')).length, 1);
    await store.ack(key, claimed[0].streamId);
  } finally {
    await redis.del(key, indexKey, readyKey);
    await handle.close();
  }
});

test('retention restores existing result streams after a Gateway restart', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => {
  const endpoint = new URL(redisUrl);
  const handle = createRedisClient({
    ioredis: Redis,
    domain: 'gateway',
    configId: 'async-result-retention',
    role: 'shared',
    mode: 'standalone',
    host: endpoint.hostname,
    port: Number(endpoint.port || 6379),
    ...(endpoint.password ? { password: decodeURIComponent(endpoint.password) } : {}),
    logger: { error() {} }
  });
  await handle.connect();
  const redis = handle.client;
  const prefix = `gateway-results-retention-${process.pid}`;
  const key = resultStreamKey(prefix, 'client-a', 'billing/a');
  const indexKey = `${prefix}:results:index`;
  const readyKey = `${indexKey}:ready`;
  const store = new AsyncResultStore({ redis, keyPrefix: prefix, consumerId: 'gateway-consumer', asyncResultTtlMs: 1 });
  try {
    await redis.del(key, indexKey, readyKey);
    await redis.xgroup('CREATE', key, GROUP, '0', 'MKSTREAM');
    for (let index = 1; index <= 200; index += 1) {
      await redis.xadd(key, `${1000000 + index}-0`, 'data', JSON.stringify({ requestId: `old-${index}` }));
    }

    await store.startRetention(3600000);
    assert.equal(store.streams.has(key), true);
    await store.trimExpired();

    assert.equal(await redis.xlen(key), 0);
    assert.equal(store.expiredByOperation.get('client-a\u0000billing/a') > 0, true);
  } finally {
    store.close();
    await redis.del(key, indexKey, readyKey);
    await handle.close();
  }
});
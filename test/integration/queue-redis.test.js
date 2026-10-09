'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const EventEmitter = require('node:events');
const { randomUUID } = require('node:crypto');
const net = require('node:net');
const Redis = require('ioredis');
const { createRedisClient } = require('@redkern/node-red-kit/redis');
const { GatewayClient } = require('../../lib/client.js');
const { createGcraLimiter } = require('../../lib/server/gcra.js');
const { RedisIdempotencyStore, makeIdempotencyKeys } = require('../../lib/server/idempotency.js');
const { AsyncResultStore } = require('../../lib/server/async-results.js');
const { GatewayRuntime } = require('../../lib/gateway-runtime.js');
const { RedisOperationQueue, streamKeys } = require('../../lib/server/queue.js');

const redisUrl = process.env.REDIS_GCRA_TEST_URL;

async function withRedis(run) {
  const endpoint = new URL(redisUrl);
  const handle = createRedisClient({
    ioredis: Redis,
    domain: 'gateway',
    configId: 'queue-test-01',
    role: 'shared',
    mode: 'standalone',
    host: endpoint.hostname,
    port: Number(endpoint.port || 6379),
    ...(endpoint.password ? { password: decodeURIComponent(endpoint.password) } : {}),
    logger: { error() {} }
  });
  await handle.connect();
  try { await run(handle.client); } finally { await handle.close(); }
}

test('Redis queue enqueues per operation and ACKs/deletes completed entries', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => withRedis(async (redis) => {
  const prefix = `gateway-queue-test-${process.pid}`;
  const keys = streamKeys(prefix, 'billing/calculate');
  await redis.del(keys.normal, keys.bulk, keys.delayed, keys.delayedData);
  const queue = new RedisOperationQueue({ redis, keyPrefix: prefix, consumerId: 'test-consumer', maxQueueDepth: 10 });
  await queue.register('billing/calculate', {
    concurrency: 1,
    execute: async (message) => ({ doubled: message.payload.value * 2 })
  });
  const resultPromise = new Promise((resolve, reject) => {
    queue.once('result', resolve);
    queue.once('errorResult', ({ error }) => reject(error));
  });
  await queue.enqueue({
    requestId: 'req-1', operation: 'billing/calculate', message: { payload: { value: 21 } },
    priority: 'normal', queueTimeoutMs: 1000, execTimeoutMs: 500, deadlineAt: Date.now() + 2000
  });
  const result = await resultPromise;
  assert.deepEqual(result.result, { doubled: 42 });
  assert.equal(await redis.xlen(keys.normal), 0);
  assert.equal(await redis.xpending(keys.normal, 'redkern-gateway').then((value) => value[0]), 0);
  assert.equal(await queue.pendingForConsumer(), 0);
  assert.equal(Number(await redis.get(keys.bytes)), 0);
  await queue.close();
  await redis.del(keys.normal, keys.bulk, keys.delayed, keys.delayedData);
}));

test('terminal failures honor maxDeliveries and are retained in a bounded DLQ', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => withRedis(async (redis) => {
  const prefix = `gateway-dlq-test-${process.pid}`;
  const keys = streamKeys(prefix, 'billing/fail');
  await redis.del(keys.normal, keys.bulk, keys.delayed, keys.delayedData, keys.bytes, keys.deadLetter);
  const queue = new RedisOperationQueue({
    redis, keyPrefix: prefix, consumerId: 'dlq-consumer', maxDlqEntries: 2, dlqRetentionMs: 60000
  });
  const attempts = new Map();
  await queue.register('billing/fail', {
    concurrency: 1,
    retryClass: 'safe',
    maxDeliveries: 2,
    execute: async (_message, context) => {
      attempts.set(context.requestId, (attempts.get(context.requestId) || 0) + 1);
      const error = Object.assign(new Error('temporary upstream failure'), { code: 'ECONNRESET', retryable: true, retryAfterMs: 0 });
      throw error;
    }
  });
  let completed = 0;
  let resolveCompleted;
  const allFailed = new Promise((resolve) => { resolveCompleted = resolve; });
  queue.on('deadLetter', () => {
    completed += 1;
    if (completed === 3) resolveCompleted();
  });
  queue.on('errorResult', () => {});
  for (let index = 0; index < 3; index += 1) {
    await queue.enqueue({
      requestId: `dlq-${index}`, operation: 'billing/fail', message: {},
      queueTimeoutMs: 3000, execTimeoutMs: 1000, deadlineAt: Date.now() + 10000
    });
  }
  await allFailed;
  assert.deepEqual([...attempts.values()], [2, 2, 2]);
  const deadLetters = await redis.xrange(keys.deadLetter, '-', '+');
  assert.equal(deadLetters.length, 2);
  for (const [, fields] of deadLetters) {
    const dataIndex = fields.indexOf('data');
    const errorIndex = fields.indexOf('error');
    assert.ok(dataIndex >= 0 && errorIndex >= 0);
    assert.equal(JSON.parse(fields[errorIndex + 1]).attempt, 2);
    assert.equal(JSON.parse(fields[dataIndex + 1]).operation, 'billing/fail');
  }
  assert.equal(await redis.xlen(keys.normal), 0);
  assert.equal(Number(await redis.get(keys.bytes) || 0), 0);
  assert.equal((await redis.xpending(keys.normal, 'redkern-gateway'))[0], 0);
  await queue.close();
  await redis.del(keys.normal, keys.bulk, keys.delayed, keys.delayedData, keys.bytes, keys.deadLetter);
}));

test('queue depth includes queued entries and rejects work beyond maxQueueDepth', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => withRedis(async (redis) => {
  const prefix = `gateway-depth-test-${process.pid}`;
  const keys = streamKeys(prefix, 'billing/slow');
  await redis.del(keys.normal, keys.bulk, keys.delayed, keys.delayedData, keys.bytes);
  const queue = new RedisOperationQueue({ redis, keyPrefix: prefix, consumerId: 'depth-consumer', maxQueueDepth: 1 });
  let releaseExecutor;
  const executorStarted = new Promise((resolve) => {
    releaseExecutor = resolve;
  });
  let continueExecutor;
  const hold = new Promise((resolve) => { continueExecutor = resolve; });
  await queue.register('billing/slow', {
    concurrency: 1,
    execute: async () => { releaseExecutor(); await hold; return { ok: true }; }
  });
  await queue.enqueue({ requestId: 'hold-1', operation: 'billing/slow', message: {}, queueTimeoutMs: 3000, execTimeoutMs: 2000, deadlineAt: Date.now() + 5000 });
  await executorStarted;
  await assert.rejects(queue.enqueue({ requestId: 'hold-2', operation: 'billing/slow', message: {}, queueTimeoutMs: 3000, execTimeoutMs: 2000, deadlineAt: Date.now() + 5000 }), { code: 'QUEUE_FULL' });
  continueExecutor();
  await new Promise((resolve, reject) => {
    queue.once('result', resolve);
    queue.once('errorResult', ({ error }) => reject(error));
  });
  await queue.close();
  await redis.del(keys.normal, keys.bulk, keys.delayed, keys.delayedData, keys.bytes);
}));

test('atomic byte admission rejects total queue footprint above maxQueueBytes', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => withRedis(async (redis) => {
  const prefix = `gateway-bytes-test-${process.pid}`;
  const keys = streamKeys(prefix, 'billing/bytes');
  await redis.del(keys.normal, keys.bulk, keys.delayed, keys.delayedData, keys.bytes);
  const queue = new RedisOperationQueue({ redis, keyPrefix: prefix, consumerId: 'bytes-consumer', maxQueueDepth: 10, maxQueueBytes: 16 });
  await queue.register('billing/bytes', { concurrency: 1, execute: async () => ({ ok: true }) });
  await assert.rejects(queue.enqueue({
    requestId: 'bytes-1', operation: 'billing/bytes', message: { payload: 'body larger than budget' },
    queueTimeoutMs: 1000, execTimeoutMs: 500, deadlineAt: Date.now() + 2000
  }), { code: 'QUEUE_FULL' });
  assert.equal(await redis.xlen(keys.normal), 0);
  assert.equal(Number(await redis.get(keys.bytes) || 0), 0);
  await queue.close();
  await redis.del(keys.normal, keys.bulk, keys.delayed, keys.delayedData, keys.bytes);
}));

test('queue byte factor and per-entry overhead calibrate admission and ACK accounting', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => withRedis(async (redis) => {
  const prefix = `gateway-byte-calibration-${process.pid}`;
  const keys = streamKeys(prefix, 'billing/calibrated');
  await redis.del(...Object.values(keys));
  const queue = new RedisOperationQueue({
    redis, keyPrefix: prefix, consumerId: 'calibration-consumer',
    queueBytesFactor: 1, queueEntryOverheadBytes: 0
  });
  let releaseExecutor;
  let executorStarted;
  const started = new Promise((resolve) => { executorStarted = resolve; });
  const hold = new Promise((resolve) => { releaseExecutor = resolve; });
  await queue.register('billing/calibrated', { execute: async () => { executorStarted(); await hold; return {}; } });
  await queue.enqueue({
    requestId: 'calibrate-1', operation: 'billing/calibrated', message: { payload: 'sample' },
    queueTimeoutMs: 3000, execTimeoutMs: 1000, deadlineAt: Date.now() + 5000
  });
  await started;
  const entry = (await redis.xrange(keys.normal, '-', '+'))[0];
  const payload = entry[1][entry[1].indexOf('data') + 1];
  assert.equal(Number(await redis.get(keys.bytes)), Buffer.byteLength(payload));
  const result = new Promise((resolve) => queue.once('result', resolve));
  releaseExecutor();
  await result;
  assert.equal(Number(await redis.get(keys.bytes)), 0);
  await queue.close();
  await redis.del(...Object.values(keys));
}));

test('rate reservations move to delayed storage and promote to the operation stream', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => withRedis(async (redis) => {
  const prefix = `gateway-delay-test-${process.pid}`;
  const keys = streamKeys(prefix, 'billing/rate');
  const gcraKeys = [`${prefix}:{billing/rate}:g`];
  await redis.del(keys.normal, keys.bulk, keys.delayed, keys.delayedData, ...gcraKeys);
  const queue = new RedisOperationQueue({
    redis,
    gcra: createGcraLimiter(redis, prefix),
    keyPrefix: prefix,
    consumerId: 'delay-consumer',
    maxQueueDepth: 10
  });
  await queue.register('billing/rate', {
    concurrency: 2,
    rateLimits: [{ name: 'g', rate: 10, burst: 1 }],
    bucket: 'billing/rate',
    execute: async (message) => ({ value: message.payload.value, executedAt: Date.now() })
  });
  const results = [];
  const complete = new Promise((resolve, reject) => {
    queue.on('result', (result) => {
      results.push(result);
      if (results.length === 2) resolve();
    });
    queue.on('errorResult', ({ error }) => reject(error));
  });
  const deadlineAt = Date.now() + 5000;
  for (const requestId of ['rate-1', 'rate-2']) {
    await queue.enqueue({ requestId, operation: 'billing/rate', message: { payload: { value: requestId } }, priority: 'normal', queueTimeoutMs: 3000, execTimeoutMs: 1000, deadlineAt });
  }
  await complete;
  results.sort((left, right) => left.result.executedAt - right.result.executedAt);
  assert.ok(results[1].result.executedAt - results[0].result.executedAt >= 70);
  assert.equal(await redis.zcard(keys.delayed), 0);
  assert.equal(await redis.xlen(keys.normal), 0);
  assert.equal(await redis.pttl(gcraKeys[0]), -1);
  await queue.close();
  await redis.del(keys.normal, keys.bulk, keys.delayed, keys.delayedData, ...gcraKeys);
}));

test('Gateway WS call is accepted from Redis XADD and returns the queued executor result', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async (context) => withRedis(async (redis) => {
  const prefix = `gateway-e2e-test-${process.pid}`;
  const keys = streamKeys(prefix, 'billing/queued');
  const gcraKey = `${prefix}:{billing/queued}:g`;
  await redis.del(keys.normal, keys.bulk, keys.delayed, keys.delayedData, gcraKey);
  const queue = new RedisOperationQueue({
    redis,
    gcra: createGcraLimiter(redis, prefix),
    keyPrefix: prefix,
    consumerId: 'e2e-consumer',
    maxQueueDepth: 100,
    defaultLimits: [{ name: 'g', rate: 100, burst: 10 }]
  });
  const execute = async (message) => ({ payload: { doubled: message.payload.value * 2 }, statusCode: 200 });
  await queue.register('billing/queued', {
    execute,
    concurrency: 4,
    bucket: 'billing/queued',
    rateLimits: [{ name: 'g', rate: 100, burst: 10 }],
    contract: 'http'
  });
  const socket = net.createServer();
  await new Promise((resolve, reject) => socket.listen(0, '127.0.0.1', resolve).once('error', reject));
  const port = socket.address().port;
  await new Promise((resolve) => socket.close(resolve));
  const runtime = new GatewayRuntime({ host: '127.0.0.1', port, queue, redis, redisPolicy: 'noeviction' });
  runtime.on('error', (error) => assert.fail(error.message));
  const metrics = [];
  runtime.on('metric', (metric) => metrics.push(metric));
  runtime.registerAccount({ name: 'queue-client', operationPrefixes: ['billing/*'], token: 'queue-client-secret' });
  runtime.registerExecutor('billing/queued', execute, { contract: 'http' });
  await runtime.start();
  const client = new GatewayClient({ url: `ws://127.0.0.1:${port}`, token: 'queue-client-secret', podId: 'queue-pod' });
  context.after(async () => {
    await client.close();
    await runtime.close();
    await queue.close();
  });
  await client.start();
  const result = await client.call('billing/queued', { payload: { value: 21 } }, {
    deadlineMs: 2000,
    queueTimeoutMs: 1000,
    execTimeoutMs: 500
  });
  assert.deepEqual(result, { payload: { doubled: 42 }, statusCode: 200 });
  assert.deepEqual(metrics.map((metric) => metric.event), ['request.processing', 'upstream.completed', 'request.completed']);
  assert.equal(metrics[0].operationKey, 'billing/queued');
  assert.equal(metrics[1].contract, '');
  assert.ok(metrics[2].timings.totalMs >= metrics[2].timings.upstreamMs);
  assert.equal(await redis.xlen(keys.normal), 0);
  await redis.del(keys.normal, keys.bulk, keys.delayed, keys.delayedData, gcraKey);
}));

test('Redis queue retries safe network failures but never retries post-send failures for never class', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => withRedis(async (redis) => {
  const prefix = `gateway-retry-test-${process.pid}`;
  const safeKeys = streamKeys(prefix, 'billing/safe');
  const neverKeys = streamKeys(prefix, 'billing/never');
  await redis.del(...Object.values(safeKeys), ...Object.values(neverKeys));
  const queue = new RedisOperationQueue({
    redis,
    gcra: createGcraLimiter(redis, prefix),
    keyPrefix: prefix,
    consumerId: 'retry-consumer',
    maxQueueDepth: 10,
    defaultLimits: [{ name: 'g', rate: 100, burst: 10 }]
  });
  let safeAttempts = 0;
  let neverAttempts = 0;
  await queue.register('billing/safe', {
    concurrency: 1,
    bucket: 'billing/safe',
    rateLimits: [{ name: 'g', rate: 100, burst: 10 }],
    retryClass: 'safe',
    execute: async () => {
      safeAttempts += 1;
      if (safeAttempts === 1) throw Object.assign(new Error('socket reset'), { code: 'ECONNRESET' });
      return { ok: true };
    }
  });
  await queue.register('billing/never', {
    concurrency: 1,
    bucket: 'billing/never',
    rateLimits: [{ name: 'g', rate: 100, burst: 10 }],
    retryClass: 'never',
    execute: async () => {
      neverAttempts += 1;
      throw Object.assign(new Error('socket reset'), { code: 'ECONNRESET' });
    }
  });
  const safeResult = new Promise((resolve, reject) => {
    queue.once('result', resolve);
    queue.once('errorResult', ({ error }) => reject(error));
  });
  await queue.enqueue({ requestId: 'safe-1', operation: 'billing/safe', message: {}, queueTimeoutMs: 3000, execTimeoutMs: 500, deadlineAt: Date.now() + 5000 });
  await safeResult;
  assert.equal(safeAttempts, 2);

  const neverError = new Promise((resolve) => queue.once('errorResult', resolve));
  await queue.enqueue({ requestId: 'never-1', operation: 'billing/never', message: {}, queueTimeoutMs: 3000, execTimeoutMs: 500, deadlineAt: Date.now() + 5000 });
  const failed = await neverError;
  assert.equal(failed.error.code, 'ECONNRESET');
  assert.equal(neverAttempts, 1);
  await queue.close();
  await redis.del(...Object.values(safeKeys), ...Object.values(neverKeys), `${prefix}:{billing/safe}:g`, `${prefix}:{billing/never}:g`);
}));

test('bulk stream gets a pull slot after ten consecutive normal reads', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => withRedis(async (redis) => {
  const prefix = `gateway-fairness-test-${process.pid}`;
  const keys = streamKeys(prefix, 'billing/fair');
  await redis.del(...Object.values(keys));
  const queue = new RedisOperationQueue({ redis, keyPrefix: prefix, consumerId: 'fair-consumer', maxQueueDepth: 50 });
  const order = [];
  let releaseFirst;
  let startedFirst;
  const firstStarted = new Promise((resolve) => { startedFirst = resolve; });
  const holdFirst = new Promise((resolve) => { releaseFirst = resolve; });
  let callCount = 0;
  await queue.register('billing/fair', {
    concurrency: 1,
    execute: async (message) => {
      callCount += 1;
      if (callCount === 1) {
        startedFirst();
        await holdFirst;
      }
      return { id: message.payload.id };
    }
  });
  let complete;
  const allResults = new Promise((resolve) => { complete = resolve; });
  queue.on('result', ({ task }) => {
    order.push(task.requestId);
    if (order.length === 12) complete();
  });
  await queue.enqueue({ requestId: 'normal-0', operation: 'billing/fair', message: { payload: { id: 0 } }, priority: 'normal', queueTimeoutMs: 10000, execTimeoutMs: 5000, deadlineAt: Date.now() + 20000 });
  await firstStarted;
  for (let index = 1; index < 11; index += 1) {
    await queue.enqueue({ requestId: `normal-${index}`, operation: 'billing/fair', message: { payload: { id: index } }, priority: 'normal', queueTimeoutMs: 10000, execTimeoutMs: 5000, deadlineAt: Date.now() + 20000 });
  }
  await queue.enqueue({ requestId: 'bulk-0', operation: 'billing/fair', message: { payload: { id: 'bulk' } }, priority: 'bulk', queueTimeoutMs: 10000, execTimeoutMs: 5000, deadlineAt: Date.now() + 20000 });
  releaseFirst();
  await allResults;
  assert.equal(order[10], 'bulk-0');
  await queue.close();
  await redis.del(...Object.values(keys));
}));

test('expired queue tasks fail before calling the executor and release queue bytes', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => withRedis(async (redis) => {
  const prefix = `gateway-deadline-test-${process.pid}`;
  const keys = streamKeys(prefix, 'billing/expired');
  await redis.del(...Object.values(keys), keys.bytes);
  const queue = new RedisOperationQueue({ redis, keyPrefix: prefix, consumerId: 'deadline-consumer', maxQueueDepth: 10 });
  let executed = 0;
  await queue.register('billing/expired', { execute: async () => { executed += 1; return { ok: true }; } });
  const failure = new Promise((resolve) => queue.once('errorResult', resolve));
  await queue.enqueue({
    requestId: 'expired-1', operation: 'billing/expired', message: {},
    queuedAt: Date.now() - 2000, queueTimeoutMs: 100, execTimeoutMs: 50, deadlineAt: Date.now() - 1000
  });
  const result = await failure;
  assert.equal(result.error.code, 'DEADLINE_EXCEEDED_IN_QUEUE');
  assert.equal(executed, 0);
  assert.equal(Number(await redis.get(keys.bytes) || 0), 0);
  await queue.close();
  await redis.del(...Object.values(keys), keys.bytes);
}));

test('long operation deadline keeps older pending stream entries beyond short global windows', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => withRedis(async (redis) => {
  const prefix = `gateway-trim-test-${process.pid}`;
  const keys = streamKeys(prefix, 'billing/long');
  await redis.del(...Object.values(keys), keys.bytes);
  const queue = new RedisOperationQueue({ redis, keyPrefix: prefix, consumerId: 'trim-consumer', maxQueueDepth: 10 });
  await queue.register('billing/long', {
    concurrency: 1,
    deadlineMs: 60 * 60 * 1000,
    execute: async (message) => ({ ok: message.payload.id })
  });
  const oldId = `${Date.now() - 15 * 60 * 1000}-0`;
  await redis.xadd(keys.normal, oldId, 'data', JSON.stringify({ payload: { id: 'old-pending' } }));
  await redis.xreadgroup('GROUP', 'redkern-gateway', 'previous-consumer', 'COUNT', 1, 'STREAMS', keys.normal, '>');

  const result = new Promise((resolve) => queue.once('result', resolve));
  await queue.enqueue({
    requestId: 'trim-current', operation: 'billing/long', message: { payload: { id: 'current' } },
    queueTimeoutMs: 30000, execTimeoutMs: 1000, deadlineAt: Date.now() + 60 * 60 * 1000
  });
  await result;
  assert.equal((await redis.xrange(keys.normal, oldId, oldId)).length, 1);
  await queue.close();
  await redis.del(...Object.values(keys), keys.bytes);
}));

test('lease loss aborts active executor and transfers task to delayed storage without dropping bytes', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => withRedis(async (redis) => {
  const prefix = `gateway-fence-test-${process.pid}`;
  const keys = streamKeys(prefix, 'billing/fenced');
  await redis.del(...Object.values(keys), keys.bytes);
  const lease = Object.assign(new EventEmitter(), { fresh: true, isFresh() { return this.fresh; } });
  const queue = new RedisOperationQueue({ redis, keyPrefix: prefix, consumerId: 'fenced-consumer', maxQueueDepth: 10, lease });
  let executorStarted;
  const started = new Promise((resolve) => { executorStarted = resolve; });
  let signalAborted;
  const aborted = new Promise((resolve) => { signalAborted = resolve; });
  await queue.register('billing/fenced', {
    concurrency: 1,
    execute: (_message, { signal }) => new Promise((_resolve, reject) => {
      executorStarted();
      signal.addEventListener('abort', () => {
        signalAborted();
        reject(Object.assign(new Error('lease lost'), { name: 'AbortError' }));
      }, { once: true });
    })
  });
  await queue.enqueue({
    requestId: 'fence-1', operation: 'billing/fenced', message: {},
    queueTimeoutMs: 3000, execTimeoutMs: 2000, deadlineAt: Date.now() + 5000
  });
  await started;
  lease.fresh = false;
  lease.emit('lost');
  await aborted;
  for (let attempt = 0; attempt < 20 && Number(await redis.zcard(keys.delayed)) === 0; attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  assert.equal(await redis.zcard(keys.delayed), 1);
  assert.notEqual(await redis.hget(keys.delayedData, 'fence-1'), null);
  assert.equal(Number(await redis.get(keys.bytes)) > 0, true);
  assert.equal((await redis.xpending(keys.normal, 'redkern-gateway'))[0], 0);
  await queue.close();
  await redis.del(...Object.values(keys), keys.bytes);
}));

test('complete drain state survives a queue instance replacement until explicit off', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => withRedis(async (redis) => {
  const prefix = `gateway-drain-test-${process.pid}`;
  const keys = streamKeys(prefix, 'billing/drain');
  const drainKey = `${prefix}:drain`;
  await redis.del(...Object.values(keys), keys.bytes, drainKey);
  const makeQueue = () => new RedisOperationQueue({ redis, keyPrefix: prefix, consumerId: `drain-${randomUUID()}`, maxQueueDepth: 10 });
  const first = makeQueue();
  await first.register('billing/drain', { execute: async () => ({ ok: true }) });
  const firstReport = await first.drain('complete', 100);
  assert.equal(firstReport.remaining, 0);
  assert.equal(first.drainMode, 'complete');
  await first.close();

  const replacement = makeQueue();
  await replacement.register('billing/drain', { execute: async () => ({ ok: true }) });
  assert.equal(await replacement.restoreDrainState(), 'complete');
  await assert.rejects(replacement.enqueue({ requestId: 'drained', operation: 'billing/drain', message: {} }), { code: 'GATEWAY_DRAINING' });
  await replacement.drain('off', 100);
  assert.equal(await redis.get(drainKey), null);
  await replacement.close();
  await redis.del(...Object.values(keys), keys.bytes, drainKey);
}));

test('graceful close drain aborts active upstream work and hands the PEL task back to Redis', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => withRedis(async (redis) => {
  const prefix = `gateway-close-drain-${process.pid}`;
  const keys = streamKeys(prefix, 'billing/close');
  await redis.del(...Object.values(keys), keys.bytes);
  const queue = new RedisOperationQueue({ redis, keyPrefix: prefix, consumerId: 'close-consumer', maxQueueDepth: 10 });
  let signalAbort;
  let markStarted;
  const started = new Promise((resolve) => { markStarted = resolve; });
  const aborted = new Promise((resolve) => { signalAbort = resolve; });
  await queue.register('billing/close', {
    concurrency: 1,
    execute: (_message, { signal }) => new Promise((_resolve, reject) => {
      markStarted();
      signal.addEventListener('abort', () => {
        signalAbort();
        reject(Object.assign(new Error('aborted by close deadline'), { name: 'AbortError' }));
      }, { once: true });
    })
  });
  await queue.enqueue({ requestId: 'close-1', operation: 'billing/close', message: {}, queueTimeoutMs: 3000, execTimeoutMs: 2000, deadlineAt: Date.now() + 5000 });
  await started;
  const report = await queue.drain('graceful', 20);
  await aborted;
  assert.equal(report.active, 0);
  assert.equal(report.remaining, 1);
  assert.equal(await redis.zcard(keys.delayed), 1);
  assert.equal(await queue.pendingForConsumer(), 0);
  await queue.close();
  await redis.del(...Object.values(keys), keys.bytes);
}));

test('removed executor stops pulling, waits for active slot, then reports queued task as OPERATION_NOT_FOUND (GW-LIFE-9)', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => withRedis(async (redis) => {
  const prefix = `gateway-executor-grace-${process.pid}`;
  const keys = streamKeys(prefix, 'billing/remove');
  await redis.del(...Object.values(keys), keys.bytes);
  const queue = new RedisOperationQueue({
    redis, keyPrefix: prefix, consumerId: 'grace-consumer',
    maxQueueDepth: 10, executorGraceMs: 25
  });
  let releaseFirst;
  const firstStarted = new Promise((resolve) => { releaseFirst = resolve; });
  let finishFirst;
  const holdFirst = new Promise((resolve) => { finishFirst = resolve; });
  let executionCount = 0;
  const unregister = await queue.register('billing/remove', {
    concurrency: 1,
    execute: async (message) => {
      executionCount += 1;
      if (message.payload.id === 'active') {
        releaseFirst();
        await holdFirst;
      }
      return { id: message.payload.id };
    }
  });
  const results = [];
  queue.on('result', (result) => results.push(result));
  const missing = new Promise((resolve) => queue.once('errorResult', resolve));
  await queue.enqueue({ requestId: 'active-1', operation: 'billing/remove', message: { payload: { id: 'active' } }, queueTimeoutMs: 3000, execTimeoutMs: 2000, deadlineAt: Date.now() + 5000 });
  await firstStarted;
  await queue.enqueue({ requestId: 'queued-1', operation: 'billing/remove', message: { payload: { id: 'queued' } }, queueTimeoutMs: 3000, execTimeoutMs: 2000, deadlineAt: Date.now() + 5000 });
  const unregisterPromise = unregister();
  finishFirst();
  await unregisterPromise;
  const error = await missing;
  assert.equal(error.error.code, 'OPERATION_NOT_FOUND');
  assert.equal(executionCount, 1);
  assert.equal(results.length, 1);
  assert.equal(await queue.depth('billing/remove'), 0);
  await queue.close();
  await redis.del(...Object.values(keys), keys.bytes);
}));

test('duplicate client key coalesces queued calls and result ACK removes only the sync buffer', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async (context) => withRedis(async (redis) => {
  const prefix = `gateway-idem-e2e-${process.pid}`;
  const keys = streamKeys(prefix, 'billing/idem');
  await redis.del(...Object.values(keys), keys.bytes);
  const queue = new RedisOperationQueue({
    redis, gcra: createGcraLimiter(redis, prefix), keyPrefix: prefix,
    consumerId: 'idem-consumer', maxQueueDepth: 20,
    defaultLimits: [{ name: 'g', rate: 100, burst: 10 }]
  });
  const idempotency = new RedisIdempotencyStore({ redis, keyPrefix: prefix, consumerId: 'idem-consumer', dedupWindowMs: 60000, resultTtlMs: 5000 });
  let executionCount = 0;
  let releaseExecutor;
  let executorStarted;
  const started = new Promise((resolve) => { executorStarted = resolve; });
  const blocked = new Promise((resolve) => { releaseExecutor = resolve; });
  const execute = async () => {
    executionCount += 1;
    executorStarted();
    await blocked;
    return { payload: { total: 42 }, statusCode: 200 };
  };
  await queue.register('billing/idem', {
    execute, concurrency: 2, rateLimits: [{ name: 'g', rate: 100, burst: 10 }], bucket: 'billing/idem', contract: 'http'
  });
  const port = await new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      server.close(() => resolve(address.port));
    });
  });
  const runtime = new GatewayRuntime({ host: '127.0.0.1', port, redis, queue, idempotency, redisPolicy: 'noeviction' });
  runtime.on('error', (error) => assert.fail(error.message));
  runtime.registerAccount({ name: 'idem-client', operationPrefixes: ['billing/*'], token: 'idem-secret' });
  runtime.registerExecutor('billing/idem', execute, { contract: 'http', queueTimeoutMs: 2000, execTimeoutMs: 1000, deadlineMs: 3000 });
  const client = new GatewayClient({ url: `ws://127.0.0.1:${port}`, token: 'idem-secret', podId: 'idem-pod' });
  await runtime.start();
  context.after(async () => {
    releaseExecutor();
    await client.close();
    await runtime.close();
    await queue.close();
  });
  await client.start();
  const firstRequest = client.call('billing/idem', { payload: { value: 21 } }, { idempotencyKey: 'same-key', deadlineMs: 3000 });
  await started;
  const secondRequest = client.call('billing/idem', { payload: { value: 21 } }, { idempotencyKey: 'same-key', deadlineMs: 3000 });
  for (let attempt = 0; attempt < 100 && (runtime.pendingDuplicates.get('idem-original') || []).length === 0; attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
  const originalId = [...runtime.pendingDuplicates.keys()][0];
  assert.equal(runtime.pendingDuplicates.get(originalId)?.length, 1);
  releaseExecutor();
  const [first, second] = await Promise.all([firstRequest, secondRequest]);
  assert.deepEqual(first, { payload: { total: 42 }, statusCode: 200 });
  assert.deepEqual(second, first);
  assert.equal(executionCount, 1);
  const idempotencyKeys = makeIdempotencyKeys({
    keyPrefix: prefix, protocolVersion: '1.1', client: 'idem-client',
    operation: 'billing/idem', idempotencyKey: 'same-key', requestId: originalId
  });
  assert.equal((await redis.get(idempotencyKeys.marker)) !== null, true);
  assert.equal(await redis.exists(idempotencyKeys.result), 0);
}));

test('same protocol 1.1 session resumes a Redis-buffered result after WebSocket reconnect', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async (context) => withRedis(async (redis) => {
  const prefix = `gateway-resume-test-${process.pid}`;
  const keys = streamKeys(prefix, 'billing/resume');
  await redis.del(...Object.values(keys), keys.bytes);
  const queue = new RedisOperationQueue({
    redis, gcra: createGcraLimiter(redis, prefix), keyPrefix: prefix,
    consumerId: 'resume-consumer', maxQueueDepth: 10,
    defaultLimits: [{ name: 'g', rate: 100, burst: 10 }]
  });
  const idempotency = new RedisIdempotencyStore({ redis, keyPrefix: prefix, consumerId: 'resume-consumer', resultTtlMs: 5000 });
  let releaseExecutor;
  let executorStarted;
  const started = new Promise((resolve) => { executorStarted = resolve; });
  const blocked = new Promise((resolve) => { releaseExecutor = resolve; });
  let executionCount = 0;
  const execute = async (message) => {
    executionCount += 1;
    executorStarted();
    await blocked;
    return { payload: { invoiceId: message.payload.invoiceId, total: 42 }, statusCode: 200 };
  };
  await queue.register('billing/resume', {
    execute, concurrency: 1, rateLimits: [{ name: 'g', rate: 100, burst: 10 }], bucket: 'billing/resume', contract: 'http'
  });
  const port = await new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      server.close(() => resolve(address.port));
    });
  });
  const runtime = new GatewayRuntime({ host: '127.0.0.1', port, redis, queue, idempotency, redisPolicy: 'noeviction' });
  runtime.on('error', (error) => assert.fail(error.message));
  runtime.registerAccount({ name: 'resume-client', operationPrefixes: ['billing/*'], token: 'resume-secret' });
  runtime.registerExecutor('billing/resume', execute, { contract: 'http', queueTimeoutMs: 2000, execTimeoutMs: 1000, deadlineMs: 4000 });
  const client = new GatewayClient({ url: `ws://127.0.0.1:${port}`, token: 'resume-secret', podId: 'resume-pod' });
  await runtime.start();
  context.after(async () => {
    releaseExecutor();
    await client.close();
    await runtime.close();
    await queue.close();
  });
  await client.start();
  const pendingCall = client.call('billing/resume', { payload: { invoiceId: 'INV-1' } }, {
    idempotencyKey: 'resume-key', deadlineMs: 4000, queueTimeoutMs: 2000, execTimeoutMs: 1000
  });
  await started;
  for (let attempt = 0; attempt < 100 && ![...client.pending.values()][0]?.accepted; attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
  assert.equal([...client.pending.values()][0]?.accepted, true);
  const socketClosed = new Promise((resolve) => client.socket.once('close', resolve));
  client.socket.close(1012, 'test reconnect');
  await socketClosed;
  releaseExecutor();

  const response = await pendingCall;
  assert.deepEqual(response, { payload: { invoiceId: 'INV-1', total: 42 }, statusCode: 200 });
  assert.equal(executionCount, 1);
  const keysForResult = makeIdempotencyKeys({
    keyPrefix: prefix, protocolVersion: '1.1', client: 'resume-client',
    operation: 'billing/resume', idempotencyKey: 'resume-key', requestId: 'unused'
  });
  assert.equal(await redis.exists(keysForResult.result), 0);
  assert.ok(await redis.pttl(keysForResult.marker) > 0);
}));

test('Gateway out event is accepted and in subscription receives durable async result with ACK', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async (context) => withRedis(async (redis) => {
  const prefix = `gateway-async-e2e-${process.pid}`;
  const stream = `${prefix}:results:{async-client}:billing/async`;
  const queueKeys = streamKeys(prefix, 'billing/async');
  await redis.del(stream, ...Object.values(queueKeys), queueKeys.bytes);
  const queue = new RedisOperationQueue({ redis, gcra: createGcraLimiter(redis, prefix), keyPrefix: prefix, consumerId: 'async-queue', maxQueueDepth: 10 });
  const results = new AsyncResultStore({ redis, keyPrefix: prefix, consumerId: 'async-gateway' });
  await queue.register('billing/async', {
    execute: async (message) => {
      if (message.payload.payload.fail) throw Object.assign(new Error('async worker failed'), { code: 'WORKER_FAILED' });
      return { invoiceId: message.payload.payload.invoiceId, total: 42 };
    },
    concurrency: 1,
    rateLimits: [{ name: 'g', rate: 100, burst: 10 }],
    bucket: 'billing/async',
    contract: 'passthrough'
  });
  const port = await new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      server.close(() => resolve(address.port));
    });
  });
  const runtime = new GatewayRuntime({ host: '127.0.0.1', port, redis, queue, asyncResults: results, redisPolicy: 'noeviction' });
  runtime.on('error', (error) => assert.fail(error.message));
  runtime.registerAccount({ name: 'async-client', operationPrefixes: ['billing/*'], token: 'async-secret' });
  runtime.registerExecutor('billing/async', async () => ({}), { contract: 'passthrough' });
  const client = new GatewayClient({ url: `ws://127.0.0.1:${port}`, token: 'async-secret', podId: 'async-pod' });
  const received = new Promise((resolve) => client.once('asyncResult', resolve));
  client.subscribeResults('billing/async');
  await runtime.start();
  context.after(async () => {
    await client.close();
    await runtime.close();
    await queue.close();
  });
  await client.start();
  const accepted = await client.call('billing/async', { payload: { invoiceId: 'INV-async' } }, {
    messageType: 'event', deadlineMs: 2000, queueTimeoutMs: 1000, execTimeoutMs: 500
  });
  assert.equal(accepted.accepted, true);
  const result = await received;
  assert.equal(result.operation, 'billing/async');
  assert.deepEqual(result.response, { invoiceId: 'INV-async', total: 42 });
  const failedResultPromise = new Promise((resolve) => client.once('asyncResult', resolve));
  const failedAccepted = await client.call('billing/async', { payload: { fail: true } }, {
    messageType: 'event', deadlineMs: 2000, queueTimeoutMs: 1000, execTimeoutMs: 500
  });
  assert.equal(failedAccepted.accepted, true);
  const failedResult = await failedResultPromise;
  assert.equal(failedResult.requestId, failedAccepted.requestId);
  assert.deepEqual(failedResult.error, { code: 'WORKER_FAILED', message: 'async worker failed', retryable: false });
  for (let attempt = 0; attempt < 100 && Number((await redis.xpending(stream, 'redkern-gateway-results'))[0]) > 0; attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
  assert.equal(await redis.xlen(stream), 0);
  assert.equal((await redis.xpending(stream, 'redkern-gateway-results'))[0], 0);
}));

test('queue reclaims a previous consumer PEL and resumes the original task', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => withRedis(async (redis) => {
  const prefix = `gateway-pel-test-${process.pid}`;
  const keys = streamKeys(prefix, 'billing/pel');
  await redis.del(...Object.values(keys), keys.bytes);
  const idempotency = new RedisIdempotencyStore({ redis, keyPrefix: prefix, consumerId: 'new-consumer' });
  const idemRequest = {
    protocolVersion: '1.1', client: 'legacy-client', operation: 'billing/pel',
    podId: 'old-pod', idempotencyKey: 'pel-idem', requestId: 'pel-1'
  };
  const oldIdempotency = new RedisIdempotencyStore({ redis, keyPrefix: prefix, consumerId: 'old-consumer' });
  const oldClaim = await oldIdempotency.claim(idemRequest);
  const queue = new RedisOperationQueue({ redis, keyPrefix: prefix, consumerId: 'new-consumer', maxQueueDepth: 10, idempotency });
  let executed = 0;
  await queue.register('billing/pel', {
    execute: async (message) => { executed += 1; return { resumed: message.payload.value }; }
  });
  const task = {
    requestId: 'pel-1', operation: 'billing/pel', message: { payload: { value: 'old-pending' } },
    queuedAt: Date.now() - 100, queueTimeoutMs: 3000, execTimeoutMs: 1000,
    deadlineMs: 4000, deadlineAt: Date.now() + 5000, priority: 'normal', attempt: 1,
    protocolVersion: '1.1', client: 'legacy-client', podId: 'old-pod', idempotencyKey: 'pel-idem', consumerId: 'old-consumer'
  };
  const streamId = await redis.xadd(keys.normal, '*', 'data', JSON.stringify(task));
  await redis.xreadgroup('GROUP', 'redkern-gateway', 'old-consumer', 'COUNT', 1, 'STREAMS', keys.normal, '>');
  let resolveResult;
  const result = new Promise((resolve) => { resolveResult = resolve; });
  queue.setResultHandlers({
    onResult: async (task, response) => {
      await idempotency.complete(oldClaim, idemRequest, response);
      resolveResult({ task, response });
    }
  });
  assert.equal(await queue.reclaimConsumer('old-consumer', 0), 1);
  const recovered = await result;
  assert.equal(recovered.task.requestId, task.requestId);
  assert.equal(recovered.task.consumerId, 'new-consumer');
  assert.deepEqual(recovered.response, { resumed: 'old-pending' });
  assert.equal(executed, 1);
  const marker = JSON.parse(await redis.get(oldClaim.keys.marker));
  assert.equal(marker.status, 'complete');
  assert.equal((await redis.xpending(keys.normal, 'redkern-gateway'))[0], 0);
  assert.equal((await redis.xpending(keys.normal, 'redkern-gateway', '-', '+', 10, 'old-consumer')).length, 0);
  await queue.close();
  await redis.del(streamId, ...Object.values(keys), keys.bytes);
}));

test('maxDeliveries includes repeated Redis PEL claims before executor execution', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => withRedis(async (redis) => {
  const prefix = `gateway-pel-attempts-${process.pid}`;
  const keys = streamKeys(prefix, 'billing/pel-attempts');
  await redis.del(...Object.values(keys));
  const queue = new RedisOperationQueue({ redis, keyPrefix: prefix, consumerId: 'final-consumer' });
  let executions = 0;
  await queue.register('billing/pel-attempts', {
    maxDeliveries: 2,
    execute: async () => { executions += 1; return { unexpected: true }; }
  });
  const task = {
    requestId: 'pel-attempts-1', operation: 'billing/pel-attempts', client: 'test', podId: 'pod',
    message: {}, queuedAt: Date.now(), queueTimeoutMs: 5000, execTimeoutMs: 1000,
    deadlineAt: Date.now() + 10000, attempt: 1
  };
  const streamId = await redis.xadd(keys.normal, '*', 'data', JSON.stringify(task));
  await redis.xreadgroup('GROUP', 'redkern-gateway', 'old-consumer', 'COUNT', 1, 'STREAMS', keys.normal, '>');
  await redis.xclaim(keys.normal, 'redkern-gateway', 'intermediate-consumer', 0, streamId);

  const errorResult = new Promise((resolve) => queue.once('errorResult', ({ error }) => resolve(error)));
  assert.equal(await queue.reclaimConsumer('intermediate-consumer', 0), 1);
  const error = await errorResult;
  assert.equal(error.code, 'MAX_DELIVERIES_EXCEEDED');
  assert.equal(executions, 0);
  const deadLetters = await redis.xrange(keys.deadLetter, '-', '+');
  assert.equal(deadLetters.length, 1);
  assert.equal(JSON.parse(deadLetters[0][1][deadLetters[0][1].indexOf('error') + 1]).attempt, 3);
  await queue.close();
  await redis.del(...Object.values(keys));
}));

test('legacy migration report retains source entries for missing executors and full destinations', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => withRedis(async (redis) => {
  const prefix = `gateway-migration-report-${process.pid}`;
  const legacyStream = `${prefix}:queue:normal`;
  const keys = streamKeys(prefix, 'billing/migration-report');
  await redis.del(legacyStream, ...Object.values(keys));
  await redis.xgroup('CREATE', legacyStream, 'pod-gateway', '0', 'MKSTREAM');
  const queue = new RedisOperationQueue({ redis, keyPrefix: prefix, consumerId: 'report-migrator', maxQueueDepth: 1 });
  const entries = [
    { requestId: 'missing-executor', operation: 'billing/not-registered' },
    { requestId: 'destination-full', operation: 'billing/migration-report' }
  ];
  for (const item of entries) {
    const payload = {
      podId: 'legacy-pod', request: {
        type: 'call', requestId: item.requestId, operation: item.operation,
        payload: {}, deadlineAt: new Date(Date.now() + 60000).toISOString()
      },
      operation: { value: item.operation }, receivedAt: Date.now(), enqueuedAt: Date.now()
    };
    await redis.xadd(legacyStream, '*', 'payload', JSON.stringify(payload));
  }

  let releaseExecutor;
  let executorStarted;
  const started = new Promise((resolve) => { executorStarted = resolve; });
  const hold = new Promise((resolve) => { releaseExecutor = resolve; });
  await queue.register('billing/migration-report', { execute: async () => { executorStarted(); await hold; return {}; } });
  await queue.enqueue({
    requestId: 'occupy-target', operation: 'billing/migration-report', message: {},
    queuedAt: Date.now(), queueTimeoutMs: 5000, execTimeoutMs: 1000, deadlineAt: Date.now() + 10000
  });
  await started;

  const blocked = await queue.migrateLegacyQueue();
  assert.equal(blocked.complete, false);
  assert.equal(blocked.blocked, 1);
  assert.equal(blocked.missingExecutors, 1);
  assert.equal(await redis.xlen(legacyStream), 2);

  releaseExecutor();
  await new Promise((resolve) => queue.once('result', resolve));
  await queue.register('billing/not-registered', { execute: async () => ({}) });
  const recovered = await queue.migrateLegacyQueue();
  assert.equal(recovered.complete, true);
  assert.equal(recovered.streams, 2);
  assert.equal(await redis.xlen(legacyStream), 0);
  await queue.close();
  await redis.del(legacyStream, ...Object.values(keys));
}));

test('legacy rollback restores unexecuted stream and retry work once after graceful drain', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => withRedis(async (redis) => {
  const prefix = `gateway-rollback-pending-${process.pid}`;
  const legacyStream = `${prefix}:queue:normal`;
  const legacyRetry = `${prefix}:retry:bulk`;
  const legacyRetryData = `${prefix}:retry:data:bulk`;
  const streamKeysForOp = streamKeys(prefix, 'billing/rollback-stream');
  const retryKeysForOp = streamKeys(prefix, 'billing/rollback-retry');
  await redis.del(legacyStream, legacyRetry, legacyRetryData, ...Object.values(streamKeysForOp), ...Object.values(retryKeysForOp));
  const queue = new RedisOperationQueue({ redis, keyPrefix: prefix, consumerId: 'rollback-consumer' });
  await queue.register('billing/rollback-stream', { execute: async () => ({}) });
  await queue.register('billing/rollback-retry', { execute: async () => ({}) });
  await queue.drain('graceful', 10);
  await redis.xgroup('CREATE', legacyStream, 'pod-gateway', '0', 'MKSTREAM');
  const sourceFields = ['payload', JSON.stringify({
    podId: 'legacy-pod',
    request: { type: 'call', requestId: 'rollback-stream-1', operation: 'billing/rollback-stream', payload: {}, deadlineAt: new Date(Date.now() + 60000).toISOString() },
    operation: { value: 'billing/rollback-stream' }
  })];
  await redis.xadd(legacyStream, '*', ...sourceFields);
  const retryMember = 'bulk:legacy-pod:rollback-retry-1';
  const retryPayload = JSON.stringify({
    podId: 'legacy-pod',
    request: { type: 'call', requestId: 'rollback-retry-1', operation: 'billing/rollback-retry', payload: {}, deadlineAt: new Date(Date.now() + 60000).toISOString() },
    operation: { value: 'billing/rollback-retry' }
  });
  const retryScore = Date.now() + 60000;
  await redis.hset(legacyRetryData, retryMember, retryPayload);
  await redis.zadd(legacyRetry, retryScore, retryMember);

  const migrated = await queue.migrateLegacyQueue();
  assert.equal(migrated.complete, true);
  const rolledBack = await queue.rollbackLegacyMigration();
  assert.equal(rolledBack.complete, true);
  assert.equal(rolledBack.restoredStreams, 1);
  assert.equal(rolledBack.restoredRetries, 1);
  assert.equal(await redis.xlen(legacyStream), 1);
  assert.equal(await redis.zscore(legacyRetry, retryMember), String(retryScore));
  assert.equal(await redis.hget(legacyRetryData, retryMember), retryPayload);
  assert.equal(await redis.xlen(streamKeysForOp.normal), 0);
  assert.equal(await redis.zcard(retryKeysForOp.delayed), 0);

  const repeated = await queue.rollbackLegacyMigration();
  assert.equal(repeated.complete, true);
  assert.equal(repeated.restoredStreams, 0);
  assert.equal(await redis.xlen(legacyStream), 1);
  await queue.close();
  await redis.del(legacyStream, legacyRetry, legacyRetryData, ...Object.values(streamKeysForOp), ...Object.values(retryKeysForOp));
}));

test('legacy rollback refuses after a migrated task may have executed', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => withRedis(async (redis) => {
  const prefix = `gateway-rollback-started-${process.pid}`;
  const legacyStream = `${prefix}:queue:normal`;
  const keys = streamKeys(prefix, 'billing/rollback-started');
  await redis.del(legacyStream, ...Object.values(keys));
  const queue = new RedisOperationQueue({ redis, keyPrefix: prefix, consumerId: 'rollback-started' });
  await queue.register('billing/rollback-started', { execute: async () => ({ committed: true }) });
  await redis.xgroup('CREATE', legacyStream, 'pod-gateway', '0', 'MKSTREAM');
  const legacyPayload = JSON.stringify({
    podId: 'legacy-pod',
    request: { type: 'call', requestId: 'rollback-started-1', operation: 'billing/rollback-started', payload: {}, deadlineAt: new Date(Date.now() + 60000).toISOString() },
    operation: { value: 'billing/rollback-started' }
  });
  await redis.xadd(legacyStream, '*', 'payload', legacyPayload);
  queue.on('result', () => {});
  await queue.migrateLegacyQueue();
  await new Promise((resolve) => setTimeout(resolve, 20));
  await queue.drain('graceful', 10);
  const rollback = await queue.rollbackLegacyMigration();
  assert.equal(rollback.complete, false);
  assert.equal(rollback.irreversible, 1);
  assert.equal(rollback.restoredStreams, 0);
  assert.equal(await redis.xlen(legacyStream), 0);
  await queue.close();
  await redis.del(legacyStream, ...Object.values(keys));
}));

test('legacy 2.1.x queue and retry keys migrate into operation streams and delayed storage (GW-MIG-2)', {
  skip: !redisUrl && 'Set REDIS_GCRA_TEST_URL to run the Redis integration test'
}, async () => withRedis(async (redis) => {
  const prefix = `pod-gateway-migration-${process.pid}`;
  const legacyNormal = `${prefix}:queue:normal`;
  const legacyRetry = `${prefix}:retry:bulk`;
  const legacyRetryData = `${prefix}:retry:data:bulk`;
  const normalKeys = streamKeys(prefix, 'billing/legacy-normal');
  const retryKeys = streamKeys(prefix, 'billing/legacy-retry');
  await redis.del(legacyNormal, legacyRetry, legacyRetryData, ...Object.values(normalKeys), normalKeys.bytes, ...Object.values(retryKeys), retryKeys.bytes);
  const queue = new RedisOperationQueue({ redis, keyPrefix: prefix, consumerId: 'migrator', maxQueueDepth: 20 });
  const results = [];
  await queue.register('billing/legacy-normal', { execute: async (message) => ({ migrated: message.payload.id }) });
  await queue.register('billing/legacy-retry', { execute: async (message) => ({ migrated: message.payload.id }) });
  await redis.xgroup('CREATE', legacyNormal, 'pod-gateway', '0', 'MKSTREAM');
  const oldItem = {
    connectionId: 'old-connection', podId: 'old-pod',
    request: { type: 'call', requestId: 'old-normal', operation: 'billing/legacy-normal', payload: { id: 'normal' }, deadlineAt: new Date(Date.now() + 60000).toISOString() },
    operation: { value: 'billing/legacy-normal' }, receivedAt: Date.now(), enqueuedAt: Date.now(), attempts: 0
  };
  const oldStreamId = await redis.xadd(legacyNormal, '*', 'payload', JSON.stringify(oldItem));
  await redis.xreadgroup('GROUP', 'pod-gateway', 'old-server', 'COUNT', 1, 'STREAMS', legacyNormal, '>');
  const oldRetry = {
    connectionId: 'old-connection', podId: 'old-pod',
    request: { type: 'call', requestId: 'old-retry', operation: 'billing/legacy-retry', payload: { id: 'retry' }, deadlineAt: new Date(Date.now() + 60000).toISOString(), priority: 'bulk' },
    operation: { value: 'billing/legacy-retry' }, receivedAt: Date.now(), enqueuedAt: Date.now(), retrySequence: 2
  };
  await redis.hset(legacyRetryData, 'bulk:old-pod:old-retry', JSON.stringify(oldRetry));
  await redis.zadd(legacyRetry, Date.now() - 1, 'bulk:old-pod:old-retry');
  queue.on('result', (result) => results.push(result));

  const originalXack = redis.xack.bind(redis);
  let failLegacyAck = true;
  redis.xack = async (stream, group, id) => {
    if (stream === legacyNormal && failLegacyAck) {
      failLegacyAck = false;
      throw new Error('simulated crash before legacy ACK');
    }
    return originalXack(stream, group, id);
  };
  await assert.rejects(queue.migrateLegacyQueue(), /simulated crash before legacy ACK/);
  assert.equal(await redis.hlen(normalKeys.migrationReceipts), 1);
  redis.xack = originalXack;
  const report = await queue.migrateLegacyQueue();
  assert.equal(report.streams, 0);
  assert.equal(report.retries, 1);
  assert.equal(report.alreadyMigrated, 1);
  assert.equal(report.complete, true);
  assert.equal(await redis.hlen(normalKeys.migrationReceipts), 0);
  assert.equal(await redis.hlen(retryKeys.migrationReceipts), 0);
  assert.equal(await redis.xlen(legacyNormal), 0);
  assert.equal((await redis.xpending(legacyNormal, 'pod-gateway'))[0], 0);
  assert.equal(await redis.zcard(legacyRetry), 0);
  for (let attempt = 0; attempt < 100 && results.length < 2; attempt += 1) await new Promise((resolve) => setTimeout(resolve, 5));
  assert.deepEqual(results.map(({ task }) => task.requestId).sort(), ['old-normal', 'old-retry']);
  assert.equal((await redis.xrange(normalKeys.normal, '-', '+')).length, 0);
  assert.equal((await redis.xrange(retryKeys.bulk, '-', '+')).length, 0);
  await queue.close();
  await redis.del(legacyNormal, legacyRetry, legacyRetryData, ...Object.values(normalKeys), normalKeys.bytes, ...Object.values(retryKeys), retryKeys.bytes, oldStreamId);
}));
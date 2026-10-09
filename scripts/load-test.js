'use strict';

const { performance } = require('node:perf_hooks');
const net = require('node:net');
const Redis = require('ioredis');
const { createRedisClient } = require('@redkern/node-red-kit/redis');
const { GatewayClient } = require('../lib/client.js');
const { GatewayRuntime } = require('../lib/gateway-runtime.js');
const { createGcraLimiter } = require('../lib/server/gcra.js');
const { RedisOperationQueue, streamKeys } = require('../lib/server/queue.js');

const redisUrl = process.env.REDIS_GCRA_TEST_URL || process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const requestCount = positiveInteger(process.env.GATEWAY_LOAD_REQUESTS, 1000);
const concurrency = positiveInteger(process.env.GATEWAY_LOAD_CONCURRENCY, 50);
const minimumRps = optionalPositiveNumber(process.env.GATEWAY_LOAD_MIN_RPS);
const maximumP95Ms = optionalPositiveNumber(process.env.GATEWAY_LOAD_MAX_P95_MS);
const maximumErrorRate = optionalPositiveNumber(process.env.GATEWAY_LOAD_MAX_ERROR_RATE_PERCENT);

async function unusedPort() {
  const server = net.createServer();
  await new Promise((resolve, reject) => server.listen(0, '127.0.0.1', resolve).once('error', reject));
  const port = server.address().port;
  await new Promise((resolve) => server.close(resolve));
  return port;
}

function percentile(sortedValues, percentileValue) {
  if (sortedValues.length === 0) return 0;
  const index = Math.min(sortedValues.length - 1, Math.ceil(percentileValue * sortedValues.length) - 1);
  return sortedValues[index];
}

async function main() {
  const endpoint = new URL(redisUrl);
  const configId = `load-${process.pid}`;
  const keyPrefix = `redkern-load-${process.pid}-${Date.now()}`;
  const operation = 'load/echo';
  const queueKeys = streamKeys(keyPrefix, operation);
  const gcraKeys = [`${keyPrefix}:{${operation}}:g`];
  const redis = createRedisClient({
    ioredis: Redis,
    domain: 'gateway',
    configId,
    role: 'shared',
    mode: 'standalone',
    host: endpoint.hostname,
    port: Number(endpoint.port || 6379),
    db: Number(endpoint.pathname.slice(1) || 0),
    ...(endpoint.username ? { username: decodeURIComponent(endpoint.username) } : {}),
    ...(endpoint.password ? { password: decodeURIComponent(endpoint.password) } : {}),
    ...(endpoint.protocol === 'rediss:' ? { tls: { rejectUnauthorized: true } } : {}),
    logger: { error() {} }
  });
  await redis.connect();
  const gcra = createGcraLimiter(redis.client, keyPrefix);
  const queue = new RedisOperationQueue({
    redis: redis.client,
    gcra,
    keyPrefix,
    consumerId: `load-${process.pid}`,
    maxQueueDepth: requestCount + concurrency,
    maxQueueBytes: 512 * 1024 * 1024,
    defaultLimits: [{ name: 'g', rate: 100000, burst: requestCount + concurrency }]
  });
  await queue.register(operation, {
    concurrency,
    bucket: operation,
    contract: 'passthrough',
    rateLimits: [{ name: 'g', rate: 100000, burst: requestCount + concurrency }],
    maxDeliveries: 1,
    deadlineMs: 120000,
    execute: async (message) => message.payload
  });

  const port = await unusedPort();
  const runtime = new GatewayRuntime({
    host: '127.0.0.1', port, redis: redis.client, queue, gcra,
    redisPolicy: 'noeviction', maxInFlightPerConnection: concurrency + 10
  });
  runtime.registerAccount({ name: 'load-client', operationPrefixes: ['load/*'], token: 'local-load-test-token-0000000000000000' });
  runtime.registerExecutor(operation, async () => ({}), { contract: 'passthrough' });
  const client = new GatewayClient({
    url: `ws://127.0.0.1:${port}`,
    token: 'local-load-test-token-0000000000000000',
    podId: `load-${process.pid}`,
    deliveryMarginMs: 5000
  });
  const latencies = [];
  let failures = 0;
  try {
    await runtime.start();
    await client.start();
    const startedAt = performance.now();
    let next = 0;
    const workers = Array.from({ length: Math.min(concurrency, requestCount) }, async () => {
      while (next < requestCount) {
        const id = next;
        next += 1;
        const requestStartedAt = performance.now();
        try {
          await client.call(operation, { payload: { id } }, { deadlineMs: 120000, execTimeoutMs: 10000 });
          latencies.push(performance.now() - requestStartedAt);
        } catch (error) {
          failures += 1;
          if (!process.env.GATEWAY_LOAD_CONTINUE_ON_ERROR) throw error;
        }
      }
    });
    await Promise.all(workers);
    const elapsedMs = performance.now() - startedAt;
    const sorted = latencies.sort((left, right) => left - right);
    const errorRatePercent = failures / requestCount * 100;
    const report = {
      requests: requestCount,
      concurrency,
      successes: latencies.length,
      failures,
      elapsedMs: round(elapsedMs),
      requestsPerSecond: round(latencies.length / (elapsedMs / 1000)),
      latencyMs: { p50: round(percentile(sorted, 0.50)), p95: round(percentile(sorted, 0.95)), p99: round(percentile(sorted, 0.99)) },
      errorRatePercent: round(errorRatePercent),
      thresholds: { minimumRps, maximumP95Ms, maximumErrorRatePercent: maximumErrorRate },
      thresholdsConfigured: minimumRps !== undefined || maximumP95Ms !== undefined || maximumErrorRate !== undefined
    };
    console.log(JSON.stringify(report, null, 2));
    if (minimumRps !== undefined && report.requestsPerSecond < minimumRps) process.exitCode = 1;
    if (maximumP95Ms !== undefined && report.latencyMs.p95 > maximumP95Ms) process.exitCode = 1;
    if (maximumErrorRate !== undefined && report.errorRatePercent > maximumErrorRate) process.exitCode = 1;
  } finally {
    await client.close();
    await runtime.close();
    await queue.close();
    await redis.client.del(...Object.values(queueKeys), ...gcraKeys);
    await redis.close();
  }
}

function positiveInteger(value, fallback) {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function optionalPositiveNumber(value) {
  if (value === undefined || value === '') return undefined;
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) throw new TypeError('load thresholds must be positive numbers');
  return parsed;
}

function round(value) {
  return Math.round(value * 100) / 100;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
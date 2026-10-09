'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const EventEmitter = require('node:events');
const net = require('node:net');
const WebSocket = require('ws');
const { GatewayRuntime } = require('../../lib/gateway-runtime.js');

async function unusedPort() {
  const server = net.createServer();
  await new Promise((resolve, reject) => server.listen(0, '127.0.0.1', resolve).once('error', reject));
  const { port } = server.address();
  await new Promise((resolve) => server.close(resolve));
  return port;
}

function createInbox(socket) {
  const messages = [];
  const waiters = [];
  socket.on('message', (data) => {
    const message = JSON.parse(data.toString());
    const waiterIndex = waiters.findIndex((waiter) => waiter.expectedType === message.type);
    if (waiterIndex >= 0) {
      const [waiter] = waiters.splice(waiterIndex, 1);
      clearTimeout(waiter.timer);
      waiter.resolve(message);
    } else {
      messages.push(message);
    }
  });
  socket.on('error', (error) => {
    for (const waiter of waiters.splice(0)) {
      clearTimeout(waiter.timer);
      waiter.reject(error);
    }
  });
  return (expectedType) => {
    const messageIndex = messages.findIndex((message) => message.type === expectedType);
    if (messageIndex >= 0) return Promise.resolve(messages.splice(messageIndex, 1)[0]);
    return new Promise((resolve, reject) => {
      const waiter = { expectedType, resolve, reject };
      waiter.timer = setTimeout(() => {
        waiters.splice(waiters.indexOf(waiter), 1);
        reject(new Error(`Timed out waiting for ${expectedType}`));
      }, 3000);
      waiters.push(waiter);
    });
  };
}

test('server rejects startup without an account or explicit bind host', async () => {
  const noAccount = new GatewayRuntime({ host: '127.0.0.1', port: 12345 });
  await assert.rejects(noAccount.start(), /account/i);
  const noHost = new GatewayRuntime({ host: '', port: 12345 });
  noHost.registerAccount({ name: 'client', operationPrefixes: ['*'], token: 'secret' });
  await assert.rejects(noHost.start(), /host is required/i);
});

test('authenticated POD receives filtered capabilities and invokes only local executors', async (context) => {
  const port = await unusedPort();
  const runtime = new GatewayRuntime({ host: '127.0.0.1', port, maxPayload: 1024 * 1024 });
  runtime.on('error', (error) => assert.fail(error.message));
  runtime.registerAccount({ name: 'billing-client', operationPrefixes: ['billing/*'], token: 'billing-secret' });
  runtime.registerExecutor('billing/calculateInvoice', async (message) => ({ invoice: message.payload.id }));
  runtime.registerExecutor('crm/lookup', async () => ({ shouldNotBeVisible: true }));
  await runtime.start();

  const socket = new WebSocket(`ws://127.0.0.1:${port}`);
  const nextMessage = createInbox(socket);
  context.after(async () => {
    if (socket.readyState !== WebSocket.CLOSED) {
      await new Promise((resolve) => {
        socket.once('close', resolve);
        socket.close();
      });
    }
    await runtime.close();
  });
  await new Promise((resolve, reject) => socket.once('open', resolve).once('error', reject));
  const helloAckPromise = nextMessage('hello_ack');
  socket.send(JSON.stringify({ type: 'hello', protocolVersion: '1.1', podId: 'pod-a', sessionId: 'session-a', token: 'billing-secret' }));
  assert.equal((await helloAckPromise).type, 'hello_ack');

  const capabilities = await nextMessage('capabilities');
  assert.deepEqual(capabilities, { type: 'capabilities', items: [{ operation: 'billing/calculateInvoice' }] });

  const acceptedPromise = nextMessage('accepted');
  socket.send(JSON.stringify({ type: 'call', protocolVersion: '1.1', requestId: 'req-1', operation: 'billing/calculateInvoice', payload: { id: 42 } }));
  assert.equal((await acceptedPromise).type, 'accepted');
  const result = await nextMessage('result');
  assert.equal(result.type, 'result');
  assert.deepEqual(result.response, { invoice: 42 });
});

test('capacity queries are capability-filtered, limited to 10 rps, and rate pushes are coalesced', async (context) => {
  const port = await unusedPort();
  const runtime = new GatewayRuntime({ host: '127.0.0.1', port });
  runtime.on('error', (error) => assert.fail(error.message));
  runtime.registerAccount({ name: 'billing-client', operationPrefixes: ['billing/*'], token: 'billing-secret' });
  runtime.registerExecutor('billing/calculate', async () => ({}));
  runtime.queue = { operations: new Map([
    ['billing/calculate', { bucket: 'billing', rateLimits: [{ name: 'g', rate: 5, burst: 2 }] }],
    ['billing/lookup', { bucket: 'billing', rateLimits: [{ name: 'g', rate: 5, burst: 2 }] }]
  ]) };
  let capacityReads = 0;
  runtime.gcra = { async capacity() { capacityReads += 1; return [{ name: 'g', limit: 5, remaining: 2, resetAt: Date.now() }]; } };
  await runtime.start();

  const client = new (require('../../lib/client.js').GatewayClient)({
    url: `ws://127.0.0.1:${port}`, token: 'billing-secret', podId: 'capacity-pod', sessionId: 'capacity-session'
  });
  context.after(async () => {
    await client.close();
    await runtime.close();
  });
  await client.start();

  const snapshots = await client.capacity(['billing/calculate', 'billing/lookup', 'crm/lookup']);
  assert.deepEqual(snapshots.map((item) => item.operation), ['billing/calculate', 'billing/lookup']);
  assert.equal(capacityReads, 1);
  for (let index = 0; index < 9; index += 1) await client.capacity(['billing/calculate']);
  await assert.rejects(client.capacity(['billing/calculate']), (error) => error.code === 'CAPACITY_QUERY_RATE_LIMITED');

  const connection = [...runtime.connections][0];
  runtime.pendingRequests.set('push-1', connection);
  let pushCount = 0;
  client.on('rateLimitDecision', () => { pushCount += 1; });
  const decision = { task: { requestId: 'push-1' }, bucket: 'billing', limits: [{ name: 'g', limit: 5, remaining: 1, resetAt: Date.now() }] };
  runtime._pushRateDecision(decision);
  runtime._pushRateDecision(decision);
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(pushCount, 1);
});

test('a live protocol 1.1 session rejects another session with the same podId', async (context) => {
  const port = await unusedPort();
  const runtime = new GatewayRuntime({ host: '127.0.0.1', port });
  runtime.on('error', (error) => assert.fail(error.message));
  runtime.registerAccount({ name: 'pod-client', operationPrefixes: ['*'], token: 'client-secret' });
  await runtime.start();
  const existingClient = new (require('../../lib/client.js').GatewayClient)({
    url: `ws://127.0.0.1:${port}`, token: 'client-secret', podId: 'replica-a', sessionId: 'session-old'
  });
  context.after(async () => {
    await existingClient.close();
    await runtime.close();
  });
  await existingClient.start();

  const socket = new WebSocket(`ws://127.0.0.1:${port}`, { headers: { Authorization: 'Bearer client-secret' } });
  const nextMessage = createInbox(socket);
  context.after(() => socket.readyState === WebSocket.CLOSED ? undefined : socket.close());
  await new Promise((resolve, reject) => socket.once('open', resolve).once('error', reject));
  socket.send(JSON.stringify({ type: 'hello', protocolVersion: '1.1', podId: 'replica-a', sessionId: 'session-new' }));
  const conflict = await nextMessage('hello_error');
  assert.equal(conflict.error.code, 'POD_ID_CONFLICT');
});

test('server preserves the legacy 1.0 hello and result envelope', async (context) => {
  const port = await unusedPort();
  const runtime = new GatewayRuntime({ host: '127.0.0.1', port });
  runtime.on('error', (error) => assert.fail(error.message));
  runtime.registerAccount({ name: 'legacy', operationPrefixes: ['billing/*'], token: 'legacy-secret' });
  runtime.registerExecutor('billing/calculate', async (message) => ({ total: message.payload.total * 2 }));
  await runtime.start();

  const socket = new WebSocket(`ws://127.0.0.1:${port}`);
  const nextMessage = createInbox(socket);
  context.after(async () => {
    if (socket.readyState !== WebSocket.CLOSED) {
      await new Promise((resolve) => {
        socket.once('close', resolve);
        socket.close();
      });
    }
    await runtime.close();
  });
  await new Promise((resolve, reject) => socket.once('open', resolve).once('error', reject));
  socket.send(JSON.stringify({ type: 'hello', protocolVersion: '1.0', podId: 'legacy-pod', token: 'legacy-secret' }));
  const helloAck = await nextMessage('hello_ack');
  assert.equal(helloAck.protocolVersion, '1.0');
  assert.deepEqual(helloAck.capabilities, [{ operation: 'billing/calculate' }]);

  const acceptedPromise = nextMessage('accepted');
  socket.send(JSON.stringify({ type: 'call', requestId: 'legacy-req', operation: 'billing/calculate', payload: { total: 21 } }));
  await acceptedPromise;
  const result = await nextMessage('result');
  assert.equal(result.ok, true);
  assert.deepEqual(result.payload, { total: 42 });
  assert.ok(result._request.input);
});

test('noeviction memory watermark rejects new requests at 80 percent (GW-NFR-5)', async () => {
  const runtime = new GatewayRuntime({
    host: '127.0.0.1', port: 9555,
    redis: { async info() { return 'used_memory:801\r\nmaxmemory:1000\r\n'; } },
    redisPolicy: 'noeviction',
    queue: Object.assign(new EventEmitter(), {
      async footprintBytes() { return 0; },
      async enqueue() { assert.fail('request must be rejected before XADD'); }
    })
  });
  runtime.registerExecutor('billing/a', async () => ({ ok: true }));
  const sent = [];
  const connection = { account: { name: 'client', operationPrefixes: ['*'] }, family: '1.1', inFlight: 0,
    socket: { readyState: WebSocket.OPEN, send(value) { sent.push(JSON.parse(value)); } } };
  await runtime._dispatch(connection, { type: 'call', requestId: 'memory-1', operation: 'billing/a', deadlineMs: 1000 });
  assert.equal(sent[0].error.code, 'REDIS_MEMORY_PRESSURE');
  assert.equal(sent[0].error.retryable, true);
});

test('allowed volatile eviction limits only the Gateway-owned queue footprint', async () => {
  let enqueued = false;
  const runtime = new GatewayRuntime({
    host: '127.0.0.1', port: 9556,
    redis: { async info() { assert.fail('volatile mode must not use shared Redis memory ratio'); } },
    redisPolicy: 'volatile-lru',
    allowEvictingRedis: true,
    maxGatewayMemoryBytes: 100,
    queue: Object.assign(new EventEmitter(), {
      async footprintBytes() { return 100; },
      async enqueue() { enqueued = true; }
    })
  });
  runtime.registerExecutor('billing/a', async () => ({ ok: true }));
  const sent = [];
  const connection = { account: { name: 'client', operationPrefixes: ['*'] }, family: '1.1', inFlight: 0,
    socket: { readyState: WebSocket.OPEN, send(value) { sent.push(JSON.parse(value)); } } };
  await runtime._dispatch(connection, { type: 'call', requestId: 'memory-2', operation: 'billing/a', deadlineMs: 1000 });
  assert.equal(enqueued, false);
  assert.equal(sent[0].error.code, 'REDIS_MEMORY_PRESSURE');
});

test('volatile Redis eviction sampling exports totals and warns on new evictions', async () => {
  let evictedKeys = 11;
  const warnings = [];
  const runtime = new GatewayRuntime({
    host: '127.0.0.1', port: 9561, redisPolicy: 'volatile-lru',
    redis: { async info(section) { assert.equal(section, 'stats'); return `evicted_keys:${evictedKeys}\r\n`; } },
    logger: { warn(_message, event) { warnings.push(event); } }
  });
  let event;
  runtime.on('redisEvictions', (value) => { event = value; });
  assert.equal(await runtime.sampleRedisEvictions(), 11);
  evictedKeys = 14;
  assert.equal(await runtime.sampleRedisEvictions(), 14);
  assert.deepEqual(event, { total: 14, delta: 3 });
  assert.deepEqual(warnings, [{ total: 14, delta: 3 }]);
});

test('concurrent memory watermark checks share only the in-flight Redis INFO read', async () => {
  let reads = 0;
  const runtime = new GatewayRuntime({
    host: '127.0.0.1', port: 9563,
    redis: { async info() { reads += 1; await new Promise((resolve) => setImmediate(resolve)); return 'used_memory:40\r\nmaxmemory:100\r\n'; } }
  });
  const burst = await Promise.all(Array.from({ length: 50 }, () => runtime._sampleRedisMemoryRatio()));
  assert.equal(reads, 1);
  assert.equal(burst.every((ratio) => ratio === 0.4), true);
  await runtime._sampleRedisMemoryRatio();
  assert.equal(reads, 2);
});

test('concurrent volatile footprint checks share only the in-flight queue read', async () => {
  let reads = 0;
  const queue = new EventEmitter();
  queue.footprintBytes = async () => { reads += 1; await new Promise((resolve) => setImmediate(resolve)); return 42; };
  const runtime = new GatewayRuntime({ host: '127.0.0.1', port: 9564, queue, redisPolicy: 'volatile-lru', allowEvictingRedis: true });
  const burst = await Promise.all(Array.from({ length: 50 }, () => runtime._sampleQueueFootprint()));
  assert.equal(reads, 1);
  assert.equal(burst.every((bytes) => bytes === 42), true);
  await runtime._sampleQueueFootprint();
  assert.equal(reads, 2);
});

test('queued requests consume maxInFlightPerConnection until the queue accepts them', async () => {
  const queue = new EventEmitter();
  let releaseEnqueue;
  let beganEnqueue;
  const enqueueStarted = new Promise((resolve) => { beganEnqueue = resolve; });
  queue.enqueue = () => new Promise((resolve) => {
    beganEnqueue();
    releaseEnqueue = () => resolve({ streamId: '1-0', depth: 1 });
  });
  queue.footprintBytes = async () => 0;
  const runtime = new GatewayRuntime({
    host: '127.0.0.1', port: 9557, maxInFlightPerConnection: 1,
    redis: { async info() { return 'used_memory:1\r\nmaxmemory:100\r\n'; } },
    queue, redisPolicy: 'noeviction'
  });
  runtime.registerAccount({ name: 'client', operationPrefixes: ['*'] });
  runtime.registerExecutor('billing/a', async () => ({ ok: true }));
  const sent = [];
  const connection = { account: { name: 'client', operationPrefixes: ['*'] }, family: '1.1', inFlight: 0,
    socket: { readyState: WebSocket.OPEN, send(value) { sent.push(JSON.parse(value)); } } };
  const first = runtime._dispatch(connection, { type: 'call', requestId: 'flight-1', operation: 'billing/a', deadlineMs: 1000, execTimeoutMs: 100 });
  await enqueueStarted;
  await runtime._dispatch(connection, { type: 'call', requestId: 'flight-2', operation: 'billing/a', deadlineMs: 1000, execTimeoutMs: 100 });
  assert.equal(sent.at(-1).error.code, 'TOO_MANY_IN_FLIGHT');
  releaseEnqueue();
  await first;
  assert.equal(connection.inFlight, 1);
  runtime._deliverQueuedResult({ requestId: 'flight-1', protocolVersion: '1.1', operation: 'billing/a' }, { ok: true }, 'passthrough');
  assert.equal(connection.inFlight, 0);
});

test('legacy HTTP adapter results preserve statusCode without exposing executor metadata', async () => {
  const runtime = new GatewayRuntime({ host: '127.0.0.1', port: 9559 });
  const sent = [];
  const connection = { inFlight: 1, socket: { readyState: WebSocket.OPEN, send(value) { sent.push(JSON.parse(value)); } } };
  runtime.pendingRequests.set('http-legacy-1', connection);
  const metrics = [];
  runtime.on('metric', (metric) => metrics.push(metric));
  await runtime._deliverQueuedResult({
    requestId: 'http-legacy-1', operation: 'billing/http', protocolVersion: '1.0', message: { payload: {} }, receivedAt: Date.now() - 5
  }, { payload: { ok: true }, statusCode: 201 }, 'http');
  assert.equal(sent[0].response.statusCode, 201);
  assert.deepEqual(sent[0].payload, { ok: true });
  assert.equal(Object.hasOwn(sent[0], 'contract'), false);
  assert.equal(connection.inFlight, 0);
  assert.equal(metrics[0].event, 'request.completed');
  assert.equal(metrics[0].service, 'billing');
  assert.equal(metrics[0].operation, 'http');
  assert.equal(metrics[0].contract, '');
  assert.equal(metrics[0].httpStatus, 201);
});

test('metric listener failures are isolated from queue result delivery', () => {
  const runtime = new GatewayRuntime({ host: '127.0.0.1', port: 9562, logger: { warn() {} } });
  runtime.on('metric', () => { throw new Error('metrics sink unavailable'); });
  assert.doesNotThrow(() => runtime._emitMetric('request.processing', { operation: 'billing/invoice' }));
});

test('async result PEL is reassigned only after session reconnect grace expires', async () => {
  let claimed = 0;
  const runtime = new GatewayRuntime({
    host: '127.0.0.1', port: 9560, sessionReconnectGraceMs: 10,
    asyncResults: {
      async claimConsumer() {
        claimed += 1;
        return [{
          stream: 'results', streamId: '1-0',
          entry: { requestId: 'async-1', operation: 'billing/a', response: { value: 42 } }
        }];
      }
    }
  });
  const oldConnection = {
    podId: 'pod-old', sessionId: 'session-old',
    resultSubscriptions: new Set(['client-a\u0000billing/a'])
  };
  const newSent = [];
  const newConnection = {
    account: { name: 'client-a' }, podId: 'pod-new', sessionId: 'session-new',
    socket: { readyState: WebSocket.OPEN, send(value) { newSent.push(JSON.parse(value)); } }
  };
  runtime.resultSubscriptions.set('client-a\u0000billing/a', new Map([
    ['pod-old:session-old', oldConnection],
    ['pod-new:session-new', newConnection]
  ]));
  runtime._handleAsyncDisconnect(oldConnection);
  await new Promise((resolve) => setTimeout(resolve, 2));
  assert.equal(claimed, 0);
  await new Promise((resolve) => setTimeout(resolve, 15));
  assert.equal(claimed, 1);
  assert.equal(newSent[0].type, 'async.result');
  assert.equal(newSent[0].requestId, 'async-1');
  assert.equal(runtime.pendingAsyncAcks.get('async-1').consumerId, 'pod-new:session-new');
  await runtime.close();
});

test('request budgets can only narrow operation queue, execution, and deadline maxima (GW-DL-3)', async () => {
  const queue = Object.assign(new EventEmitter(), { async enqueue(task) { this.task = task; return { streamId: '1-0', depth: 1 }; } });
  queue.footprintBytes = async () => 0;
  const runtime = new GatewayRuntime({
    host: '127.0.0.1', port: 9558,
    redis: { async info() { return 'used_memory:10\r\nmaxmemory:100\r\n'; } },
    queue, redisPolicy: 'noeviction'
  });
  runtime.registerExecutor('billing/a', async () => ({ ok: true }), {
    queueTimeoutMs: 100, execTimeoutMs: 50, deadlineMs: 200
  });
  const connection = { account: { name: 'client', operationPrefixes: ['*'] }, family: '1.1', inFlight: 0,
    socket: { readyState: WebSocket.OPEN, send() {} } };
  await runtime._dispatch(connection, {
    type: 'call', requestId: 'budget-1', operation: 'billing/a',
    queueTimeoutMs: 1000, execTimeoutMs: 500, deadlineMs: 5000
  });
  assert.equal(queue.task.queueTimeoutMs, 100);
  assert.equal(queue.task.execTimeoutMs, 50);
  assert.equal(queue.task.deadlineMs, 200);
  runtime._deliverQueuedError(queue.task, new Error('test complete'));
});
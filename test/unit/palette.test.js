'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const EventEmitter = require('node:events');
const net = require('node:net');
const { createServerState, initializePalette, renderMetrics } = require('../../lib/palette.js');

async function unusedPort() {
  const server = net.createServer();
  await new Promise((resolve, reject) => server.listen(0, '127.0.0.1', resolve).once('error', reject));
  const port = server.address().port;
  await new Promise((resolve) => server.close(resolve));
  return port;
}

test('worker requests correlate one worker-in message with its worker-out response', async () => {
  const state = createServerState({ id: 'server-1' }, {});
  let dispatched;
  const executor = () => {};
  executor.emitWorker = (message) => { dispatched = message; };
  state.executors.set('billing/calculate', executor);

  const resultPromise = state.requestWorker('request-1', {
    operation: 'billing/calculate',
    payload: { payload: { invoiceId: 42 }, topic: 'invoice/request' },
    idempotencyKey: 'key-1',
    deadlineAt: 12345,
    attempt: 2
  }, 1000);

  assert.deepEqual(dispatched, {
    payload: { invoiceId: 42 },
    topic: 'invoice/request',
    redkern: { gateway: {
      requestId: 'request-1',
      operation: 'billing/calculate',
      idempotencyKey: 'key-1',
      deadline: 12345,
      attempt: 2
    } }
  });
  assert.equal(state.respondWorker('request-1', 'crm/lookup', { payload: 'wrong' }), false);
  assert.equal(state.respondWorker('request-1', 'billing/calculate', { payload: 'done' }), true);
  assert.deepEqual(await resultPromise, { payload: 'done' });
});

test('worker requests fail with a stable timeout code', async () => {
  const state = createServerState({ id: 'server-1' }, {});
  state.executors.set('billing/calculate', { emitWorker() {} });
  const timedOut = assert.rejects(
    state.requestWorker('request-2', { operation: 'billing/calculate', payload: null }, 1),
    { code: 'WORKER_FLOW_TIMEOUT' }
  );
  await new Promise((resolve) => setTimeout(resolve, 10));
  await timedOut;
});

test('palette metrics render valid Prometheus text and count registered Gateway listeners', () => {
  const state = {
    node: { id: 'server-a' },
    queue: { deadLetterCounts: new Map([['billing/calculate', 2]]) },
    runtime: { httpServer: { listening: true }, connections: new Set([{}, {}]), redisStats: { evictedKeys: 4 } },
    legacyMigrationReport: { complete: false, blocked: 1, invalidEntries: 1, missingExecutors: 2 }
  };
  const output = renderMetrics({ servers: new Map([['server-a', state]]) });
  assert.match(output, /redkern_gateway_up 1/);
  assert.match(output, /redkern_gateway_connections\{instance="server-a"\} 2/);
  assert.match(output, /redkern_gateway_dead_letter_total\{instance="server-a",operation="billing\/calculate"\} 2/);
  assert.match(output, /redkern_gateway_redis_evicted_keys_total\{instance="server-a"\} 4/);
  assert.match(output, /redkern_gateway_legacy_migration_complete\{instance="server-a"\} 0/);
  assert.match(output, /redkern_gateway_legacy_migration_retained_entries\{instance="server-a"\} 4/);
  assert.ok(output.endsWith('\n'));
});

test('Gateway preStop and bearer metrics share the kit internal port with route-level auth (GW-MIG-3)', async () => {
  const port = await unusedPort();
  const originalPort = process.env.REDKERN_GATEWAY_INTERNAL_PORT;
  const originalToken = process.env.REDKERN_GATEWAY_METRICS_TOKEN;
  const token = '0123456789abcdefghijklmnopqrstuv';
  process.env.REDKERN_GATEWAY_INTERNAL_PORT = String(port);
  process.env.REDKERN_GATEWAY_METRICS_TOKEN = token;
  const RED = {
    events: new EventEmitter(),
    settings: {},
    log: { error() {}, warn() {}, info() {}, debug() {} }
  };
  const palette = initializePalette(RED);
  const node = new EventEmitter();
  node.id = 'prestop-server';
  node.status = () => {};
  const k = palette.rk.bind(node);
  const drainReports = [];
  const rollbackReports = [];
  const gatewayState = {
    options: { drainTimeoutMs: 125, completeDrainTimeoutMs: 500 },
    runtime: { async drain(mode, timeoutMs) { const report = { mode, timeoutMs, remaining: 0 }; drainReports.push(report); return report; } },
    queue: { async rollbackLegacyMigration() { const report = { complete: true, restoredStreams: 1, restoredRetries: 0, irreversible: 0 }; rollbackReports.push(report); return report; } }
  };
  palette.servers.set(node.id, { node: { id: node.id }, runtime: undefined });
  const internalLease = k.internalServer.acquire(node.id, { node: { gatewayState } });
  try {
    await internalLease.ready;
    const base = `http://127.0.0.1:${port}/redkern/gateway`;
    const deniedMetrics = await fetch(`${base}/metrics`);
    assert.equal(deniedMetrics.status, 401);
    const metrics = await fetch(`${base}/metrics`, { headers: { authorization: `Bearer ${token}` } });
    assert.equal(metrics.status, 200);
    assert.match(await metrics.text(), /redkern_gateway_up/);

    const remotePreStop = await fetch(`${base}/prestop`, {
      method: 'POST', headers: { 'content-type': 'application/json', 'x-forwarded-for': '127.0.0.1' }, body: JSON.stringify({ mode: 'graceful' })
    });
    assert.equal(remotePreStop.status, 200);
    assert.deepEqual(await remotePreStop.json(), { mode: 'graceful', gateways: [{ mode: 'graceful', timeoutMs: 125, remaining: 0 }] });
    assert.equal(drainReports.length, 1);

    const rollback = await fetch(`${base}/prestop`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ mode: 'rollback' })
    });
    assert.equal(rollback.status, 200);
    assert.deepEqual(await rollback.json(), {
      mode: 'rollback',
      gateways: [{ mode: 'rollback', complete: true, restoredStreams: 1, restoredRetries: 0, irreversible: 0,
        drain: { mode: 'graceful', timeoutMs: 125, remaining: 0 } }]
    });
    assert.deepEqual(drainReports.map((report) => report.mode), ['graceful', 'graceful']);
    assert.equal(rollbackReports.length, 1);
  } finally {
    await internalLease.release();
    await new Promise((resolve) => node.emit('close', false, resolve));
    if (originalPort === undefined) delete process.env.REDKERN_GATEWAY_INTERNAL_PORT;
    else process.env.REDKERN_GATEWAY_INTERNAL_PORT = originalPort;
    if (originalToken === undefined) delete process.env.REDKERN_GATEWAY_METRICS_TOKEN;
    else process.env.REDKERN_GATEWAY_METRICS_TOKEN = originalToken;
  }
});
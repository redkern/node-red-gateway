'use strict';

const kit = require('@redkern/node-red-kit');
const { GatewayRuntime } = require('./gateway-runtime.js');

const palettes = new WeakMap();
const metricsContentType = 'text/plain; version=0.0.4; charset=utf-8';

function escapeMetricLabel(value) {
  return String(value).replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n');
}

function renderMetrics(palette) {
  const servers = [...palette.servers.values()];
  const ready = servers.filter((state) => state.runtime?.httpServer?.listening).length;
  const queueBytes = servers.reduce((total, state) => total + (state.queue?.lastFootprintBytes || 0), 0);
  const lines = [
    '# HELP redkern_gateway_up Number of Gateway listeners that are accepting connections.',
    '# TYPE redkern_gateway_up gauge',
    `redkern_gateway_up ${ready}`,
    '# HELP redkern_gateway_connections Current WebSocket connections.',
    '# TYPE redkern_gateway_connections gauge',
    '# HELP redkern_gateway_memory_bytes Gateway-owned queued bytes in Redis.',
    '# TYPE redkern_gateway_memory_bytes gauge',
    `redkern_gateway_memory_bytes ${queueBytes}`,
    '# HELP redkern_gateway_dead_letter_total Terminal queue failures written to the dead-letter stream.',
    '# TYPE redkern_gateway_dead_letter_total counter',
    '# HELP redkern_gateway_redis_evicted_keys_total Redis keys evicted under a volatile policy.',
    '# TYPE redkern_gateway_redis_evicted_keys_total counter',
    '# HELP redkern_gateway_legacy_migration_complete Whether startup legacy migration completed without skipped entries.',
    '# TYPE redkern_gateway_legacy_migration_complete gauge',
    '# HELP redkern_gateway_legacy_migration_retained_entries Legacy entries retained because migration was blocked or invalid.',
    '# TYPE redkern_gateway_legacy_migration_retained_entries gauge'
  ];
  for (const state of servers) {
    const instance = escapeMetricLabel(state.node.id);
    lines.push(`redkern_gateway_connections{instance="${instance}"} ${state.runtime?.connections.size || 0}`);
    lines.push(`redkern_gateway_redis_evicted_keys_total{instance="${instance}"} ${state.runtime?.redisStats?.evictedKeys || 0}`);
    if (state.legacyMigrationReport) {
      const report = state.legacyMigrationReport;
      const retained = Number(report.blocked || 0) + Number(report.invalidEntries || 0) + Number(report.missingExecutors || 0);
      lines.push(`redkern_gateway_legacy_migration_complete{instance="${instance}"} ${report.complete ? 1 : 0}`);
      lines.push(`redkern_gateway_legacy_migration_retained_entries{instance="${instance}"} ${retained}`);
    }
    for (const [key, count] of state.asyncResults?.expiredByOperation || []) {
      const [client, operation] = key.split('\u0000');
      lines.push(`redkern_gateway_async_results_expired_total{client="${escapeMetricLabel(client)}",operation="${escapeMetricLabel(operation)}"} ${count}`);
    }
    for (const [operation, count] of state.queue?.deadLetterCounts || []) {
      lines.push(`redkern_gateway_dead_letter_total{instance="${instance}",operation="${escapeMetricLabel(operation)}"} ${count}`);
    }
  }
  return `${lines.join('\n')}\n`;
}

function initializePalette(RED) {
  let palette = palettes.get(RED);
  if (palette) return palette;
  const state = { servers: new Map(), clients: new Map() };
  const rk = kit.init(RED, {
    domain: 'gateway',
    settingsType: 'redkern-gateway-server-config',
    secrets: { metrics: { required: false } },
    metrics: { contentType: metricsContentType, metrics: () => renderMetrics(state) },
    internalServer: {
      port: 9552,
      routes: [{
        method: 'GET',
        path: '/metrics',
        auth: { token: 'metrics' },
        handler: async (_request, response) => response.text(renderMetrics(state), metricsContentType)
      }, {
        method: 'POST',
        path: '/prestop',
        auth: 'local',
        handler: async (request, response, owners) => {
          const mode = request.body?.mode || 'complete';
          if (!['complete', 'off', 'graceful', 'rollback'].includes(mode)) {
            response.status(400).json({ code: 'INVALID_DRAIN_MODE' });
            return;
          }
          const reports = [];
          for (const owner of owners.values()) {
            const gateway = owner.node?.gatewayState;
            const runtime = gateway?.runtime;
            if (!runtime) continue;
            const drainMode = mode === 'rollback' ? 'graceful' : mode;
            const timeoutMs = drainMode === 'complete'
              ? gateway.options?.completeDrainTimeoutMs || 600000
              : gateway.options?.drainTimeoutMs || 12000;
            const drain = await runtime.drain(drainMode, timeoutMs);
            if (mode === 'rollback') {
              if (drain.active > 0) reports.push({ mode, complete: false, code: 'MIGRATION_ROLLBACK_BUSY', drain });
              else reports.push({ mode, ...(await gateway.queue?.rollbackLegacyMigration()), drain });
            } else {
              reports.push(drain);
            }
          }
          response.json({ mode, gateways: reports });
        }
      }]
    }
  });
  palette = { rk, ...state };
  palettes.set(RED, palette);
  return palette;
}

function createServerState(node, options) {
  const state = {
    node,
    options,
    accounts: new Map(),
    executors: new Map(),
    executorMetadata: new Map(),
    pendingWorkers: new Map(),
    dependents: new Set(),
    queue: undefined,
    runtime: undefined
  };
  state.addAccount = (account) => {
    if (state.accounts.has(account.name)) throw new Error(`Duplicate gateway account: ${account.name}`);
    state.accounts.set(account.name, account);
    const release = state.runtime ? state.runtime.registerAccount(account) : undefined;
    return () => {
      state.accounts.delete(account.name);
      release?.();
    };
  };
  state.addExecutor = (operation, execute, metadata = {}) => {
    if (state.executors.has(operation)) throw new Error(`Executor already registered: ${operation}`);
    state.executors.set(operation, execute);
    state.executorMetadata.set(operation, metadata);
    let unregisterQueue;
    const queueReady = state.queue
      ? state.queue.register(operation, {
          execute,
          concurrency: metadata.concurrency || 32,
          rateLimits: metadata.rateLimits,
          bucket: metadata.bucket || 'default',
          contract: metadata.contract || 'passthrough',
          retryClass: metadata.retryClass || 'never',
          deadlineMs: metadata.deadlineMs || 30000
        }).then((release) => { unregisterQueue = release; })
      : Promise.resolve();
    const release = state.runtime ? state.runtime.registerExecutor(operation, execute, metadata) : undefined;
    const unregister = async () => {
      if (state.executors.get(operation) === execute) {
        state.executors.delete(operation);
        state.executorMetadata.delete(operation);
      }
      release?.();
      if (unregisterQueue) await unregisterQueue();
    };
    unregister.ready = queueReady;
    return unregister;
  };
  state.requestWorker = (requestId, message, timeoutMs) => new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      state.pendingWorkers.delete(requestId);
      reject(Object.assign(new Error('Worker flow did not return a response'), { code: 'WORKER_FLOW_TIMEOUT' }));
    }, timeoutMs);
    timer.unref?.();
    state.pendingWorkers.set(requestId, { resolve, reject, timer, operation: message.operation });
    const workerMessage = message.payload && typeof message.payload === 'object' && !Buffer.isBuffer(message.payload)
      ? { ...message.payload }
      : { payload: message.payload };
    workerMessage.redkern = {
      ...(workerMessage.redkern && typeof workerMessage.redkern === 'object' ? workerMessage.redkern : {}),
      gateway: {
        requestId,
        operation: message.operation,
        idempotencyKey: message.idempotencyKey,
        deadline: message.deadlineAt,
        attempt: Number(message.attempt || 1)
      }
    };
    const dispatch = state.executors.get(message.operation)?.emitWorker;
    if (typeof dispatch !== 'function') {
      clearTimeout(timer);
      state.pendingWorkers.delete(requestId);
      reject(Object.assign(new Error('Worker is not ready'), { code: 'NODE_NOT_READY', retryable: true }));
      return;
    }
    dispatch(workerMessage);
  });
  state.respondWorker = (requestId, operation, response) => {
    const pending = state.pendingWorkers.get(requestId);
    if (!pending || pending.operation !== operation) return false;
    clearTimeout(pending.timer);
    state.pendingWorkers.delete(requestId);
    pending.resolve(response);
    return true;
  };
  state.setStatus = (status) => {
    for (const dependent of state.dependents) dependent(status);
  };
  return state;
}

function parsePrefixes(value) {
  if (Array.isArray(value)) return value;
  if (typeof value !== 'string' || value.trim() === '') return [];
  return value.split(',').map((prefix) => prefix.trim()).filter(Boolean);
}

function statusText(status) {
  return String(status || 'starting').slice(0, 20);
}

function statusFor(node, color, text) {
  node.status({ fill: color, shape: color === 'green' ? 'dot' : 'ring', text: statusText(text) });
}

module.exports = { createServerState, initializePalette, parsePrefixes, renderMetrics, statusFor };
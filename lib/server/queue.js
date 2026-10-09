'use strict';

const EventEmitter = require('node:events');
const { defineScripts, hashTag } = require('@redkern/node-red-kit/redis');
const { parseOperation } = require('../protocol.js');
const clientsWithQueueScripts = new WeakSet();

const GROUP = 'redkern-gateway';
const ADMIT_LUA = `
local depth = redis.call('XLEN', KEYS[1]) + redis.call('XLEN', KEYS[2]) + redis.call('ZCARD', KEYS[3])
local queuedBytes = tonumber(redis.call('GET', KEYS[4])) or 0
local payloadBytes = tonumber(ARGV[3])
if depth >= tonumber(ARGV[4]) or queuedBytes + payloadBytes > tonumber(ARGV[5]) then
  return {0, depth, queuedBytes}
end
local streamId = redis.call('XADD', KEYS[tonumber(ARGV[1])], '*', 'data', ARGV[2])
redis.call('INCRBY', KEYS[4], payloadBytes)
return {1, streamId, depth + 1, queuedBytes + payloadBytes}
`;
const MIGRATE_TASK_LUA = `
local existing = redis.call('HGET', KEYS[5], ARGV[6])
if existing then
  redis.call('PEXPIRE', KEYS[5], tonumber(ARGV[7]))
  return {2, existing}
end
local depth = redis.call('XLEN', KEYS[1]) + redis.call('XLEN', KEYS[2]) + redis.call('ZCARD', KEYS[3])
local queuedBytes = tonumber(redis.call('GET', KEYS[4])) or 0
if depth >= tonumber(ARGV[4]) or queuedBytes + tonumber(ARGV[3]) > tonumber(ARGV[5]) then
  return {0, depth, queuedBytes}
end
local streamId = redis.call('XADD', KEYS[tonumber(ARGV[1])], '*', 'data', ARGV[2])
redis.call('INCRBY', KEYS[4], ARGV[3])
redis.call('HSET', KEYS[5], ARGV[6], streamId)
redis.call('PEXPIRE', KEYS[5], ARGV[7])
return {1, streamId}
`;
const MIGRATE_RETRY_LUA = `
local existing = redis.call('HGET', KEYS[4], ARGV[4])
if existing then
  redis.call('PEXPIRE', KEYS[4], tonumber(ARGV[6]))
  return {2, existing}
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
redis.call('INCRBY', KEYS[3], ARGV[5])
redis.call('HSET', KEYS[4], ARGV[4], ARGV[1])
redis.call('PEXPIRE', KEYS[4], ARGV[6])
return {1, ARGV[1]}
`;
const ACK_DELETE_LUA = `
local entry = redis.call('XRANGE', KEYS[1], ARGV[2], ARGV[2], 'COUNT', 1)
local payloadBytes = 0
if #entry > 0 then
  for index = 1, #entry[1][2], 2 do
    if entry[1][2][index] == 'data' then payloadBytes = math.ceil(string.len(entry[1][2][index + 1]) * tonumber(ARGV[4]) + tonumber(ARGV[5])) end
  end
end
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
if payloadBytes > 0 then redis.call('DECRBY', KEYS[2], payloadBytes) end
redis.call('XTRIM', KEYS[1], 'MINID', '~', ARGV[3])
return 1
`;
const DEFER_LUA = `
local entry = redis.call('XRANGE', KEYS[1], ARGV[1], ARGV[1], 'COUNT', 1)
if #entry == 0 then return 0 end
local payload
for index = 1, #entry[1][2], 2 do
  if entry[1][2][index] == 'data' then payload = entry[1][2][index + 1] end
end
if not payload then return redis.error_reply('QUEUE_ENTRY_MISSING_DATA') end
local oldBytes = math.ceil(string.len(payload) * tonumber(ARGV[6]) + tonumber(ARGV[7]))
local task = cjson.decode(payload)
task.reservedUntil = tonumber(ARGV[3])
task.rateLimitWaitMs = (tonumber(task.rateLimitWaitMs) or 0) + (tonumber(ARGV[5]) or 0)
payload = cjson.encode(task)
redis.call('HSET', KEYS[3], ARGV[2], payload)
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
redis.call('XACK', KEYS[1], ARGV[4], ARGV[1])
redis.call('XDEL', KEYS[1], ARGV[1])
redis.call('INCRBY', KEYS[4], math.ceil(string.len(payload) * tonumber(ARGV[6]) + tonumber(ARGV[7])) - oldBytes)
return 1
`;
const PROMOTE_DELAYED_LUA = `
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + tonumber(time[2]) / 1000
local dueAt = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not dueAt then return {0, 0} end
local waitMs = tonumber(dueAt) - now
if waitMs > 0 then return {0, waitMs} end
local payload = redis.call('HGET', KEYS[2], ARGV[1])
if not payload then
  redis.call('ZREM', KEYS[1], ARGV[1])
  return redis.error_reply('DELAYED_QUEUE_DATA_MISSING')
end
local streamId = redis.call('XADD', KEYS[3], '*', 'data', payload)
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return {1, streamId}
`;
const RETRY_DELAYED_LUA = `
local entry = redis.call('XRANGE', KEYS[1], ARGV[1], ARGV[1], 'COUNT', 1)
local oldBytes = 0
if #entry > 0 then
  for index = 1, #entry[1][2], 2 do
    if entry[1][2][index] == 'data' then oldBytes = math.ceil(string.len(entry[1][2][index + 1]) * tonumber(ARGV[6]) + tonumber(ARGV[7])) end
  end
end
redis.call('HSET', KEYS[3], ARGV[2], ARGV[3])
redis.call('ZADD', KEYS[2], ARGV[4], ARGV[2])
redis.call('XACK', KEYS[1], ARGV[5], ARGV[1])
redis.call('XDEL', KEYS[1], ARGV[1])
redis.call('INCRBY', KEYS[4], math.ceil(string.len(ARGV[3]) * tonumber(ARGV[6]) + tonumber(ARGV[7])) - oldBytes)
return 1
`;
const DEAD_LETTER_LUA = `
local entry = redis.call('XRANGE', KEYS[1], ARGV[1], ARGV[1], 'COUNT', 1)
if #entry == 0 then return 0 end
local payload
for index = 1, #entry[1][2], 2 do
  if entry[1][2][index] == 'data' then payload = entry[1][2][index + 1] end
end
if not payload then return redis.error_reply('QUEUE_ENTRY_MISSING_DATA') end
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + tonumber(time[2]) / 1000
local dlqId = redis.call('XADD', KEYS[3], '*', 'data', payload, 'error', ARGV[4], 'failedAt', string.format('%.0f', now))
redis.call('XACK', KEYS[1], ARGV[2], ARGV[1])
redis.call('XDEL', KEYS[1], ARGV[1])
redis.call('DECRBY', KEYS[2], math.ceil(string.len(payload) * tonumber(ARGV[6]) + tonumber(ARGV[7])))
redis.call('XTRIM', KEYS[3], 'MINID', string.format('%.0f', now - tonumber(ARGV[3])) .. '-0')
redis.call('XTRIM', KEYS[3], 'MAXLEN', tonumber(ARGV[5]))
redis.call('PEXPIRE', KEYS[3], tonumber(ARGV[3]))
return dlqId
`;

function streamKeys(keyPrefix, operation) {
  const base = `${keyPrefix}:queue:{${operation}}`;
  return {
    normal: `${base}:normal`,
    bulk: `${base}:bulk`,
    bytes: `${base}:bytes`,
    migrationReceipts: `${keyPrefix}:migration-receipts:{${operation}}`,
    migrationStats: `${keyPrefix}:migration-stats:{${operation}}`,
    deadLetter: `${keyPrefix}:dead-letter:{${operation}}`,
    delayed: `${keyPrefix}:delayed:{${operation}}`,
    delayedData: `${keyPrefix}:delayed:data:{${operation}}`
  };
}

class RedisOperationQueue extends EventEmitter {
  constructor({
    redis, gcra, keyPrefix, consumerId,
    maxQueueDepth = 10000,
    maxQueueBytes = 64 * 1024 * 1024,
    maxGatewayMemoryBytes = 1024 * 1024 * 1024,
    queueBytesFactor = 1.5,
    queueEntryOverheadBytes = 128,
    maxDeliveries = 3,
    dlqRetentionMs = 86400000,
    maxDlqEntries = 10000,
    defaultLimits = [],
    executorGraceMs = 30000,
    lease,
    idempotency
  }) {
    super();
    if (!redis || typeof redis.xadd !== 'function') throw new TypeError('Redis Streams client is required');
    if (!Number.isSafeInteger(maxDeliveries) || maxDeliveries < 1) throw new TypeError('maxDeliveries must be a positive integer');
    if (!Number.isSafeInteger(dlqRetentionMs) || dlqRetentionMs < 1) throw new TypeError('dlqRetentionMs must be a positive integer');
    if (!Number.isSafeInteger(maxDlqEntries) || maxDlqEntries < 1) throw new TypeError('maxDlqEntries must be a positive integer');
    if (!Number.isFinite(queueBytesFactor) || queueBytesFactor < 1) throw new TypeError('queueBytesFactor must be at least 1');
    if (!Number.isSafeInteger(queueEntryOverheadBytes) || queueEntryOverheadBytes < 0) throw new TypeError('queueEntryOverheadBytes must be a non-negative integer');
    this.redis = redis;
    this.gcra = gcra;
    this.keyPrefix = keyPrefix;
    this.drainKey = `${keyPrefix}:drain`;
    this.consumerId = consumerId;
    this.maxQueueDepth = maxQueueDepth;
    this.maxQueueBytes = maxQueueBytes;
    this.maxGatewayMemoryBytes = maxGatewayMemoryBytes;
    this.queueBytesFactor = queueBytesFactor;
    this.queueEntryOverheadBytes = queueEntryOverheadBytes;
    this.maxDeliveries = maxDeliveries;
    this.dlqRetentionMs = dlqRetentionMs;
    this.maxDlqEntries = maxDlqEntries;
    this.migrationReceiptTtlMs = 30 * 24 * 60 * 60 * 1000;
    this.executorGraceMs = executorGraceMs;
    this.defaultLimits = defaultLimits;
    this.lease = lease;
    this.idempotency = idempotency;
    this.deadLetterCounts = new Map();
    this.resultHandler = undefined;
    this.errorHandler = undefined;
    this.operations = new Map();
    this.closed = false;
    this.drainMode = 'off';
    this.slowTimer = undefined;
    if (!clientsWithQueueScripts.has(redis)) {
      defineScripts(redis, {
        gatewayAckDelete: { lua: ACK_DELETE_LUA, numberOfKeys: 2 },
        gatewayDeferEntry: { lua: DEFER_LUA, numberOfKeys: 4 },
        gatewayPromoteDelayed: { lua: PROMOTE_DELAYED_LUA, numberOfKeys: 3 },
        gatewayRetryDelayed: { lua: RETRY_DELAYED_LUA, numberOfKeys: 4 },
        gatewayDeadLetter: { lua: DEAD_LETTER_LUA, numberOfKeys: 3 },
        gatewayMigrateTask: { lua: MIGRATE_TASK_LUA, numberOfKeys: 5 },
        gatewayMigrateRetry: { lua: MIGRATE_RETRY_LUA, numberOfKeys: 4 },
        gatewayAdmitTask: { lua: ADMIT_LUA, numberOfKeys: 4 }
      });
      clientsWithQueueScripts.add(redis);
    }
  }

  setResultHandlers({ onResult, onError }) {
    if (onResult !== undefined && typeof onResult !== 'function') throw new TypeError('onResult must be a function');
    if (onError !== undefined && typeof onError !== 'function') throw new TypeError('onError must be a function');
    this.resultHandler = onResult;
    this.errorHandler = onError;
  }

  async register(operation, {
    execute,
    concurrency = 32,
    rateLimits,
    bucket = 'default',
    contract = 'passthrough',
    retryClass = 'never',
    deadlineMs = 30000,
    maxDeliveries = this.maxDeliveries
  }) {
    const previous = this.operations.get(operation);
    if (previous?.accepting) throw new Error(`Queue operation already registered: ${operation}`);
    if (previous?.missingTimer) clearTimeout(previous.missingTimer);
    if (typeof execute !== 'function' || !Number.isSafeInteger(concurrency) || concurrency < 1 ||
        !Number.isSafeInteger(maxDeliveries) || maxDeliveries < 1) {
      throw new TypeError('operation requires an executor, positive concurrency, and positive maxDeliveries');
    }
    const keys = streamKeys(this.keyPrefix, operation);
    for (const stream of [keys.normal, keys.bulk]) {
      try { await this.redis.xgroup('CREATE', stream, GROUP, '0', 'MKSTREAM'); } catch (error) {
        if (!String(error.message).includes('BUSYGROUP')) throw error;
      }
    }
    const state = {
      operation,
      execute,
      concurrency,
      rateLimits: rateLimits || this.defaultLimits,
      bucket,
      contract,
      retryClass,
      maxDeliveries,
      trimAfterMs: deadlineMs + 600000,
      keys,
      active: 0,
      activeEntries: new Set(),
      accepting: true,
      pumping: false,
      pumpAgain: false,
      normalStreak: 0,
      delayedTimers: new Map(),
      recovered: []
    };
    this.operations.set(operation, state);
    return () => this.unregister(operation, state);
  }

  async enqueue(task) {
    if (this.drainMode !== 'off') throw Object.assign(new Error('Gateway is draining'), { code: 'GATEWAY_DRAINING', retryable: true });
    const state = this.operations.get(task.operation);
    if (!state) throw Object.assign(new Error('Operation not found'), { code: 'OPERATION_NOT_FOUND' });
    if (!state.accepting) throw Object.assign(new Error('Operation executor is closing'), { code: 'OPERATION_NOT_FOUND' });
    const priority = task.priority === 'bulk' ? 'bulk' : 'normal';
    const queued = { ...task, queuedAt: Date.now(), priority, contract: state.contract, attempt: Number(task.attempt || 1) };
    const payload = JSON.stringify(queued);
    const payloadBytes = this._estimateBytes(Buffer.byteLength(payload));
    const admitted = await this.redis.gatewayAdmitTask(
      state.keys.normal,
      state.keys.bulk,
      state.keys.delayed,
      state.keys.bytes,
      priority === 'normal' ? 1 : 2,
      payload,
      payloadBytes,
      this.maxQueueDepth,
      this.maxQueueBytes
    );
    if (Number(admitted?.[0]) !== 1) {
      throw Object.assign(new Error('Gateway queue is full'), { code: 'QUEUE_FULL', retryable: true });
    }
    const streamId = String(admitted[1]);
    const depth = Number(admitted[2]);
    this._pump(state).catch((error) => this.emitError(error, task));
    return { streamId, depth };
  }

  async depth(operation) {
    const state = this.operations.get(operation);
    if (!state) return 0;
    const [normal, bulk, delayed] = await Promise.all([
      this.redis.xlen(state.keys.normal),
      this.redis.xlen(state.keys.bulk),
      this.redis.zcard(state.keys.delayed)
    ]);
    return Number(normal) + Number(bulk) + Number(delayed);
  }

  async footprintBytes() {
    const values = await Promise.all([...this.operations.values()].map((state) => this.redis.get(state.keys.bytes)));
    const footprint = values.reduce((total, value) => total + Math.max(0, Number(value) || 0), 0);
    this.lastFootprintBytes = footprint;
    return footprint;
  }

  _estimateBytes(payloadLength) {
    return Math.ceil(payloadLength * this.queueBytesFactor + this.queueEntryOverheadBytes);
  }

  async _recordMigrationStatus(task, status) {
    if (!task?.migrationOrigin) return;
    const state = this.operations.get(task.operation);
    if (!state) return;
    await this.redis.hset(state.keys.migrationStats, task.requestId, status);
    await this.redis.pexpire(state.keys.migrationStats, this.migrationReceiptTtlMs);
  }

  startRecovery(intervalMs = 5000) {
    if (this.slowTimer) return;
    this.slowTimer = setInterval(() => {
      for (const state of this.operations.values()) {
        this._promoteAll(state).catch((error) => this.emitError(error));
        this._pump(state).catch((error) => this.emitError(error));
      }
    }, intervalMs);
    this.slowTimer.unref?.();
  }

  async restoreDrainState() {
    const value = await this.redis.get(this.drainKey);
    if (!value) return 'off';
    let state;
    try { state = JSON.parse(value); } catch { state = { mode: 'complete' }; }
    if (state.mode === 'complete' || state.mode === 'graceful') {
      this.drainMode = state.mode;
      if (this.drainMode === 'complete') for (const operation of this.operations.keys()) this._pump(this.operations.get(operation)).catch((error) => this.emitError(error));
      return this.drainMode;
    }
    return 'off';
  }

  async drain(mode, timeoutMs) {
    if (!['graceful', 'complete', 'off'].includes(mode)) throw new TypeError('drain mode must be graceful, complete, or off');
    if (mode === 'off') {
      await this.redis.del(this.drainKey);
      this.drainMode = 'off';
      for (const state of this.operations.values()) this._pump(state).catch((error) => this.emitError(error));
    } else {
      this.drainMode = mode;
      await this.redis.set(this.drainKey, JSON.stringify({ mode, startedAt: Date.now() }));
      if (mode === 'complete') for (const state of this.operations.values()) this._pump(state).catch((error) => this.emitError(error));
    }
    const deadline = Date.now() + timeoutMs;
    let remaining = await this.totalDepth();
    while (Date.now() < deadline) {
      const active = [...this.operations.values()].reduce((total, state) => total + state.active, 0);
      if (active === 0 && (mode !== 'complete' || remaining === 0)) break;
      if (mode === 'complete') for (const state of this.operations.values()) this._pump(state).catch((error) => this.emitError(error));
      await new Promise((resolve) => setTimeout(resolve, 25));
      remaining = await this.totalDepth();
    }
    if (mode !== 'off') {
      for (const state of this.operations.values()) {
        for (const entry of state.activeEntries) {
          entry.forcedHandoff = true;
          entry.abortController?.abort();
        }
      }
      const settleDeadline = Date.now() + 1000;
      while (Date.now() < settleDeadline && [...this.operations.values()].some((state) => state.active > 0)) {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
    }
    remaining = await this.totalDepth();
    return { mode, remaining, active: [...this.operations.values()].reduce((total, state) => total + state.active, 0) };
  }

  async totalDepth() {
    const counts = await Promise.all([...this.operations.keys()].map((operation) => this.depth(operation)));
    return counts.reduce((total, count) => total + count, 0);
  }
  async pendingForConsumer() {
    const pending = await Promise.all([...this.operations.values()].flatMap((state) =>
      [state.keys.normal, state.keys.bulk].map((stream) =>
        this.redis.xpending(stream, GROUP, '-', '+', 1000, this.consumerId)
      )
    ));
    return pending.reduce((total, entries) => total + (Array.isArray(entries) ? entries.length : 0), 0);
  }

  async reclaimConsumer(previousConsumerId, minIdleMs) {
    if (typeof previousConsumerId !== 'string' || previousConsumerId.length === 0 || previousConsumerId === this.consumerId) return 0;
    let reclaimed = 0;
    for (const state of this.operations.values()) {
      for (const stream of [state.keys.normal, state.keys.bulk]) {
        while (true) {
          const pending = await this.redis.xpending(stream, GROUP, '-', '+', 1000, previousConsumerId);
          if (!Array.isArray(pending) || pending.length === 0) break;
          const ids = pending.map((entry) => entry[0]);
          const deliveryCounts = new Map(pending.map((entry) => [entry[0], Number(entry[3] || 1) + 1]));
          const claimed = await this.redis.xclaim(stream, GROUP, this.consumerId, minIdleMs, ...ids);
          for (const [streamId, fields] of claimed || []) {
            const dataIndex = fields.indexOf('data');
            if (dataIndex < 0) continue;
            try {
              const task = JSON.parse(fields[dataIndex + 1]);
              task.attempt = Math.max(Number(task.attempt || 1), deliveryCounts.get(streamId) || 1);
              state.recovered.push({ state, stream, streamId, task });
              reclaimed += 1;
            } catch {
              await this._ackDelete(state, stream, streamId);
            }
          }
        }
        await this.redis.xgroup('DELCONSUMER', stream, GROUP, previousConsumerId);
      }
      this._pump(state).catch((error) => this.emitError(error));
    }
    return reclaimed;
  }

  async migrateLegacyQueue() {
    const legacyGroup = 'pod-gateway';
    const report = { streams: 0, retries: 0, alreadyMigrated: 0, blocked: 0, invalidEntries: 0, missingExecutors: 0 };
    for (const priority of ['normal', 'bulk']) {
      const legacyStream = `${this.keyPrefix}:queue:${priority}`;
      try { await this.redis.xgroup('CREATE', legacyStream, legacyGroup, '0', 'MKSTREAM'); } catch (error) {
        if (!String(error.message).includes('BUSYGROUP')) throw error;
      }
      while (true) {
        const entries = await this.redis.xrange(legacyStream, '-', '+', 'COUNT', 100);
        if (!entries?.length) break;
        let processed = 0;
        for (const [streamId, fields] of entries) {
          const dataIndex = fields.indexOf('payload');
          const task = dataIndex < 0 ? undefined : legacyTask(fields[dataIndex + 1], priority);
          if (!task) {
            report.invalidEntries += 1;
            continue;
          }
          if (!this.operations.has(task.operation)) {
            report.missingExecutors += 1;
            this.emit('legacyMissingExecutor', task);
            continue;
          }
          const state = this.operations.get(task.operation);
          const queued = {
            ...task, queuedAt: Date.now(), priority, contract: state.contract,
            migrationOrigin: { kind: 'stream', sourceKey: legacyStream, sourceId: streamId, sourceFields: fields, token: `${legacyStream}:${streamId}` }
          };
          const payload = JSON.stringify(queued);
          const payloadBytes = this._estimateBytes(Buffer.byteLength(payload));
          const migrated = await this.redis.gatewayMigrateTask(
            state.keys.normal, state.keys.bulk, state.keys.delayed, state.keys.bytes, state.keys.migrationReceipts,
            priority === 'normal' ? 1 : 2, payload, payloadBytes, this.maxQueueDepth, this.maxQueueBytes,
            `${legacyStream}:${streamId}`, this.migrationReceiptTtlMs
          );
          if (Number(migrated?.[0]) === 0) {
            report.blocked += 1;
            continue;
          }
          await this.redis.xack(legacyStream, legacyGroup, streamId);
          await this.redis.xdel(legacyStream, streamId);
          await this.redis.hdel(state.keys.migrationReceipts, `${legacyStream}:${streamId}`);
          this._pump(state).catch((error) => this.emitError(error, task));
          if (Number(migrated[0]) === 2) report.alreadyMigrated += 1;
          else report.streams += 1;
          processed += 1;
        }
        if (processed === 0) break;
      }
    }

    for (const priority of ['normal', 'bulk']) {
      const retryStream = `${this.keyPrefix}:retry:${priority}`;
      const retryData = `${this.keyPrefix}:retry:data:${priority}`;
      const members = await this.redis.zrange(retryStream, 0, -1, 'WITHSCORES');
      for (let index = 0; index < members.length; index += 2) {
        const [member, score] = [members[index], members[index + 1]];
        const encoded = await this.redis.hget(retryData, member);
        if (!encoded) {
          await this.redis.zrem(retryStream, member);
          report.invalidEntries += 1;
          continue;
        }
        const task = legacyTask(encoded, priority);
        if (!task) {
          report.invalidEntries += 1;
          continue;
        }
        if (!this.operations.has(task.operation)) {
          report.missingExecutors += 1;
          this.emit('legacyMissingExecutor', task);
          continue;
        }
        const state = this.operations.get(task.operation);
        const delayedId = `${task.requestId}:${task.attempt || 1}`;
        const payload = JSON.stringify({
          ...task, queuedAt: Date.now(),
          migrationOrigin: { kind: 'retry', sourceSortedSet: retryStream, sourceData: retryData, sourceMember: member, sourceScore: Number(score), sourceEncoded: encoded, token: `${retryStream}:${member}` }
        });
        const payloadBytes = this._estimateBytes(Buffer.byteLength(payload));
        const migrated = await this.redis.gatewayMigrateRetry(
          state.keys.delayed, state.keys.delayedData, state.keys.bytes, state.keys.migrationReceipts,
          delayedId, payload, Number(score), `${retryStream}:${member}`, payloadBytes, this.migrationReceiptTtlMs
        );
        if (Number(migrated?.[0]) === 0) {
          report.blocked += 1;
          continue;
        }
        await this.redis.zrem(retryStream, member);
        await this.redis.hdel(retryData, member);
        await this.redis.hdel(state.keys.migrationReceipts, `${retryStream}:${member}`);
        this._schedulePromotion(state, delayedId, Math.max(1, Number(score) - Date.now()));
        if (Number(migrated[0]) === 2) report.alreadyMigrated += 1;
        else report.retries += 1;
      }
    }
    report.complete = report.blocked === 0 && report.invalidEntries === 0 && report.missingExecutors === 0;
    return report;
  }

  async rollbackLegacyMigration() {
    if (this.drainMode !== 'graceful') {
      throw Object.assign(new Error('Rollback requires graceful drain mode'), { code: 'MIGRATION_ROLLBACK_REQUIRES_DRAIN' });
    }
    if (this.lease && !this.lease.isFresh()) {
      throw Object.assign(new Error('Rollback requires the active Gateway lease'), { code: 'GATEWAY_STANDBY', retryable: true });
    }
    const states = [...this.operations.values()];
    if (states.some((state) => state.active > 0 || state.pumping)) {
      throw Object.assign(new Error('Rollback requires all active work to stop'), { code: 'MIGRATION_ROLLBACK_BUSY', retryable: true });
    }
    const report = { complete: false, restoredStreams: 0, restoredRetries: 0, alreadyRestored: 0, skipped: 0, irreversible: 0 };
    for (const state of states) {
      report.irreversible += Number(await this.redis.hlen(state.keys.migrationStats) || 0);
    }
    if (report.irreversible > 0) return report;

    for (const state of states) {
      for (const stream of [state.keys.normal, state.keys.bulk]) {
        const entries = await this.redis.xrange(stream, '-', '+');
        for (const [streamId, fields] of entries) {
          const dataIndex = fields.indexOf('data');
          let task;
          try { task = dataIndex >= 0 ? JSON.parse(fields[dataIndex + 1]) : undefined; } catch {}
          const origin = task?.migrationOrigin;
          if (origin?.kind !== 'stream' || !Array.isArray(origin.sourceFields) || origin.sourceFields.length % 2 !== 0) {
            report.skipped += 1;
            continue;
          }
          const restored = await this._restoreLegacyStream(origin);
          await this._ackDelete(state, stream, streamId);
          if (restored) report.restoredStreams += 1;
          else report.alreadyRestored += 1;
        }
      }

      const delayedIds = await this.redis.zrange(state.keys.delayed, 0, -1);
      for (const delayedId of delayedIds) {
        const encoded = await this.redis.hget(state.keys.delayedData, delayedId);
        let task;
        try { task = encoded ? JSON.parse(encoded) : undefined; } catch {}
        const origin = task?.migrationOrigin;
        if (origin?.kind !== 'retry' || typeof origin.sourceMember !== 'string') {
          report.skipped += 1;
          continue;
        }
        const existingPayload = await this.redis.hget(origin.sourceData, origin.sourceMember);
        const existingScore = await this.redis.zscore(origin.sourceSortedSet, origin.sourceMember);
        if (!existingPayload) await this.redis.hset(origin.sourceData, origin.sourceMember, origin.sourceEncoded);
        if (!existingScore) await this.redis.zadd(origin.sourceSortedSet, origin.sourceScore, origin.sourceMember);
        await this.redis.zrem(state.keys.delayed, delayedId);
        if (encoded) await this.redis.decrby(state.keys.bytes, this._estimateBytes(Buffer.byteLength(encoded)));
        await this.redis.hdel(state.keys.delayedData, delayedId);
        if (existingPayload || existingScore) report.alreadyRestored += 1;
        else report.restoredRetries += 1;
      }
    }
    report.complete = report.skipped === 0;
    return report;
  }

  async _restoreLegacyStream(origin) {
    const entries = await this.redis.xrange(origin.sourceKey, '-', '+');
    const alreadyRestored = entries.some(([, fields]) => {
      const tokenIndex = fields.indexOf('__redkern_rollback_token');
      return tokenIndex >= 0 && fields[tokenIndex + 1] === origin.token;
    });
    if (alreadyRestored) return false;
    await this.redis.xadd(origin.sourceKey, '*', ...origin.sourceFields, '__redkern_rollback_token', origin.token);
    return true;
  }

  async close() {
    this.closed = true;
    clearInterval(this.slowTimer);
    for (const state of this.operations.values()) {
      for (const timer of state.delayedTimers.values()) clearTimeout(timer);
      state.delayedTimers.clear();
      clearTimeout(state.missingTimer);
    }
    await Promise.all([...this.operations.keys()].map((operation) => this._waitForOperation(operation)));
  }

  async unregister(operation, expectedState) {
    const state = this.operations.get(operation);
    if (!state || (expectedState && state !== expectedState)) return;
    state.accepting = false;
    for (const timer of state.delayedTimers.values()) clearTimeout(timer);
    state.delayedTimers.clear();
    await this._waitForState(state);
    state.missingTimer = setTimeout(() => {
      this._expireMissingOperation(state).catch((error) => this.emitError(error));
    }, this.executorGraceMs);
    state.missingTimer.unref?.();
  }

  async _expireMissingOperation(state) {
    if (this.operations.get(state.operation) !== state || state.accepting) return;
    for (const stream of [state.keys.normal, state.keys.bulk]) {
      const entries = await this.redis.xrange(stream, '-', '+');
      for (const [streamId, fields] of entries) {
        const dataIndex = fields.indexOf('data');
        let task;
        try { task = dataIndex >= 0 ? JSON.parse(fields[dataIndex + 1]) : undefined; } catch {}
        await this._ackDelete(state, stream, streamId);
        if (task) this.emit('errorResult', {
          task,
          error: Object.assign(new Error('Operation executor was not registered before the grace period expired'), { code: 'OPERATION_NOT_FOUND' })
        });
      }
    }
    const delayedIds = await this.redis.zrange(state.keys.delayed, 0, -1);
    for (const requestId of delayedIds) {
      const encoded = await this.redis.hget(state.keys.delayedData, requestId);
      if (encoded) {
        try {
          const task = JSON.parse(encoded);
          const bytes = this._estimateBytes(Buffer.byteLength(encoded));
          await this.redis.decrby(state.keys.bytes, bytes);
          this.emit('errorResult', {
            task,
            error: Object.assign(new Error('Operation executor was not registered before the grace period expired'), { code: 'OPERATION_NOT_FOUND' })
          });
        } catch {}
      }
      await this.redis.zrem(state.keys.delayed, requestId);
      await this.redis.hdel(state.keys.delayedData, requestId);
    }
    this.operations.delete(state.operation);
  }

  async _pump(state) {
    if (this.closed || !state.accepting || !this.operations.has(state.operation)) return;
    if (this.drainMode === 'graceful') return;
    if (this.lease && !this.lease.isFresh()) return;
    if (state.pumping) {
      state.pumpAgain = true;
      return;
    }
    state.pumping = true;
    try {
      while (!this.closed && state.accepting && this.operations.has(state.operation) && state.active < state.concurrency) {
        const entry = await this._readOne(state);
        if (!entry) break;
        state.active += 1;
        this._run(state, entry).catch((error) => this.emitError(error, entry.task));
      }
    } finally {
      state.pumping = false;
      if (state.pumpAgain) {
        state.pumpAgain = false;
        queueMicrotask(() => this._pump(state).catch((error) => this.emitError(error)));
      }
    }
  }

  async _readOne(state) {
    const recovered = state.recovered.shift();
    if (recovered) return recovered;
    const preferBulk = state.normalStreak >= 10;
    const first = preferBulk ? 'bulk' : 'normal';
    const second = first === 'normal' ? 'bulk' : 'normal';
    let result = await this._readStream(state.keys[first]);
    let selected = first;
    if (!result) {
      result = await this._readStream(state.keys[second]);
      selected = second;
    }
    if (!result) return undefined;
    if (selected === 'normal') state.normalStreak += 1;
    else state.normalStreak = 0;
    const [streamId, fields] = result;
    const index = fields.indexOf('data');
    if (index < 0) {
      await this._ackDelete(state, state.keys[selected], streamId);
      return undefined;
    }
    let task;
    try { task = JSON.parse(fields[index + 1]); } catch {
      await this._ackDelete(state, state.keys[selected], streamId);
      return undefined;
    }
    task.attempt = Math.max(1, Number(task.attempt || 1));
    return { state, stream: state.keys[selected], streamId, task };
  }

  async _readStream(stream) {
    const result = await this.redis.xreadgroup('GROUP', GROUP, this.consumerId, 'COUNT', 1, 'STREAMS', stream, '>');
    return result?.[0]?.[1]?.[0];
  }

  async _run(state, entry) {
    const { task } = entry;
    let deferred = false;
    state.activeEntries.add(entry);
    try {
      if (this.idempotency && task.idempotencyKey) {
        const ownership = await this.idempotency.takeover(task);
        if (!ownership.acquired) {
          if (ownership.completed && ownership.resultAvailable) this.emit('cachedResult', { task, response: ownership.response });
          await this._ackDelete(state, entry.stream, entry.streamId);
          return;
        }
        task.consumerId = this.idempotency.consumerId;
      }
      if (this.lease && !this.lease.isFresh()) {
        await this.redis.gatewayDeferEntry(
          entry.stream, state.keys.delayed, state.keys.delayedData, state.keys.bytes,
          entry.streamId, task.requestId, Date.now() + 1, GROUP, 0,
          this.queueBytesFactor, this.queueEntryOverheadBytes
        );
        deferred = true;
        this._releaseActive(state, entry);
        return;
      }
      const queueAge = Date.now() - task.queuedAt;
      const queueRemaining = Math.min(task.queueTimeoutMs - queueAge, task.deadlineAt - Date.now() - task.execTimeoutMs);
      if (queueRemaining <= 0) {
        await this._finishError(entry, Object.assign(new Error('Task deadline exceeded in queue'), { code: 'DEADLINE_EXCEEDED_IN_QUEUE' }));
        return;
      }
      const reservationGrace = state.rateLimits.reduce((maximum, limit) => Math.max(maximum, (1000 / limit.rate) * limit.burst), 0);
      const reservationFresh = Number.isFinite(task.reservedUntil) &&
        Date.now() <= task.reservedUntil + reservationGrace;
      const decision = this.gcra && state.rateLimits.length && !reservationFresh
        ? await this.gcra.reserve(state.bucket, state.rateLimits, queueRemaining)
        : { allowed: true, waitMs: 0 };
      if (this.gcra && state.rateLimits.length && decision.limits) {
        this.emit('rateDecision', { task, bucket: state.bucket, limits: decision.limits });
      }
      if (!decision.allowed) {
        await this._finishError(entry, Object.assign(new Error('Rate limit wait exceeds queue budget'), { code: 'DEADLINE_EXCEEDED_IN_QUEUE' }));
        return;
      }
      if (decision.waitMs > 0) {
        const delayedId = task.requestId;
        await this.redis.gatewayDeferEntry(
          entry.stream, state.keys.delayed, state.keys.delayedData, state.keys.bytes,
          entry.streamId, delayedId, decision.allowAt, GROUP, decision.waitMs,
          this.queueBytesFactor, this.queueEntryOverheadBytes
        );
        deferred = true;
        this._releaseActive(state, entry);
        this._schedulePromotion(state, delayedId, decision.waitMs);
        this._pump(state).catch((error) => this.emitError(error));
        return;
      }
      const remaining = task.deadlineAt - Date.now();
      if (remaining <= 0) {
        await this._finishError(entry, Object.assign(new Error('Task deadline exceeded'), { code: 'DEADLINE_EXCEEDED_IN_QUEUE' }));
        return;
      }
      if (this.lease && !this.lease.isFresh()) {
        await this._handoff(entry);
        deferred = true;
        this._releaseActive(state, entry);
        return;
      }
      if (Number(task.attempt || 1) > state.maxDeliveries) {
        await this._finishError(entry, Object.assign(new Error('Maximum delivery count exceeded'), { code: 'MAX_DELIVERIES_EXCEEDED' }));
        return;
      }
      const leaseAbort = new AbortController();
      const onLeaseLost = () => leaseAbort.abort();
      this.lease?.once('lost', onLeaseLost);
      task.queueWaitMs = Math.max(0, Date.now() - task.queuedAt - Number(task.rateLimitWaitMs || 0));
      this.emit('processing', { task, queueWaitMs: task.queueWaitMs, rateLimitMs: Number(task.rateLimitWaitMs || 0) });
      let result;
      const upstreamStartedAt = Date.now();
      try {
        await this._recordMigrationStatus(task, 'started');
        result = await this._executeWithTimeout(state, entry, Math.min(task.execTimeoutMs, remaining), leaseAbort.signal);
        const durationMs = Math.max(0, Date.now() - upstreamStartedAt);
        task.upstreamMs = Number(task.upstreamMs || 0) + durationMs;
        this.emit('upstreamComplete', { task, durationMs, error: undefined, contract: state.contract });
        await this._recordMigrationStatus(task, 'completed');
      } catch (error) {
        const durationMs = Math.max(0, Date.now() - upstreamStartedAt);
        task.upstreamMs = Number(task.upstreamMs || 0) + durationMs;
        this.emit('upstreamComplete', { task, durationMs, error, contract: state.contract });
        if (entry.forcedHandoff) {
          await this._handoff(entry);
          deferred = true;
          this._releaseActive(state, entry);
          return;
        }
        if (error.code === 'GATEWAY_STANDBY') {
          await this._handoff(entry);
          deferred = true;
          this._releaseActive(state, entry);
          return;
        }
        throw error;
      } finally {
        this.lease?.removeListener('lost', onLeaseLost);
      }
      if (this.resultHandler) await this.resultHandler(task, result, state.contract);
      else this.emit('result', { task, result, contract: state.contract });
      await this._ackDelete(state, entry.stream, entry.streamId);
    } catch (error) {
      const retried = await this._retry(state, entry, error);
      if (!retried) await this._finishError(entry, error);
    } finally {
      if (!deferred) {
        this._releaseActive(state, entry);
        this._pump(state).catch((error) => this.emitError(error));
      }
    }
  }

  async _handoff(entry) {
    await this.redis.gatewayDeferEntry(
      entry.stream,
      entry.state.keys.delayed,
      entry.state.keys.delayedData,
      entry.state.keys.bytes,
      entry.streamId,
      entry.task.requestId,
      Date.now() + 1,
      GROUP,
      0,
      this.queueBytesFactor,
      this.queueEntryOverheadBytes
    );
  }

  _releaseActive(state, entry) {
    if (state.activeEntries.delete(entry) && state.active > 0) state.active -= 1;
  }

  _executeWithTimeout(state, entry, timeoutMs, leaseSignal) {
    const { task } = entry;
    return new Promise((resolve, reject) => {
      const controller = new AbortController();
      entry.abortController = controller;
      const onForcedAbort = () => {
        if (entry.forcedHandoff) reject(Object.assign(new Error('Gateway close drain deadline reached'), { code: 'GATEWAY_DRAINING', retryable: true }));
      };
      controller.signal.addEventListener('abort', onForcedAbort, { once: true });
      const abortForLease = () => {
        controller.abort();
        reject(Object.assign(new Error('Gateway lease was lost during execution'), { code: 'GATEWAY_STANDBY', retryable: true }));
      };
      leaseSignal?.addEventListener('abort', abortForLease, { once: true });
      const timer = setTimeout(() => {
        controller.abort();
        reject(Object.assign(new Error('Gateway execution timed out'), { code: 'UPSTREAM_TIMEOUT', retryable: true }));
      }, timeoutMs);
      Promise.resolve(state.execute(task.message, { requestId: task.requestId, attempt: task.attempt || 1, signal: controller.signal }))
        .then(resolve, reject)
        .finally(() => {
          clearTimeout(timer);
          entry.abortController = undefined;
          controller.signal.removeEventListener('abort', onForcedAbort);
          leaseSignal?.removeEventListener('abort', abortForLease);
        });
    });
  }

  async _finishError(entry, error) {
    if (this.errorHandler) await this.errorHandler(entry.task, error);
    else this.emit('errorResult', { task: entry.task, error });
    const errorRecord = JSON.stringify({
      code: error.code || 'UPSTREAM_ERROR',
      message: String(error.message || 'Gateway operation failed').slice(0, 512),
      statusCode: Number.isFinite(error.statusCode) ? error.statusCode : undefined,
      attempt: Number(entry.task.attempt || 1)
    });
    const id = await this.redis.gatewayDeadLetter(
      entry.stream,
      entry.state.keys.bytes,
      entry.state.keys.deadLetter,
      entry.streamId,
      GROUP,
      this.dlqRetentionMs,
      errorRecord,
      this.maxDlqEntries,
      this.queueBytesFactor,
      this.queueEntryOverheadBytes
    );
    if (id && Number(id) !== 0) {
      const operation = entry.task.operation;
      this.deadLetterCounts.set(operation, (this.deadLetterCounts.get(operation) || 0) + 1);
      this.emit('deadLetter', { operation, id: String(id), error: JSON.parse(errorRecord) });
    }
  }

  async _ackDelete(state, stream, streamId) {
    const trimBefore = Math.max(0, Date.now() - state.trimAfterMs);
    await this.redis.gatewayAckDelete(
      stream, state.keys.bytes, GROUP, streamId, `${trimBefore}-0`,
      this.queueBytesFactor, this.queueEntryOverheadBytes
    );
  }

  async _retry(state, entry, error) {
    const task = entry.task;
    const attempt = Number(task.attempt || 1);
    if (attempt >= state.maxDeliveries || !isRetryable(error, state.retryClass)) return false;
    const delayMs = Number.isFinite(error.retryAfterMs) && error.retryAfterMs >= 0
      ? Math.min(error.retryAfterMs, 60000)
      : Math.min(1000 * (2 ** (attempt - 1)), 30000);
    const allowAt = Date.now() + delayMs;
    if (allowAt + task.execTimeoutMs >= task.deadlineAt) return false;
    const retryTask = { ...task, attempt: attempt + 1, lastError: error.code || 'UPSTREAM_ERROR' };
    const delayedId = `${task.requestId}:${attempt + 1}`;
    await this.redis.gatewayRetryDelayed(
      entry.stream,
      state.keys.delayed,
      state.keys.delayedData,
      state.keys.bytes,
      entry.streamId,
      delayedId,
      JSON.stringify(retryTask),
      allowAt,
      GROUP,
      this.queueBytesFactor,
      this.queueEntryOverheadBytes
    );
    this._schedulePromotion(state, delayedId, delayMs);
    return true;
  }

  _schedulePromotion(state, requestId, waitMs) {
    const current = state.delayedTimers.get(requestId);
    if (current) clearTimeout(current);
    const timer = setTimeout(() => {
      state.delayedTimers.delete(requestId);
      this._promote(state, requestId).catch((error) => this.emitError(error));
    }, Math.max(1, Math.min(waitMs + 5, 1000)));
    timer.unref?.();
    state.delayedTimers.set(requestId, timer);
  }

  async _promote(state, requestId) {
    if (this.closed) return;
    const encoded = await this.redis.hget(state.keys.delayedData, requestId);
    if (!encoded) return;
    let priority = 'normal';
    try { priority = JSON.parse(encoded).priority === 'bulk' ? 'bulk' : 'normal'; } catch {}
    const result = await this.redis.gatewayPromoteDelayed(state.keys.delayed, state.keys.delayedData, state.keys[priority], requestId);
    const promoted = Number(result?.[0]) === 1;
    if (promoted) this._pump(state).catch((error) => this.emitError(error));
    else if (Number(result?.[1]) > 0) this._schedulePromotion(state, requestId, Number(result[1]));
  }

  async _promoteAll(state) {
    const requestIds = await this.redis.zrange(state.keys.delayed, 0, 99);
    for (const requestId of requestIds) await this._promote(state, requestId);
  }

  async _waitForOperation(operation) {
    const state = this.operations.get(operation);
    if (state) await this._waitForState(state);
  }

  async _waitForState(state) {
    while (state.active > 0 || state.pumping) await new Promise((resolve) => setTimeout(resolve, 5));
  }

  emitError(error, task) {
    this.emit('queueError', { error, task });
  }
}

function isRetryable(error, retryClass) {
  const code = String(error?.code || '').toUpperCase();
  const networkBeforeSend = ['ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN', 'ETIMEDOUT', 'UPSTREAM_CONNECT_ERROR'].includes(code);
  if (networkBeforeSend || Number(error?.statusCode) === 429 || code === 'UPSTREAM_RATE_LIMITED') return true;
  if (retryClass === 'never') return false;
  return error?.retryable === true || ['ECONNRESET', 'UPSTREAM_TIMEOUT'].includes(code);
  }

function legacyTask(encoded, priority) {
  let item;
  try { item = JSON.parse(encoded); } catch { return undefined; }
  const request = item?.request && typeof item.request === 'object' ? item.request : item;
  const rawOperation = item?.operation?.value || item?.operationKey || request?.operation;
  let operation;
  try { operation = parseOperation(rawOperation); } catch { return undefined; }
  const requestId = request?.requestId;
  if (typeof requestId !== 'string' || requestId.length === 0) return undefined;
  const parsedDeadline = Date.parse(request.deadlineAt || item.deadlineAt || '');
  const deadlineAt = Number.isFinite(parsedDeadline) ? parsedDeadline : Date.now() + 60000;
  const remaining = Math.max(1, deadlineAt - Date.now());
  const execTimeoutMs = Math.max(1, Math.min(30000, Math.floor(remaining / 2)));
  return {
    requestId,
    operation,
    client: item.client || item.clientName || 'legacy',
    podId: item.podId,
    protocolVersion: '1.0',
    priority: priority === 'bulk' ? 'bulk' : 'normal',
    idempotencyKey: request.idempotencyKey,
    message: request,
    queuedAt: Number(item.enqueuedAt || item.lastQueuedAt || item.receivedAt || Date.now()),
    queueTimeoutMs: Math.max(1, remaining - execTimeoutMs),
    execTimeoutMs,
    deadlineMs: remaining,
    deadlineAt,
    attempt: Math.max(1, Number(item.attempts || item.retrySequence || 1))
  };
}

module.exports = {
  RedisOperationQueue, GROUP, streamKeys, ACK_DELETE_LUA, DEFER_LUA,
  PROMOTE_DELAYED_LUA, RETRY_DELAYED_LUA, isRetryable
};
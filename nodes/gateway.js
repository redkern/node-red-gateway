'use strict';

const { randomUUID } = require('node:crypto');
const kit = require('@redkern/node-red-kit');
const Redis = require('ioredis');
const { createRedisClient } = require('@redkern/node-red-kit/redis');
const { GatewayClient } = require('../lib/client.js');
const { GatewayRuntime } = require('../lib/gateway-runtime.js');
const { createHttpAdapter } = require('../lib/http-adapter.js');
const { parseRedisNodes } = require('../lib/redis-config.js');
const { createGcraLimiter } = require('../lib/server/gcra.js');
const { RedisOperationQueue } = require('../lib/server/queue.js');
const { memoryRatio, parseRedisInfo, validateEvictionPolicy } = require('../lib/server/redis-health.js');
const { LeaseCoordinator } = require('../lib/server/lease.js');
const { RedisIdempotencyStore } = require('../lib/server/idempotency.js');
const { AsyncResultStore } = require('../lib/server/async-results.js');

const processBootId = randomUUID();
let serverGeneration = 0;
const { createServerState, initializePalette, parsePrefixes, renderMetrics, statusFor } = require('../lib/palette.js');

module.exports = function registerGatewayNodes(RED) {
  const palette = initializePalette(RED);
  const { rk } = palette;

  function ServerConfig(config) {
    RED.nodes.createNode(this, config);
    const k = rk.bind(this);
    const parsed = k.configure(config, {
      host: { type: 'str', required: true },
      port: { type: 'int', required: true, min: 1, max: 65535 },
      maxPayload: { type: 'int', default: 1048576, min: 16384, max: 16777216 },
      maxConnections: { type: 'int', default: 500, min: 1 },
      maxUnauthenticatedConnections: { type: 'int', default: 50, min: 1 },
      helloTimeoutMs: { type: 'int', default: 5000, min: 1 },
      maxInFlightPerConnection: { type: 'int', default: 1000, min: 1 },
      maxResultBytes: { type: 'int', default: 262144, min: 1 },
      maxQueueDepth: { type: 'int', default: 10000, min: 1 },
      maxQueueBytes: { type: 'int', default: 67108864, min: 1 },
      queueBytesFactor: { type: 'float', default: 1.5, min: 1 },
      queueEntryOverheadBytes: { type: 'int', default: 128, min: 0 },
      maxDeliveries: { type: 'int', default: 3, min: 1 },
      dlqRetentionMs: { type: 'int', default: 86400000, min: 1 },
      maxDlqEntries: { type: 'int', default: 10000, min: 1 },
      maxGatewayMemoryBytes: { type: 'int', default: 1073741824, min: 1 },
      redisEvictionSampleMs: { type: 'int', default: 15000, min: 1000 },
      defaultRate: { type: 'float', default: 100, min: 0.001 },
      defaultBurst: { type: 'int', default: 100, min: 1 },
      leaseTtlMs: { type: 'int', default: 15000, min: 1000 },
      leaseSafetyMarginMs: { type: 'int', default: 5000, min: 1 },
      leaseRenewEveryMs: { type: 'int', default: 5000, min: 100 },
      drainTimeoutMs: { type: 'int', default: 12000, min: 1 },
      closeBudgetMs: { type: 'int', default: 12000, min: 1, max: 14000 },
      completeDrainTimeoutMs: { type: 'int', default: 600000, min: 1 },
      dedupWindowMs: { type: 'int', default: 3600000, min: 1 },
      resultTtlMs: { type: 'int', default: 120000, min: 1 },
      asyncResultTtlMs: { type: 'int', default: 3600000, min: 1 },
      sessionReconnectGraceMs: { type: 'int', default: 10000, min: 0 },
      redisMode: { type: 'enum', values: ['standalone', 'cluster'], default: 'standalone' },
      redisHost: { type: 'str', default: '' },
      redisPort: { type: 'int', default: 6379, min: 1, max: 65535 },
      redisNodes: { type: 'str', default: '' },
      redisDb: { type: 'int', default: 0, min: 0 },
      redisKeyPrefix: { type: 'str', default: 'pod-gateway' },
      redisConnectTimeoutMs: { type: 'int', default: 10000, min: 1 },
      redisCommandTimeoutMs: { type: 'int', default: 5000, min: 1 },
      allowEvictingRedis: { type: 'bool', default: false },
      redisTls: { type: 'bool', default: false }
    });
    const state = createServerState(this, parsed.ok ? parsed.value : {});
    const generation = ++serverGeneration;
    let activeQueue;
    palette.servers.set(this.id, state);
    this.gatewayState = state;
    this.addAccount = state.addAccount;
    this.addExecutor = state.addExecutor;
    this.respondWorker = state.respondWorker;
    this.addDependent = (notify) => {
      state.dependents.add(notify);
      return () => state.dependents.delete(notify);
    };
    let activeLease;
    let retryLeaseTimer;
    let shuttingDown = false;

    k.onStart(async (attempt) => {
      statusFor(this, 'yellow', `starting ${attempt.number}`);
      if (!parsed.ok) throw new kit.ConfigError(parsed.errors);
      if (parsed.value.redisMode === 'standalone' && !parsed.value.redisHost.trim()) {
        throw new kit.ConfigError([{ field: 'redisHost', code: 'REQUIRED', message: 'redisHost is required in standalone mode' }]);
      }
      const metricsRelease = k.metrics.acquire();
      const metricsServer = k.internalServer.acquire(this.id, { node: this });
      attempt.onClose(async () => {
        metricsRelease();
        await metricsServer.release();
      });
      await metricsServer.ready;
      const redisNodes = parsed.value.redisMode === 'cluster' ? parseRedisNodes(parsed.value.redisNodes) : undefined;
      const redis = createRedisClient({
        ioredis: Redis,
        domain: 'gateway',
        configId: this.id,
        role: 'shared',
        mode: parsed.value.redisMode,
        ...(parsed.value.redisMode === 'cluster'
          ? { nodes: redisNodes }
          : { host: parsed.value.redisHost, port: parsed.value.redisPort, db: parsed.value.redisDb }),
        ...(this.credentials?.redisPassword ? { password: this.credentials.redisPassword } : {}),
        ...(parsed.value.redisTls ? { tls: { rejectUnauthorized: true } } : {}),
        connectTimeout: parsed.value.redisConnectTimeoutMs,
        commandTimeout: parsed.value.redisCommandTimeoutMs,
        logger: { error: (error, meta) => k.log.error(error, meta) }
      });
      attempt.onClose(() => redis.close());
      await redis.connect();
      const redisMemory = parseRedisInfo(await redis.client.info('memory'));
      const redisPolicy = validateEvictionPolicy(redisMemory.maxmemory_policy, parsed.value.allowEvictingRedis);
      const clusterInfo = parseRedisInfo(await redis.client.info('cluster'));
      k.log.info(`Redis ready mode=${clusterInfo.cluster_enabled === '1' ? 'cluster' : 'standalone'} policy=${redisPolicy}`, { key: 'REDIS_READY' });
      const leaseOptions = {
        redis: redis.client,
        keyPrefix: parsed.value.redisKeyPrefix,
        consumerId: `${kit.instanceId()}:${processBootId}:${generation}`,
        leaseTtlMs: parsed.value.leaseTtlMs,
        safetyMarginMs: parsed.value.leaseSafetyMarginMs,
        renewEveryMs: parsed.value.leaseRenewEveryMs,
        drainTimeoutMs: parsed.value.drainTimeoutMs
      };
      const createLease = () => new LeaseCoordinator(leaseOptions);
      let lease = createLease();
      const acquired = await lease.acquire();
      if (!acquired.acquired) {
        statusFor(this, 'yellow', 'standby');
        state.setStatus({ fill: 'yellow', text: 'standby' });
        throw Object.assign(new Error('Another Gateway instance owns the Redis lease'), { code: 'GATEWAY_STANDBY', retryable: true });
      }
      activeLease = lease;
      lease.startRenewal();
      const idempotency = new RedisIdempotencyStore({
        redis: redis.client,
        keyPrefix: parsed.value.redisKeyPrefix,
        consumerId: lease.consumerId,
        dedupWindowMs: parsed.value.dedupWindowMs,
        resultTtlMs: parsed.value.resultTtlMs,
        maxResultBytes: parsed.value.maxResultBytes
      });
      const asyncResults = new AsyncResultStore({
        redis: redis.client,
        keyPrefix: parsed.value.redisKeyPrefix,
        consumerId: lease.consumerId,
        asyncResultTtlMs: parsed.value.asyncResultTtlMs
      });
      state.asyncResults = asyncResults;
      asyncResults.startRetention();
      attempt.onClose(() => asyncResults.close());
      attempt.onClose(async () => {
        shuttingDown = true;
        clearTimeout(retryLeaseTimer);
        retryLeaseTimer = undefined;
        const owner = activeLease;
        activeLease = undefined;
        const pending = activeQueue ? await activeQueue.pendingForConsumer() : 0;
        if (owner) await owner.release({ clean: pending === 0 });
      });
      const gcra = createGcraLimiter(redis.client, parsed.value.redisKeyPrefix);
      const defaultLimits = [{ name: 'g', rate: parsed.value.defaultRate, burst: parsed.value.defaultBurst }];
      const queue = new RedisOperationQueue({
        redis: redis.client,
        gcra,
        keyPrefix: parsed.value.redisKeyPrefix,
        consumerId: lease.consumerId,
        maxQueueDepth: parsed.value.maxQueueDepth,
        maxQueueBytes: parsed.value.maxQueueBytes,
        queueBytesFactor: parsed.value.queueBytesFactor,
        queueEntryOverheadBytes: parsed.value.queueEntryOverheadBytes,
        maxGatewayMemoryBytes: parsed.value.maxGatewayMemoryBytes,
        maxDeliveries: parsed.value.maxDeliveries,
        dlqRetentionMs: parsed.value.dlqRetentionMs,
        maxDlqEntries: parsed.value.maxDlqEntries,
        lease,
        idempotency,
        defaultLimits
      });
      let startRuntime;
      const scheduleLeaseRetry = () => {
        if (retryLeaseTimer || shuttingDown) return;
        retryLeaseTimer = setTimeout(() => {
          retryLeaseTimer = undefined;
          k.track((async () => {
            let candidate;
            try {
              candidate = createLease();
              const result = await candidate.acquire();
              if (!result.acquired) {
                scheduleLeaseRetry();
                return;
              }
              activeLease = candidate;
              queue.lease = candidate;
              queue.consumerId = candidate.consumerId;
              idempotency.consumerId = candidate.consumerId;
              candidate.startRenewal();
              await startRuntime(candidate);
            } catch (error) {
              if (candidate) await candidate.release({ clean: false });
              if (activeLease === candidate) activeLease = undefined;
              statusFor(this, 'yellow', 'standby');
              state.setStatus({ fill: 'yellow', text: 'standby' });
              k.log.error(error, { key: error.code || 'GATEWAY_RESTART_FAILED' });
              scheduleLeaseRetry();
            }
          })(), { label: 'Gateway lease reacquire', key: 'GATEWAY_STANDBY' });
        }, 5000);
        retryLeaseTimer.unref?.();
      };
      queue.lease = lease;
      state.queue = queue;
      activeQueue = queue;
      attempt.onClose(async () => {
        state.queue = undefined;
        await queue.close();
      });
      for (const [operation, execute] of state.executors) {
        const metadata = state.executorMetadata.get(operation) || {};
        await queue.register(operation, {
          execute,
          concurrency: metadata.concurrency || 32,
          rateLimits: metadata.rateLimits,
          bucket: metadata.bucket || 'default',
          contract: metadata.contract || 'passthrough',
          retryClass: metadata.retryClass || 'never',
          maxDeliveries: metadata.maxDeliveries,
          deadlineMs: metadata.deadlineMs || 30000
        });
      }
      const priorConsumerId = acquired.previousOwner?.consumerId || acquired.cleanConsumerId;
      if (priorConsumerId && priorConsumerId !== lease.consumerId) {
        const wasClean = acquired.cleanConsumerId === priorConsumerId;
        if (!wasClean) {
          const takeoverDelay = parsed.value.leaseTtlMs + (acquired.previousOwner?.drainTimeoutMs || parsed.value.drainTimeoutMs);
          await new Promise((resolve) => setTimeout(resolve, takeoverDelay));
        }
        await queue.reclaimConsumer(priorConsumerId, 0);
      }
      const legacyQueueMigration = await queue.migrateLegacyQueue();
      state.legacyMigrationReport = legacyQueueMigration;
      if (legacyQueueMigration.missingExecutors > 0) {
        k.log.warn(`Legacy queue has ${legacyQueueMigration.missingExecutors} tasks whose executors are not registered`, { key: 'LEGACY_QUEUE_OPERATION_MISSING' });
      }
      if (legacyQueueMigration.blocked > 0 || legacyQueueMigration.invalidEntries > 0 || legacyQueueMigration.missingExecutors > 0) {
        k.log.warn('Legacy queue migration is incomplete; source data was retained', {
          key: 'LEGACY_MIGRATION_INCOMPLETE',
          report: legacyQueueMigration
        });
      } else if (legacyQueueMigration.streams > 0 || legacyQueueMigration.retries > 0 || legacyQueueMigration.alreadyMigrated > 0) {
        k.log.info('Legacy queue migration completed', { key: 'LEGACY_MIGRATION_COMPLETE', report: legacyQueueMigration });
      }
      await queue.restoreDrainState();
      queue.startRecovery();
      startRuntime = async (ownerLease) => {
        const tlsKey = this.credentials?.tlsKey;
        const tlsCert = this.credentials?.tlsCert;
        if (Boolean(tlsKey) !== Boolean(tlsCert)) throw new Error('TLS key and certificate must be configured together');
        const runtime = new GatewayRuntime({
          ...parsed.value,
          redis: redis.client,
          redisHandle: redis,
          gcra,
          idempotency,
          asyncResults,
          keyPrefix: parsed.value.redisKeyPrefix,
          queue,
          lease: ownerLease,
          redisPolicy,
          allowEvictingRedis: parsed.value.allowEvictingRedis,
          maxGatewayMemoryBytes: parsed.value.maxGatewayMemoryBytes,
          tls: tlsKey && tlsCert ? { key: tlsKey, cert: tlsCert } : undefined,
          logger: { error: (...args) => k.log.error(args[0], { key: 'GATEWAY_RUNTIME' }), warn: (...args) => k.log.warn(args[0], { key: 'GATEWAY_RUNTIME' }) }
        });
        runtime.on('metric', (metric) => this.emit('gateway-metric', metric));
        runtime.on('error', (error) => {
          statusFor(this, 'red', error.code === 'EADDRINUSE' ? 'port busy' : error.code || 'error');
          k.log.error(error, { key: error.code || 'GATEWAY_RUNTIME' });
          state.setStatus({ fill: 'red', text: error.code === 'EADDRINUSE' ? 'port busy' : error.code || 'error' });
        });
        for (const account of state.accounts.values()) runtime.registerAccount(account);
        for (const [operation, execute] of state.executors) {
          runtime.registerExecutor(operation, execute, state.executorMetadata.get(operation));
        }
        ownerLease.once('lost', () => {
          if (activeLease !== ownerLease) return;
          activeLease = undefined;
          statusFor(this, 'yellow', 'standby');
          state.setStatus({ fill: 'yellow', text: 'standby' });
          state.runtime = undefined;
          queue.lease = ownerLease;
          k.track((async () => {
            await runtime.close(1013, 'GATEWAY_STANDBY');
            if (!shuttingDown) scheduleLeaseRetry();
          })(), { label: 'Gateway lease fencing', key: 'GATEWAY_STANDBY' });
        });
        await runtime.start();
        if (!ownerLease.isFresh()) {
          await runtime.close(1013, 'GATEWAY_STANDBY');
          throw Object.assign(new Error('Gateway lease expired during listener startup'), { code: 'GATEWAY_STANDBY', retryable: true });
        }
        state.runtime = runtime;
        statusFor(this, 'green', 'ready');
        state.setStatus({ fill: 'green', text: 'ready' });
      };
      attempt.onClose(async () => {
        const runtime = state.runtime;
        state.runtime = undefined;
        if (runtime) {
          await runtime.drain('graceful', parsed.value.closeBudgetMs);
          await runtime.close(1012, 'Service Restart');
        }
      });
      await startRuntime(lease);
    }, { startAlertMs: 30000 });

    k.onClose(async () => {
      const runtime = state.runtime;
      state.runtime = undefined;
      await runtime?.close();
      palette.servers.delete(this.id);
    });
  }

  function Account(config) {
    RED.nodes.createNode(this, config);
    const k = rk.bind(this);
    this.server = RED.nodes.getNode(config.server);
    this.accountName = config.accountName;
    this.operationPrefixes = parsePrefixes(config.operationPrefixes);
    const credentials = this.credentials || {};
    if (!this.server || typeof this.server.addAccount !== 'function') {
      k.onStart(async () => { throw Object.assign(new Error('Gateway server config is missing'), { code: 'CONFIG_INVALID' }); });
      return;
    }
    let release;
    try {
      release = this.server.addAccount({
        name: this.accountName,
        operationPrefixes: this.operationPrefixes,
        token: credentials.token,
        nextToken: credentials.nextToken
      });
    } catch (error) {
      statusFor(this, 'red', 'config error');
      k.log.error(error, { key: error.code || 'ACCOUNT_CONFIG' });
    }
    this.on('close', () => release?.());
  }

  function ClientConfig(config) {
    RED.nodes.createNode(this, config);
    const k = rk.bind(this);
    const parsed = k.configure(config, {
      url: { type: 'str', required: true },
      podId: { type: 'str', required: true },
      connectTimeoutMs: { type: 'int', default: 10000, min: 1 },
      deliveryMarginMs: { type: 'int', default: 2000, min: 0 }
    });
    const client = new GatewayClient({
      ...(parsed.ok ? parsed.value : {}),
      token: this.credentials?.token,
      sessionId: randomUUID(),
      logger: { warn: (...args) => k.log.warn(args[0], { key: 'GATEWAY_CLIENT' }) }
    });
    this.gatewayClient = client;
    palette.clients.set(this.id, client);
    this.gatewayDependents = new Set();
    const onClientStatus = (value) => {
      const status = value === 'connected' ? { fill: 'green', text: 'ready' } : { fill: 'red', text: 'disconnected' };
      for (const dependent of this.gatewayDependents) dependent(status);
    };
    client.on('status', onClientStatus);
    k.onStart(async (attempt) => {
      if (!parsed.ok) throw new kit.ConfigError(parsed.errors);
      if (typeof this.credentials?.token !== 'string' || !this.credentials.token) throw new Error('Gateway token is required');
      statusFor(this, 'yellow', `starting ${attempt.number}`);
      attempt.onClose(() => client.close());
      await client.start();
    }, { startAlertMs: 30000 });
    k.onClose(async () => {
      client.removeListener('status', onClientStatus);
      palette.clients.delete(this.id);
      await client.close();
    });
  }

  function ApiConfig(config) {
    RED.nodes.createNode(this, config);
    const k = rk.bind(this);
    const parsed = k.configure(config, {
      apiName: { type: 'str', default: '' },
      url: { type: 'str', required: true },
      method: { type: 'enum', values: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE'], default: 'GET' },
      tokenHeader: { type: 'str', default: 'Authorization' },
      timeoutMs: { type: 'int', default: 30000, min: 1 },
      maxResponseBytes: { type: 'int', default: 262144, min: 1 },
      rate: { type: 'float', default: 0, min: 0 },
      burst: { type: 'int', default: 1, min: 1 }
    });
    let url;
    if (parsed.ok) {
      try {
        url = new URL(parsed.value.url);
        if (!['http:', 'https:'].includes(url.protocol)) throw new Error('API URL must use HTTP or HTTPS');
      } catch (error) {
        statusFor(this, 'red', 'config error');
        k.log.error(error, { key: 'ADAPTER_INVALID' });
      }
    }
    this.gatewayApi = parsed.ok && url ? {
      ...parsed.value,
      url: parsed.value.url,
      bucket: parsed.value.apiName || url.host,
      token: this.credentials?.token
    } : undefined;
  }

  function addDependent(node, server, onStatus) {
    if (server && typeof server.addDependent === 'function') return server.addDependent(onStatus);
    node.status({ fill: 'red', shape: 'ring', text: 'config missing' });
    return () => {};
  }

  function Call(config) {
    RED.nodes.createNode(this, config);
    const k = rk.bind(this);
    const parsed = k.configure(config, {
      operation: { type: 'str', required: true },
      queueTimeoutMs: { type: 'int', default: 30000, min: 1 },
      execTimeoutMs: { type: 'int', default: 30000, min: 1 },
      deadlineMs: { type: 'int', default: 60000, min: 1 },
      deliveryMarginMs: { type: 'int', default: 2000, min: 0 }
    });
    const clientConfig = RED.nodes.getNode(config.client);
    const client = clientConfig?.gatewayClient;
    this.gatewayClient = client;
    if (clientConfig) {
      clientConfig.gatewayDependents ||= new Set();
      const notify = (status) => this.status(status);
      clientConfig.gatewayDependents.add(notify);
      this.on('close', () => clientConfig.gatewayDependents.delete(notify));
    }
    k.onInput(async (msg, send) => {
      if (!parsed.ok) throw new kit.ConfigError(parsed.errors);
      if (!client) throw Object.assign(new Error('Gateway client config is missing'), { code: 'GATEWAY_NOT_CONNECTED', retryable: true });
      const request = msg.redkern?.gateway || {};
      const capability = Array.isArray(client.capabilities)
        ? client.capabilities.find((item) => item.operation === parsed.value.operation)
        : undefined;
      const requestedQueue = Math.min(parsed.value.queueTimeoutMs, positiveBudget(request.queueTimeoutMs, parsed.value.queueTimeoutMs));
      const requestedExec = Math.min(parsed.value.execTimeoutMs, positiveBudget(request.execTimeoutMs, parsed.value.execTimeoutMs));
      const deadlineMs = Math.min(parsed.value.deadlineMs, positiveBudget(request.deadlineMs, parsed.value.deadlineMs));
      if (deadlineMs <= requestedExec) throw Object.assign(new Error('deadlineMs must be greater than execTimeoutMs'), { code: 'REQUEST_VALIDATION_FAILED' });
      const requestId = request.requestId || randomUUID();
      const response = await client.call(parsed.value.operation, msg, {
        requestId,
        priority: config.priority || 'normal',
        deadlineMs,
        queueTimeoutMs: requestedQueue,
        execTimeoutMs: requestedExec,
        idempotencyKey: request.idempotencyKey
      });
      if (capability?.contract === 'passthrough' && response && typeof response === 'object') {
        Object.assign(msg, response);
      } else if (capability?.contract === 'http' && response && typeof response === 'object') {
        msg.payload = response.payload;
        if (Number.isInteger(response.statusCode)) {
          msg.redkern = { ...(msg.redkern || {}), gateway: { ...request, requestId, operation: parsed.value.operation, httpStatus: response.statusCode } };
        }
      } else {
        msg.payload = response;
      }
      msg.redkern = { ...(msg.redkern && typeof msg.redkern === 'object' ? msg.redkern : {}), gateway: {
        ...request,
        requestId,
        operation: parsed.value.operation,
        idempotencyKey: request.idempotencyKey,
        deadline: Date.now() + deadlineMs,
        attempt: 1
      } };
      send(msg);
    }, { concurrency: 32, maxQueue: 0, errorOutput: true, outputCount: 2 });
  }

  function WorkerIn(config) {
    RED.nodes.createNode(this, config);
    const k = rk.bind(this);
    const parsed = k.configure(config, {
      operation: { type: 'str', required: true },
      execTimeoutMs: { type: 'int', default: 30000, min: 1 },
      concurrency: { type: 'int', default: 32, min: 1 },
      retryClass: { type: 'enum', values: ['safe', 'idempotent', 'never'], default: 'never' },
      maxDeliveries: { type: 'int', default: 3, min: 1 }
    });
    const server = RED.nodes.getNode(config.server);
    const removeDependent = addDependent(this, server, (status) => this.status(status));
    let release;
    let ready = false;
    if (parsed.ok && server && server.gatewayState) {
      const execute = (message, context) => server.gatewayState.requestWorker(
        context.requestId,
        message,
        parsed.value.execTimeoutMs
      );
      execute.emitWorker = (message) => this.send(message);
      try {
        release = server.gatewayState.addExecutor(parsed.value.operation, execute, {
          contract: 'passthrough', concurrency: parsed.value.concurrency,
          execTimeoutMs: parsed.value.execTimeoutMs, queueTimeoutMs: 30000, deadlineMs: 60000,
          retryClass: parsed.value.retryClass, maxDeliveries: parsed.value.maxDeliveries
        });
      } catch (error) {
        statusFor(this, 'red', 'config error');
        k.log.error(error, { key: error.code || 'WORKER_CONFIG' });
      }
      k.onStart(async (attempt) => {
        if (!parsed.ok) throw new kit.ConfigError(parsed.errors);
        if (!release) throw new Error('Worker could not register with Gateway server');
        await release.ready;
        statusFor(this, 'yellow', `starting ${attempt.number}`);
        ready = true;
        statusFor(this, 'green', 'ready');
        attempt.onClose(() => release?.());
      }, { startAlertMs: 30000 });
      k.onClose(async () => {
        ready = false;
        await release?.();
        removeDependent();
      });
      this.isReady = () => ready;
      return;
    }
    k.onStart(async () => {
      if (!parsed.ok) throw new kit.ConfigError(parsed.errors);
      throw new Error('Gateway server config is missing');
    });
  }

  function WorkerOut(config) {
    RED.nodes.createNode(this, config);
    const k = rk.bind(this);
    const parsed = k.configure(config, { operation: { type: 'str', required: true } });
    const server = RED.nodes.getNode(config.server);
    addDependent(this, server, (status) => this.status(status));
    k.onInput(async (msg) => {
      if (!parsed.ok) throw new kit.ConfigError(parsed.errors);
      const gateway = msg.redkern?.gateway;
      if (!gateway?.requestId) throw Object.assign(new Error('msg.redkern.gateway.requestId is required'), { code: 'WORKER_PAYLOAD_INVALID' });
      const accepted = server?.respondWorker?.(gateway.requestId, parsed.value.operation, msg);
      if (!accepted) throw Object.assign(new Error('Worker request is no longer active'), { code: 'WORKER_PAYLOAD_INVALID' });
    }, { concurrency: 32, maxQueue: 1000 });
  }

  function Adapter(config) {
    RED.nodes.createNode(this, config);
    const k = rk.bind(this);
    const parsed = k.configure(config, {
      operation: { type: 'str', required: true },
      concurrency: { type: 'int', default: 32, min: 1 },
      queueTimeoutMs: { type: 'int', default: 30000, min: 1 },
      execTimeoutMs: { type: 'int', default: 30000, min: 1 },
      deadlineMs: { type: 'int', default: 60000, min: 1 },
      rate: { type: 'float', default: 0, min: 0 },
      burst: { type: 'int', default: 1, min: 1 },
      retryClass: { type: 'enum', values: ['safe', 'idempotent', 'never'], default: 'never' },
      maxDeliveries: { type: 'int', default: 3, min: 1 },
      forwardIdempotencyKey: { type: 'bool', default: false }
    });
    const server = RED.nodes.getNode(config.server);
    const apiNode = RED.nodes.getNode(config.api);
    const api = apiNode?.gatewayApi;
    const removeDependent = addDependent(this, server, (status) => this.status(status));
    let release;
    if (parsed.ok && server?.gatewayState && api) {
      const execute = createHttpAdapter({ ...api, forwardIdempotencyKey: parsed.value.forwardIdempotencyKey });
      try {
        const rateLimits = parsed.value.rate > 0
          ? [{ name: 'g', rate: parsed.value.rate, burst: parsed.value.burst }]
          : api.rate > 0
            ? [{ name: 'g', rate: api.rate, burst: api.burst }]
            : undefined;
        const bucket = parsed.value.rate > 0
          ? parsed.value.operation
          : api.rate > 0 ? api.bucket : 'default';
        release = server.gatewayState.addExecutor(parsed.value.operation, execute, {
          contract: 'http', concurrency: parsed.value.concurrency, bucket, rateLimits,
          queueTimeoutMs: parsed.value.queueTimeoutMs, execTimeoutMs: parsed.value.execTimeoutMs,
          deadlineMs: parsed.value.deadlineMs, retryClass: parsed.value.retryClass,
          maxDeliveries: parsed.value.maxDeliveries,
          forwardIdempotencyKey: parsed.value.forwardIdempotencyKey
        });
      } catch (error) {
        statusFor(this, 'red', 'config error');
        k.log.error(error, { key: error.code || 'ADAPTER_INVALID' });
      }
      k.onStart(async (attempt) => {
        if (!release) throw new Error('Adapter could not register with Gateway server');
        await release.ready;
        statusFor(this, 'yellow', `starting ${attempt.number}`);
        attempt.onClose(() => release?.());
        statusFor(this, 'green', 'ready');
      }, { startAlertMs: 30000 });
    } else {
      k.onStart(async () => {
        if (!parsed.ok) throw new kit.ConfigError(parsed.errors);
        throw Object.assign(new Error('Gateway server or API config is missing'), { code: 'ADAPTER_INVALID' });
      });
    }
    k.onClose(async () => {
      await release?.();
      removeDependent();
    });
  }

  function GatewayOut(config) {
    RED.nodes.createNode(this, config);
    const k = rk.bind(this);
    const parsed = k.configure(config, { operation: { type: 'str', required: true } });
    const clientNode = RED.nodes.getNode(config.client);
    const client = clientNode?.gatewayClient;
    if (clientNode) {
      const notify = (status) => this.status(status);
      clientNode.gatewayDependents.add(notify);
      this.on('close', () => clientNode.gatewayDependents.delete(notify));
    }
    k.onInput(async (msg) => {
      if (!parsed.ok) throw new kit.ConfigError(parsed.errors);
      if (!client) throw Object.assign(new Error('Gateway client config is missing'), { code: 'GATEWAY_NOT_CONNECTED', retryable: true });
      await client.call(parsed.value.operation, msg, {
        messageType: 'event',
        priority: config.priority || 'normal',
        idempotencyKey: msg.redkern?.gateway?.idempotencyKey
      });
    }, { concurrency: 32, maxQueue: 1000 });
  }

  function GatewayIn(config) {
    RED.nodes.createNode(this, config);
    const k = rk.bind(this);
    const parsed = k.configure(config, { operation: { type: 'str', required: true } });
    const clientNode = RED.nodes.getNode(config.client);
    const client = clientNode?.gatewayClient;
    const listener = (result) => {
      if (!parsed.ok || result.operation !== parsed.value.operation) return;
      const output = result.response && typeof result.response === 'object'
        ? { ...result.response }
        : { payload: result.response };
      output.redkern = { ...(output.redkern || {}), gateway: {
        ...(output.redkern?.gateway || {}), requestId: result.requestId, operation: result.operation,
        ...(result.error ? { error: result.error } : {})
      } };
      this.send(output);
    };
    if (client && parsed.ok) {
      client.subscribeResults(parsed.value.operation);
      client.on('asyncResult', listener);
    }
    if (clientNode) {
      const notify = (status) => this.status(status);
      clientNode.gatewayDependents.add(notify);
      this.on('close', () => clientNode.gatewayDependents.delete(notify));
    } else {
      statusFor(this, 'red', 'config missing');
    }
    this.on('close', () => client?.removeListener('asyncResult', listener));
  }

  function GatewayMetrics(config) {
    RED.nodes.createNode(this, config);
    const k = rk.bind(this);
    const parsed = k.configure(config, { events: { type: 'str', default: '' } });
    const server = RED.nodes.getNode(config.server);
    const filters = new Set(parsed.ok ? parsed.value.events.split(',').map((value) => value.trim()).filter(Boolean) : []);
    const listener = (metric) => {
      if (filters.size && !filters.has(metric.event)) return;
      this.send({
        topic: `pod-gateway/${metric.event}`,
        payload: metric,
        metric,
        gateway: {
          event: metric.event,
          requestId: metric.requestId,
          service: metric.service,
          operation: metric.operation,
          operationKey: metric.operationKey,
          status: metric.status || metric.outcome || metric.event,
          timestamp: metric.timestamp
        }
      });
    };
    if (parsed.ok && server && typeof server.on === 'function') server.on('gateway-metric', listener);
    k.onStart(async (attempt) => {
      if (!parsed.ok) throw new kit.ConfigError(parsed.errors);
      if (!server || typeof server.on !== 'function') throw new Error('Gateway Server Config is not configured');
      statusFor(this, 'yellow', `starting ${attempt.number}`);
      attempt.onClose(() => {
        server.removeListener('gateway-metric', listener);
      });
      statusFor(this, 'green', 'ready');
    }, { startAlertMs: 30000 });
    this.on('close', () => server?.removeListener?.('gateway-metric', listener));
  }

  function positiveBudget(value, fallback) {
    return Number.isInteger(value) && value > 0 ? value : fallback;
  }

  RED.nodes.registerType('redkern-gateway-server-config', ServerConfig, {
    credentials: {
      tlsKey: { type: 'password' },
      tlsCert: { type: 'password' },
      redisPassword: { type: 'password' }
    }
  });
  RED.nodes.registerType('redkern-gateway-account', Account, {
    credentials: { token: { type: 'password' }, nextToken: { type: 'password' } }
  });
  RED.nodes.registerType('redkern-gateway-client-config', ClientConfig, {
    credentials: { token: { type: 'password' } }
  });
  RED.nodes.registerType('redkern-gateway-api-config', ApiConfig, {
    credentials: { token: { type: 'password' } }
  });
  RED.nodes.registerType('redkern-gateway-call', Call);
  RED.nodes.registerType('redkern-gateway-out', GatewayOut);
  RED.nodes.registerType('redkern-gateway-in', GatewayIn);
  RED.nodes.registerType('redkern-gateway-adapter', Adapter);
  RED.nodes.registerType('redkern-gateway-worker-in', WorkerIn);
  RED.nodes.registerType('redkern-gateway-worker-out', WorkerOut);
  RED.nodes.registerType('redkern-gateway-metrics', GatewayMetrics);
};
'use strict';

const EventEmitter = require('node:events');
const http = require('node:http');
const https = require('node:https');
const { WebSocketServer, WebSocket } = require('ws');
const { extractToken, secureCompare } = require('@redkern/node-red-kit');
const { accountAllows, authenticateAccount, validateAccounts } = require('./security.js');
const { errorPayload, parseOperation, validateEnvelope } = require('./protocol.js');
const { memoryRatio, parseRedisInfo } = require('./server/redis-health.js');

const PRE_AUTH_MAX_PAYLOAD = 16 * 1024;

class GatewayRuntime extends EventEmitter {
  constructor(options) {
    super();
    this.host = options.host;
    this.port = Number(options.port);
    this.maxPayload = Number(options.maxPayload || 1024 * 1024);
    this.maxConnections = Number(options.maxConnections || 500);
    this.maxUnauthenticatedConnections = Number(options.maxUnauthenticatedConnections || 50);
    this.helloTimeoutMs = Number(options.helloTimeoutMs || 5000);
    this.maxInFlightPerConnection = Number(options.maxInFlightPerConnection || 1000);
    this.maxResultBytes = Number(options.maxResultBytes || 256 * 1024);
    this.accounts = new Map();
    this.executors = new Map();
    this.executorMetadata = new Map();
    this.connections = new Set();
    this.httpServer = undefined;
    this.webSocketServer = undefined;
    this.heartbeatTimer = undefined;
    this.draining = false;
      this.cleanDrainComplete = false;
    this.closed = false;
    this.secureCompare = options.secureCompare || secureCompare;
    this.logger = options.logger || console;
    this.tls = options.tls;
    this.redis = options.redis;
    this.redisHandle = options.redisHandle;
    this.redisMemorySamplePromise = undefined;
    this.queueFootprintSamplePromise = undefined;
    this.gcra = options.gcra;
    this.keyPrefix = options.keyPrefix || 'pod-gateway';
    this.queue = options.queue;
    this.idempotency = options.idempotency;
    this.asyncResults = options.asyncResults;
    this.sessionReconnectGraceMs = Number(options.sessionReconnectGraceMs || 10000);
    this.redisPolicy = options.redisPolicy || 'noeviction';
    this.allowEvictingRedis = Boolean(options.allowEvictingRedis);
    this.maxGatewayMemoryBytes = Number(options.maxGatewayMemoryBytes || 1024 * 1024 * 1024);
    this.redisEvictionSampleMs = Number(options.redisEvictionSampleMs || 15000);
    this.redisStats = { evictedKeys: 0 };
    this.previousEvictedKeys = undefined;
    this.redisEvictionTimer = undefined;
    this.lease = options.lease;
    this.pendingRequests = new Map();
    this.idempotencyClaims = new Map();
    this.pendingDuplicates = new Map();
    this.pendingResultAcks = new Map();
    this.resultSubscriptions = new Map();
    this.pendingAsyncAcks = new Map();
    this.asyncReads = new Set();
    this.asyncReconnectTimers = new Map();
    if (this.queue) {
      this.queue.on('rateDecision', (decision) => this._pushRateDecision(decision));
      this.queue.on('processing', ({ task, queueWaitMs, rateLimitMs }) => {
        this._emitMetric('request.processing', task, { queueWaitMs, rateLimitMs });
      });
      this.queue.on('upstreamComplete', ({ task, durationMs, error, contract }) => {
        this._emitMetric('upstream.completed', task, {
          attempt: task.attempt || 1,
          durationMs,
          outcome: error ? 'error' : 'success',
          errorCode: error?.code || '',
          httpStatus: Number(error?.statusCode || 0),
          contract
        });
      });
      this.queue.on('cachedResult', ({ task, response }) => {
        this._deliverCachedResult(task, response).catch((error) => this.logger.error?.('[redkern:gateway] Cached result delivery failed', { error }));
      });
      if (typeof this.queue.setResultHandlers === 'function') {
        this.queue.setResultHandlers({
          onResult: (task, result, contract) => this._deliverQueuedResult(task, result, contract),
          onError: (task, error) => this._deliverQueuedError(task, error)
        });
      } else {
        this.queue.on('result', ({ task, result, contract }) => {
          this._deliverQueuedResult(task, result, contract).catch((error) => this.logger.error?.('[redkern:gateway] Queue result delivery failed', { error }));
        });
        this.queue.on('errorResult', ({ task, error }) => {
          this._deliverQueuedError(task, error).catch((deliveryError) => this.logger.error?.('[redkern:gateway] Queue error delivery failed', { error: deliveryError }));
        });
      }
    }
    this.lease?.once('lost', () => {
      this.draining = true;
      this.close(1013, 'GATEWAY_STANDBY').catch((error) => this.emit('error', error));
    });
  }

  registerAccount(account) {
    if (this.accounts.has(account.name)) throw new Error(`Duplicate gateway account: ${account.name}`);
    this.accounts.set(account.name, account);
    this._broadcastCapabilities();
    return () => {
      this.accounts.delete(account.name);
      this._broadcastCapabilities();
    };
  }

  registerExecutor(operation, execute, metadata = {}) {
    const name = parseOperation(operation);
    if (typeof execute !== 'function') throw new TypeError('executor must be a function');
    if (this.executors.has(name)) throw new Error(`Executor already registered: ${name}`);
    this.executors.set(name, execute);
    this.executorMetadata.set(name, { ...metadata });
    this._broadcastCapabilities();
    this.emit('executor', { operation: name, available: true });
    return () => {
      if (this.executors.get(name) === execute) {
        this.executors.delete(name);
        this.executorMetadata.delete(name);
      }
      this._broadcastCapabilities();
      this.emit('executor', { operation: name, available: false });
    };
  }

  async start() {
    if (this.closed) throw new Error('Gateway runtime is closed');
    if (typeof this.host !== 'string' || this.host.trim() === '') throw new Error('Gateway host is required');
    if (!Number.isInteger(this.port) || this.port < 1 || this.port > 65535) throw new Error('Gateway port must be between 1 and 65535');
    if (!Number.isInteger(this.maxPayload) || this.maxPayload < PRE_AUTH_MAX_PAYLOAD || this.maxPayload > 16 * 1024 * 1024) {
      throw new Error('maxPayload must be between 16384 and 16777216 bytes');
    }
    if (!Number.isInteger(this.maxConnections) || this.maxConnections < 1) throw new Error('maxConnections must be positive');
    if (!Number.isInteger(this.maxUnauthenticatedConnections) || this.maxUnauthenticatedConnections < 1) {
      throw new Error('maxUnauthenticatedConnections must be positive');
    }
    validateAccounts([...this.accounts.values()]);

    const serverOptions = this.tls ? { key: this.tls.key, cert: this.tls.cert } : undefined;
    const createServer = this.tls ? https.createServer : http.createServer;
    this.httpServer = createServer(serverOptions, (request, response) => this._handleHttp(request, response));
    this.webSocketServer = new WebSocketServer({
      noServer: true,
      maxPayload: this.maxPayload,
      perMessageDeflate: false,
      clientTracking: true
    });
    this.httpServer.on('upgrade', (request, socket, head) => this._handleUpgrade(request, socket, head));
    this.httpServer.on('error', (error) => this.emit('error', error));

    await new Promise((resolve, reject) => {
      const onError = (error) => {
        this.httpServer.removeListener('listening', onListening);
        reject(error);
      };
      const onListening = () => {
        this.httpServer.removeListener('error', onError);
        resolve();
      };
      this.httpServer.once('error', onError);
      this.httpServer.once('listening', onListening);
      this.httpServer.listen(this.port, this.host);
    });
    this.heartbeatTimer = setInterval(() => this._heartbeat(), 15000);
    this.heartbeatTimer.unref?.();
    if (this.redisPolicy.startsWith('volatile-') && this.redis && typeof this.redis.info === 'function') {
      this.sampleRedisEvictions().catch((error) => this.logger.warn?.('[redkern:gateway] Redis eviction sample failed', { error }));
      this.redisEvictionTimer = setInterval(() => {
        this.sampleRedisEvictions().catch((error) => this.logger.warn?.('[redkern:gateway] Redis eviction sample failed', { error }));
      }, this.redisEvictionSampleMs);
      this.redisEvictionTimer.unref?.();
    }
    this.emit('ready');
  }

  async close(closeCode = 1012, closeReason = 'Service Restart') {
    this.closed = true;
    clearInterval(this.heartbeatTimer);
    clearInterval(this.redisEvictionTimer);
    for (const timer of this.asyncReconnectTimers.values()) clearTimeout(timer);
    this.asyncReconnectTimers.clear();
    for (const connection of this.connections) connection.socket.close(closeCode, closeReason);
    if (this.webSocketServer) await new Promise((resolve) => this.webSocketServer.close(resolve));
    if (this.httpServer && this.httpServer.listening) {
      await new Promise((resolve) => this.httpServer.close(resolve));
    }
  }

  async drain(mode = 'graceful', timeoutMs = 12000) {
    if (!['graceful', 'complete', 'off'].includes(mode)) throw new TypeError('drain mode must be graceful, complete, or off');
    if (mode === 'off') {
      this.draining = false;
      const report = await this.queue?.drain('off', 1);
      return { mode, remaining: report?.remaining || 0 };
    }
    this.draining = true;
    const report = await this.queue?.drain(mode, timeoutMs) || { remaining: 0, active: 0 };
      this.cleanDrainComplete = mode === 'complete' && report.remaining === 0 && report.active === 0;
    for (const connection of this.connections) connection.socket.close(1012, 'Service Restart');
    return { mode, ...report };
  }

  _handleHttp(request, response) {
    const status = request.url === '/healthz/live' ? 200
      : request.url === '/healthz/ready' && this.httpServer.listening && !this.draining && (!this.lease || this.lease.isFresh()) ? 200
        : request.url === '/healthz/ready' ? 503 : 404;
    response.writeHead(status, { 'content-type': 'application/json; charset=utf-8', connection: 'close' });
    response.end(JSON.stringify({ status: status === 200 ? 'ok' : 'unavailable' }));
  }

  async sampleRedisEvictions() {
    if (!this.redisPolicy.startsWith('volatile-') || !this.redis || typeof this.redis.info !== 'function') return undefined;
    const values = parseRedisInfo(await this.redis.info('stats'));
    const count = Number(values.evicted_keys);
    if (!Number.isSafeInteger(count) || count < 0) throw Object.assign(new Error('Redis evicted_keys statistic is unavailable'), { code: 'REDIS_STATS_UNAVAILABLE' });
    const previous = this.previousEvictedKeys;
    this.previousEvictedKeys = count;
    this.redisStats.evictedKeys = count;
    if (previous !== undefined && count > previous) {
      const event = { total: count, delta: count - previous };
      this.emit('redisEvictions', event);
      this.logger.warn?.('[redkern:gateway] Redis evicted keys under volatile policy', event);
    }
    return count;
  }

  _handleUpgrade(request, socket, head) {
    if (this.lease && !this.lease.isFresh()) {
      this._rejectUpgrade(socket, 503, 'GATEWAY_STANDBY');
      return;
    }
    let url;
    try {
      url = new URL(request.url, 'http://localhost');
    } catch {
      socket.destroy();
      return;
    }
    if (url.searchParams.has('token') || url.searchParams.has('access_token')) {
      this._rejectUpgrade(socket, 400, 'Query tokens are not accepted');
      return;
    }
    const token = extractToken(request);
    const account = token === undefined ? undefined : authenticateAccount([...this.accounts.values()], token, this.secureCompare);
    if (token !== undefined && !account) {
      this._rejectUpgrade(socket, 401, 'Unauthorized');
      return;
    }
    const authenticatedCount = [...this.connections].filter((connection) => connection.account).length;
    const unauthenticatedCount = this.connections.size - authenticatedCount;
    if (!account && unauthenticatedCount >= this.maxUnauthenticatedConnections) {
      this._rejectUpgrade(socket, 503, 'Unauthenticated connection limit');
      return;
    }

    this.webSocketServer.handleUpgrade(request, socket, head, (webSocket) => {
      const connection = { socket: webSocket, account, podId: undefined, sessionId: undefined, family: undefined, inFlight: 0 };
      connection.ratePushAt = new Map();
      connection.capacityQueryAt = [];
      this.connections.add(connection);
      webSocket.on('pong', () => {
        clearTimeout(connection.pongTimer);
        connection.pongTimer = undefined;
      });
      connection.helloTimer = setTimeout(() => this._closeUnauthenticated(connection), this.helloTimeoutMs);
      connection.helloTimer.unref?.();
      webSocket.on('message', (data, isBinary) => {
        this._receive(connection, data, isBinary).catch((error) => {
          this.logger.error?.('[redkern:gateway] WebSocket message failed', { error });
          this._sendError(connection, undefined, error, 'INVALID_MESSAGE');
        });
      });
      webSocket.on('close', () => this._removeConnection(connection));
      webSocket.on('error', (error) => this.logger.warn?.('[redkern:gateway] WebSocket connection error', { error }));
      if (account) this._markAuthenticated(connection, false);
      this.emit('connection', connection);
    });
  }

  async _receive(connection, data, isBinary) {
    const bytes = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (!connection.account && bytes.byteLength > PRE_AUTH_MAX_PAYLOAD) {
      connection.socket.close(1009, 'MESSAGE_TOO_LARGE');
      return;
    }
    if (isBinary) {
      this._sendError(connection, undefined, Object.assign(new Error('Binary messages are not supported'), { code: 'INVALID_MESSAGE' }));
      return;
    }
    let message;
    try {
      message = JSON.parse(bytes.toString('utf8'));
      validateEnvelope(message);
    } catch (error) {
      this._sendError(connection, message && message.requestId, error, error.code || 'INVALID_MESSAGE');
      return;
    }
    if (message.type === 'hello') {
      await this._hello(connection, message);
      return;
    }
    if (message.type === 'heartbeat') {
      this._send(connection, { type: 'heartbeat', timestamp: new Date().toISOString() });
      return;
    }
    if (message.type === 'result.ack') {
      const claim = this.pendingResultAcks.get(message.requestId);
      if (claim) {
        this.pendingResultAcks.delete(message.requestId);
        this.idempotency?.ack(claim).catch((error) => this.logger.warn?.('[redkern:gateway] Result ACK cleanup failed', { error }));
      } else if (connection.account && connection.family === '1.1' && this.idempotency) {
        const operations = this._capabilityItems(connection).map((item) => item.operation);
        this.idempotency.ackForSession({
          client: connection.account.name,
          podId: connection.podId,
          sessionId: connection.sessionId,
          operations,
          requestId: message.requestId
        }).catch((error) => this.logger.warn?.('[redkern:gateway] Resumed result ACK cleanup failed', { error }));
      }
      const asyncAck = this.pendingAsyncAcks.get(message.requestId);
      if (asyncAck && connection.account?.name === asyncAck.client) {
        this.pendingAsyncAcks.delete(message.requestId);
        try {
          await this.asyncResults?.ack(asyncAck.stream, asyncAck.streamId);
          await this._deliverAsyncAvailable(asyncAck.client, asyncAck.operation);
        } catch (error) {
          this.logger.warn?.('[redkern:gateway] Async result ACK failed', { error });
        }
      }
      return;
    }
    if (message.type === 'result.subscribe') {
      if (!connection.account || connection.family !== '1.1') {
        this._sendError(connection, undefined, Object.assign(new Error('result.subscribe requires protocol 1.1 authentication'), { code: 'NOT_AUTHENTICATED' }));
        return;
      }
      await this._subscribeAsyncResults(connection, message.operation);
      return;
    }
    if (message.type === 'capacity.query') {
      if (!connection.account || connection.family !== '1.1') {
        this._sendError(connection, message.requestId, Object.assign(new Error('capacity.query requires protocol 1.1 authentication'), { code: 'NOT_AUTHENTICATED' }));
        return;
      }
      await this._capacityQuery(connection, message);
      return;
    }
    if (message.type === 'result.resume') {
      if (!connection.account || connection.family !== '1.1') {
        this._sendError(connection, undefined, Object.assign(new Error('result.resume requires protocol 1.1 authentication'), { code: 'NOT_AUTHENTICATED' }));
        return;
      }
      await this._resumeResults(connection, message.requestIds);
      return;
    }
    if (!connection.account) {
      this._sendError(connection, message.requestId, Object.assign(new Error('hello is required before requests'), { code: 'NOT_AUTHENTICATED' }));
      return;
    }
    if (message.type !== 'call' && message.type !== 'event') {
      this._sendError(connection, message.requestId, Object.assign(new Error('unsupported message type'), { code: 'INVALID_MESSAGE' }));
      return;
    }
    await this._dispatch(connection, message);
  }

  async _hello(connection, message) {
    const protocolVersion = message.protocolVersion || '1.0';
    if (message.protocolVersion && !['1.0', '1.1'].includes(message.protocolVersion)) {
      this._send(connection, { type: 'hello_error', error: { code: 'UNSUPPORTED_PROTOCOL', message: 'Unsupported protocol version' } });
      connection.socket.close(1008, 'UNSUPPORTED_PROTOCOL');
      return;
    }
    if (connection.account && message.token) {
      const helloAccount = authenticateAccount([...this.accounts.values()], message.token, this.secureCompare);
      if (helloAccount !== connection.account) {
        this._authFailed(connection);
        return;
      }
    } else if (!connection.account) {
      connection.account = authenticateAccount([...this.accounts.values()], message.token, this.secureCompare);
      if (!connection.account) {
        this._authFailed(connection);
        return;
      }
      const authenticatedCount = [...this.connections].filter((item) => item.account).length;
      if (authenticatedCount > this.maxConnections) {
        connection.account = undefined;
        connection.socket.close(1013, 'CONNECTION_LIMIT');
        return;
      }
      this._markAuthenticated(connection);
    }
    if (protocolVersion === '1.1' && (typeof message.sessionId !== 'string' || message.sessionId.length === 0)) {
      this._send(connection, { type: 'hello_error', error: { code: 'INVALID_MESSAGE', message: 'sessionId is required for protocol 1.1' } });
      connection.socket.close(1008, 'INVALID_MESSAGE');
      return;
    }
    connection.family = protocolVersion;
    connection.podId = message.podId;
    connection.sessionId = protocolVersion === '1.1' ? message.sessionId : undefined;
    for (const other of this.connections) {
      if (other === connection || !other.account || other.account.name !== connection.account.name ||
          other.family !== protocolVersion || other.podId !== connection.podId) continue;
      if (protocolVersion === '1.1' && other.sessionId !== connection.sessionId) {
        if (await this._probeConnection(other, 2000)) {
          this._send(connection, { type: 'hello_error', error: { code: 'POD_ID_CONFLICT', message: 'podId is active in another session' } });
          connection.socket.close(1008, 'POD_ID_CONFLICT');
          return;
        }
        other.socket.terminate();
      } else if (protocolVersion === '1.0') {
        this.logger.warn?.('[redkern:gateway] Legacy podId connection takeover', { podId: connection.podId });
        this.emit('podidTakeover', connection);
      }
      other.socket.close(1012, 'Session Replaced');
    }
    clearTimeout(connection.helloTimer);
    const capabilities = this._capabilityItems(connection);
    this._send(connection, { type: 'hello_ack', protocolVersion, podId: connection.podId, capabilities });
    this._sendCapabilities(connection);
    this.emit('authenticated', connection);
  }

  _probeConnection(connection, timeoutMs) {
    if (connection.socket.readyState !== WebSocket.OPEN) return Promise.resolve(false);
    return new Promise((resolve) => {
      let complete = false;
      const finish = (responded) => {
        if (complete) return;
        complete = true;
        clearTimeout(timer);
        connection.socket.removeListener('pong', onPong);
        resolve(responded);
      };
      const onPong = () => finish(true);
      const timer = setTimeout(() => finish(false), timeoutMs);
      connection.socket.once('pong', onPong);
      connection.socket.ping((error) => {
        if (error) finish(false);
      });
    });
  }

  _sampleRedisMemoryRatio() {
    if (!this.redisMemorySamplePromise) {
      const sample = Promise.resolve().then(() => this.redis.info('memory')).then(memoryRatio);
      this.redisMemorySamplePromise = sample;
      sample.finally(() => {
        if (this.redisMemorySamplePromise === sample) this.redisMemorySamplePromise = undefined;
      }).catch(() => {});
    }
    return this.redisMemorySamplePromise;
  }

  _sampleQueueFootprint() {
    if (!this.queueFootprintSamplePromise) {
      const sample = Promise.resolve().then(() => this.queue.footprintBytes());
      this.queueFootprintSamplePromise = sample;
      sample.finally(() => {
        if (this.queueFootprintSamplePromise === sample) this.queueFootprintSamplePromise = undefined;
      }).catch(() => {});
    }
    return this.queueFootprintSamplePromise;
  }

  async _dispatch(connection, message) {
    if (this.lease && !this.lease.isFresh()) {
      this._sendError(connection, message.requestId, Object.assign(new Error('Gateway does not own the active lease'), { code: 'GATEWAY_STANDBY', retryable: true }));
      return;
    }
    if (this.draining) {
      this._sendError(connection, message.requestId, Object.assign(new Error('Gateway is draining'), { code: 'GATEWAY_DRAINING', retryable: true }));
      return;
    }
    if (!accountAllows(connection.account, message.operation)) {
      this._sendError(connection, message.requestId, Object.assign(new Error('Operation is not allowed'), { code: 'FORBIDDEN_OPERATION' }));
      return;
    }
    if (connection.inFlight >= this.maxInFlightPerConnection) {
      this._sendError(connection, message.requestId, Object.assign(new Error('Too many requests in flight'), { code: 'TOO_MANY_IN_FLIGHT', retryable: true }));
      return;
    }
    const execute = this.executors.get(message.operation);
    if (!execute) {
      this._sendError(connection, message.requestId, Object.assign(new Error('Operation not found'), { code: 'OPERATION_NOT_FOUND' }));
      return;
    }
    if (this.queue) {
      try {
        if (this.redisPolicy === 'noeviction') {
          const ratio = await this._sampleRedisMemoryRatio();
          if (ratio !== undefined && ratio > 0.8) {
            this._sendError(connection, message.requestId, Object.assign(new Error('Redis memory watermark reached'), { code: 'REDIS_MEMORY_PRESSURE', retryable: true }));
            return;
          }
        } else if (this.allowEvictingRedis && await this._sampleQueueFootprint() >= this.maxGatewayMemoryBytes) {
          this._sendError(connection, message.requestId, Object.assign(new Error('Gateway Redis footprint limit reached'), { code: 'REDIS_MEMORY_PRESSURE', retryable: true }));
          return;
        }
      } catch (error) {
        this._sendError(connection, message.requestId, Object.assign(new Error('Redis memory status is unavailable'), { code: 'QUEUE_UNAVAILABLE', retryable: true, cause: error }));
        return;
      }
      const metadata = this.executorMetadata.get(message.operation) || {};
      const maxQueueTimeoutMs = positiveInteger(metadata.queueTimeoutMs, 30000);
      const maxExecTimeoutMs = positiveInteger(metadata.execTimeoutMs, 30000);
      const maxDeadlineMs = positiveInteger(metadata.deadlineMs, maxQueueTimeoutMs + maxExecTimeoutMs);
      const queueTimeoutMs = Math.min(maxQueueTimeoutMs, positiveInteger(message.queueTimeoutMs, maxQueueTimeoutMs));
      const execTimeoutMs = Math.min(maxExecTimeoutMs, positiveInteger(message.execTimeoutMs, maxExecTimeoutMs));
      const deadlineMs = Math.min(maxDeadlineMs, positiveInteger(message.deadlineMs, maxDeadlineMs));
      if (deadlineMs <= execTimeoutMs || queueTimeoutMs < 1) {
        this._sendError(connection, message.requestId, Object.assign(new Error('Request deadline budget is invalid'), { code: 'REQUEST_VALIDATION_FAILED' }));
        return;
      }
      const deadlineAt = message.deadlineAt
        ? parseLegacyDeadline(message.deadlineAt, Date.now() + deadlineMs)
        : Date.now() + deadlineMs;
      let idempotencyClaim;
      const idempotencyRequest = {
        protocolVersion: connection.family,
        client: connection.account.name,
        operation: message.operation,
        podId: connection.podId,
        sessionId: connection.sessionId,
        idempotencyKey: message.idempotencyKey,
        requestId: message.requestId
      };
      if (this.idempotency && message.idempotencyKey) {
        try {
          idempotencyClaim = await this.idempotency.claim(idempotencyRequest);
        } catch (error) {
          this._sendError(connection, message.requestId, Object.assign(new Error('Idempotency store is unavailable'), { code: 'IDEMPOTENCY_UNAVAILABLE', retryable: true, cause: error }));
          return;
        }
        if (!idempotencyClaim.claimed) {
          if (idempotencyClaim.completed) {
            if (!idempotencyClaim.resultAvailable) {
              this._sendError(connection, message.requestId, Object.assign(new Error('Original request completed and its result buffer expired'), { code: 'DUPLICATE_COMPLETED' }));
              return;
            }
            const duplicate = { ...idempotencyClaim.response, requestId: message.requestId, duplicateOf: idempotencyClaim.marker.requestId };
            this._send(connection, duplicate);
            return;
          }
          const originalRequestId = idempotencyClaim.originalRequestId;
          if (!originalRequestId) {
            this._sendError(connection, message.requestId, Object.assign(new Error('Idempotency owner is unavailable'), { code: 'IDEMPOTENCY_UNAVAILABLE', retryable: true }));
            return;
          }
          const duplicate = { connection, requestId: message.requestId, protocolVersion: connection.family };
          if (!this.pendingDuplicates.has(originalRequestId)) this.pendingDuplicates.set(originalRequestId, []);
          this.pendingDuplicates.get(originalRequestId).push(duplicate);
          connection.inFlight += 1;
          this._send(connection, { type: 'accepted', requestId: message.requestId, duplicateOf: originalRequestId });
          return;
        }
      }
      const task = {
        requestId: message.requestId,
        operation: message.operation,
        client: connection.account.name,
        podId: connection.podId,
        sessionId: connection.sessionId,
        consumerId: this.idempotency?.consumerId,
        protocolVersion: connection.family,
        priority: message.priority === 'bulk' ? 'bulk' : 'normal',
        receivedAt: Date.now(),
        idempotencyKey: message.idempotencyKey,
        message,
        queueTimeoutMs,
        execTimeoutMs,
        deadlineMs,
        deadlineAt
      };
      if (idempotencyClaim) this.idempotencyClaims.set(message.requestId, { claim: idempotencyClaim, request: idempotencyRequest });
      connection.inFlight += 1;
      this.pendingRequests.set(message.requestId, connection);
      try {
        const queued = await this.queue.enqueue(task);
        this._send(connection, {
          type: 'accepted',
          requestId: message.requestId,
          queueId: queued.streamId,
          queuePosition: queued.depth
        });
      } catch (error) {
        this.pendingRequests.delete(message.requestId);
        connection.inFlight = Math.max(0, connection.inFlight - 1);
        if (idempotencyClaim) {
          this.idempotencyClaims.delete(message.requestId);
          await this.idempotency.release(idempotencyClaim, message.requestId).catch(() => {});
        }
        this._sendError(connection, message.requestId, error, 'QUEUE_UNAVAILABLE');
      }
      return;
    }
    connection.inFlight += 1;
    const startedAt = Date.now();
    this._send(connection, { type: 'accepted', requestId: message.requestId });
    try {
      const result = await execute(message, {
        client: connection.account.name,
        operation: message.operation,
        podId: connection.podId,
        protocolVersion: connection.family,
        requestId: message.requestId
      });
      const response = connection.family === '1.0'
        ? {
            type: 'result', requestId: message.requestId, operation: message.operation,
            ok: true, payload: result, durationMs: Date.now() - startedAt,
            response: { statusCode: null },
            _request: { input: { payload: message.payload }, output: { body: result } }
          }
        : { type: 'result', requestId: message.requestId, response: result };
      if (Buffer.byteLength(JSON.stringify(response)) > this.maxResultBytes) {
        throw Object.assign(new Error('Result exceeds configured limit'), { code: 'RESULT_TOO_LARGE' });
      }
      if (message.type === 'event' && connection.family === '1.1') {
        this._send(connection, { type: 'async.result', requestId: message.requestId, operation: message.operation, response: result });
      } else {
        this._send(connection, response);
      }
    } catch (error) {
      this._sendError(connection, message.requestId, error, 'UPSTREAM_ERROR');
    } finally {
      connection.inFlight = Math.max(0, connection.inFlight - 1);
    }
  }

  async _deliverQueuedResult(task, result, contract) {
    const deliveryStartedAt = Date.now();
    if (task.message?.type === 'event' && task.protocolVersion === '1.1') {
      const error = result?.error;
      const asyncEntry = {
        requestId: task.requestId,
        operation: task.operation,
        client: task.client,
        response: error ? undefined : result,
        ...(error ? { error } : {}),
        completedAt: Date.now()
      };
      const claim = this.idempotencyClaims.get(task.requestId);
      if (claim) {
        await this.idempotency.completeAsync(claim.claim, claim.request, error);
        this.idempotencyClaims.delete(task.requestId);
      }
      await this.asyncResults.append(task.client, task.operation, asyncEntry);
      await this._deliverAsyncAvailable(task.client, task.operation);
      const origin = this.pendingRequests.get(task.requestId);
      this.pendingRequests.delete(task.requestId);
      if (origin) origin.inFlight = Math.max(0, origin.inFlight - 1);
      const totalMs = Math.max(0, Date.now() - Number(task.receivedAt || task.queuedAt || Date.now()));
      this._emitMetric('request.completed', task, {
        outcome: error ? 'error' : 'success', status: error ? 'failed' : 'completed',
        durationMs: totalMs, delivered: true, errorCode: error?.code || '', httpStatus: Number(error?.statusCode || 0),
        timings: {
          queueMs: Number(task.queueWaitMs || 0), rateLimitMs: Number(task.rateLimitWaitMs || 0),
          upstreamMs: Number(task.upstreamMs || 0), deliveryMs: Math.max(0, Date.now() - deliveryStartedAt), totalMs
        }, contract
      });
      return;
    }
    const connection = this.pendingRequests.get(task.requestId);
    this.pendingRequests.delete(task.requestId);
    if (connection) connection.inFlight = Math.max(0, connection.inFlight - 1);
    const response = result;
    const payload = task.protocolVersion === '1.0'
      ? {
          type: 'result', requestId: task.requestId, operation: task.operation,
          ok: true, payload: contract === 'http' ? result.payload : result,
          durationMs: Math.max(0, Date.now() - (task.queuedAt || Date.now())),
          response: { statusCode: contract === 'http' ? result.statusCode : null },
          _request: { input: { payload: task.message.payload }, output: { body: contract === 'http' ? result.payload : result } }
        }
      : { type: 'result', requestId: task.requestId, response };
    if (Buffer.byteLength(JSON.stringify(payload)) > this.maxResultBytes) {
      await this._deliverQueuedError(task, Object.assign(new Error('Result exceeds configured limit'), { code: 'RESULT_TOO_LARGE' }));
      return;
    }
    const idempotency = this.idempotencyClaims.get(task.requestId);
    if (idempotency) {
      await this.idempotency.complete(idempotency.claim, idempotency.request, payload);
      if (task.protocolVersion === '1.1') this.pendingResultAcks.set(task.requestId, idempotency.claim);
      this.idempotencyClaims.delete(task.requestId);
    }
    const delivered = connection ? this._send(connection, payload) : false;
    const duplicates = this.pendingDuplicates.get(task.requestId) || [];
    this.pendingDuplicates.delete(task.requestId);
    for (const duplicate of duplicates) {
      const duplicatePayload = { ...payload, requestId: duplicate.requestId, ...(payload.duplicateOf ? {} : { duplicateOf: task.requestId }) };
      duplicate.connection.inFlight = Math.max(0, duplicate.connection.inFlight - 1);
      if (duplicate.protocolVersion === '1.1' && idempotency) this.pendingResultAcks.set(duplicate.requestId, idempotency.claim);
      this._send(duplicate.connection, duplicatePayload);
    }
    const totalMs = Math.max(0, Date.now() - Number(task.receivedAt || task.queuedAt || Date.now()));
    this._emitMetric('request.completed', task, {
      outcome: 'success', status: 'completed', durationMs: totalMs, delivered,
      errorCode: '', httpStatus: Number(contract === 'http' ? result.statusCode || 0 : 0),
      timings: {
        queueMs: Number(task.queueWaitMs || 0), rateLimitMs: Number(task.rateLimitWaitMs || 0),
        upstreamMs: Number(task.upstreamMs || 0), deliveryMs: Math.max(0, Date.now() - deliveryStartedAt), totalMs
      }, contract
    });
  }

  async _deliverQueuedError(task, error) {
    const deliveryStartedAt = Date.now();
    if (task?.message?.type === 'event' && task.protocolVersion === '1.1') {
      const details = errorPayload(error, { code: 'UPSTREAM_ERROR', retryable: false });
      const claim = this.idempotencyClaims.get(task.requestId);
      if (claim) {
        await this.idempotency.completeAsync(claim.claim, claim.request, details);
        this.idempotencyClaims.delete(task.requestId);
      }
      await this.asyncResults.append(task.client, task.operation, {
        requestId: task.requestId,
        operation: task.operation,
        client: task.client,
        error: details,
        completedAt: Date.now()
      });
      await this._deliverAsyncAvailable(task.client, task.operation);
      const origin = this.pendingRequests.get(task.requestId);
      this.pendingRequests.delete(task.requestId);
      if (origin) origin.inFlight = Math.max(0, origin.inFlight - 1);
      const totalMs = Math.max(0, Date.now() - Number(task.receivedAt || task.queuedAt || Date.now()));
      this._emitMetric('request.completed', task, {
        outcome: 'error', status: 'failed', durationMs: totalMs, delivered: true,
        errorCode: details.code, httpStatus: Number(error?.statusCode || 0),
        timings: {
          queueMs: Number(task.queueWaitMs || 0), rateLimitMs: Number(task.rateLimitWaitMs || 0),
          upstreamMs: Number(task.upstreamMs || 0), deliveryMs: Math.max(0, Date.now() - deliveryStartedAt), totalMs
        }
      });
      return;
    }
    const connection = this.pendingRequests.get(task?.requestId);
    this.pendingRequests.delete(task?.requestId);
    const idempotency = this.idempotencyClaims.get(task?.requestId);
    const errorPayloadValue = errorPayload(error, { code: 'QUEUE_UNAVAILABLE', retryable: true });
    const payload = task?.protocolVersion === '1.0'
      ? {
          type: 'result', requestId: task.requestId, operation: task.operation,
          ok: false, error: errorPayloadValue, response: { statusCode: error.statusCode || null },
          _request: { input: { payload: task.message?.payload }, output: { error: errorPayloadValue } }
        }
      : { type: 'failed', requestId: task?.requestId, error: errorPayloadValue };
    if (idempotency) {
      await this.idempotency.complete(idempotency.claim, idempotency.request, payload);
      this.idempotencyClaims.delete(task.requestId);
    }
    if (connection) {
      connection.inFlight = Math.max(0, connection.inFlight - 1);
      this._send(connection, payload);
    }
    const duplicates = this.pendingDuplicates.get(task?.requestId) || [];
    this.pendingDuplicates.delete(task?.requestId);
    for (const duplicate of duplicates) {
      duplicate.connection.inFlight = Math.max(0, duplicate.connection.inFlight - 1);
      this._send(duplicate.connection, { ...payload, requestId: duplicate.requestId, duplicateOf: task.requestId });
    }
    const totalMs = Math.max(0, Date.now() - Number(task?.receivedAt || task?.queuedAt || Date.now()));
    this._emitMetric('request.completed', task, {
      outcome: 'error', status: 'failed', durationMs: totalMs, delivered: Boolean(connection),
      errorCode: error?.code || 'UPSTREAM_ERROR', httpStatus: Number(error?.statusCode || 0),
      timings: {
        queueMs: Number(task?.queueWaitMs || 0), rateLimitMs: Number(task?.rateLimitWaitMs || 0),
        upstreamMs: Number(task?.upstreamMs || 0), deliveryMs: Math.max(0, Date.now() - deliveryStartedAt), totalMs
      }
    });
  }

  async _deliverCachedResult(task, response) {
    const connection = this.pendingRequests.get(task.requestId);
    this.pendingRequests.delete(task.requestId);
    if (connection) connection.inFlight = Math.max(0, connection.inFlight - 1);
    const claim = task.idempotencyKey && this.idempotency
      ? await this.idempotency.claim({
          protocolVersion: task.protocolVersion,
          client: task.client,
          operation: task.operation,
          podId: task.podId,
          idempotencyKey: task.idempotencyKey,
          requestId: task.requestId
        })
      : undefined;
    if (connection) {
      this._send(connection, { ...response, requestId: task.requestId });
      if (task.protocolVersion === '1.1' && claim?.keys) this.pendingResultAcks.set(task.requestId, claim);
    }
  }

  async _subscribeAsyncResults(connection, operation) {
    if (!this.asyncResults || !accountAllows(connection.account, operation)) {
      this._sendError(connection, undefined, Object.assign(new Error('Async result subscription is unavailable'), { code: 'FORBIDDEN_OPERATION' }));
      return;
    }
    const subscriptionKey = `${connection.account.name}\u0000${operation}`;
    const sessionConsumer = `${connection.podId}:${connection.sessionId}`;
    const reconnectKey = `${subscriptionKey}\u0000${sessionConsumer}`;
    const reconnectTimer = this.asyncReconnectTimers.get(reconnectKey);
    if (reconnectTimer) clearTimeout(reconnectTimer);
    this.asyncReconnectTimers.delete(reconnectKey);
    if (!this.resultSubscriptions.has(subscriptionKey)) this.resultSubscriptions.set(subscriptionKey, new Map());
    this.resultSubscriptions.get(subscriptionKey).set(sessionConsumer, connection);
    if (!connection.resultSubscriptions) connection.resultSubscriptions = new Set();
    connection.resultSubscriptions.add(subscriptionKey);
    await this.asyncResults.ensureGroup(connection.account.name, operation);
    await this._deliverAsyncAvailable(connection.account.name, operation);
  }

  async _deliverAsyncAvailable(client, operation) {
    if (!this.asyncResults) return;
    const key = `${client}\u0000${operation}`;
    const subscribers = this.resultSubscriptions.get(key);
    if (!subscribers?.size) return;
    const readKey = `${key}\u0000read`;
    if (this.asyncReads.has(readKey)) return;
    this.asyncReads.add(readKey);
    try {
      for (const [consumerId, connection] of subscribers) {
        if (!connection.account || connection.socket.readyState !== WebSocket.OPEN) continue;
        const pending = await this.asyncResults.readPending(client, operation, consumerId, 100);
        const fresh = pending.length < 100
          ? await this.asyncResults.read(client, operation, 100 - pending.length, consumerId)
          : [];
        for (const item of [...pending, ...fresh]) this._sendAsyncResult(connection, consumerId, client, operation, item);
      }
    } finally {
      this.asyncReads.delete(readKey);
    }
  }

  _sendAsyncResult(connection, consumerId, client, operation, item) {
    const entry = item.entry;
    this.pendingAsyncAcks.set(entry.requestId, { stream: item.stream, streamId: item.streamId, client, operation, consumerId });
    this._send(connection, {
      type: 'async.result',
      protocolVersion: '1.1',
      requestId: entry.requestId,
      operation: entry.operation,
      response: entry.response,
      ...(entry.error ? { error: entry.error } : {})
    });
  }

  _handleAsyncDisconnect(connection) {
    if (!this.asyncResults || !connection.resultSubscriptions) return;
    const consumerId = `${connection.podId}:${connection.sessionId}`;
    for (const subscriptionKey of connection.resultSubscriptions) {
      const subscribers = this.resultSubscriptions.get(subscriptionKey);
      if (subscribers?.get(consumerId) === connection) subscribers.delete(consumerId);
      if (subscribers?.size === 0) this.resultSubscriptions.delete(subscriptionKey);
      for (const [requestId, pending] of this.pendingAsyncAcks) {
        if (pending.consumerId === consumerId) this.pendingAsyncAcks.delete(requestId);
      }
      const reconnectKey = `${subscriptionKey}\u0000${consumerId}`;
      if (this.asyncReconnectTimers.has(reconnectKey)) continue;
      const [client, operation] = subscriptionKey.split('\u0000');
      const timer = setTimeout(() => {
        this.asyncReconnectTimers.delete(reconnectKey);
        this._reassignAsyncResults(client, operation, consumerId).catch((error) =>
          this.logger.warn?.('[redkern:gateway] Async result reassign failed', { error }));
      }, this.sessionReconnectGraceMs);
      timer.unref?.();
      this.asyncReconnectTimers.set(reconnectKey, timer);
    }
  }

  async _reassignAsyncResults(client, operation, previousConsumer) {
    const subscribers = this.resultSubscriptions.get(`${client}\u0000${operation}`);
    const next = subscribers && [...subscribers.entries()].find(([, connection]) => connection.socket.readyState === WebSocket.OPEN);
    if (!next) return;
    const [consumerId, connection] = next;
    const entries = await this.asyncResults.claimConsumer(
      client, operation, previousConsumer, consumerId, this.sessionReconnectGraceMs, 100
    );
    for (const item of entries) this._sendAsyncResult(connection, consumerId, client, operation, item);
  }

  async _resumeResults(connection, requestIds) {
    if (!Array.isArray(requestIds) || requestIds.length === 0 || !this.idempotency) return;
    const operations = this._capabilityItems(connection).map((item) => item.operation);
    const results = await this.idempotency.resume({
      client: connection.account.name,
      podId: connection.podId,
      sessionId: connection.sessionId,
      operations,
      requestIds: requestIds.filter((requestId) => typeof requestId === 'string').slice(0, 1000)
    });
    for (const result of results) {
      const response = { ...result.response, requestId: result.requestId };
      this._send(connection, response);
    }
  }


  async _capacityQuery(connection, message) {
    const now = Date.now();
    connection.capacityQueryAt = connection.capacityQueryAt.filter((timestamp) => now - timestamp < 1000);
    if (connection.capacityQueryAt.length >= 10) {
      this._sendError(connection, message.requestId, Object.assign(new Error('capacity.query rate limit exceeded'), { code: 'CAPACITY_QUERY_RATE_LIMITED', retryable: true }));
      return;
    }
    connection.capacityQueryAt.push(now);
    const items = [];
    const snapshots = new Map();
    for (const operation of message.operations) {
      if (!accountAllows(connection.account, operation)) continue;
      const state = this.queue?.operations.get(operation);
      if (!state?.rateLimits?.length) continue;
      let limits = snapshots.get(state.bucket);
      if (!limits) {
        limits = await this.gcra.capacity(state.bucket, state.rateLimits);
        snapshots.set(state.bucket, limits);
      }
      items.push({ operation, bucket: state.bucket, limits, remaining: Math.min(...limits.map((limit) => limit.remaining)) });
    }
    this._send(connection, { type: 'capacity.result', requestId: message.requestId, items });
  }

  _pushRateDecision({ task, bucket, limits }) {
    const connection = this.pendingRequests.get(task?.requestId);
    if (!connection || connection.family !== '1.1') return;
    const now = Date.now();
    if (now - (connection.ratePushAt.get(bucket) || 0) < 1000) return;
    connection.ratePushAt.set(bucket, now);
    this._send(connection, {
      type: 'rate_limit.decision', bucket, limits,
      remaining: Math.min(...limits.map((limit) => limit.remaining))
    });
  }

  _emitMetric(event, task = {}, extra = {}) {
    const operationKey = String(task.operation || '');
    const separator = operationKey.indexOf('/');
    const metric = {
      event,
      timestamp: new Date().toISOString(),
      requestId: task.requestId || '',
      podId: task.podId || '',
      service: separator < 0 ? operationKey : operationKey.slice(0, separator),
      operation: separator < 0 ? '' : operationKey.slice(separator + 1),
      operationKey,
      ...extra,
      contract: extra.contract === 'http' ? '' : extra.contract || task.contract || ''
    };
    try {
      this.emit('metric', metric);
    } catch (error) {
      this.logger.warn?.('[redkern:gateway] Metric listener failed', { error });
    }
    return metric;
  }

  _markAuthenticated(connection, clearHelloTimer = true) {
    if (clearHelloTimer) clearTimeout(connection.helloTimer);
    const authenticatedCount = [...this.connections].filter((item) => item.account).length;
    if (authenticatedCount > this.maxConnections) {
      connection.socket.close(1013, 'CONNECTION_LIMIT');
      return;
    }
    connection.socket._gatewayAuthenticated = true;
  }

  _closeUnauthenticated(connection) {
    if (!connection.family && connection.socket.readyState === WebSocket.OPEN) {
      connection.socket.close(1008, 'GATEWAY_AUTH_FAILED');
    }
  }

  _authFailed(connection) {
    this._send(connection, { type: 'hello_error', error: { code: 'GATEWAY_AUTH_FAILED', message: 'Invalid gateway token' } });
    connection.socket.close(1008, 'GATEWAY_AUTH_FAILED');
  }

  _sendError(connection, requestId, error, fallbackCode) {
    let payload = errorPayload(error, { code: fallbackCode, retryable: false });
    if (connection.family === '1.0') payload = { ...payload, code: legacyErrorCode(payload.code) };
    this._send(connection, {
      type: 'failed',
      ...(requestId ? { requestId } : {}),
      error: payload
    });
  }

  _send(connection, message) {
    if (connection.socket.readyState !== WebSocket.OPEN) return false;
    connection.socket.send(JSON.stringify(message), (error) => {
      if (error) this.logger.warn?.('[redkern:gateway] WebSocket send failed', { error });
    });
    return true;
  }

  _sendCapabilities(connection) {
    const items = this._capabilityItems(connection);
    this._send(connection, { type: 'capabilities', items });
  }

  _capabilityItems(connection) {
    return [...this.executors.keys()]
      .filter((operation) => accountAllows(connection.account, operation))
      .map((operation) => ({ operation, ...(this.executorMetadata.get(operation) || {}) }));
  }

  _broadcastCapabilities() {
    for (const connection of this.connections) {
      if (connection.account && connection.family) this._sendCapabilities(connection);
    }
  }

  _removeConnection(connection) {
    clearTimeout(connection.helloTimer);
    clearTimeout(connection.pongTimer);
    this.connections.delete(connection);
    this._handleAsyncDisconnect(connection);
    this.emit('disconnection', connection);
  }

  _heartbeat() {
    for (const connection of this.connections) {
      if (connection.socket.readyState !== WebSocket.OPEN || connection.pongTimer) continue;
      connection.pongTimer = setTimeout(() => {
        connection.pongTimer = undefined;
        if (connection.socket.readyState === WebSocket.OPEN) connection.socket.terminate();
      }, 10000);
      connection.pongTimer.unref?.();
      connection.socket.ping((error) => {
        if (error) connection.socket.terminate();
      });
    }
  }

  _rejectUpgrade(socket, status, message) {
    socket.write(`HTTP/1.1 ${status} ${http.STATUS_CODES[status]}\r\nConnection: close\r\nContent-Length: ${Buffer.byteLength(message)}\r\n\r\n${message}`);
    socket.destroy();
  }
}

function legacyErrorCode(code) {
  const aliases = {
    GATEWAY_STANDBY: 'GATEWAY_UNAVAILABLE',
    GATEWAY_DRAINING: 'GATEWAY_UNAVAILABLE',
    FORBIDDEN_OPERATION: 'OPERATION_NOT_FOUND',
    DEADLINE_EXCEEDED_IN_QUEUE: 'DEADLINE_EXCEEDED',
    DELIVERY_LIMIT_EXCEEDED: 'UPSTREAM_ERROR',
    RESULT_TOO_LARGE: 'UPSTREAM_ERROR',
    TOO_MANY_IN_FLIGHT: 'QUEUE_FULL',
    NODE_NOT_READY: 'QUEUE_UNAVAILABLE',
    REDIS_MEMORY_PRESSURE: 'QUEUE_FULL',
    REDIS_OUT_OF_MEMORY: 'QUEUE_UNAVAILABLE'
  };
  return aliases[code] || code;
}

function positiveInteger(value, fallback) {
  return Number.isSafeInteger(value) && value > 0 ? value : fallback;
}

function parseLegacyDeadline(value, fallback) {
  const deadlineAt = typeof value === 'number' ? value : Date.parse(value);
  return Number.isFinite(deadlineAt) ? Math.min(deadlineAt, fallback) : fallback;
}

module.exports = { GatewayRuntime, PRE_AUTH_MAX_PAYLOAD };
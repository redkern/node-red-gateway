'use strict';

const { randomUUID } = require('node:crypto');
const EventEmitter = require('node:events');
const WebSocket = require('ws');

class GatewayClientError extends Error {
  constructor(code, message, retryable = false) {
    super(message);
    this.name = 'GatewayClientError';
    this.code = code;
    this.retryable = retryable;
  }
}

class GatewayClient extends EventEmitter {
  constructor(options) {
    super();
    this.url = options.url;
    this.token = options.token;
    this.podId = options.podId;
    this.sessionId = options.sessionId || randomUUID();
    this.protocolVersion = '1.1';
    this.connectTimeoutMs = Number(options.connectTimeoutMs || 10000);
    this.deliveryMarginMs = Number(options.deliveryMarginMs || 2000);
    this.WebSocket = options.WebSocket || WebSocket;
    this.logger = options.logger || console;
    this.socket = undefined;
    this.pending = new Map();
    this.reconnectTimer = undefined;
    this.started = false;
    this.closed = false;
    this.ready = false;
    this.capabilities = [];
    this.resultSubscriptions = new Set();
  }

  async start() {
    this.started = true;
    this.closed = false;
    try {
      await this._connect();
    } catch (error) {
      if (error.code !== 'UNSUPPORTED_PROTOCOL') throw error;
      this.protocolVersion = '1.0';
      await this._connect();
    }
  }

  async close() {
    this.closed = true;
    this.started = false;
    this.ready = false;
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = undefined;
    this._failPending(new GatewayClientError('GATEWAY_CONNECTION_LOST', 'Gateway client closed', true));
    if (this.socket && this.socket.readyState < this.WebSocket.CLOSING) {
      await new Promise((resolve) => {
        const timer = setTimeout(resolve, 1000);
        this.socket.once('close', () => {
          clearTimeout(timer);
          resolve();
        });
        this.socket.close(1000, 'Client Shutdown');
      });
    }
  }

  async call(operation, message, options = {}) {
    if (!this.ready || !this.socket || this.socket.readyState !== this.WebSocket.OPEN) {
      throw new GatewayClientError('GATEWAY_NOT_CONNECTED', 'Gateway is not connected', true);
    }
    const requestId = options.requestId || randomUUID();
    const deadlineMs = Math.max(1, Number(options.deadlineMs || 30000));
    const timeoutMs = deadlineMs + this.deliveryMarginMs;
    const envelope = {
      type: options.messageType === 'event' ? 'event' : 'call',
      protocolVersion: this.protocolVersion,
      requestId,
      operation,
      payload: this.capabilities.find((item) => item.operation === operation)?.contract === 'passthrough'
        ? message
        : message.payload,
      _request: message._request || {},
      priority: options.priority || 'normal',
      ...(this.protocolVersion === '1.0' ? { deadlineAt: new Date(Date.now() + deadlineMs).toISOString() } : {}),
      ...(options.idempotencyKey ? { idempotencyKey: options.idempotencyKey } : {}),
      deadlineMs,
      ...(options.queueTimeoutMs ? { queueTimeoutMs: options.queueTimeoutMs } : {}),
      ...(options.execTimeoutMs ? { execTimeoutMs: options.execTimeoutMs } : {})
    };
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(requestId);
        reject(new GatewayClientError('GATEWAY_RESULT_TIMEOUT', 'Gateway result timed out', false));
      }, timeoutMs);
      this.pending.set(requestId, { resolve, reject, timer, message, envelope, messageType: envelope.type, operation, accepted: false });
      this.socket.send(JSON.stringify(envelope), (error) => {
        if (!error) return;
        clearTimeout(timer);
        this.pending.delete(requestId);
        reject(new GatewayClientError('GATEWAY_CONNECTION_LOST', 'Failed to send request to Gateway', true));
      });
    });
  }

  subscribeResults(operation) {
    this.resultSubscriptions.add(operation);
    if (this.ready && this.socket?.readyState === this.WebSocket.OPEN && this.protocolVersion === '1.1') {
      this.socket.send(JSON.stringify({ type: 'result.subscribe', protocolVersion: '1.1', operation }));
    }
  }

  async capacity(operations) {
    if (!this.ready || !this.socket || this.socket.readyState !== this.WebSocket.OPEN || this.protocolVersion !== '1.1') {
      throw new GatewayClientError('GATEWAY_NOT_CONNECTED', 'Capacity queries require a connected protocol 1.1 client', true);
    }
    if (!Array.isArray(operations) || operations.length === 0 || operations.length > 100) {
      throw new TypeError('capacity requires 1-100 operations');
    }
    const requestId = randomUUID();
    const envelope = { type: 'capacity.query', protocolVersion: '1.1', requestId, operations };
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(requestId);
        reject(new GatewayClientError('GATEWAY_RESULT_TIMEOUT', 'Capacity query timed out', true));
      }, this.deliveryMarginMs + 3000);
      this.pending.set(requestId, { resolve, reject, timer, kind: 'capacity', envelope });
      this.socket.send(JSON.stringify(envelope), (error) => {
        if (!error) return;
        clearTimeout(timer);
        this.pending.delete(requestId);
        reject(new GatewayClientError('GATEWAY_CONNECTION_LOST', 'Failed to send capacity query', true));
      });
    });
  }

  _connect() {
    if (this.closed) return Promise.reject(new GatewayClientError('GATEWAY_CONNECTION_LOST', 'Gateway client is closed'));
    return new Promise((resolve, reject) => {
      let settled = false;
      const socket = new this.WebSocket(this.url, {
        headers: this.protocolVersion === '1.1' ? { Authorization: `Bearer ${this.token}` } : {},
        maxPayload: 1024 * 1024,
        perMessageDeflate: false
      });
      this.socket = socket;
      const timer = setTimeout(() => {
        if (settled) return;
        settled = true;
        socket.terminate();
        reject(new GatewayClientError('GATEWAY_CONNECT_TIMEOUT', 'Gateway connection timed out', true));
      }, this.connectTimeoutMs);
      timer.unref?.();

      socket.on('open', () => {
        socket.send(JSON.stringify({
          type: 'hello',
          protocolVersion: this.protocolVersion,
          podId: this.podId,
          ...(this.protocolVersion === '1.1' ? { sessionId: this.sessionId } : { instanceId: this.podId, token: this.token })
        }));
      });
      socket.on('message', (data) => this._receive(data, socket, () => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        resolve();
      }, (error) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        reject(error);
      }));
      socket.on('error', (error) => {
        this.logger.warn?.('[redkern:gateway] Gateway client socket error', { error });
        if (!settled) {
          settled = true;
          clearTimeout(timer);
          reject(new GatewayClientError('GATEWAY_CONNECT_TIMEOUT', 'Gateway connection failed', true));
        }
      });
      socket.on('close', (code, reason) => {
        const wasReady = this.ready;
        clearTimeout(timer);
        this.ready = false;
        if (!settled) {
          settled = true;
          reject(new GatewayClientError('GATEWAY_CONNECT_TIMEOUT', reason.toString() || 'Gateway connection closed', true));
        }
        if (this.protocolVersion === '1.0' || code === 1008) {
          this._failPending(new GatewayClientError('GATEWAY_CONNECTION_LOST', 'Gateway connection lost', true));
        }
        this.emit('status', 'disconnected');
        if (wasReady && code !== 1008 && this.started && !this.closed) this._scheduleReconnect();
      });
    });
  }

  _receive(data, socket, onReady, onFailure) {
    let message;
    try {
      message = JSON.parse(data.toString());
    } catch {
      socket.close(1007, 'INVALID_MESSAGE');
      return;
    }
    if (message.type === 'hello_ack') {
      this.ready = true;
      this.emit('status', 'connected');
      if (Array.isArray(message.capabilities)) {
        this.capabilities = message.capabilities;
        this.emit('capabilities', this.capabilities);
      }
      if (this.protocolVersion === '1.1' && this.pending.size > 0) {
        const acceptedIds = [...this.pending.values()]
          .filter((pending) => pending.accepted)
          .map((pending) => pending.envelope.requestId);
        if (acceptedIds.length > 0) {
          socket.send(JSON.stringify({ type: 'result.resume', protocolVersion: '1.1', requestIds: acceptedIds }));
        }
        for (const pending of this.pending.values()) {
          if (!pending.accepted) socket.send(JSON.stringify(pending.envelope));
        }
      }
      if (this.protocolVersion === '1.1') {
        for (const operation of this.resultSubscriptions) {
          socket.send(JSON.stringify({ type: 'result.subscribe', protocolVersion: '1.1', operation }));
        }
      }
      onReady();
      return;
    }
    if (message.type === 'hello_error') {
      const error = message.error || {};
      const failure = new GatewayClientError(error.code || 'GATEWAY_AUTH_FAILED', error.message || 'Gateway handshake failed');
      this.emit('errorMessage', failure);
      onFailure(failure);
      socket.close(1008, error.code || 'GATEWAY_AUTH_FAILED');
      return;
    }
    if (message.type === 'capabilities') {
      this.capabilities = Array.isArray(message.items) ? message.items : [];
      this.emit('capabilities', this.capabilities);
      return;
    }
    if (message.type === 'heartbeat') return;
    if (message.type === 'async.result') {
      this.emit('asyncResult', { operation: message.operation, requestId: message.requestId, response: message.response, error: message.error });
      if (this.protocolVersion === '1.1') {
        socket.send(JSON.stringify({ type: 'result.ack', protocolVersion: '1.1', requestId: message.requestId }));
      }
      return;
    }
    if (message.type === 'rate_limit.decision') {
      this.emit('rateLimitDecision', message);
      return;
    }
    if (!message.requestId) return;
    const pending = this.pending.get(message.requestId);
    if (!pending) return;
    if (message.type === 'accepted') {
      pending.accepted = true;
      if (pending.messageType === 'event' && this.protocolVersion === '1.1') {
        this.pending.delete(message.requestId);
        clearTimeout(pending.timer);
        pending.resolve({ accepted: true, requestId: message.requestId });
      }
      return;
    }
    this.pending.delete(message.requestId);
    clearTimeout(pending.timer);
    if (pending.kind === 'capacity' && message.type === 'capacity.result') {
      pending.resolve(message.items || []);
    } else if (message.type === 'result') {
      if (message.ok === false) {
        const details = message.error || {};
        pending.reject(new GatewayClientError(details.code || 'GATEWAY_ERROR', details.message || 'Gateway request failed', Boolean(details.retryable)));
      } else {
        const response = Object.hasOwn(message, 'payload') ? message.payload : message.response;
        pending.resolve(response);
        if (pending.messageType === 'event') {
          this.emit('asyncResult', { operation: pending.operation, requestId: message.requestId, response });
        }
      }
      if (this.protocolVersion === '1.1') {
        socket.send(JSON.stringify({ type: 'result.ack', protocolVersion: '1.1', requestId: message.requestId }));
      }
    } else if (message.type === 'failed') {
      const details = message.error || {};
      pending.reject(new GatewayClientError(details.code || 'GATEWAY_ERROR', details.message || 'Gateway request failed', Boolean(details.retryable)));
    }
  }

  _scheduleReconnect() {
    if (this.reconnectTimer || this.closed) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      this._connect().catch((error) => {
        this.logger.warn?.('[redkern:gateway] Gateway reconnect failed', { error });
        this._scheduleReconnect();
      });
    }, 1000);
    this.reconnectTimer.unref?.();
  }

  _failPending(error) {
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pending.clear();
  }
}

module.exports = { GatewayClient, GatewayClientError };
'use strict';

const { randomUUID } = require('node:crypto');

const SUPPORTED_PROTOCOLS = new Set(['1.0', '1.1']);
const MESSAGE_TYPES = new Set([
  'hello', 'heartbeat', 'call', 'event', 'accepted', 'result', 'failed',
  'capabilities', 'hello_ack', 'hello_error', 'result.ack', 'result.resume',
  'result.subscribe', 'async.result', 'capacity.query', 'capacity.result', 'rate_limit.decision'
]);

function parseOperation(operation) {
  if (typeof operation !== 'string') throw new TypeError('operation must be a string');
  const value = operation.trim();
  const parts = value.split('/');
  if (parts.length < 2 || parts.some((part) => !/^[A-Za-z0-9._:-]+$/.test(part))) {
    throw new TypeError('operation must use domain/action format');
  }
  return value;
}

function validateEnvelope(message) {
  if (!message || typeof message !== 'object' || Array.isArray(message)) {
    throw new TypeError('message must be an object');
  }
  if (!MESSAGE_TYPES.has(message.type)) throw new TypeError('unsupported message type');
  if (message.protocolVersion !== undefined && !SUPPORTED_PROTOCOLS.has(message.protocolVersion)) {
    throw Object.assign(new TypeError('unsupported protocol version'), { code: 'UNSUPPORTED_PROTOCOL' });
  }
  if (message.type === 'hello' && (typeof message.podId !== 'string' || message.podId.trim() === '')) {
    throw Object.assign(new TypeError('podId is required'), { code: 'POD_ID_MISSING' });
  }
  if (message.type === 'result.subscribe') parseOperation(message.operation);
  if (message.type === 'capacity.query' && (!Array.isArray(message.operations) || message.operations.length === 0 || message.operations.length > 100)) {
    throw Object.assign(new TypeError('capacity.query requires 1-100 operations'), { code: 'INVALID_MESSAGE' });
  }
  if (message.type === 'call' || message.type === 'event') {
    if (typeof message.requestId !== 'string' || message.requestId.length === 0 || message.requestId.length > 128) {
      throw new TypeError('requestId is required');
    }
    parseOperation(message.operation);
  }
  return message;
}

function makeRequestId() {
  return randomUUID();
}

function errorPayload(error, defaults = {}) {
  return {
    code: error && error.code ? error.code : (defaults.code || 'GATEWAY_ERROR'),
    message: error && error.message ? error.message : (defaults.message || 'Gateway request failed'),
    retryable: Boolean(error && error.retryable !== undefined ? error.retryable : defaults.retryable)
  };
}

module.exports = { SUPPORTED_PROTOCOLS, errorPayload, makeRequestId, parseOperation, validateEnvelope };
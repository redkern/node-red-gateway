'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fixtures = require('../fixtures/legacy/messages.json');
const { parseOperation, validateEnvelope, errorPayload } = require('../../lib/protocol.js');

test('operations accept nested paths but reject empty or malformed segments', () => {
  assert.equal(parseOperation('billing/calculateInvoice'), 'billing/calculateInvoice');
  assert.equal(parseOperation('billing/invoice/retry'), 'billing/invoice/retry');
  for (const value of ['', 'billing', '/action', 'billing/', 'billing//retry', 'billing/a?b']) {
    assert.throws(() => parseOperation(value));
  }
});

test('legacy messages default to protocol 1.0 and new messages accept 1.1', () => {
  const legacy = { type: 'call', requestId: 'r-1', operation: 'billing/calculate', payload: {} };
  assert.equal(validateEnvelope(legacy), legacy);
  assert.equal(validateEnvelope({ ...legacy, protocolVersion: '1.1' }).protocolVersion, '1.1');
  assert.throws(() => validateEnvelope({ ...legacy, protocolVersion: '2.0' }), { code: 'UNSUPPORTED_PROTOCOL' });
});

test('request envelopes require bounded request IDs and a podId on hello (GW-ERR-1)', () => {
  assert.throws(() => validateEnvelope({ type: 'call', requestId: '', operation: 'billing/a' }));
  assert.throws(() => validateEnvelope({ type: 'call', requestId: 'a'.repeat(129), operation: 'billing/a' }));
  assert.throws(() => validateEnvelope({ type: 'hello' }), { code: 'POD_ID_MISSING' });
  assert.throws(() => validateEnvelope(null));
});

test('error payloads expose stable public fields only', () => {
  assert.deepEqual(errorPayload(Object.assign(new Error('failed'), { code: 'UPSTREAM_ERROR', retryable: true })), {
    code: 'UPSTREAM_ERROR', message: 'failed', retryable: true
  });
});

test('frozen 2.1.x hello and call fixtures remain valid protocol 1.0 messages', () => {
  assert.equal(validateEnvelope(fixtures.hello).protocolVersion, '1.0');
  assert.equal(validateEnvelope(fixtures.call).type, 'call');
  assert.equal(fixtures.result.ok, true);
  assert.ok(fixtures.result._request.input);
});

test('protocol 1.1 async subscriptions name a valid operation', () => {
  assert.equal(validateEnvelope({ type: 'result.subscribe', protocolVersion: '1.1', operation: 'billing/a' }).type, 'result.subscribe');
  assert.throws(() => validateEnvelope({ type: 'result.subscribe', protocolVersion: '1.1', operation: 'invalid' }));
});
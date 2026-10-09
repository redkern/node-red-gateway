'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createHash } = require('node:crypto');
const { makeIdempotencyKeys, CLAIM_LUA, COMPLETE_LUA, RELEASE_LUA } = require('../../lib/server/idempotency.js');

function legacyHash(value) { return createHash('sha256').update(String(value)).digest('hex'); }

test('protocol 1.0 idempotency key is byte-for-byte compatible with legacy 2.1.x scope', () => {
  const keys = makeIdempotencyKeys({
    keyPrefix: 'pod-gateway', protocolVersion: '1.0', podId: 'pod-a',
    operation: 'billing/calculate', idempotencyKey: 'idem-1', requestId: 'req-1'
  });
  assert.equal(keys.marker, `pod-gateway:idem:${legacyHash('pod-a:idem-1')}`);
  assert.equal(keys.legacy, true);
  assert.equal(keys.result, undefined);
});

test('protocol 1.1 idempotency scope includes client and operation with operation hash tag', () => {
  const first = makeIdempotencyKeys({
    keyPrefix: 'pod-gateway', protocolVersion: '1.1', client: 'client-a',
    operation: 'billing/calculate', idempotencyKey: 'same', requestId: 'req-1'
  });
  const otherClient = makeIdempotencyKeys({
    keyPrefix: 'pod-gateway', protocolVersion: '1.1', client: 'client-b',
    operation: 'billing/calculate', idempotencyKey: 'same', requestId: 'req-2'
  });
  const otherOperation = makeIdempotencyKeys({
    keyPrefix: 'pod-gateway', protocolVersion: '1.1', client: 'client-a',
    operation: 'billing/other', idempotencyKey: 'same', requestId: 'req-3'
  });
  assert.notEqual(first.marker, otherClient.marker);
  assert.notEqual(first.marker, otherOperation.marker);
  assert.equal(first.marker.includes('{billing/calculate}'), true);
  assert.equal(first.result, 'pod-gateway:result:{billing/calculate}:req-1');
  assert.equal(first.result.includes('{billing/calculate}'), true);
});

test('idempotency scripts use owner CAS and preserve full legacy response compatibility', () => {
  assert.match(CLAIM_LUA, /'NX'/);
  assert.match(COMPLETE_LUA, /marker\.requestId ~= ARGV\[1\]/);
  assert.match(COMPLETE_LUA, /ARGV\[5\] == 'legacy'/);
  assert.match(RELEASE_LUA, /marker\.consumerId ~= ARGV\[2\]/);
});
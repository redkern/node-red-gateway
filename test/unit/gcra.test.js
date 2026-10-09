'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createGcraLimiter, defineGcraScripts, makeBucketKeys, MAX_LIMITS_PER_BUCKET, LUA } = require('../../lib/server/gcra.js');

class FakeRedis {
  constructor() { this.commands = new Map(); }
  defineCommand(name, definition) { this.commands.set(name, definition); }
}

test('defines fixed-arity EVALSHA commands for multi-limit buckets', () => {
  const client = new FakeRedis();
  defineGcraScripts(client);
  assert.equal(client.commands.size, MAX_LIMITS_PER_BUCKET * 2);
  assert.equal(client.commands.get('gcra1').numberOfKeys, 1);
  assert.equal(client.commands.get('gcra8').numberOfKeys, 8);
  assert.equal(client.commands.get('gcraCapacity1').numberOfKeys, 1);
  assert.equal(client.commands.get('gcraCapacity8').numberOfKeys, 8);
  assert.equal(client.commands.get('gcra2').lua, client.commands.get('gcra1').lua);
  assert.match(LUA, /redis\.call\('TIME'\)/);
  assert.match(LUA, /redis\.call\('PERSIST'/);
});

test('bucket keys preserve the legacy GCRA key and share one Redis Cluster hash tag', () => {
  assert.deepEqual(makeBucketKeys('pod-gateway', 'user/getInfo', [
    { name: 'g', rate: 10, burst: 2 },
    { name: 'minute', rate: 100, burst: 10 }
  ]), [
    'pod-gateway:{user/getInfo}:g',
    'pod-gateway:{user/getInfo}:minute'
  ]);
});

test('validates bucket limits and queue budgets before issuing Redis commands', async () => {
  const client = new FakeRedis();
  const limiter = createGcraLimiter(client, 'pod-gateway');
  assert.throws(() => limiter.keys('bucket', []), /limits/);
  assert.throws(() => limiter.keys('bucket', Array.from({ length: 9 }, (_, index) => ({ name: `l${index}`, rate: 1, burst: 1 }))), /1-8/);
  assert.throws(() => limiter.keys('bucket', [{ name: 'bad:name', rate: 1, burst: 1 }]), /safe name/);
  assert.throws(() => limiter.keys('bucket', [{ name: 'g', rate: 0, burst: 1 }]), /positive rate/);
  await assert.rejects(limiter.reserve('bucket', [{ name: 'g', rate: 1, burst: 1 }], -1), /non-negative/);
});
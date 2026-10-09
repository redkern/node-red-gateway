'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { LeaseCoordinator, leaseKeys, ACQUIRE_LUA, RENEW_LUA, RELEASE_LUA } = require('../../lib/server/lease.js');

class FakeRedis {
  constructor() { this.commands = new Map(); }
  defineCommand(name, definition) { this.commands.set(name, definition); }
}

test('lease and clean marker use one Redis Cluster hash slot and no TTL contract', () => {
  const keys = leaseKeys('pod-gateway');
  assert.equal(keys.lease, 'pod-gateway:{lease}:lease');
  assert.equal(keys.clean, 'pod-gateway:{lease}:lease:clean');
  assert.match(ACQUIRE_LUA, /redis\.call\('TIME'\)/);
  assert.match(ACQUIRE_LUA, /redis\.call\('DEL', KEYS\[2\]\)/);
  assert.match(RENEW_LUA, /lease\.consumerId ~= ARGV\[1\]/);
  assert.match(RELEASE_LUA, /redis\.call\('SET', KEYS\[2\], ARGV\[1\]\)/);
});

test('lease coordinator defines acquire/renew/release scripts and validates monotonic safety margin', () => {
  const redis = new FakeRedis();
  const lease = new LeaseCoordinator({ redis, keyPrefix: 'pod-gateway', consumerId: 'instance:boot:g1' });
  assert.equal(redis.commands.get('gatewayLeaseAcquire').numberOfKeys, 2);
  assert.equal(redis.commands.get('gatewayLeaseRenew').numberOfKeys, 1);
  assert.equal(redis.commands.get('gatewayLeaseRelease').numberOfKeys, 2);
  assert.equal(lease.isFresh(), false);
  assert.throws(() => new LeaseCoordinator({ redis, keyPrefix: 'gw', consumerId: 'c', leaseTtlMs: 100, safetyMarginMs: 100 }), /safety margin/);
});

test('delayed renewal response does not extend freshness from response arrival time', async () => {
  const redis = new FakeRedis();
  const lease = new LeaseCoordinator({
    redis, keyPrefix: 'pod-gateway', consumerId: 'instance:boot:g1',
    leaseTtlMs: 60, safetyMarginMs: 20
  });
  redis.gatewayLeaseAcquire = async () => [1, JSON.stringify({ consumerId: lease.consumerId, expiresAt: Date.now() + 60, drainTimeoutMs: 20 })];
  redis.gatewayLeaseRenew = async () => new Promise((resolve) => setTimeout(() => {
    resolve([1, JSON.stringify({ consumerId: lease.consumerId, expiresAt: Date.now() + 60, drainTimeoutMs: 20 })]);
  }, 45));
  await lease.acquire();
  assert.equal(lease.isFresh(), true);
  await lease.renew();
  assert.equal(lease.isFresh(), false);
});
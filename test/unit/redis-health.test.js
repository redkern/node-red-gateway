'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { memoryRatio, parseRedisInfo, validateEvictionPolicy } = require('../../lib/server/redis-health.js');

test('parses Redis INFO fields and calculates used memory ratio', () => {
  const info = parseRedisInfo('# Memory\r\nused_memory:800\r\nmaxmemory:1000\r\nmaxmemory_policy:noeviction\r\n');
  assert.equal(info.maxmemory_policy, 'noeviction');
  assert.equal(memoryRatio(info), 0.8);
});

test('rejects allkeys and unapproved volatile Redis eviction policies', () => {
  assert.equal(validateEvictionPolicy('noeviction', false), 'noeviction');
  assert.equal(validateEvictionPolicy('volatile-lru', true), 'volatile-lru');
  assert.throws(() => validateEvictionPolicy('volatile-lru', false), { code: 'REDIS_EVICTION_NOT_ALLOWED' });
  assert.throws(() => validateEvictionPolicy('allkeys-lru', true), { code: 'REDIS_POLICY_UNSUPPORTED' });
});
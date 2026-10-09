'use strict';

function parseRedisInfo(info) {
  if (typeof info !== 'string') throw new TypeError('Redis INFO response must be a string');
  const values = {};
  for (const line of info.split(/\r?\n/)) {
    if (!line || line.startsWith('#')) continue;
    const separator = line.indexOf(':');
    if (separator < 1) continue;
    values[line.slice(0, separator)] = line.slice(separator + 1);
  }
  return values;
}

function validateEvictionPolicy(policy, allowEvictingRedis) {
  if (typeof policy !== 'string' || policy.length === 0) throw Object.assign(new Error('Redis maxmemory_policy is unavailable'), { code: 'REDIS_POLICY_UNAVAILABLE' });
  if (policy.startsWith('allkeys-')) throw Object.assign(new Error('Redis allkeys eviction is not supported by Gateway'), { code: 'REDIS_POLICY_UNSUPPORTED' });
  if (policy.startsWith('volatile-') && !allowEvictingRedis) {
    throw Object.assign(new Error('volatile Redis eviction requires allowEvictingRedis'), { code: 'REDIS_EVICTION_NOT_ALLOWED' });
  }
  if (!policy.startsWith('volatile-') && policy !== 'noeviction') {
    throw Object.assign(new Error(`Redis eviction policy ${policy} is unsupported`), { code: 'REDIS_POLICY_UNSUPPORTED' });
  }
  return policy;
}

function memoryRatio(info) {
  const values = typeof info === 'string' ? parseRedisInfo(info) : info;
  const used = Number(values.used_memory);
  const maximum = Number(values.maxmemory);
  if (!Number.isFinite(used) || !Number.isFinite(maximum) || maximum <= 0) return undefined;
  return used / maximum;
}

module.exports = { memoryRatio, parseRedisInfo, validateEvictionPolicy };
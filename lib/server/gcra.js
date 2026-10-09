'use strict';

const { defineScripts, hashTag } = require('@redkern/node-red-kit/redis');

const MAX_LIMITS_PER_BUCKET = 8;
const LUA = `
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + tonumber(time[2]) / 1000
local queueBudget = tonumber(ARGV[1])
local allowAt = now
local emissions = {}
local tats = {}

for index = 1, #KEYS do
  local rate = tonumber(ARGV[index * 2])
  local burst = tonumber(ARGV[index * 2 + 1])
  if not rate or rate <= 0 or not burst or burst < 1 then
    return redis.error_reply('INVALID_GCRA_CONFIG')
  end
  local emission = 1000 / rate
  local tat = tonumber(redis.call('HGET', KEYS[index], 'tat')) or now
  local eligibleAt = tat - emission * (burst - 1)
  if eligibleAt > allowAt then allowAt = eligibleAt end
  emissions[index] = emission
  tats[index] = tat
end

local waitMs = math.max(0, allowAt - now)
if waitMs > queueBudget then
  local result = {0, waitMs, now, allowAt}
  for index = 1, #KEYS do
    local remaining = math.floor((emissions[index] * tonumber(ARGV[index * 2 + 1]) - math.max(0, tats[index] - now)) / emissions[index])
    remaining = math.max(0, math.min(tonumber(ARGV[index * 2 + 1]), remaining))
    table.insert(result, remaining)
    table.insert(result, math.max(tats[index], now))
  end
  return result
end

for index = 1, #KEYS do
  local nextTat = math.max(tats[index], allowAt) + emissions[index]
  redis.call('HSET', KEYS[index], 'tat', string.format('%.3f', nextTat))
  redis.call('PERSIST', KEYS[index])
  tats[index] = nextTat
end

local result = {1, waitMs, now, allowAt}
for index = 1, #KEYS do
  local remaining = math.floor((emissions[index] * tonumber(ARGV[index * 2 + 1]) - math.max(0, tats[index] - now)) / emissions[index])
  remaining = math.max(0, math.min(tonumber(ARGV[index * 2 + 1]), remaining))
  table.insert(result, remaining)
  table.insert(result, math.max(tats[index], now))
end
return result
`;
const CAPACITY_LUA = `
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + tonumber(time[2]) / 1000
local result = { now }
for index = 1, #KEYS do
  local rate = tonumber(ARGV[index * 2 - 1])
  local burst = tonumber(ARGV[index * 2])
  if not rate or rate <= 0 or not burst or burst < 1 then return redis.error_reply('INVALID_GCRA_CONFIG') end
  local emission = 1000 / rate
  local tat = tonumber(redis.call('HGET', KEYS[index], 'tat')) or now
  local remaining = math.floor((emission * burst - math.max(0, tat - now)) / emission)
  remaining = math.max(0, math.min(burst, remaining))
  table.insert(result, remaining)
  table.insert(result, math.max(tat, now))
end
return result
`;

function defineGcraScripts(client) {
  const scripts = {};
  for (let numberOfKeys = 1; numberOfKeys <= MAX_LIMITS_PER_BUCKET; numberOfKeys += 1) {
    scripts[`gcra${numberOfKeys}`] = { lua: LUA, numberOfKeys };
    scripts[`gcraCapacity${numberOfKeys}`] = { lua: CAPACITY_LUA, numberOfKeys };
  }
  defineScripts(client, scripts);
}

function makeBucketKeys(keyPrefix, bucket, limits) {
  if (typeof keyPrefix !== 'string' || keyPrefix.length === 0 || /[{}]/.test(keyPrefix)) {
    throw new TypeError('keyPrefix must be a non-empty string without braces');
  }
  if (typeof bucket !== 'string' || bucket.length === 0 || /[{}]/.test(bucket)) {
    throw new TypeError('bucket must be a non-empty string without braces');
  }
  validateLimits(limits);
  return limits.map((limit) => `${hashTag(keyPrefix, bucket)}:${limit.name}`);
}

function validateLimits(limits) {
  if (!Array.isArray(limits) || limits.length < 1 || limits.length > MAX_LIMITS_PER_BUCKET) {
    throw new TypeError(`limits must contain 1-${MAX_LIMITS_PER_BUCKET} entries`);
  }
  const names = new Set();
  for (const limit of limits) {
    if (!limit || typeof limit.name !== 'string' || !/^[A-Za-z0-9_-]+$/.test(limit.name)) {
      throw new TypeError('each GCRA limit requires a safe name');
    }
    if (names.has(limit.name)) throw new TypeError(`duplicate GCRA limit name: ${limit.name}`);
    names.add(limit.name);
    if (!Number.isFinite(limit.rate) || limit.rate <= 0 || !Number.isFinite(limit.burst) || limit.burst < 1) {
      throw new TypeError(`GCRA limit ${limit.name} requires positive rate and burst`);
    }
  }
}

function createGcraLimiter(client, keyPrefix) {
  defineGcraScripts(client);
  return {
    async reserve(bucket, limits, queueBudgetMs) {
      validateLimits(limits);
      if (!Number.isFinite(queueBudgetMs) || queueBudgetMs < 0) throw new TypeError('queueBudgetMs must be non-negative');
      const keys = makeBucketKeys(keyPrefix, bucket, limits);
      const args = [queueBudgetMs];
      for (const limit of limits) args.push(limit.rate, limit.burst);
      const result = await client[`gcra${limits.length}`](...keys, ...args);
      if (!Array.isArray(result) || result.length < 4) throw new Error('GCRA returned an invalid response');
      return {
        allowed: Number(result[0]) === 1,
        waitMs: Number(result[1]),
        now: Number(result[2]),
        allowAt: Number(result[3]),
        limits: limits.map((limit, index) => ({
          name: limit.name,
          limit: limit.rate,
          remaining: Number(result[4 + index * 2]),
          resetAt: Number(result[5 + index * 2])
        }))
      };
    },
    async capacity(bucket, limits) {
      validateLimits(limits);
      const keys = makeBucketKeys(keyPrefix, bucket, limits);
      const args = [];
      for (const limit of limits) args.push(limit.rate, limit.burst);
      const values = await client[`gcraCapacity${limits.length}`](...keys, ...args);
      const now = Number(values[0]);
      return limits.map((limit, index) => ({
        name: limit.name,
        limit: limit.rate,
        remaining: Number(values[index * 2 + 1]),
        resetAt: Number(values[index * 2 + 2]),
        now
      }));
    },
    keys(bucket, limits) {
      return makeBucketKeys(keyPrefix, bucket, limits);
    }
  };
}

module.exports = { createGcraLimiter, defineGcraScripts, makeBucketKeys, MAX_LIMITS_PER_BUCKET, LUA, CAPACITY_LUA };
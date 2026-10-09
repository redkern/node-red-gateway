'use strict';

const EventEmitter = require('node:events');
const { performance } = require('node:perf_hooks');
const { defineScripts, hashTag } = require('@redkern/node-red-kit/redis');
const clientsWithScripts = new WeakSet();

const ACQUIRE_LUA = `
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + tonumber(time[2]) / 1000
local current = redis.call('GET', KEYS[1])
local previous = current or ''
if current then
  local ok, lease = pcall(cjson.decode, current)
  if ok and tonumber(lease.expiresAt) > now and lease.consumerId ~= ARGV[1] then
    return {0, current, '', current}
  end
end
local cleanConsumer = redis.call('GET', KEYS[2]) or ''
local lease = cjson.encode({ consumerId = ARGV[1], expiresAt = now + tonumber(ARGV[2]), drainTimeoutMs = tonumber(ARGV[3]) })
redis.call('SET', KEYS[1], lease)
redis.call('DEL', KEYS[2])
return {1, lease, cleanConsumer, previous}
`;
const RENEW_LUA = `
local current = redis.call('GET', KEYS[1])
if not current then return {0, ''} end
local ok, lease = pcall(cjson.decode, current)
if not ok or lease.consumerId ~= ARGV[1] then return {0, current} end
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + tonumber(time[2]) / 1000
lease.expiresAt = now + tonumber(ARGV[2])
lease.drainTimeoutMs = tonumber(ARGV[3])
local encoded = cjson.encode(lease)
redis.call('SET', KEYS[1], encoded)
return {1, encoded}
`;
const RELEASE_LUA = `
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
local ok, lease = pcall(cjson.decode, current)
if not ok or lease.consumerId ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
if ARGV[2] == '1' then redis.call('SET', KEYS[2], ARGV[1]) else redis.call('DEL', KEYS[2]) end
return 1
`;

function leaseKeys(keyPrefix) {
  const tagged = hashTag(keyPrefix, 'lease');
  return { lease: `${tagged}:lease`, clean: `${tagged}:lease:clean` };
}

class LeaseCoordinator extends EventEmitter {
  constructor({ redis, keyPrefix, consumerId, leaseTtlMs = 15000, safetyMarginMs = 5000, drainTimeoutMs = 12000, renewEveryMs = 5000 }) {
    super();
    if (!redis || typeof redis.defineCommand !== 'function') throw new TypeError('Redis scripts client is required');
    if (typeof consumerId !== 'string' || consumerId.length === 0) throw new TypeError('consumerId is required');
    if (!Number.isSafeInteger(leaseTtlMs) || leaseTtlMs < 1 || !Number.isSafeInteger(safetyMarginMs) || safetyMarginMs < 0 || safetyMarginMs >= leaseTtlMs) {
      throw new TypeError('lease TTL and freshness safety margin are invalid');
    }
    this.redis = redis;
    this.consumerId = consumerId;
    this.leaseTtlMs = leaseTtlMs;
    this.safetyMarginMs = safetyMarginMs;
    this.drainTimeoutMs = drainTimeoutMs;
    this.renewEveryMs = renewEveryMs;
    this.keys = leaseKeys(keyPrefix);
    this.lastRenewSentAt = undefined;
    this.value = undefined;
    this.timer = undefined;
    this.closed = false;
    if (!clientsWithScripts.has(redis)) {
      defineScripts(redis, {
        gatewayLeaseAcquire: { lua: ACQUIRE_LUA, numberOfKeys: 2 },
        gatewayLeaseRenew: { lua: RENEW_LUA, numberOfKeys: 1 },
        gatewayLeaseRelease: { lua: RELEASE_LUA, numberOfKeys: 2 }
      });
      clientsWithScripts.add(redis);
    }
  }

  async acquire() {
    const sentAt = performance.now();
    const [acquired, value, cleanConsumerId, previousValue] = await this.redis.gatewayLeaseAcquire(
      this.keys.lease, this.keys.clean, this.consumerId, this.leaseTtlMs, this.drainTimeoutMs
    );
    if (Number(acquired) !== 1) {
      let owner;
      try { owner = JSON.parse(value); } catch {}
      return { acquired: false, owner };
    }
    this.lastRenewSentAt = sentAt;
    this.value = JSON.parse(value);
    this.cleanConsumerId = cleanConsumerId || undefined;
    try { this.previousOwner = previousValue ? JSON.parse(previousValue) : undefined; } catch { this.previousOwner = undefined; }
    this.emit('acquired', this.value);
    return {
      acquired: true,
      lease: this.value,
      cleanConsumerId: this.cleanConsumerId,
      previousOwner: this.previousOwner
    };
  }

  async renew() {
    if (this.closed) return false;
    const sentAt = performance.now();
    const [renewed, value] = await this.redis.gatewayLeaseRenew(
      this.keys.lease, this.consumerId, this.leaseTtlMs, this.drainTimeoutMs
    );
    if (Number(renewed) !== 1) {
      this.lastRenewSentAt = undefined;
      this.value = undefined;
      clearInterval(this.timer);
      this.timer = undefined;
      this.closed = true;
      this.emit('lost');
      return false;
    }
    this.lastRenewSentAt = sentAt;
    this.value = JSON.parse(value);
    this.emit('renewed', this.value);
    return true;
  }

  isFresh() {
    return this.lastRenewSentAt !== undefined &&
      performance.now() - this.lastRenewSentAt < this.leaseTtlMs - this.safetyMarginMs;
  }

  startRenewal() {
    if (this.timer || this.closed) return;
    this.timer = setInterval(() => {
      this.renew().catch((error) => {
        this.lastRenewSentAt = undefined;
        this.value = undefined;
        this.emit('lost', error);
      });
    }, this.renewEveryMs);
    this.timer.unref?.();
  }

  async release({ clean = false } = {}) {
    clearInterval(this.timer);
    this.timer = undefined;
    const released = Number(await this.redis.gatewayLeaseRelease(
      this.keys.lease, this.keys.clean, this.consumerId, clean ? 1 : 0
    )) === 1;
    this.closed = true;
    this.lastRenewSentAt = undefined;
    this.value = undefined;
    return released;
  }
}

module.exports = {
  LeaseCoordinator,
  leaseKeys,
  ACQUIRE_LUA,
  RENEW_LUA,
  RELEASE_LUA
};
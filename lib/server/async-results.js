'use strict';

const { defineScripts, hashTag } = require('@redkern/node-red-kit/redis');
const clientsWithAsyncScripts = new WeakSet();

const GROUP = 'redkern-gateway-results';
const ACK_LUA = `
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
return 1
`;

function resultStreamKey(keyPrefix, client, operation) {
  if (typeof client !== 'string' || !/^[A-Za-z0-9._:-]+$/.test(client)) throw new TypeError('client name must be a safe identifier');
  if (typeof operation !== 'string' || !/^(?:[A-Za-z0-9._:-]+\/)+[A-Za-z0-9._:-]+$/.test(operation)) throw new TypeError('operation must use domain/action format');
  return `${hashTag(`${keyPrefix}:results`, client)}:${operation}`;
}

class AsyncResultStore {
  constructor({ redis, keyPrefix, consumerId, asyncResultTtlMs = 3600000 }) {
    if (!redis || typeof redis.defineCommand !== 'function') throw new TypeError('Redis scripts client is required');
    this.redis = redis;
    this.keyPrefix = keyPrefix;
    this.consumerId = consumerId;
    this.asyncResultTtlMs = asyncResultTtlMs;
    this.groups = new Set();
    this.streams = new Map();
    this.expiredByOperation = new Map();
    this.retentionTimer = undefined;
    if (!clientsWithAsyncScripts.has(redis)) {
      defineScripts(redis, { gatewayAsyncResultAck: { lua: ACK_LUA, numberOfKeys: 1 } });
      clientsWithAsyncScripts.add(redis);
    }
  }

  async ensureGroup(client, operation) {
    const stream = resultStreamKey(this.keyPrefix, client, operation);
    if (this.groups.has(stream)) return stream;
    try { await this.redis.xgroup('CREATE', stream, GROUP, '0', 'MKSTREAM'); } catch (error) {
      if (!String(error.message).includes('BUSYGROUP')) throw error;
    }
    this.groups.add(stream);
    return stream;
  }

  async append(client, operation, entry) {
    const stream = await this.ensureGroup(client, operation);
    this.streams.set(stream, { client, operation });
    const streamId = await this.redis.xadd(stream, '*', 'data', JSON.stringify(entry));
    await this.trimStream(stream);
    return { stream, streamId };
  }

  startRetention(intervalMs = 60000) {
    if (this.retentionTimer) return;
    this.retentionTimer = setInterval(() => {
      this.trimExpired().catch(() => {});
    }, intervalMs);
    this.retentionTimer.unref?.();
  }

  close() {
    clearInterval(this.retentionTimer);
    this.retentionTimer = undefined;
  }

  async trimExpired() {
    const expired = [];
    for (const stream of this.streams.keys()) {
      const count = await this.trimStream(stream);
      if (count > 0) expired.push({ stream, count });
    }
    return expired;
  }

  async trimStream(stream) {
    const trimBefore = Math.max(0, Date.now() - this.asyncResultTtlMs);
    const count = Number(await this.redis.xtrim(stream, 'MINID', '~', `${trimBefore}-0`));
    if (count > 0) {
      const owner = this.streams.get(stream);
      if (owner) {
        const key = `${owner.client}\u0000${owner.operation}`;
        this.expiredByOperation.set(key, (this.expiredByOperation.get(key) || 0) + count);
      }
    }
    return count;
  }

  async read(client, operation, count = 1, consumerId = this.consumerId, startId = '>') {
    const stream = await this.ensureGroup(client, operation);
    const result = await this.redis.xreadgroup('GROUP', GROUP, consumerId, 'COUNT', count, 'STREAMS', stream, startId);
    return (result?.[0]?.[1] || []).map(([streamId, fields]) => {
      const index = fields.indexOf('data');
      if (index < 0) return undefined;
      try { return { stream, streamId, entry: JSON.parse(fields[index + 1]) }; } catch { return undefined; }
    }).filter(Boolean);
  }

  async readPending(client, operation, consumerId, count = 100) {
    return this.read(client, operation, count, consumerId, '0');
  }

  async claimConsumer(client, operation, previousConsumer, nextConsumer, minIdleMs, count = 100) {
    const stream = await this.ensureGroup(client, operation);
    const pending = await this.redis.xpending(stream, GROUP, '-', '+', count, previousConsumer, minIdleMs);
    if (!Array.isArray(pending) || pending.length === 0) return [];
    const ids = pending.map((item) => item[0]);
    const claimed = await this.redis.xclaim(stream, GROUP, nextConsumer, minIdleMs, ...ids);
    return (claimed || []).map(([streamId, fields]) => {
      const index = fields.indexOf('data');
      if (index < 0) return undefined;
      try { return { stream, streamId, entry: JSON.parse(fields[index + 1]) }; } catch { return undefined; }
    }).filter(Boolean);
  }

  async ack(stream, streamId) {
    return Number(await this.redis.gatewayAsyncResultAck(stream, GROUP, streamId)) === 1;
  }
}

module.exports = { AsyncResultStore, resultStreamKey, GROUP, ACK_LUA };
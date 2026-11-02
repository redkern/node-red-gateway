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
    this.indexKey = `${keyPrefix}:results:index`;
    this.indexReadyKey = `${keyPrefix}:results:index:ready`;
    this.groups = new Set();
    this.streams = new Map();
    this.expiredByOperation = new Map();
    this.retentionTimer = undefined;
    this.ready = Promise.resolve();
    if (!clientsWithAsyncScripts.has(redis)) {
      defineScripts(redis, { gatewayAsyncResultAck: { lua: ACK_LUA, numberOfKeys: 1 } });
      clientsWithAsyncScripts.add(redis);
    }
  }

  async ensureGroup(client, operation) {
    const stream = resultStreamKey(this.keyPrefix, client, operation);
    await this._rememberStream(stream, client, operation);
    if (this.groups.has(stream)) return stream;
    try { await this.redis.xgroup('CREATE', stream, GROUP, '0', 'MKSTREAM'); } catch (error) {
      if (!String(error.message).includes('BUSYGROUP')) throw error;
    }
    this.groups.add(stream);
    return stream;
  }

  async append(client, operation, entry) {
    const stream = await this.ensureGroup(client, operation);
    const streamId = await this.redis.xadd(stream, '*', 'data', JSON.stringify(entry));
    await this.trimStream(stream);
    return { stream, streamId };
  }

  startRetention(intervalMs = 60000) {
    if (this.retentionTimer) return;
    this.ready = this._restoreStreamIndex();
    this.ready.then(() => this.trimExpired()).catch(() => {});
    this.retentionTimer = setInterval(() => {
      this.ready.then(() => this.trimExpired()).catch(() => {});
    }, intervalMs);
    this.retentionTimer.unref?.();
    return this.ready;
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

  async _rememberStream(stream, client, operation) {
    if (this.streams.has(stream)) return;
    await this.redis.sadd(this.indexKey, stream);
    this.streams.set(stream, { client, operation });
  }

  async _restoreStreamIndex() {
    if (!await this.redis.get(this.indexReadyKey)) {
      const clients = typeof this.redis.nodes === 'function'
        ? this.redis.nodes('master')
        : [this.redis];
      const pattern = `${this.keyPrefix}:results:*`;
      for (const client of (clients?.length ? clients : [this.redis])) {
        let cursor = '0';
        do {
          const [nextCursor, keys] = await client.scan(cursor, 'MATCH', pattern, 'COUNT', 500);
          const streams = keys.map((stream) => [stream, this._streamOwner(stream)]).filter(([, owner]) => owner);
          for (const [stream, owner] of streams) this.streams.set(stream, owner);
          if (keys.length) await this.redis.sadd(this.indexKey, ...keys.filter((stream) => this._streamOwner(stream)));
          cursor = nextCursor;
        } while (cursor !== '0');
      }
      await this.redis.set(this.indexReadyKey, '1');
    }
    const indexedStreams = await this.redis.smembers(this.indexKey);
    for (const stream of indexedStreams || []) {
      const owner = this._streamOwner(stream);
      if (owner) this.streams.set(stream, owner);
    }
  }

  _streamOwner(stream) {
    const prefix = `${this.keyPrefix}:results:`;
    if (!stream.startsWith(prefix)) return undefined;
    const tagged = stream.slice(prefix.length);
    const separator = tagged.indexOf('}:');
    if (!tagged.startsWith('{') || separator < 2) return undefined;
    const client = tagged.slice(1, separator);
    const operation = tagged.slice(separator + 2);
    try {
      if (resultStreamKey(this.keyPrefix, client, operation) !== stream) return undefined;
    } catch {
      return undefined;
    }
    return { client, operation };
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

  async reclaimAbandoned(client, operation, nextConsumer, activeConsumers, minIdleMs, count = 100) {
    if (count < 1) return [];
    const stream = await this.ensureGroup(client, operation);
    const pending = await this.redis.xpending(stream, GROUP, 'IDLE', minIdleMs, '-', '+', count);
    const candidates = (pending || []).filter((item) =>
      !activeConsumers.has(item[1])
    );
    const claimed = [];
    const byConsumer = new Map();
    for (const [id, consumer] of candidates) {
      if (!byConsumer.has(consumer)) byConsumer.set(consumer, []);
      byConsumer.get(consumer).push(id);
    }
    for (const ids of byConsumer.values()) {
      const remaining = count - claimed.length;
      if (remaining <= 0) break;
      const result = await this.redis.xclaim(stream, GROUP, nextConsumer, minIdleMs, ...ids.slice(0, remaining));
      claimed.push(...this._decodeEntries(stream, result));
    }
    return claimed;
  }

  _decodeEntries(stream, result) {
    return (result || []).map(([streamId, fields]) => {
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
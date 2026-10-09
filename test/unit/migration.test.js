'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const { migrateFlow } = require('../../lib/migration.js');

test('legacy flow migration remaps known nodes, preserves IDs/wires, and reports credential work', () => {
  const source = [
    { id: 'client-1', type: 'pod-gateway-config', url: 'ws://gateway', podId: 'pod-a', token: 'never-copy' },
    { id: 'call-1', type: 'pod-gateway-call', gateway: 'client-1', operation: 'billing/calculate', timeout: 5000, wires: [['debug']] },
    { id: 'server-1', type: 'pod-gateway-server-config', host: '127.0.0.1', port: 8080, redisUrl: 'rediss://user:secret@redis.internal:6380/2' },
    { id: 'unknown', type: 'other-node', wires: [] }
  ];
  const migrated = migrateFlow(source);
  assert.deepEqual(migrated.flow.map(({ id, type }) => ({ id, type })), [
    { id: 'client-1', type: 'redkern-gateway-client-config' },
    { id: 'call-1', type: 'redkern-gateway-call' },
    { id: 'server-1', type: 'redkern-gateway-server-config' },
    { id: 'unknown', type: 'other-node' }
  ]);
  assert.equal(migrated.flow[1].client, 'client-1');
  assert.equal(migrated.flow[1].deadlineMs, 5000);
  assert.deepEqual(migrated.flow[1].wires, [['debug']]);
  assert.equal(Object.hasOwn(migrated.flow[0], 'token'), false);
  assert.equal(migrated.flow[2].redisUrl, undefined);
  assert.equal(migrated.flow[2].redisHost, 'redis.internal');
  assert.equal(migrated.flow[2].redisDb, 2);
  assert.equal(migrated.flow[2].redisTls, true);
  assert.equal(migrated.report.converted, 3);
  assert.equal(migrated.report.unchanged, 1);
  assert.equal(migrated.report.manualReview.length, 3);
  assert.equal(source[0].type, 'pod-gateway-config');
  assert.equal(source[2].redisUrl, 'rediss://user:secret@redis.internal:6380/2');
});

test('legacy flow migration rejects non-array exports and invalid entries', () => {
  assert.throws(() => migrateFlow({ nodes: [] }), /JSON array/);
  assert.throws(() => migrateFlow([null]), /entries must be objects/);
});

test('migration CLI writes a separate output, preserves source, and blocks manual-review completion', (context) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'redkern-flow-migration-'));
  context.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const input = path.join(directory, 'legacy.json');
  const output = path.join(directory, 'migrated.json');
  const source = [{ id: 'old', type: 'pod-gateway-server-config', host: '127.0.0.1', port: 8080 }];
  const original = `${JSON.stringify(source, null, 2)}\n`;
  fs.writeFileSync(input, original);
  const cli = path.join(__dirname, '../../bin/migrate-flow.js');
  const result = spawnSync(process.execPath, [cli, '--input', input, '--output', output], { encoding: 'utf8' });
  assert.equal(result.status, 1);
  assert.deepEqual(JSON.parse(fs.readFileSync(output, 'utf8'))[0].type, 'redkern-gateway-server-config');
  assert.equal(fs.readFileSync(input, 'utf8'), original);
  const overwrite = spawnSync(process.execPath, [cli, '--input', input, '--output', output], { encoding: 'utf8' });
  assert.equal(overwrite.status, 1);
  assert.match(overwrite.stderr, /Output already exists/);
});
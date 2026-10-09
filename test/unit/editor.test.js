'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.join(__dirname, '../..');
const editor = fs.readFileSync(path.join(root, 'nodes/gateway.html'), 'utf8');
const runtime = fs.readFileSync(path.join(root, 'nodes/gateway.js'), 'utf8');
const definitions = [...editor.matchAll(/RED\.nodes\.registerType\("([^"]+)"/g)].map((match) => match[1]);
const runtimeTypes = [...runtime.matchAll(/RED\.nodes\.registerType\('([^']+)'/g)].map((match) => match[1]);

test('each registered runtime node has a Node-RED editor definition, template, and help', () => {
  assert.deepEqual([...definitions].sort(), [...runtimeTypes].sort());
  assert.deepEqual([...runtimeTypes].sort(), [
    'redkern-gateway-account',
    'redkern-gateway-adapter',
    'redkern-gateway-api-config',
    'redkern-gateway-call',
    'redkern-gateway-client-config',
    'redkern-gateway-in',
    'redkern-gateway-metrics',
    'redkern-gateway-out',
    'redkern-gateway-server-config',
    'redkern-gateway-worker-in',
    'redkern-gateway-worker-out'
  ]);
  for (const type of runtimeTypes) {
    assert.match(editor, new RegExp(`<script type="text/html" data-template-name="${type}">`));
    assert.match(editor, new RegExp(`<script type="text/markdown" data-help-name="${type}">`));
    assert.match(editor, new RegExp(`RED\.nodes\.registerType\\("${type}"`));
  }
  assert.match(editor, /credentials:\s*\{ token:\s*\{ type: "password" \}/);
  assert.match(editor, /const color = "#A9DCD6"/);
  assert.deepEqual([...editor.matchAll(/paletteLabel:\s*"([^"]+)"/g)].map((match) => match[1]), [
    'Gateway Server', 'Gateway Account', 'Gateway Client', 'Gateway API', 'Gateway Call', 'Gateway Event Out',
    'Async Result In', 'HTTP Adapter', 'Worker Request In', 'Worker Result Out', 'Gateway Metrics'
  ]);
  for (const label of ['Gateway Call', 'Gateway Event Out', 'Async Result In', 'HTTP Adapter', 'Worker Request In', 'Worker Result Out', 'Gateway Metrics']) {
    assert.ok(editor.includes(`this.operation || "${label}"`) || editor.includes(`this.name, "${label}"`), `missing clear default label ${label}`);
  }
  assert.match(editor, /const tabLabels = \{ general: "General", reliability: "Reliability", advanced: "Advanced" \}/);
  assert.doesNotMatch(editor, /RED\._\(/);
  const icons = [...editor.matchAll(/icon: "([^"]+\.svg)"/g)].map((match) => match[1]);
  assert.equal(new Set(icons).size, icons.length);
  for (const icon of icons) {
    assert.match(icon, /^redkern-gateway-/);
    assert.ok(fs.existsSync(path.join(root, 'icons', icon)), `missing icon ${icon}`);
  }
  assert.match(editor, /data-template-name="redkern-gateway-call"[\s\S]*?node-input-client/);
  assert.match(editor, /data-template-name="redkern-gateway-account"[\s\S]*?node-config-input-server/);
});

test('packaged example is valid JSON and all palette node types exist', () => {
  const flow = JSON.parse(fs.readFileSync(path.join(root, 'examples/basic-call.json'), 'utf8'));
  const flowIds = new Set(flow.map((node) => node.id));
  const types = new Set(runtimeTypes);
  for (const node of flow) {
    if (node.type.startsWith('redkern-gateway-')) assert(types.has(node.type), `unknown Gateway node ${node.type}`);
    for (const property of ['client', 'server']) {
      if (node[property]) assert(flowIds.has(node[property]), `missing ${property} config reference ${node[property]}`);
    }
    for (const output of node.wires || []) {
      for (const target of output) assert(flowIds.has(target), `missing wire target ${target}`);
    }
  }
});
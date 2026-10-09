'use strict';

const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const packageJson = require('../package.json');

const packed = JSON.parse(execFileSync('npm', ['pack', '--dry-run', '--json'], { encoding: 'utf8' }))[0];
const files = new Set(packed.files.map((file) => file.path));
const required = [
  'README.md',
  'RELEASING.md',
  'LICENSE',
  'examples/basic-call.json',
  'examples/gateway-services.json',
  'icons/redkern-gateway.svg',
  'nodes/gateway.js',
  'nodes/gateway.html',
  'nodes/locales/en-US/node-red-gateway.json'
];

for (const path of required) assert(files.has(path), `npm tarball is missing ${path}`);
for (const [type, path] of Object.entries(packageJson['node-red'].nodes)) {
  assert(path.endsWith('.js'), `Node-RED entry ${type} must target a .js file`);
  assert(files.has(path), `npm tarball is missing registered node file ${path}`);
}
const editor = fs.readFileSync(path.join(__dirname, '../nodes/gateway.html'), 'utf8');
const runtimeNodeTypes = [...editor.matchAll(
  /RED\.nodes\.registerType\(["']([^"']+)["'],\s*\{\s*category:\s*category/g
)].map((match) => match[1]);
assert(runtimeNodeTypes.length > 0, 'Node-RED editor does not declare any runtime nodes');
const exampleNodeTypes = new Set();
for (const file of files) {
  if (!file.startsWith('examples/') || !file.endsWith('.json')) continue;
  const flow = JSON.parse(fs.readFileSync(path.join(__dirname, '..', file), 'utf8'));
  for (const node of flow) if (node.type) exampleNodeTypes.add(node.type);
}
for (const type of runtimeNodeTypes) {
  assert(exampleNodeTypes.has(type), `Examples do not use runtime node ${type}`);
}
assert.match(packageJson.engines.node, /^>=22$/);
assert.equal(packageJson['node-red'].version, '>=4.1.0 <6.0.0');
assert(!packed.files.some((file) => /\s2\.(?:js|html)$/.test(file.path)), 'npm tarball contains a duplicate * 2.* file');
console.log(`Checked ${files.size} packed files and ${Object.keys(packageJson['node-red'].nodes).length} Node-RED entrypoint(s).`);
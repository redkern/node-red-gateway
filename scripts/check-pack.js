'use strict';

const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const packageJson = require('../package.json');

const packed = JSON.parse(execFileSync('npm', ['pack', '--dry-run', '--json'], { encoding: 'utf8' }))[0];
const files = new Set(packed.files.map((file) => file.path));
const required = [
  'README.md',
  'RELEASING.md',
  'LICENSE',
  'examples/basic-call.json',
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
assert.match(packageJson.engines.node, /^>=22$/);
assert.equal(packageJson['node-red'].version, '>=4.1.0 <6.0.0');
assert(!packed.files.some((file) => /\s2\.(?:js|html)$/.test(file.path)), 'npm tarball contains a duplicate * 2.* file');
console.log(`Checked ${files.size} packed files and ${Object.keys(packageJson['node-red'].nodes).length} Node-RED entrypoint(s).`);
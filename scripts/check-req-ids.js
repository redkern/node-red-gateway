'use strict';

const fs = require('node:fs');
const path = require('node:path');

const roots = ['README.md', 'lib', 'nodes', 'test'];
const expected = ['GW-DL-3', 'GW-ERR-1', 'GW-LIFE-6', 'GW-LIFE-9', 'GW-MIG-2', 'GW-MIG-3', 'GW-NFR-5', 'GW-OPS-2'];
const ids = new Set();
const duplicates = new Set();

function visit(filePath) {
  if (!fs.existsSync(filePath)) return;
  const stat = fs.statSync(filePath);
  if (stat.isDirectory()) {
    for (const entry of fs.readdirSync(filePath)) visit(path.join(filePath, entry));
    return;
  }
  if (!/\.(?:js|md|html|json)$/.test(filePath)) return;
  const content = fs.readFileSync(filePath, 'utf8');
  for (const match of content.matchAll(/\bGW-[A-Z]+-\d+\b/g)) {
    if (ids.has(match[0])) duplicates.add(match[0]);
    ids.add(match[0]);
  }
}

for (const root of roots) visit(path.join(__dirname, '..', root));
if (duplicates.size) {
  console.error(`Duplicate requirement IDs: ${[...duplicates].sort().join(', ')}`);
  process.exitCode = 1;
} else if (expected.some((id) => !ids.has(id))) {
  console.error(`Missing requirement ID references: ${expected.filter((id) => !ids.has(id)).join(', ')}`);
  process.exitCode = 1;
} else {
  console.log(`Found ${ids.size} unique referenced gateway requirement ID(s).`);
}
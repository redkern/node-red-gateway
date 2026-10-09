'use strict';

const fs = require('node:fs');

const manifest = JSON.parse(fs.readFileSync(require.resolve('../package.json'), 'utf8'));
const changelog = fs.readFileSync(require.resolve('../CHANGELOG.md'), 'utf8');
const tag = process.env.GITHUB_REF_NAME;
const blockers = [];

if (!/^\d+\.\d+\.\d+$/.test(manifest.version)) blockers.push(`package version ${manifest.version} is not a stable semver release`);
if (tag && tag !== `v${manifest.version}`) blockers.push(`release tag ${tag} does not match v${manifest.version}`);
if (!changelog.split(/\r?\n/).some((line) => new RegExp(`^## ${manifest.version.replaceAll('.', '\\.') } - \\d{4}-\\d{2}-\\d{2}$`).test(line))) {
  blockers.push(`CHANGELOG.md has no dated entry for ${manifest.version}`);
}
if (!manifest.keywords?.includes('node-red')) blockers.push('stable palette package must include the node-red keyword');
if (manifest.publishConfig?.access !== 'public') blockers.push('publishConfig.access must be public');
if (manifest.repository?.url !== 'git+https://github.com/redkern/node-red-gateway.git') blockers.push('repository.url does not match the canonical public GitHub repository');

const thresholds = [
  ['GATEWAY_LOAD_MIN_RPS', process.env.GATEWAY_LOAD_MIN_RPS],
  ['GATEWAY_LOAD_MAX_P95_MS', process.env.GATEWAY_LOAD_MAX_P95_MS],
  ['GATEWAY_LOAD_MAX_ERROR_RATE_PERCENT', process.env.GATEWAY_LOAD_MAX_ERROR_RATE_PERCENT]
];
for (const [name, value] of thresholds) {
  const number = Number(value);
  if (value === undefined || value.trim() === '' || !Number.isFinite(number) || number <= 0) {
    blockers.push(`${name} must be configured from the approved production SLO`);
  }
}

if (blockers.length) {
  console.error(`Release blocked for ${manifest.name}@${manifest.version}:`);
  for (const blocker of blockers) console.error(`- ${blocker}`);
  process.exitCode = 1;
} else {
  console.log(`Release gates configured for ${manifest.name}@${manifest.version}`);
}
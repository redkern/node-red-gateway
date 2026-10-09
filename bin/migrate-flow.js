#!/usr/bin/env node
'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { migrateFlow } = require('../lib/migration.js');

function usage() {
  console.error('Usage: redkern-gateway-migrate --input <flow.json> --output <new-flow.json>');
  process.exitCode = 2;
}

function main(args) {
  const inputIndex = args.indexOf('--input');
  const outputIndex = args.indexOf('--output');
  const input = inputIndex >= 0 ? args[inputIndex + 1] : undefined;
  const output = outputIndex >= 0 ? args[outputIndex + 1] : undefined;
  if (!input || !output || input.startsWith('--') || output.startsWith('--')) return usage();
  const inputPath = path.resolve(input);
  const outputPath = path.resolve(output);
  if (inputPath === outputPath) throw new Error('Output must be a new path; the source flow is never overwritten');
  if (fs.existsSync(outputPath)) throw new Error(`Output already exists: ${outputPath}`);
  const source = JSON.parse(fs.readFileSync(inputPath, 'utf8'));
  const { flow, report } = migrateFlow(source);
  fs.writeFileSync(outputPath, `${JSON.stringify(flow, null, 2)}\n`, { flag: 'wx' });
  console.log(JSON.stringify({ input: inputPath, output: outputPath, ...report }, null, 2));
  if (report.manualReview.length > 0) process.exitCode = 1;
}

try { main(process.argv.slice(2)); } catch (error) {
  console.error(`Migration failed: ${error.message}`);
  process.exitCode = 1;
}
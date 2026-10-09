'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  accountAllows,
  authenticateAccount,
  matchesOperationPrefix,
  validateAccounts
} = require('../../lib/security.js');

test('gateway refuses to start without a named account and token', () => {
  assert.throws(() => validateAccounts([]), /at least one/i);
  assert.throws(() => validateAccounts([{ name: 'client', operationPrefixes: ['*'] }]), /active token/i);
  assert.doesNotThrow(() => validateAccounts([
    { name: 'client', operationPrefixes: [], token: 'active' }
  ]));
});

test('account names are unique and operation prefixes must be explicit lists', () => {
  assert.throws(() => validateAccounts([
    { name: 'client', operationPrefixes: ['*'], token: 'one' },
    { name: 'client', operationPrefixes: ['*'], token: 'two' }
  ]), /duplicate/i);
  assert.throws(() => validateAccounts([{ name: 'client', token: 'active' }]), /prefixes/i);
  assert.throws(() => validateAccounts([
    { name: 'client', operationPrefixes: ['billing*'], token: 'active' }
  ]), /invalid operation prefix/i);
});

test('operation prefixes match exact operations and nested domain paths', () => {
  assert.equal(matchesOperationPrefix('billing/*', 'billing/calculateInvoice'), true);
  assert.equal(matchesOperationPrefix('billing/*', 'billing/invoice/retry'), true);
  assert.equal(matchesOperationPrefix('billing/*', 'billing'), false);
  assert.equal(matchesOperationPrefix('billing/*', 'crm/billing/x'), false);
  assert.equal(matchesOperationPrefix('billing/calculate', 'billing/calculate/extra'), false);
  assert.equal(accountAllows({ operationPrefixes: [] }, 'billing/calculateInvoice'), false);
  assert.equal(accountAllows({ operationPrefixes: ['*'] }, 'anything/goes'), true);
});

test('authentication checks every active and rotation token using secure comparison', () => {
  const calls = [];
  const accounts = [
    { name: 'one', token: 'first', nextToken: 'rotating' },
    { name: 'two', token: 'second', nextToken: '' }
  ];
  const matched = authenticateAccount(accounts, 'rotating', (expected, supplied) => {
    calls.push([expected, supplied]);
    return expected === supplied;
  });
  assert.equal(matched, accounts[0]);
  assert.equal(calls.length, 4);
  assert.equal(authenticateAccount(accounts, 'unknown', () => false), undefined);
});
'use strict';

const { secureCompare } = require('@redkern/node-red-kit');

function validateAccounts(accounts) {
  if (!Array.isArray(accounts) || accounts.length === 0) {
    throw new Error('At least one gateway account with a token is required');
  }

  const names = new Set();
  for (const account of accounts) {
    if (!account || typeof account.name !== 'string' || account.name.trim() === '') {
      throw new Error('Every gateway account must have a name');
    }
    if (names.has(account.name)) throw new Error(`Duplicate gateway account: ${account.name}`);
    names.add(account.name);
    if (!Array.isArray(account.operationPrefixes)) {
      throw new Error(`Gateway account ${account.name} must define operation prefixes`);
    }
    for (const prefix of account.operationPrefixes) {
      const validWildcard = typeof prefix === 'string' && /^[A-Za-z0-9._:-]+\/\*$/.test(prefix);
      const validExact = typeof prefix === 'string' && /^(?:[A-Za-z0-9._:-]+\/)+[A-Za-z0-9._:-]+$/.test(prefix);
      if (prefix !== '*' && !validWildcard && !validExact) {
        throw new Error(`Gateway account ${account.name} has an invalid operation prefix`);
      }
    }
    if (![account.token, account.nextToken].some((token) => typeof token === 'string' && token.length > 0)) {
      throw new Error(`Gateway account ${account.name} must have an active token`);
    }
  }
  return accounts;
}

function matchesOperationPrefix(prefix, operation) {
  if (typeof prefix !== 'string' || typeof operation !== 'string') return false;
  if (prefix === '*') return true;
  if (prefix.endsWith('/*')) return operation.startsWith(prefix.slice(0, -1));
  return prefix === operation;
}

function accountAllows(account, operation) {
  return Boolean(account && Array.isArray(account.operationPrefixes) &&
    account.operationPrefixes.some((prefix) => matchesOperationPrefix(prefix, operation)));
}

function authenticateAccount(accounts, suppliedToken, compare = secureCompare) {
  let match;
  for (const account of accounts) {
    for (const token of [account.token, account.nextToken]) {
      const isMatch = compare(typeof token === 'string' ? token : '', suppliedToken);
      if (isMatch) match = account;
    }
  }
  return match;
}

module.exports = { accountAllows, authenticateAccount, matchesOperationPrefix, validateAccounts };
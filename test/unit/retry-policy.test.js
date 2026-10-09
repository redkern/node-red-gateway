'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { isRetryable } = require('../../lib/server/queue.js');

const classes = ['never', 'safe', 'idempotent'];

test('pre-send network failures and HTTP 429 are retried for every retry class', () => {
  const safeFailures = [
    { code: 'ECONNREFUSED' },
    { code: 'ENOTFOUND' },
    { code: 'EAI_AGAIN' },
    { code: 'ETIMEDOUT' },
    { code: 'UPSTREAM_CONNECT_ERROR' },
    { code: 'UPSTREAM_RATE_LIMITED' },
    { statusCode: 429 }
  ];
  for (const retryClass of classes) {
    for (const error of safeFailures) assert.equal(isRetryable(error, retryClass), true, `${retryClass}: ${JSON.stringify(error)}`);
  }
});

test('post-send network failures require a retry-enabled operation class', () => {
  const uncertainFailures = [
    { code: 'ECONNRESET' },
    { code: 'UPSTREAM_TIMEOUT' },
    { code: 'UPSTREAM_ERROR', statusCode: 503, retryable: true }
  ];
  for (const error of uncertainFailures) {
    assert.equal(isRetryable(error, 'never'), false);
    assert.equal(isRetryable(error, 'safe'), true);
    assert.equal(isRetryable(error, 'idempotent'), true);
  }
});

test('permanent application and validation failures are not retried', () => {
  for (const retryClass of classes) {
    assert.equal(isRetryable({ code: 'REQUEST_VALIDATION_FAILED' }, retryClass), false);
    assert.equal(isRetryable({ code: 'UPSTREAM_ERROR', statusCode: 400, retryable: false }, retryClass), false);
  }
});
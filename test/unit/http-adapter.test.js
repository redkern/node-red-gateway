'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createHttpAdapter } = require('../../lib/http-adapter.js');

function api(overrides = {}) {
  return {
    url: 'https://upstream.example/v1/{customer}',
    method: 'POST',
    tokenHeader: 'Authorization',
    token: 'server-secret',
    timeoutMs: 1000,
    maxResponseBytes: 1024,
    ...overrides
  };
}

test('adapter maps request parameters and body while protecting configured credentials', async () => {
  let sentUrl;
  let sentOptions;
  const execute = createHttpAdapter(api(), async (url, options) => {
    sentUrl = url;
    sentOptions = options;
    return new Response('{"total":42}', { status: 201, headers: { 'content-type': 'application/json' } });
  });
  const result = await execute({
    payload: { amount: 21 },
    _request: {
      params: { customer: 'a/b' },
      query: { tag: ['first', 'second'] },
      headers: { authorization: 'Bearer attacker', 'x-trace-id': 'trace-1' }
    }
  });
  assert.equal(sentUrl.pathname, '/v1/a%2Fb');
  assert.deepEqual(sentUrl.searchParams.getAll('tag'), ['first', 'second']);
  assert.equal(sentOptions.headers.get('authorization'), 'Bearer server-secret');
  assert.equal(sentOptions.headers.get('x-trace-id'), 'trace-1');
  assert.equal(sentOptions.headers.get('content-type'), 'application/json');
  assert.deepEqual(JSON.parse(sentOptions.body), { amount: 21 });
  assert.deepEqual(result, { payload: { total: 42 }, statusCode: 201 });
});

test('adapter rejects header injection and caps response bytes', async () => {
  const execute = createHttpAdapter(api(), async () => new Response('ok'));
  await assert.rejects(execute({ payload: {}, _request: { headers: { 'x-test': 'ok\r\nInjected: yes' } } }), {
    code: 'REQUEST_VALIDATION_FAILED'
  });
  const tooLarge = createHttpAdapter(api({ maxResponseBytes: 3 }), async () => new Response('four'));
  await assert.rejects(tooLarge({ payload: {}, _request: { params: { customer: 'test' } } }), { code: 'RESULT_TOO_LARGE' });
});

test('adapter preserves upstream status and response body in public errors', async () => {
  const execute = createHttpAdapter(api(), async () => new Response('{"message":"busy"}', {
    status: 429, headers: { 'content-type': 'application/json' }
  }));
  await assert.rejects(execute({ payload: {}, _request: { params: { customer: 'test' } } }), (error) => {
    assert.equal(error.code, 'UPSTREAM_ERROR');
    assert.equal(error.retryable, true);
    assert.equal(error.statusCode, 429);
    assert.deepEqual(error.responseBody, { message: 'busy' });
    return true;
  });
});

test('adapter forwards the upstream idempotency key and parses Retry-After seconds', async () => {
  let capturedHeaders;
  const execute = createHttpAdapter(api({ forwardIdempotencyKey: true }), async (_url, options) => {
    capturedHeaders = options.headers;
    return new Response('{"message":"busy"}', {
      status: 429,
      headers: { 'content-type': 'application/json', 'retry-after': '2' }
    });
  });
  await assert.rejects(execute({
    payload: {}, idempotencyKey: 'idem-1',
    _request: { params: { customer: 'test' } }
  }, { requestId: 'req-1' }), (error) => {
    assert.equal(error.retryAfterMs, 2000);
    return true;
  });
  assert.equal(capturedHeaders.get('idempotency-key'), 'idem-1');
});

test('adapter aborts upstream requests at the configured timeout', async () => {
  const execute = createHttpAdapter(api({ timeoutMs: 5 }), (_url, { signal }) => new Promise((_resolve, reject) => {
    signal.addEventListener('abort', () => reject(Object.assign(new Error('aborted'), { name: 'AbortError' })), { once: true });
  }));
  await assert.rejects(execute({ payload: {}, _request: { params: { customer: 'test' } } }), { code: 'UPSTREAM_TIMEOUT', retryable: true });
});
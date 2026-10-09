'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const net = require('node:net');
const { WebSocketServer } = require('ws');
const { GatewayClient } = require('../../lib/client.js');
const { GatewayRuntime } = require('../../lib/gateway-runtime.js');

async function unusedPort() {
  const server = net.createServer();
  await new Promise((resolve, reject) => server.listen(0, '127.0.0.1', resolve).once('error', reject));
  const { port } = server.address();
  await new Promise((resolve) => server.close(resolve));
  return port;
}

test('POD client authenticates in upgrade and completes an operation call', async (context) => {
  const port = await unusedPort();
  const runtime = new GatewayRuntime({ host: '127.0.0.1', port });
  runtime.on('error', (error) => assert.fail(error.message));
  runtime.registerAccount({ name: 'pod-client', operationPrefixes: ['billing/*'], token: 'client-secret' });
  let capturedCall;
  runtime.registerExecutor('billing/getInvoice', async (message) => {
    if (message.type === 'call') capturedCall = message;
    return { id: message.payload.id, total: 12 };
  });
  await runtime.start();

  const client = new GatewayClient({ url: `ws://127.0.0.1:${port}`, token: 'client-secret', podId: 'pod-1' });
  context.after(async () => {
    await client.close();
    await runtime.close();
  });
  const capabilitiesPromise = new Promise((resolve) => client.once('capabilities', resolve));
  await client.start();
  assert.deepEqual(await capabilitiesPromise, [{ operation: 'billing/getInvoice' }]);
  assert.deepEqual(client.capabilities, [{ operation: 'billing/getInvoice' }]);
  assert.deepEqual(await client.call('billing/getInvoice', { payload: { id: 'i-1' } }, { deadlineMs: 1000 }), {
    id: 'i-1', total: 12
  });
  assert.equal(Object.hasOwn(capturedCall, 'deadlineAt'), false);
  assert.equal(capturedCall.deadlineMs, 1000);
  const asyncResult = new Promise((resolve) => client.once('asyncResult', resolve));
  await client.call('billing/getInvoice', { payload: { id: 'i-2' } }, { messageType: 'event' });
  const delivered = await asyncResult;
  assert.equal(delivered.operation, 'billing/getInvoice');
  assert.equal(typeof delivered.requestId, 'string');
  assert.deepEqual(delivered.response, { id: 'i-2', total: 12 });
});

test('POD client fails fast when disconnected and clears pending calls on close', async () => {
  const client = new GatewayClient({ url: 'ws://127.0.0.1:9', token: 'secret', podId: 'pod-1', connectTimeoutMs: 25 });
  await assert.rejects(client.call('billing/getInvoice', { payload: {} }), { code: 'GATEWAY_NOT_CONNECTED' });
  await assert.rejects(client.start(), { code: 'GATEWAY_CONNECT_TIMEOUT' });
  await client.close();
});

test('POD client falls back to protocol 1.0 and reads the legacy result envelope', async (context) => {
  const server = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  await new Promise((resolve) => server.once('listening', resolve));
  const { port } = server.address();
  let legacyToken;
  let legacyCall;
  server.on('connection', (socket) => {
    socket.on('message', (raw) => {
      const message = JSON.parse(raw.toString());
      if (message.type === 'hello' && message.protocolVersion === '1.1') {
        socket.send(JSON.stringify({ type: 'hello_error', error: { code: 'UNSUPPORTED_PROTOCOL', message: 'Supported protocol is 1.0' } }));
      } else if (message.type === 'hello' && message.protocolVersion === '1.0') {
        legacyToken = message.token;
        socket.send(JSON.stringify({ type: 'hello_ack', protocolVersion: '1.0', capabilities: [{ operation: 'billing/getInvoice' }] }));
      } else if (message.type === 'call') {
        legacyCall = message;
        socket.send(JSON.stringify({
          type: 'result', requestId: message.requestId, operation: message.operation,
          ok: true, payload: { invoiceId: message.payload.invoiceId }, response: { statusCode: null }
        }));
      }
    });
  });
  const client = new GatewayClient({ url: `ws://127.0.0.1:${port}`, token: 'legacy-secret', podId: 'pod-legacy' });
  context.after(async () => {
    await client.close();
    await new Promise((resolve) => server.close(resolve));
  });
  const capabilitiesPromise = new Promise((resolve) => client.once('capabilities', resolve));
  await client.start();
  assert.equal(client.protocolVersion, '1.0');
  assert.equal(legacyToken, 'legacy-secret');
  assert.deepEqual(await capabilitiesPromise, [{ operation: 'billing/getInvoice' }]);
  assert.deepEqual(await client.call('billing/getInvoice', { payload: { invoiceId: 'old' } }), { invoiceId: 'old' });
  assert.equal(typeof legacyCall.deadlineAt, 'string');
});
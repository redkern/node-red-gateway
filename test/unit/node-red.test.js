'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const helper = require('node-red-node-test-helper');
const registerGatewayNodes = require('../../nodes/gateway.js');

helper.init(require.resolve('node-red'));

test('palette loads into Node-RED and registers its flow node types', async () => {
  await new Promise((resolve, reject) => helper.startServer((error) => error ? reject(error) : resolve()));
  try {
    const flow = [
      { id: 'call-node', type: 'redkern-gateway-call', operation: 'billing/calculate', wires: [[], []] },
      { id: 'worker-out-node', type: 'redkern-gateway-worker-out', operation: 'billing/calculate', wires: [] }
    ];
    await new Promise((resolve, reject) => {
      helper.load(registerGatewayNodes, flow, (error) => error ? reject(error) : resolve());
    });
    assert.equal(helper.getNode('call-node').type, 'redkern-gateway-call');
    assert.equal(helper.getNode('worker-out-node').type, 'redkern-gateway-worker-out');
  } finally {
    helper.unload();
    await new Promise((resolve) => helper.stopServer(resolve));
  }
});

test('Gateway Metrics preserves legacy event filter and Node-RED message shape', async () => {
  await new Promise((resolve, reject) => helper.startServer((error) => error ? reject(error) : resolve()));
  try {
    const registerTestNodes = (RED) => {
      registerGatewayNodes(RED);
      RED.nodes.registerType('gateway-metric-source-test', function (config) {
        RED.nodes.createNode(this, config);
      });
      RED.nodes.registerType('gateway-metric-capture-test', function (config) {
        RED.nodes.createNode(this, config);
        this.on('input', (message) => this.emit('message', message));
      });
    };
    const flow = [
      { id: 'metric-source', type: 'gateway-metric-source-test' },
      { id: 'metrics-node', type: 'redkern-gateway-metrics', server: 'metric-source', events: 'request.completed', wires: [['capture']] },
      { id: 'capture', type: 'gateway-metric-capture-test', wires: [] }
    ];
    await new Promise((resolve, reject) => helper.load(registerTestNodes, flow, (error) => error ? reject(error) : resolve()));
    const source = helper.getNode('metric-source');
    const capture = helper.getNode('capture');
    const received = new Promise((resolve) => capture.once('message', resolve));
    source.emit('gateway-metric', { event: 'request.processing', requestId: 'filtered' });
    const metric = {
      event: 'request.completed', timestamp: '2026-10-08T00:00:00.000Z', requestId: 'req-1',
      service: 'billing', operation: 'calculate', operationKey: 'billing/calculate',
      outcome: 'success', status: 'completed', timings: { totalMs: 12 }
    };
    source.emit('gateway-metric', metric);
    const message = await received;
    assert.equal(message.topic, 'pod-gateway/request.completed');
    assert.deepEqual(message.payload, metric);
    assert.equal(message.metric, message.payload);
    assert.deepEqual(message.gateway, {
      event: 'request.completed', requestId: 'req-1', service: 'billing', operation: 'calculate',
      operationKey: 'billing/calculate', status: 'completed', timestamp: '2026-10-08T00:00:00.000Z'
    });
  } finally {
    helper.unload();
    await new Promise((resolve) => helper.stopServer(resolve));
  }
});
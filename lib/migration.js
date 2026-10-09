'use strict';

const NODE_TYPES = Object.freeze({
  'pod-gateway-config': 'redkern-gateway-client-config',
  'pod-gateway-server-config': 'redkern-gateway-server-config',
  'pod-gateway-api-config': 'redkern-gateway-api-config',
  'pod-gateway-call': 'redkern-gateway-call',
  'pod-gateway-out': 'redkern-gateway-out',
  'pod-gateway-in': 'redkern-gateway-in',
  'pod-gateway-adapter': 'redkern-gateway-adapter',
  'pod-gateway-worker-in': 'redkern-gateway-worker-in',
  'pod-gateway-worker-out': 'redkern-gateway-worker-out',
  'pod-gateway-metrics': 'redkern-gateway-metrics'
});

function migrateFlow(flow) {
  if (!Array.isArray(flow)) throw new TypeError('Node-RED flow export must be a JSON array');
  const report = { converted: 0, unchanged: 0, manualReview: [] };
  const migrated = flow.map((source) => {
    if (!source || typeof source !== 'object' || Array.isArray(source)) {
      throw new TypeError('Node-RED flow entries must be objects');
    }
    const type = NODE_TYPES[source.type];
    if (!type) {
      report.unchanged += 1;
      return { ...source };
    }
    const node = { ...source, type };
    report.converted += 1;
    if (Object.hasOwn(node, 'token')) {
      delete node.token;
      report.manualReview.push({ id: source.id, action: 'Re-enter the legacy token using the new Node-RED password credential; plaintext token was removed from the migrated flow.' });
    }
    if (Object.hasOwn(node, 'apiKey')) {
      delete node.apiKey;
      report.manualReview.push({ id: source.id, action: 'Re-enter the legacy API key using the new Node-RED password credential; plaintext API key was removed from the migrated flow.' });
    }
    if (['pod-gateway-call', 'pod-gateway-in', 'pod-gateway-out'].includes(source.type) && source.gateway) {
      node.client = source.gateway;
      delete node.gateway;
    }
    if (source.type === 'pod-gateway-call' && source.timeout !== undefined && node.deadlineMs === undefined) {
      node.deadlineMs = source.timeout;
      delete node.timeout;
    }
    if (source.type === 'pod-gateway-api-config' && source.baseUrl && !source.url) {
      node.url = source.baseUrl;
      delete node.baseUrl;
    }
    if (source.type === 'pod-gateway-server-config') {
      report.manualReview.push({
        id: source.id,
        action: 'Create one or more Gateway Account config nodes and assign explicit operation prefixes; re-enter the legacy server token as a password credential.'
      });
      if (source.redisUrl) {
        try {
          const redisUrl = new URL(source.redisUrl);
          node.redisMode = 'standalone';
          node.redisHost = redisUrl.hostname;
          node.redisPort = Number(redisUrl.port || (redisUrl.protocol === 'rediss:' ? 6380 : 6379));
          node.redisDb = Number(redisUrl.pathname.slice(1) || 0);
          node.redisTls = redisUrl.protocol === 'rediss:';
          delete node.redisUrl;
          if (redisUrl.username || redisUrl.password) {
            report.manualReview.push({
              id: source.id,
              action: 'Re-enter the legacy Redis password through the Gateway Server password credential; it is not copied from redisUrl.'
            });
          }
        } catch {
          report.manualReview.push({ id: source.id, action: 'Set Redis mode and connection fields manually; the legacy redisUrl was invalid.' });
        }
      }
    }
    if (source.type === 'pod-gateway-api-config') {
      report.manualReview.push({
        id: source.id,
        action: 'Verify API URL/header mapping and re-enter API credentials as the new token password credential.'
      });
    }
    return node;
  });
  return { flow: migrated, report };
}

module.exports = { migrateFlow, NODE_TYPES };
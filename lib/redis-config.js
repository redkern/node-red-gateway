'use strict';

function parseRedisNodes(value) {
  let nodes;
  try {
    nodes = JSON.parse(value);
  } catch {
    throw Object.assign(new Error('Redis Cluster nodes must be a JSON array of { host, port }'), { code: 'CONFIG_INVALID' });
  }
  if (!Array.isArray(nodes) || nodes.length === 0 || nodes.some((node) =>
    !node || typeof node.host !== 'string' || node.host.trim() === '' ||
    !Number.isInteger(node.port) || node.port < 1 || node.port > 65535
  )) {
    throw Object.assign(new Error('Redis Cluster nodes must contain valid hosts and ports'), { code: 'CONFIG_INVALID' });
  }
  return nodes.map(({ host, port }) => ({ host: host.trim(), port }));
}

module.exports = { parseRedisNodes };
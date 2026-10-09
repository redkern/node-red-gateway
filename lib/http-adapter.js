'use strict';

const PROTECTED_HEADERS = /^(?:authorization|cookie|host|content-length|proxy-authorization|x-api-key|x-auth-token)$/i;

function createHttpAdapter(api, fetchImpl = fetch) {
  return async function execute(message, context = {}) {
    const request = message._request && typeof message._request === 'object' ? message._request : {};
    const params = request.params || {};
    const urlTemplate = api.url.replace(/\{([A-Za-z0-9._:-]+)\}/g, (_match, name) => {
      if (params[name] === undefined || params[name] === null) {
        throw Object.assign(new Error(`Missing URL parameter: ${name}`), { code: 'REQUEST_VALIDATION_FAILED' });
      }
      return encodeURIComponent(String(params[name]));
    });
    let url;
    try { url = new URL(urlTemplate); } catch {
      throw Object.assign(new Error('API URL is invalid'), { code: 'ADAPTER_INVALID' });
    }
    for (const [name, value] of Object.entries(request.query || {})) {
      for (const item of Array.isArray(value) ? value : [value]) {
        if (item !== undefined && item !== null) url.searchParams.append(name, String(item));
      }
    }

    const headers = new Headers();
    for (const [name, value] of Object.entries(request.headers || {})) {
      if (PROTECTED_HEADERS.test(name)) continue;
      if (!/^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/.test(name) || /[\r\n]/.test(String(value))) {
        throw Object.assign(new Error('Request header is invalid'), { code: 'REQUEST_VALIDATION_FAILED' });
      }
      if (value !== undefined && value !== null) headers.set(name, String(value));
    }
    if (api.token) headers.set(api.tokenHeader, api.tokenHeader.toLowerCase() === 'authorization' ? `Bearer ${api.token}` : api.token);
    if (api.forwardIdempotencyKey) {
      const idempotencyKey = context.idempotencyKey || message.idempotencyKey || context.requestId;
      if (idempotencyKey) headers.set('Idempotency-Key', String(idempotencyKey));
    }

    const method = api.method;
    const body = request.body === undefined ? message.payload : request.body;
    if (!['GET', 'HEAD'].includes(method) && body !== undefined && !headers.has('content-type')) {
      headers.set('content-type', 'application/json');
    }
    const controller = new AbortController();
    const parentSignal = context.signal;
    const onParentAbort = () => controller.abort();
    if (parentSignal?.aborted) controller.abort();
    else parentSignal?.addEventListener('abort', onParentAbort, { once: true });
    const timeout = setTimeout(() => controller.abort(), api.timeoutMs);
    try {
      const response = await fetchImpl(url, {
        method,
        headers,
        signal: controller.signal,
        ...(['GET', 'HEAD'].includes(method) || body === undefined ? {} : { body: JSON.stringify(body) })
      });
      const chunks = [];
      let size = 0;
      if (response.body) {
        const reader = response.body.getReader();
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          size += value.byteLength;
          if (size > api.maxResponseBytes) {
            await reader.cancel();
            throw Object.assign(new Error('Upstream response exceeds configured limit'), { code: 'RESULT_TOO_LARGE' });
          }
          chunks.push(Buffer.from(value));
        }
      }
      const text = Buffer.concat(chunks).toString('utf8');
      let payload = text;
      if (response.headers.get('content-type')?.includes('json') && text) {
        try { payload = JSON.parse(text); } catch {
          throw Object.assign(new Error('Upstream returned invalid JSON'), { code: 'UPSTREAM_ERROR' });
        }
      }
      if (!response.ok) {
        const retryAfter = response.headers.get('retry-after');
        const retryAfterSeconds = retryAfter === null ? undefined : Number(retryAfter);
        const retryAfterDate = retryAfter !== null && !Number.isFinite(retryAfterSeconds) ? Date.parse(retryAfter) : NaN;
        throw Object.assign(new Error(`Upstream returned HTTP ${response.status}`), {
          code: 'UPSTREAM_ERROR',
          retryable: response.status === 429 || response.status >= 500,
          ...(Number.isFinite(retryAfterSeconds) ? { retryAfterMs: Math.max(0, retryAfterSeconds * 1000) }
            : Number.isFinite(retryAfterDate) ? { retryAfterMs: Math.max(0, retryAfterDate - Date.now()) } : {}),
          statusCode: response.status,
          responseBody: payload
        });
      }
      return { payload, statusCode: response.status };
    } catch (error) {
      if (error.name === 'AbortError') throw Object.assign(new Error('Upstream request timed out'), { code: 'UPSTREAM_TIMEOUT', retryable: true });
      throw error;
    } finally {
      clearTimeout(timeout);
        parentSignal?.removeEventListener('abort', onParentAbort);
    }
  };
}

module.exports = { createHttpAdapter };
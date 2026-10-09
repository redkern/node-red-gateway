# @redkern/node-red-gateway

Node-RED palette for authenticated WebSocket calls to locally registered Gateway workers.

> **Release preparation:** package manifest is `1.0.0`; the stable artifact has not yet been tagged or published. Implemented: Node-RED palette, protocol 1.0/1.1, kit/ioredis shared Redis, GCRA multi-limit reservations and capacity queries, coalesced rate decisions, per-operation normal/bulk streams, delayed scheduling/retry matrix, configurable delivery ceiling and bounded DLQ retention, queue depth/byte bounds, configurable footprint estimate, operation deadlines, HTTP adapter, workers, Redis policy/watermark, lease/fencing, bounded drain, PEL reclaim, executor grace, loopback preStop and token-auth metrics on the shared kit listener, receipt-protected legacy queue/retry migration with reports and guarded rollback for unexecuted work, non-destructive legacy flow migration CLI, sync idempotency/result resume and async per-client/per-operation Redis result streams with ACK, reconnect grace and PEL transfer. Production release gates use 1500 RPS minimum, p95 <=500 ms and error rate <=0.1%. Do not use this package as a production replacement for `@yroshcha/node-red-contrib-pod-gateway` until the tagged release workflow passes.

## Requirements

- Node.js 22 or newer
- Node-RED 4.1.x or 5.x
- One Gateway server-config flow instance and at least one account with a token
- For POD flows, a client-config with a token accepted by the account

Install with:

```sh
cd ~/.node-red
npm install @redkern/node-red-gateway
```

The package supports Node-RED `>=4.1.0 <6.0.0`. The package uses the `node-red` npm keyword; the tarball whitelist and single entrypoint are checked by `npm run check:pack` (GW-OPS-2). Gateway depends on published `@redkern/node-red-kit@^1.0.1`.

## Nodes

- **Gateway Server** is a configuration node that binds the WebSocket listener. Its `host` is required; there is no implicit `0.0.0.0` bind. Optional TLS credentials enable `wss://`.
- **Gateway Account** binds a client name to operation prefixes and one or two rotating password credentials. `billing/*` covers nested operations, a bare operation is exact, and `*` grants all operations.
- **Gateway Client** holds the POD URL, `POD_ID`, and token. Protocol 1.1 sends the token in the WebSocket `Authorization` upgrade header and generates a fresh `sessionId` for each runtime start. If an old Gateway replies `UNSUPPORTED_PROTOCOL`, the client retries as protocol 1.0.
- **Gateway Call** invokes one operation. The first output carries the response with input message properties preserved; the second output handles errors.
- **Gateway Out/In** send an event and receive its operation result through client/operation Redis Streams. Delivery is at-least-once; `in` ACKs after handing the message to its Node-RED flow, and same-session reconnect replays pending results.
- **Gateway API Config/Adapter** store upstream settings and credentials, then register one local HTTP executor for a `domain/action` operation.
- **Gateway Worker In/Out** register a local `domain/action` operation. Connect worker-in to the shared flow and end the path with a matching worker-out. Worker calls are at-least-once across process failures; business effects should be idempotent.
- **Gateway Metrics** emits filtered `request.processing`, `upstream.completed`, and `request.completed` lifecycle events in the legacy `msg.metric` envelope. Prometheus snapshots remain available from the token-protected internal endpoint.

Configure Redis on Gateway Server. Standalone mode uses `redisHost`, `redisPort`, and `redisDb`; Cluster mode uses a JSON seed list in `redisNodes`. `redisKeyPrefix` defaults to `pod-gateway` and preserves the legacy GCRA key layout. `volatile-*` requires `allowEvictingRedis`; `allkeys-*` is rejected. Under volatile policies, `INFO stats` is sampled every `redisEvictionSampleMs` (default 15 seconds); new evictions emit a warning and increment `redkern_gateway_redis_evicted_keys_total`. Queue bytes are estimated as `payloadBytes * queueBytesFactor + queueEntryOverheadBytes` (defaults `1.5` and `128`); tune these against measured workloads before production.

Lease defaults are 15 seconds with a 5 second safety margin and 5 second renew interval. Sync idempotency defaults to a one-hour dedup window and a two-minute result body buffer; async results default to one-hour retention. Terminal queue failures are stored in per-operation Redis Streams with configurable `maxDeliveries`, `dlqRetentionMs`, and `maxDlqEntries`; `redkern_gateway_dead_letter_total` reports writes by operation. Legacy migration uses per-operation Redis receipts to avoid duplicating tasks if startup fails after destination write but before source ACK. `redkern_gateway_legacy_migration_complete` and `redkern_gateway_legacy_migration_retained_entries` expose its startup report. The loopback rollback mode restores only tasks with no durable `started` marker and refuses irreversible work. `closeBudgetMs` defaults to 12 seconds so Node-RED can complete its close handler within its 15 second lifecycle limit.

Retry policy retries connection failures that occur before send and HTTP 429 for all operation classes. Post-send transient errors require `safe` or `idempotent`; `never` opts out of those ambiguous retries. The delivery ceiling applies to scheduled retries and Redis PEL redeliveries; tasks exceeding it are written to the bounded dead-letter stream.

The Gateway never accepts remote executor registration. Capabilities are filtered to the authenticated account's prefixes. Requests fail fast while the POD client is disconnected.

## Configuration and security

Configure the server and account in the central Node-RED flow. Configure a client node in each POD flow. Tokens, next tokens, TLS key, and TLS certificate are Node-RED password credentials and are not exported in flow JSON. Never put a token in a URL query parameter.

The server exposes minimal unauthenticated probe responses on its WebSocket port:

- `GET /healthz/live`
- `GET /healthz/ready`

The kit internal listener defaults to port `9552`. Its token-protected metrics endpoint is `/redkern/gateway/metrics`; configure its token with `REDKERN_GATEWAY_METRICS_TOKEN` and override the port with `REDKERN_GATEWAY_INTERNAL_PORT`. The preStop endpoint shares that listener at `POST http://127.0.0.1:9552/redkern/gateway/prestop`; it uses the peer socket address for loopback-only authorization and accepts a JSON body such as `{"mode":"complete"}`. Supported modes are `complete`, `graceful`, `off`, and `rollback`.

Use TLS termination at an ingress/service mesh or configure the TLS credentials directly. Bind to a private interface when TLS is terminated upstream.

## Message contract

`call` reads `msg.payload` and `msg.redkern.gateway`. It preserves unrelated input properties and writes Gateway metadata to `msg.redkern.gateway` (`requestId`, `operation`, `idempotencyKey`, `deadline`, and `attempt`). Configured queue, execution, and total deadline budgets are in milliseconds; values on the message may only narrow them.

`out` submits an async event and returns after queue acceptance. `in` subscribes to its configured operation, adds `{requestId, operation}` to `msg.redkern.gateway`, and attaches terminal errors as `msg.redkern.gateway.error`; it acknowledges the result stream after the flow handoff.

Worker-in emits the complete request message with `msg.redkern.gateway` correlation metadata. A matching worker-out returns its message as the response. Node-RED Catch nodes can handle call-node failures from the error output.

Gateway Metrics emits metadata-only messages on `msg.topic` (`pod-gateway/<event>`), with the complete event in both `msg.payload` and `msg.metric`; configure a comma-separated event filter or leave it empty for all events. Request completion includes queue, rate-limit, upstream, delivery, and total timings. The node emits no request bodies, headers, or credentials.

## Examples and compatibility

Import `examples/basic-call.json` from the Node-RED import menu and set the client URL, POD ID, and token. The frozen protocol 2.1.x hello/call/result and idempotency JSON shapes used by compatibility tests are in `test/fixtures/legacy/`.

## Migration

Run `redkern-gateway-migrate --input old-flow.json --output migrated-flow.json` to convert recognized `@yroshcha/node-red-contrib-pod-gateway` node type names and references. The source file is never modified and an existing output is never overwritten. Review the JSON report and re-enter Node-RED password credentials manually; plaintext legacy token/API key fields are removed. Server account prefixes, provider-specific API settings, and any unrecognized custom nodes require review before import. To roll back a flow conversion, discard the generated output and continue using the untouched source. To roll back Redis queue migration, first request `mode: "rollback"` on the loopback preStop endpoint; it restores only queued/delayed legacy work after graceful drain. If any migrated task may have started execution, rollback is refused and reports it as irreversible.

## Development

```sh
npm ci
npm test
npm run test:integration
npm run test:load
npm run check:pack
npm run verify:requirements
npm run check:release
npm audit --omit=dev
```

`test:load` prints end-to-end requests/sec, p50/p95/p99 latency, and error rate. Set `GATEWAY_LOAD_MIN_RPS`, `GATEWAY_LOAD_MAX_P95_MS`, and/or `GATEWAY_LOAD_MAX_ERROR_RATE_PERCENT` to enforce workload acceptance values from the deployment SLO; unset thresholds are reported but not guessed by the package.

The CI matrix is configured and locally verified for Node.js 22 and 24 with Node-RED 4.1.x and 5.x; each combination passed 58 unit and 32 Redis integration tests. The kit TLS Cluster/Toxiproxy gate also passed. A 5,000-request/100-concurrency local load run measured 1,616 RPS, p95 84 ms, and 0% errors against the release thresholds; CI reruns the same SLO gate. The package tarball, requirement-reference, and production dependency audit checks pass. See [RELEASING.md](RELEASING.md) for the npm Trusted Publishing setup and tag procedure. The ambient Node 20.15 runtime is below the package engine requirement; Node 22/24 were used for verification.

## License

Apache-2.0. See [LICENSE](LICENSE).
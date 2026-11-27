# Changelog

## 1.0.8 - 2026-11-27

- Read queued tasks from Redis Streams in batches up to free executor capacity, reducing round trips while preserving normal/bulk fairness.

## 1.0.7 - 2026-11-21

- Upgrade to `@redkern/node-red-kit@^1.1.0`. Kit 1.1.0 rejects loopback and token routes on one internal listener, so the internal port now has two modes:
  - no `REDKERN_GATEWAY_METRICS_TOKEN`: the listener is loopback-only and `POST /redkern/gateway/prestop` works without a token, as in 1.0.5; metrics go through the kit metrics source (prometheus palette);
  - `REDKERN_GATEWAY_METRICS_TOKEN` set: `/metrics` and `/prestop` both need bearer tokens; set `REDKERN_GATEWAY_PRESTOP_TOKEN` for preStop, otherwise the preStop route is off and a warning is logged.
- Fixed: a missing metrics or preStop token no longer disables the whole palette with `SECRET_REQUIRED`.
- Gateway 1.0.5 installed without a lockfile resolves kit 1.1.0 and fails at startup; upgrade to 1.0.7.
- `v1.0.6` was tagged but never published to npm; it disabled the palette unless both tokens were set.

## 1.0.5 - 2026-11-02

- Recover abandoned async-result PEL entries when a session reconnects after the grace window, and distribute new results across live sessions.
- Bound per-session unacknowledged results and WebSocket output buffers; close persistently slow consumers with `SLOW_CONSUMER`.
- Restore async-result stream retention after Gateway restart and buffer protocol 1.1 sync results even without an idempotency key.
- ACK failed protocol 1.1 results so their Redis buffers are removed after delivery.

## 1.0.4 - 2026-10-12

- Place Node-RED node icons beside their runtime/editor files so the editor resolves the custom SVGs.

## 1.0.3 - 2026-10-12

- Replace repeated Gateway bell artwork with distinct node-specific icons and center the adapter plug.

## 1.0.2 - 2026-10-09

- Add example flows covering every Gateway runtime node and enforce example coverage during package checks.
- Improve palette and workspace node labels, editor tabs, and port labels.

## 1.0.1 - 2026-10-09

- Publish through GitHub Actions using npm trusted publishing.

## 1.0.0 - 2026-10-09

- Add GCRA capacity snapshots, protocol 1.1 capacity queries, and coalesced rate-limit decisions.
- Add configurable delivery ceilings, bounded dead-letter streams, and terminal-failure metrics.
- Preserve legacy Gateway Metrics message events and route asynchronous failures through durable result streams.
- Make legacy Redis queue migration receipt-protected, retryable after partial startup failure, and observable.
- Add loopback-only preStop on the kit internal listener and configurable queue footprint estimates.
- Upgrade the shared runtime kit to 1.0.1 for mixed local-only and token-authenticated internal routes.
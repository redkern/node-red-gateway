# Changelog

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
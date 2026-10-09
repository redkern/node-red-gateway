# Changelog

## 1.0.1 - 2026-10-09

- Publish through GitHub Actions using npm trusted publishing.

## 1.0.0 - 2026-10-09

- Add GCRA capacity snapshots, protocol 1.1 capacity queries, and coalesced rate-limit decisions.
- Add configurable delivery ceilings, bounded dead-letter streams, and terminal-failure metrics.
- Preserve legacy Gateway Metrics message events and route asynchronous failures through durable result streams.
- Make legacy Redis queue migration receipt-protected, retryable after partial startup failure, and observable.
- Add loopback-only preStop on the kit internal listener and configurable queue footprint estimates.
- Upgrade the shared runtime kit to 1.0.1 for mixed local-only and token-authenticated internal routes.
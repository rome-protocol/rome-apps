# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Removed
- **op-geth deployment mode**: the hybrid op-geth / Engine API deployment and the Rhea
  mempool relay service are retired. Single-state mode, with the Proxy as the JSON-RPC
  endpoint, is the only supported deployment.

### Changed
- **rome-via-enrich**: the throughput-record Seed and hourly Backstop rebuild the record by paging 50,000 blocks at a time instead of loading every block in one query (15.6 GB on Hadrian, over the 9.6 GB container cap, so the record was never rebuilt); the result is the same record, written once at the end, and every page (incremental and rebuild) now ends on a complete block number.
- **rome-via-enrich**: the throughput-record worker resumes from its saved cursor on restart instead of rescanning every block of the chain (which reached 15.6 GB on Hadrian and crash-looped under the container cap), and each incremental poll reads at most 50,000 blocks so a stale cursor catches up a page at a time. The periodic full recompute is timed from process start.
- **rome-via-api**: the action-tag classifier headlines the gas-wrapper precompile
  legs — `HelperProgram.deposit_from_ata(uint256)` tags `unwrap` and
  `Withdraw.withdraw_to_ata/withdraw_to_pda(uint256)` tag `wrap` (each ahead of the
  generic `helper_call` / `withdraw_precompile` tag), so direct wrap/unwrap calls no
  longer render as a bare "Helper"/"Withdraw" chip with an em-dash amount. These legs
  emit no logs and no value, so the resolved sub-method is the only classification
  signal. The Rome Via frontend mirrors the same mapping.
- **proxy**: rebuilt against rome-sdk `main` to pick up the iterative-transaction fixes:
  unified holder transmit-skip (fixes a double transmit on the iterative path), iterative
  grow-loop opcode tuning (`OPCODE_START`/`OPCODE_INCREMENT` = 1500), `EXTRA_HEAP`
  1024→3072, and a level-gated grow-loop trace.
- **hercules**: rebuilt against rome-sdk `main` to pick up per-leg Solana ordinal
  persistence and execution-order reconstruction (`get_sol_legs_for_evm_tx`, migration
  `2026-06-20_sol_tx_ordinal`).
- **rome-via**: sqlx Postgres connections now negotiate TLS. The workspace `sqlx`
  dependency enables `tls-rustls`, so rome-via-{api,sync,enrich} (and cardo-service) honor
  `sslmode=` on their connection URLs instead of connecting in plaintext. The libpq-based
  clients (proxy and hercules via diesel) already negotiated TLS.

### Fixed
- **hercules**: provider URLs no longer leak in logs. The `MultiplexedSolanaClient`
  polling task printed the full configured URL on every state transition, exposing
  query-string API keys used by some Solana RPC providers. Both `client.url()` and the
  URL embedded in `reqwest::Error`'s Display are now redacted at every log site.
- **hercules**: the indexer no longer crashes with `No providers for slot N` when the
  Solana RPC client's first `getFirstAvailableBlock` probe loses the race to the rollup
  indexer's first block fetch (typical when networking is not ready in the first second
  after a container starts). `MultiplexedSolanaClient::get_providers` now waits up to 30 s
  for the polling task to populate `first_available_slot`, and `StandaloneIndexer` starts
  the block loader before the rollup indexer. Initial-probe failures are now logged
  explicitly.

### Added
- Issue templates for bug reports and feature requests.
- `VERSION` files for the proxy, hercules and cli services.
- `SECURITY.md` and `CONTRIBUTING.md`.

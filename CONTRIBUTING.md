# Contributing

## Pull requests

This repository is **source-available**, with all rights reserved (see [LICENSE](LICENSE)).
**External pull requests are not accepted at this time** and will be closed without review.

## Issues

Bug reports are welcome. Please open a GitHub issue using the bug report template and
include:

- the component (proxy, hercules, cli, rome-via-*, rome-audit, cardo-service),
- the version, image tag or commit,
- the relevant configuration with secrets removed,
- steps to reproduce, and the expected and actual behavior.

Feature requests are welcome too, but may not be acted on.

**Security vulnerabilities must not be reported as issues.** Follow [SECURITY.md](SECURITY.md).

---

## Developer guide

The rest of this document describes how the codebase is organized and how changes are
validated. It is written for the Rome Protocol team and for anyone reading the code.

### Workspace layout

| Crate | Package name | Role |
|---|---|---|
| `proxy/` | `proxy` | Ethereum JSON-RPC endpoint |
| `hercules/` | `hercules` | Solana-to-Ethereum block indexer |
| `cli/` | `cli` | Admin CLI |
| `rome-via-api/` | `rome-via-api` | Explorer REST API |
| `rome-via-sync/` | `rome-via-sync` | Hercules DB → `rome_via_db` mirror (base migrations) |
| `rome-via-enrich/` | `rome-via-enrich` | Enrichment workers (derived migrations) |
| `rome-via-classify/` | `rome-via-classify` | Shared classification library |
| `rome-audit/` | `rome-audit` | Compliance audit-trail pipeline |
| `cardo-service/` | `cardo-service` | Manifest catalog: REST + MCP |
| `dammv1-pool-constructor/` | (npm) | Meteora DAMM v1 devnet pool script; not in the workspace |

All services depend on `rome-sdk` (and through it `rome-evm` and `mollusk`) from sibling
checkouts; see the [README](README.md#sibling-repositories). All services use `rome-obs`
for OpenTelemetry; new code paths should emit spans.

Background reading:

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — the explorer pipeline and its invariants.
- [`docs/SOLANA_ORIGIN_TX_PARITY.md`](docs/SOLANA_ORIGIN_TX_PARITY.md) — Solana-origin
  transactions on `eth_*`.
- [`proxy/docs/PROXY.md`](proxy/docs/PROXY.md) — Proxy request paths, lanes, batching and
  tuning.

### Build and test

```sh
cargo build --release                 # all binaries
cargo build --workspace               # quick check that everything compiles
cargo test --workspace                # see README "Testing" for Postgres-backed tests
cargo clippy --workspace
```

The Rust toolchain is pinned to **1.93.1** in `rust-toolchain.toml`. The Docker builder
image (`rust:<version>-slim-trixie` in `docker/Dockerfile`) must use the same compiler
version. **Bump both together**, never one without the other. The Docker build compiles
with `RUSTFLAGS="-D warnings"`, so new warnings fail the image build.

### Design rules

These are the invariants reviewers check for:

- **Standard JSON-RPC stays standard.** Don't change the shape of any `eth_*` method;
  Rome-specific behavior goes under `rome_*` (or the standard `debug_*` namespace).
- **A returned transaction hash means Solana confirmation.** Never add a path that
  returns from `eth_sendRawTransaction` before the transaction is confirmed.
- **Opt-in features stay default-off.** Jito bundle routing (`jito_bundler`) and mempool
  batching (`batching`) must remain opt-in, with plain Solana RPC submission as the
  default. Struct literals for these configs use `..Default::default()` so new fields don't
  break call sites.
- **Read/write lane isolation.** Read emulation (`read_pool_enabled`, on by default) must
  never share or drain the write-lane pool. Take care in `proxy/src/api/` and
  `proxy/src/read_pool.rs`.
- **Bounded resources.** Queues, filter registries and connection counts must be bounded;
  fail closed under load instead of growing memory.
- **Emulator backend.** The production image ships only the in-process emulator. Don't add
  `emulator-rpc` to `docker/Dockerfile` unless an emulator RPC service exists to route to.
- **Single-state only.** The op-geth / Engine API deployment and the Rhea relay service
  are retired; don't reintroduce a hybrid path. ("Rhea", "Remus" and "Romulus" remain the
  names of the transaction types used by the explorer's cross-chain classification.)
- **Classification has one home.** Action tags, cross-VM seams, status / revert reasons and
  `ORACLE_SELECTORS` live in `rome-via-classify`. Never duplicate a rule in
  `rome-via-api` or `rome-via-enrich`.
- **Propagate, don't swallow.** In rome-via-enrich, a failed per-row write must hold the
  worker cursor through `workers/rpc_verdict::valve_persist_cursor` (transient vs terminal
  miss). `let _ = …`, `if let Err(..) { warn!(..) }` or `.unwrap_or_default()` on a write
  is a bug.
- **No faucet or free-mint endpoints** in any service.

### Database migrations

| Crate | Tool | Directory | Version band |
|---|---|---|---|
| hercules (via rome-sdk) | diesel | `rome-sdk/rome-evm-client/src/indexer/pg_storage/migrations` | date-stamped |
| rome-via-sync | sqlx | `rome-via-sync/migrations/` | `0001`–`0099` |
| rome-via-enrich | sqlx | `rome-via-enrich/migrations/` | `0100`–`0899` |
| rome-audit | sqlx | `rome-audit/migrations/` | `0901` and up |
| cardo-service | sqlx | `cardo-service/migrations/` | timestamp-prefixed; use a separate database |

Rules:

- rome-via-sync, rome-via-enrich and rome-audit can share one database and one
  `_sqlx_migrations` table, each running with `ignore_missing = true`. **Version numbers
  must be unique across all three.** Before adding a migration, list the directory and take
  the next free number in the crate's band. A collision aborts startup with
  "migration N was previously applied but has been modified".
- Provide both `.up.sql` and `.down.sql`.
- Never edit a migration that has shipped; add a new one.
- Don't write automatic migrations that rewrite historical data across every chain. Use
  an opt-in maintenance operation instead (see `rome-via-enrich maintenance`).
- The `sol_block` mirror in `rome_via_db` was dropped on purpose; don't reintroduce it.
  (The Hercules DB's own `sol_block` table is unrelated and still used.)

### Change impact map

| If you change… | Also check / update… |
|---|---|
| Proxy RPC handlers | End-to-end EVM and Uniswap suites |
| Proxy `batching` config or coalescer | `proxy/src/{batching,batcher}.rs` unit tests and an end-to-end run |
| Proxy `eth_sendRawTransaction` return path | Preserve the hash-means-Solana-confirmation invariant |
| Proxy bundle routing (`send_transaction_via_bundle`) | `rome-sdk` `rome-evm-client` and `rome-jito-bundler`; keep it default-off |
| Proxy `/metrics` format | Dashboards that consume `rome_proxy_rpc_*` |
| Hercules indexer logic | End-to-end state-comparison suites |
| CLI commands | Manual verification (there are no automated CLI tests) |
| `rome-sdk` APIs | `cargo build --workspace`: every service must compile |
| Docker entrypoint or configuration | Any compose files or deployment templates that use the same image |
| Solana base image tag | `romeprotocol/agave-validator` pin in `docker/Dockerfile` |
| Rust toolchain version | `rust-toolchain.toml` **and** the Dockerfile `rust:<version>-slim-trixie` base |
| rome-via-sync schema / migrations | rome-via-enrich (reads sync tables), rome-via-api (reads both), rome-audit (shares the migrations table) |
| rome-via-enrich migrations | Next free number in the band; check the directory first |
| rome-via-enrich workers | rome-via-api derived-data queries; supervised restart behavior on panic |
| rome-via-enrich cursor / write path | Route through `rpc_verdict::valve_persist_cursor`; never advance past a failed write |
| rome-via-api endpoints | OpenAPI annotations (`/api/v1/docs`) and the Rome Via frontend |
| Classification rules (action tags, seams, status, oracle selectors) | `rome-via-classify` only; both consumers pick it up |
| `ORACLE_SELECTORS` | `rome-via-classify/src/lib.rs`, then confirm `method_decoder` seeds the selector |
| Address-kind classification | `rome-via-api/src/api/address_kind.rs` **and** the frontend resolver |
| Meta-Hook Router log format | rome-via-enrich `hook_executions` and `meta_hook_indexer` |
| `DoTxBatch` trace format | rome-via-enrich `batch_trace` writer and proxy `debug_traceRomeTransaction` |
| rome-audit event registry / ABI | `rome-audit/src/registry.rs` and its replay tests |
| cardo-service manifest schema | cardo-service REST routes and MCP tools |

### Test selection

| What changed | Run |
|---|---|
| proxy | `cargo test -p proxy`, plus the end-to-end EVM and Uniswap suites |
| hercules | `cargo test -p hercules`, plus the end-to-end state-comparison suites |
| cli | `cargo test -p cli` and manual verification |
| rome-via-api | `cargo test -p rome-via-api` (set `HERCULES_TEST_DATABASE_URL` for DB tests) |
| rome-via-sync | `cargo test -p rome-via-sync` (set `VIA_SYNC_TEST_DATABASE_URL` for DB tests) |
| rome-via-enrich | `cargo test -p rome-via-enrich` (`--lib` for unit tests only) |
| rome-via-classify | `cargo test -p rome-via-classify`, plus rome-via-api and rome-via-enrich |
| rome-audit | `cargo test -p rome-audit` (needs Postgres; see `ROME_AUDIT_TEST_PG_ADMIN_URL`) |
| cardo-service | `cargo test -p cardo-service` |
| anything shared | `cargo build --workspace` and `cargo test --workspace` |

The end-to-end suites run against Docker images built from this repository and its
siblings.

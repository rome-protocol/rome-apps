# Rome Apps

**Rome Apps** is the off-chain service layer of [Rome Protocol](https://romeprotocol.xyz), an
EVM-compatible rollup whose execution runs on Solana. It lets standard Ethereum tooling
(MetaMask, ethers.js, viem, web3.py, Hardhat, Foundry) talk to contracts that execute on
Solana through the Rome EVM program, and it powers the Rome Via block explorer.

The workspace contains:

- an Ethereum JSON-RPC endpoint (**Proxy**),
- a Solana-to-Ethereum block indexer (**Hercules**),
- an administrative command-line tool (**CLI**),
- the **Rome Via** explorer backend (sync, enrichment and REST API services),
- a compliance audit-trail pipeline (**rome-audit**),
- a manifest catalog service (**cardo-service**).

> This repository is **source-available** (all rights reserved). See [License](#license).

## Contents

- [Components](#components)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Building](#building)
- [Testing](#testing)
- [Docker](#docker)
- [Configuration](#configuration)
- [API overview](#api-overview)
- [Observability](#observability)
- [Production deployment notes](#production-deployment-notes)
- [Repository structure](#repository-structure)
- [Contributing](#contributing)
- [Security](#security)
- [License](#license)

---

## Components

The Cargo workspace has nine members, plus one standalone TypeScript tool.

| Crate | Kind | Purpose | Default port |
|---|---|---|---|
| [`proxy`](proxy/README.md) | binary | Ethereum JSON-RPC endpoint (`eth_*`, `net_*`, `web3_*`, plus `rome_*` extensions). Submits transactions to Solana and serves blocks/receipts from the Hercules database. | `:9090` (JSON-RPC), optional `/metrics` sidecar |
| [`hercules`](hercules/README.md) | binary | Indexes Rome EVM transactions from Solana blocks and stores them as Ethereum blocks, transactions and receipts in PostgreSQL. | `:8000` admin RPC (bind to localhost) |
| [`cli`](cli/README.md) | binary | Admin tool for the Rome EVM program: rollup registration, deposits, state queries, treasury and payer-resource management. | — |
| `rome-via-api` | binary | REST API (`/api/v1`) and SSE streams for the Rome Via explorer, reading `rome_via_db`. OpenAPI UI at `/api/v1/docs`. | `:8090` |
| `rome-via-sync` | binary | Mirrors chain data from the Hercules database into `rome_via_db`, decoding RLP into denormalized columns. | `:8091` (health) |
| `rome-via-enrich` | binary | Supervised enrichment workers that compute derived explorer tables (tokens, holders, labels, cross-chain classification, hooks, throughput). | `:8092` (health) |
| `rome-via-classify` | library | Shared transaction classification (action tags, cross-VM seams, status / revert reason, oracle selectors) used by both `rome-via-api` and `rome-via-enrich`. | — |
| `rome-audit` | binary | Compliance audit-trail pipeline: reads finalized Hercules data, decodes registered EVM events through an ABI registry (unregistered events are rejected loudly), and writes an append-only event record plus rebuildable derived tables to PostgreSQL. Optional HMAC-authenticated overlay ingest endpoint. | `:8093` (health) |
| `cardo-service` | binary | Manifest catalog with a REST API and an MCP server (stdio and Streamable HTTP): task registry and unsigned Solana transaction builder. | `:8080` |
| [`dammv1-pool-constructor`](dammv1-pool-constructor/README.md) | TypeScript script | Creates a Meteora DAMM v1 pool on Solana devnet (used for gas-price pools in testing). Not part of the Cargo workspace. | — |

Per-service `VERSION` files exist for `proxy`, `hercules` and `cli`. Release notes are in
[`CHANGELOG.md`](CHANGELOG.md).

---

## Architecture

Rome Apps runs in **single-state mode**. Users talk to the Proxy directly. The Proxy
submits transactions to Solana and serves read data. Hercules indexes Solana blocks into
PostgreSQL.

```mermaid
graph TB
    subgraph Clients["Ethereum clients"]
        W[Wallets / ethers.js / viem / Hardhat]
    end

    subgraph RomeApps["Rome Apps"]
        PROXY["Proxy<br/>JSON-RPC :9090"]
        HERCULES["Hercules<br/>block indexer"]
        CLI["CLI"]
        SYNC["rome-via-sync"]
        ENRICH["rome-via-enrich"]
        API["rome-via-api :8090"]
    end

    subgraph Storage["PostgreSQL"]
        HDB[("Hercules DB")]
        VDB[("rome_via_db")]
    end

    subgraph Solana["Solana"]
        RPC["Solana RPC"]
        EVM["Rome EVM program"]
    end

    W -- "eth_* / rome_*" --> PROXY
    PROXY -- "submit + confirm" --> RPC
    RPC --> EVM
    PROXY -- "blocks, receipts, logs" --> HDB
    HERCULES -- "fetch blocks" --> RPC
    HERCULES -- "write blocks/txs" --> HDB
    SYNC -- "mirror" --> HDB
    SYNC --> VDB
    ENRICH --> VDB
    ENRICH -- "eth_call" --> PROXY
    API --> VDB
    CLI --> RPC
```

### Transaction flow

```mermaid
sequenceDiagram
    participant User as Ethereum client
    participant Proxy
    participant Solana
    participant Hercules
    participant DB as Hercules DB

    User->>Proxy: eth_sendRawTransaction(rlp)
    Proxy->>Solana: emulate, submit Solana transaction(s)
    Solana-->>Proxy: confirmation
    Proxy-->>User: tx hash (already confirmed on Solana)

    loop every Solana slot
        Hercules->>Solana: fetch block
        Hercules->>DB: store EVM block / txs / receipts
    end

    User->>Proxy: eth_getTransactionReceipt(hash)
    Proxy->>DB: query receipt
    Proxy-->>User: receipt
```

### Design principles

- **Standard Ethereum JSON-RPC.** A client using ethers.js, viem, web3.py or Hardhat can
  point at the Proxy and every standard method works without Rome-specific changes.
  Standard `eth_*` method shapes are never altered.
- **Rome features live under `rome_*`.** Extensions such as `rome_emulateTx` or
  `rome_getResources` are opt-in additions; they never replace an `eth_*` method.
- **Hercules translates faithfully.** Blocks produced from Rome execution look like Ethereum
  blocks to downstream EVM tooling; no fields that standard parsers would reject.
- **Resource pooling is internal.** Payer keypairs, holder accounts and compute-budget
  reservations are hidden; the external API is a normal "send tx, get receipt" loop.
- **No faucet or free-mint endpoints** in the services.
- **A returned transaction hash means Solana confirmation.** `eth_sendRawTransaction`
  returns only after the transaction is confirmed on Solana, never "accepted but pending".

For the explorer pipeline (Hercules → rome-via-sync → rome-via-enrich → rome-via-api) see
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md). For how Solana-originated transactions
appear on standard `eth_*` methods see
[`docs/SOLANA_ORIGIN_TX_PARITY.md`](docs/SOLANA_ORIGIN_TX_PARITY.md). For Proxy internals and
tuning see [`proxy/docs/PROXY.md`](proxy/docs/PROXY.md).

---

## Prerequisites

- **Rust 1.93.1**, pinned in [`rust-toolchain.toml`](rust-toolchain.toml) (rustup picks it up
  automatically, together with `rustfmt` and `clippy`).
- **System libraries:** `pkg-config`, `libssl-dev`, `libpq-dev`, `libudev-dev`, `cmake`,
  `protobuf-compiler`, `perl`, a C toolchain (`build-essential`).
  - On macOS, install `libpq` (`brew install libpq`) and, if the linker cannot find it,
    export `LIBRARY_PATH=/opt/homebrew/opt/libpq/lib` (and `DYLD_LIBRARY_PATH` to run).
- **PostgreSQL** for Hercules and the Rome Via services.
- **Solana CLI** for key generation and network interaction (optional).
- **Docker** with BuildKit for container builds (optional).
- **Node.js 18+** only for `dammv1-pool-constructor`.

### Sibling repositories

The workspace uses path dependencies on sibling checkouts, so the build expects this
layout in a single parent folder:

```text
parent/
  rome-apps/
  rome-sdk/
  rome-evm/
  mollusk/     (required by rome-sdk)
```

```sh
mkdir rome && cd rome
git clone https://github.com/rome-protocol/rome-apps.git
git clone https://github.com/rome-protocol/rome-sdk.git
git clone https://github.com/rome-protocol/rome-evm.git
git clone https://github.com/rome-protocol/mollusk.git
```

- `rome-sdk` provides the Rome EVM client, Solana tooling and the indexer engine Hercules
  runs on, plus `rome-obs` (telemetry) and `rome-jito-bundler`.
- `rome-evm` provides the Rome EVM program and emulator that `rome-sdk` compiles for the
  host target.
- `mollusk` is path-depended on by `rome-sdk` and is linked into the shipped binaries.

Use compatible revisions of the three siblings; their default branches are expected to
build together. This release was built and tested with:

| Repository | Revision |
|---|---|
| `rome-sdk` | `8125538` |
| `rome-evm` | tag `audited-mainnet-2026-09-22` (`644cbfd`) |
| `mollusk` | `a1372d3` |

---

## Building

```sh
cd rome-apps

# All workspace binaries (default feature: ci)
cargo build --release

# Network feature flags (pick one)
cargo build --release --features testnet
cargo build --release --features mainnet
```

Binaries are written to `target/release/`: `proxy`, `hercules`, `cli`, `rome-via-api`,
`rome-via-sync`, `rome-via-enrich`, `rome-audit`, `cardo-service`.

### Feature flags

| Feature | Applies to | Effect |
|---|---|---|
| `ci` (default) | all services | Build for the CI / development network configuration. |
| `testnet` | all services | Build for testnet. |
| `mainnet` | all services | Build for mainnet. |
| `emulator-inproc` | proxy, cli | In-process emulator backend. Pulled in by the network features; shipped in the Docker image. |
| `emulator-rpc` | proxy, cli | Adds an RPC emulator backend alongside the in-process one. At runtime, setting `EMULATOR_RPC_URL` routes read-path emulation (`eth_getBalance`, `eth_call`, `eth_getTransactionCount`, `eth_getCode`, `eth_getStorageAt`) to a standalone emulator RPC service; unset keeps the in-process backend. Not included in the Docker image. |

---

## Testing

```sh
cargo test --workspace
```

Unit tests need no external services. Some integration tests need PostgreSQL:

| Crate | Tests | Requirement |
|---|---|---|
| `rome-audit` | `tests/*_db.rs`, `src/migrations_test.rs` | **Required.** Creates and drops a disposable database per test via the admin URL in `ROME_AUDIT_TEST_PG_ADMIN_URL` (default `postgres://postgres:test@localhost:55432/postgres`). These tests fail if Postgres is not reachable. |
| `rome-via-api` | `tests/*_db.rs` | Optional. Run when `HERCULES_TEST_DATABASE_URL` is set; otherwise they print `SKIP` and pass. `audit_events_db.rs` also honors `ROME_AUDIT_TEST_PG_ADMIN_URL`. |
| `rome-via-enrich` | `tests/*_db.rs` | Optional, gated on `HERCULES_TEST_DATABASE_URL`. |
| `rome-via-sync` | DB tests in `src/sync.rs` | Optional, gated on `VIA_SYNC_TEST_DATABASE_URL`. |

A throwaway Postgres for the full suite:

```sh
docker run -d --name rome-test-pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:16
export HERCULES_TEST_DATABASE_URL=postgres://postgres:test@localhost:55432/postgres
export VIA_SYNC_TEST_DATABASE_URL=postgres://postgres:test@localhost:55432/postgres
cargo test --workspace
```

Point these variables only at a disposable database; the tests create and drop schemas and
databases. To run without Postgres, exclude rome-audit:
`cargo test --workspace --exclude rome-audit`.

End-to-end tests (EVM, opcode, state-comparison and Uniswap suites) run against built
Docker images and live in a separate test repository. See
[`CONTRIBUTING.md`](CONTRIBUTING.md) for which tests to run for which change.

---

## Docker

The image builds seven binaries: `proxy`, `cli`, `hercules`, `rome-via-api`,
`rome-via-sync`, `rome-via-enrich` and `rome-audit` (`cardo-service` is not in the image).
It also contains the Solana CLI, `diesel` with the Hercules database migrations
(`/opt/migrations`), and the helper scripts `apply_migrations`,
`cli.sh` and `cli-deploy.sh`. The runtime user is the unprivileged `rome`.

### Build

The build context is the **parent folder** that contains the sibling checkouts listed in
[Sibling repositories](#sibling-repositories). `FEATURE` is required because the build uses
`--no-default-features`:

```sh
cd parent/
DOCKER_BUILDKIT=1 docker build \
  -f rome-apps/docker/Dockerfile \
  --build-arg FEATURE=ci \
  -t rome-apps:local .
```

Pre-built images are published as `romeprotocol/rome-apps:<tag>` on Docker Hub.

### Run

The entrypoint ([`docker/entrypoint.sh`](docker/entrypoint.sh)) executes `/opt/$SERVICE_NAME`,
so **`SERVICE_NAME` is required** and must be one of the binary names above (or a helper
script such as `cli.sh`). Each service reads its configuration from the env var listed in
[Configuration](#configuration).

```sh
# Proxy
docker run --rm \
  -e SERVICE_NAME=proxy \
  -e PROXY_CONFIG=/cfg/proxy-config.yml \
  -v "$PWD/config:/cfg:ro" -v "$PWD/keys:/keys:ro" \
  -p 9090:9090 \
  rome-apps:local

# Hercules (admin RPC stays inside the container network; do not publish it)
docker run --rm \
  -e SERVICE_NAME=hercules \
  -e HERCULES_CONFIG=/cfg/hercules.yml \
  -v "$PWD/config:/cfg:ro" \
  rome-apps:local

# rome-via-api
docker run --rm \
  -e SERVICE_NAME=rome-via-api \
  -e ROME_VIA_API_CONFIG=/cfg/rome-via-api.toml \
  -v "$PWD/config:/cfg:ro" \
  -p 8090:8090 \
  rome-apps:local
```

The entrypoint does not forward container arguments, so run the CLI by overriding the
entrypoint (see [`cli/docker-compose.yml`](cli/docker-compose.yml)):

```sh
docker run --rm --entrypoint ./cli rome-apps:local \
  --program-id <PROGRAM_ID> --url <SOLANA_RPC_URL> get-rollups
```

`SERVICE_NAME=cli.sh` runs the scripted `reg-rollup` / `deposit` flow driven by the
`CHAIN_ID`, `PROGRAM_ID`, `SOLANA_RPC`, `COMMAND` and related environment variables. To apply
the Hercules database migrations, run `/opt/apply_migrations` with `DATABASE_URL` set.

---

## Configuration

Every service takes one configuration file. Its path comes from a `-c <path>` flag or an
environment variable.

| Service | Env var / flag | Format | Reference |
|---|---|---|---|
| proxy | `PROXY_CONFIG` or `-c` | YAML / JSON | [`proxy/README.md`](proxy/README.md), [`proxy/docs/PROXY.md`](proxy/docs/PROXY.md), [`proxy/proxy-config.example.yml`](proxy/proxy-config.example.yml) |
| hercules | `HERCULES_CONFIG` | YAML / JSON | [`hercules/README.md`](hercules/README.md) |
| cli | command-line flags | — | [`cli/README.md`](cli/README.md) |
| rome-via-api | `ROME_VIA_API_CONFIG` or `-c` | TOML | below |
| rome-via-sync | `ROME_VIA_SYNC_CONFIG` or `-c` | TOML | below |
| rome-via-enrich | `ROME_VIA_ENRICH_CONFIG` or `-c` | TOML | below |
| rome-audit | `ROME_AUDIT_CONFIG` or `-c` | TOML | below |
| cardo-service | environment variables | — | below |

### rome-via-sync

| Key | Required | Default | Purpose |
|---|---|---|---|
| `chain_id` | yes | — | Chain ID applied to every mirrored row. |
| `source_db_url` | yes | — | Hercules database (read). |
| `target_db_url` | yes | — | `rome_via_db` (write). Migrations run here on startup. |
| `poll_interval_ms` | no | unset | Base poll interval in ms; overrides `poll_interval_seconds` when set. |
| `poll_interval_seconds` | no | `2` | Base poll interval in seconds. |
| `max_idle_poll_interval_seconds` | no | `30` | Ceiling for the geometric idle backoff. |
| `batch_size` | no | `5000` | Rows per table per drain pass. |
| `health_addr` | no | `0.0.0.0:8091` | Health server. |

### rome-via-enrich

| Key | Required | Default | Purpose |
|---|---|---|---|
| `chain_id`, `db_url`, `rome_evm_program_id` | yes | — | Startup fails if any is missing or empty. |
| `proxy_url` | no | `http://localhost:9090` | Proxy used for `eth_call` / `eth_getCode` reads. A wrong or unreachable URL degrades enrichment silently. |
| `solana_rpc_url` | no | `http://solana:8899` | Solana RPC for transaction log reads. |
| `solana_cluster` | no | `devnet` | Reported as `solChain` on cross-chain rows; set it per network. |
| `health_addr` | no | `0.0.0.0:8092` | Health server. |
| `poll_interval_seconds`, `batch_size` | no | `5`, `500` | Worker polling. |
| `meta_hook_program_id`, `extra_infra_programs`, `cpi_plumbing_programs` | no | — | Cross-chain classifier tuning. |
| `[[contract_labels]]`, `[[program_labels]]` | no | — | Deployment-provided labels for protocol contracts and Solana programs. |
| `fourbyte_enabled`, `abi_seed_dir` | no | `true`, unset | Method-selector resolution sources (4byte.directory fallback, local ABI seeds). |
| `verifier_url` | no | unset | Sourcify-compatible verifier; enables the `verified_labels` worker. |
| `throughput_record_poll_secs`, `cross_vm_seams_poll_secs` | no | `30`, `10` | Worker intervals. |

`rome-via-enrich maintenance <op>` runs a one-shot maintenance operation and exits (dry run
unless `--apply`): `reextract-transfers`, `backfill-balances` (`--token`, `--concurrency`),
`backfill-oracle-flag`.

### rome-via-api

| Key | Required | Default | Purpose |
|---|---|---|---|
| `chain_id` | set explicitly | `121220` | Chain served. |
| `db_url` | yes | — | `rome_via_db`. |
| `bind_addr` | no | `0.0.0.0:8090` | HTTP listener. |
| `pool_max_connections` | no | `20` | sqlx pool size. |
| `cursor_secret` | **set in production** | development placeholder | HMAC key for pagination cursors. |
| `proxy_url` | no | `http://localhost:9090` | Live balance / code lookups. |
| `redis_url` | no | unset | Optional response and RPC-fallback cache. |
| `[foreign_proxies]` | no | empty | `chain_id = "proxy URL"` map for cross-chain lookups. |

### rome-audit

| Key | Required | Default | Purpose |
|---|---|---|---|
| `chain_id`, `source_db_url`, `target_db_url` | yes | — | Hercules source and the database that holds the `audit` schema. |
| `poll_interval_ms` | no | `2000` | Poll interval. |
| `confirmation_lag` | no | `32` | Slots behind head treated as final. |
| `max_slots_per_tick` | no | `1000` | Ingest batch bound. |
| `health_addr` | no | `0.0.0.0:8093` | Health server (reports ingest lag). |
| `[resolve]` | no | — | Token-scoped event-source resolution (`rpc_url`, `tokens`, `[resolve.registry]`, optional discovery). |
| `[overlay]` | no | off | HMAC-authenticated ingest server (`listen_addr`, `ingest_secret`). |

### cardo-service

| Variable | Required | Default |
|---|---|---|
| `CARDO_CDN_URL` | yes | — |
| `CARDO_POSTGRES_URL` | yes | — |
| `CARDO_POLL_INTERVAL_SECS` | no | `300` |
| `CARDO_BIND_ADDR` | no | `0.0.0.0:8080` |
| `CARDO_MCP_MODE` | no | `both` (`both`, `http`, `stdio`, `none`) |
| `CARDO_MCP_ALLOWED_HOSTS` | no | loopback |
| `CARDO_SCHEMA_PATH` | no | `/etc/cardo/catalog.schema.json` |
| `CARDO_KEYS_DIR` | no | `/etc/cardo/keys` |
| `ROME_EVM_RPC_URL`, `ROME_CHAIN_ID`, `ROME_PROGRAM_ID` | yes | — |

The Rome EVM client inside cardo-service is read-only: `/execute` and the MCP `execute` tool
return unsigned transaction material, and signing always happens on the client side.

Database URLs in examples use placeholders such as
`postgres://hercules:<password>@postgres/<db>`. Keep real credentials out of committed
files and inject them through your deployment tooling.

---

## API overview

### Proxy JSON-RPC (`:9090`)

Implements the standard [Ethereum JSON-RPC API](https://ethereum.org/en/developers/docs/apis/json-rpc/):
`eth_chainId`, `eth_blockNumber`, `eth_getBalance`, `eth_getCode`, `eth_getStorageAt`,
`eth_getTransactionCount`, `eth_call`, `eth_estimateGas`, `eth_gasPrice`,
`eth_maxPriorityFeePerGas`, `eth_feeHistory`, `eth_sendRawTransaction`,
`eth_getTransactionByHash` / `…ByBlockHashAndIndex` / `…ByBlockNumberAndIndex`,
`eth_getTransactionReceipt`, `eth_getBlockByNumber` / `…ByHash`, `eth_getBlockReceipts`,
`eth_getBlockTransactionCountByNumber` / `…ByHash`, `eth_getLogs`, filters
(`eth_newFilter`, `eth_newBlockFilter`, `eth_newPendingTransactionFilter`,
`eth_getFilterChanges`, `eth_getFilterLogs`, `eth_uninstallFilter`), uncle methods (always
empty), `eth_syncing`, `eth_accounts`, `net_version`, `net_listening`, `net_peerCount`,
`web3_clientVersion`, `rpc_modules`, `txpool_status`.

Behavior worth knowing:

- `pending` is treated as `latest`; there is no pending block or public mempool.
  `txpool_status` always reports zero.
- `eth_subscribe` / `eth_unsubscribe` over WebSocket support `newHeads` and `logs`
  (poll-backed, about 2 s). `newPendingTransactions` is accepted but never emits.
- `eth_getLogs` block span is unlimited by default; `get_logs_max_block_range` caps it and
  returns the standard `-32005` error when exceeded.
- `baseFeePerGas` on blocks reflects the live gas price, so EIP-1559 fee estimation in
  wallets works.
- `eth_feeHistory` accepts `blockCount` as a JSON number, decimal string or hex string.

Rome extensions: `rome_emulateTx`, `rome_emulateTxWithPayer`, `rome_emulateCallAccounts`,
`rome_emulateRegRollup`, `rome_sendUnsignedTransaction`, `rome_solanaTxForEvmTx`,
`rome_mintId`, `rome_buildInfo`, `rome_isCompatible`, `rome_getResources`, and
`debug_traceRomeTransaction` (the `DoTxBatch` trace recorded by rome-via-enrich).

### Hercules admin RPC

A read-only JSON-RPC status interface on `admin_rpc`: `inSync`, `lastSolanaStorageSlot`,
`lastEthereumStorageSlot`, `lastIndexedSlot`. Bind it to `127.0.0.1` (or a private
interface) and never expose it publicly.

```sh
curl -s -X POST http://127.0.0.1:8000 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"inSync","params":[],"id":1}'
```

### Rome Via REST API (`:8090`)

REST endpoints under `/api/v1/` cover blocks, transactions, addresses, tokens (holders,
transfers, gate events), meta-hooks, hooks, cross-chain and cross-VM feeds, statistics,
throughput, search and the audit event browser. Server-sent event streams live under
`/stream/` with a per-IP connection limit. Health probes are `/healthz` and `/readyz`. The
OpenAPI document at `/api/v1/docs` (`/api/v1/openapi.json`) is the authoritative reference.

---

## Observability

All services emit structured logs and support OpenTelemetry traces and metrics over OTLP
through `rome-obs`.

| Variable | Default | Description |
|---|---|---|
| `ENABLE_OTEL_TRACING` | `false` | Export OpenTelemetry traces. |
| `ENABLE_OTEL_METRICS` | `false` | Export OpenTelemetry metrics. |
| `OTLP_RECEIVER_URL` | — | OTLP receiver endpoint, e.g. `http://otel-collector:4317`. |
| `ENABLE_STDOUT_LOGGING_ENV` | `true` | JSON logs on stdout. |

The Proxy can also serve Prometheus metrics on `metrics_host`
(`rome_proxy_rpc_requests_total{method,status}`, `rome_proxy_rpc_duration_seconds{method}`,
`rome_proxy_rpc_in_flight{method}`).

---

## Production deployment notes

- **Put public endpoints behind a reverse proxy** (for example nginx) that terminates TLS
  and enforces per-client rate limits (`limit_req`, `limit_conn`), request body size limits
  (`client_max_body_size`) and timeouts. The Proxy bounds its own queues and filter
  registries but does not rate-limit per client.
- Set `max_batch_size` on public Proxy endpoints (for example `100`) and consider
  `get_logs_max_block_range` (for example `10000`).
- **Never expose the Hercules admin RPC, health ports, the Prometheus sidecar or the
  rome-audit overlay listener to the internet.** Bind them to `127.0.0.1` or a private
  network.
- Set a strong `cursor_secret` for rome-via-api and a strong `ingest_secret` for the
  rome-audit overlay.
- Use TLS for PostgreSQL where the network is untrusted (`sslmode=require` is honored by the
  Rome Via services and rome-audit).
- Keep keypair files read-only and mounted only into the containers that need them.

---

## Repository structure

```text
rome-apps/
├── Cargo.toml              # workspace manifest (9 members)
├── rust-toolchain.toml     # pinned Rust toolchain
├── proxy/                  # Ethereum JSON-RPC endpoint
│   ├── docs/               # PROXY.md (operations and tuning), GAS_PRICE_POOLS.md
│   └── proxy-config.example.yml
├── hercules/               # Solana-to-Ethereum block indexer
├── cli/                    # admin CLI (+ docker-compose.yml examples)
├── rome-via-api/           # explorer REST API
├── rome-via-sync/          # Hercules DB -> rome_via_db mirror (base migrations)
├── rome-via-enrich/        # enrichment workers (derived migrations)
├── rome-via-classify/      # shared classification library
├── rome-audit/             # compliance audit-trail pipeline
├── cardo-service/          # manifest catalog: REST + MCP
├── dammv1-pool-constructor/  # TypeScript: Meteora DAMM v1 devnet pool script
├── docker/                 # Dockerfile, entrypoint and helper scripts
├── docs/                   # ARCHITECTURE.md, SOLANA_ORIGIN_TX_PARITY.md
├── CHANGELOG.md
├── CONTRIBUTING.md
├── SECURITY.md
└── LICENSE
```

---

## Contributing

External pull requests are not accepted at this time. Bug reports are welcome as GitHub
issues. See [`CONTRIBUTING.md`](CONTRIBUTING.md) for details and the developer guide.

## Security

Please report vulnerabilities privately. See [`SECURITY.md`](SECURITY.md).

## License

This repository is **source-available**. Copyright © 2024-2026 Coin Vesting
Inc. d/b/a Rome Protocol. All rights reserved.

You may use the software free of charge for personal, non-commercial and testing purposes
under the terms of the [LICENSE](LICENSE). Commercial use requires express written
permission from Rome Protocol; contact us through
[this form](https://forms.gle/FTqouv1govNVEC9U7).

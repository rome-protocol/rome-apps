# Proxy

The Proxy is the Ethereum JSON-RPC endpoint of a Rome chain (default `:9090`). Standard
Ethereum tooling points at it and uses plain `eth_*` methods. Writes are emulated,
submitted to Solana and confirmed before the transaction hash is returned. Reads are served
by emulation against Solana state or from the Hercules database.

For the API surface see the [root README](../README.md#proxy-json-rpc-9090). For the
detailed operational reference (request paths, write lanes, mempool batching, confirmation
backend, sizing and tuning) see **[`docs/PROXY.md`](docs/PROXY.md)**. Gas-price sources
are described in [`docs/GAS_PRICE_POOLS.md`](docs/GAS_PRICE_POOLS.md).

## Configuration

The Proxy reads one YAML or JSON file, located with `-c <path>` or the `PROXY_CONFIG` env
var. Telemetry is configured separately through environment variables (below). An
annotated example with every field is in
[`proxy-config.example.yml`](proxy-config.example.yml); the full field reference is
[`docs/PROXY.md` §9](docs/PROXY.md#9-full-configuration-reference).

```yaml
# --- Solana connectivity + confirmation backend ---
solana:
  rpc_url: "https://your-solana-rpc"
  commitment: confirmed
  confirm:
    mode: ws                 # poll | ws | hybrid   (omit => poll every 200 ms)
    ws_url: "wss://your-solana-rpc"
    on_ws_fail: poll         # poll | error  (ws mode: fall back to polling while ws is down)
    poll_interval_ms: 200
    timeout_ms: 30000

program_id: "<ROME_EVM_PROGRAM_ID>"
chain_id: 1001
payers:                      # the write-lane pool
  - payer_keypair: /keys/payer0.json
    fee_recipients: ["0x....", "0x...."]   # lanes = payers x fee_recipients
proxy_host: "0.0.0.0:9090"   # put a reverse proxy in front of public endpoints
ethereum_storage:
  type: pg_storage           # pg_storage | in_memory
  connection:
    database_url: "postgres://hercules:<password>@postgres/hercules"
gas_price_mul: 1.0

# --- Solana priority fee (cu_price) --- default ON; omit the block to keep the defaults
# priority_fee:
#   enabled: true            # set false to disable the feature entirely
#   cu_price_percentile: 90  # bid percentile of the recent cluster fee window
#   max_microlamports: 0     # cap on the bid in µlamports/CU (0 = uncapped)
#   min_microlamports: 1     # fallback bid when the fee window is empty (RPC unavailable)
#   poll_interval_ms: 1000

# --- request limits ---
max_batch_size: 100          # JSON-RPC batch size limit (default 1000; lower it on public endpoints)
# get_logs_max_block_range: 10000   # eth_getLogs span cap (default unlimited)
# max_connections: 50000     # omit to derive from the process file-descriptor limit

# --- read/write lane isolation ---
read_pool_enabled: true      # default true; dedicated low-priority pool for read emulation
```

### Field summary

| Field | Type | Default | Purpose |
|---|---|---|---|
| `solana.rpc_url` | URL | — | Primary Solana RPC (reads, submission, confirmation). |
| `solana.rpc_urls` | list | none | Extra nodes; submission round-robins across them. |
| `solana.commitment` | level | `confirmed` | Commitment for reads and confirmation. |
| `solana.confirm.*` | object | poll, 200 ms | Confirmation backend; see [`docs/PROXY.md` §5](docs/PROXY.md#5-confirmation-backend-solanaconfirm). |
| `program_id` | base58 | — | Rome EVM program ID. |
| `chain_id` | u64 | — | Rome chain ID. |
| `payers` | list | — | Write-lane pool: `payer_keypair` plus exactly one of `fee_recipients` or `number_holders`. |
| `proxy_host` | socket | — | JSON-RPC bind address. |
| `ethereum_storage` | object | — | `pg_storage` (Hercules database) or `in_memory`. |
| `gas_price_mul` | f64 | — | Multiplier applied to the gas-price source. |
| `track_gas` | bool | `false` | Gas-consumption tracking. |
| `price_manager` | object | none | Gas pricing from a Meteora pool or an oracle. Required for rollups that use an SPL gas token. |
| `priority_fee` | object | **on** | Solana priority-fee bid; see [`docs/PROXY.md` §9](docs/PROXY.md#priority_fee--solana-priority-fee-cu_price). |
| `max_connections` | u32 | derived | Inbound connection cap. Omitted: half the `RLIMIT_NOFILE` soft limit, at least 100. |
| `max_batch_size` | u32 | `1000` | Maximum calls in one JSON-RPC batch; larger batches are rejected whole. |
| `read_pool_enabled` | bool | `true` | Run read-path emulation on a dedicated low-priority pool. |
| `metrics_host` | socket | none | Prometheus `/metrics` sidecar address. Keep it private. |
| `jito_bundler` | object | none (off) | Optional Jito bundle support. |
| `batching` | object | none (off) | Mempool batching of concurrent `eth_sendRawTransaction` calls; see [`docs/PROXY.md` §4](docs/PROXY.md#4-mempool-batching-multi-dotx-packing). |
| `get_logs_max_block_range` | u64 | none (unlimited) | `eth_getLogs` span cap; over-span queries get error `-32005`. |

## How the read and write lanes work

The Proxy serves two kinds of work that must not contend:

- **Write (submit and confirm)**: `eth_sendRawTransaction` leases a payer lane from the
  **write pool** (sized by `payers`: number of payers × fee recipients), submits through the
  shared submission client and waits on the shared confirmation backend.
- **Read (emulation)**: `eth_call`, `eth_estimateGas` and `rome_emulateCallAccounts` run
  synchronous SVM emulation. Reads use a **separate, non-draining** resource pool, so a read
  never consumes a write lane, and they open a fresh RPC connection per call.

Isolation guarantees:

- The read and write resource pools are distinct, so a read can never exhaust or poison the
  write pool.
- With `read_pool_enabled: true` (the default), emulation runs on a dedicated thread pool
  sized to the CPU count, with workers at low scheduling priority, separate from the async
  runtime that runs submit and confirm. A read burst cannot starve writes, and a panicking
  emulation returns an error without affecting the pool or the write path. Writes get CPU
  priority through `nice`, which is effective on Linux under CPU **shares** (`cpu.weight`)
  rather than a hard quota (`cpu.max`).
- The read-pool intake queue is bounded; when it is full, requests fail fast with
  `read pool at capacity` instead of growing memory.
- `max_connections` derives from the file-descriptor limit, leaving headroom for per-call
  read connections and outbound RPC and database connections.

Emulation deliberately builds a fresh RPC client per call. Reusing one cached connection
caused hangs when the RPC server dropped idle connections.

## Defaults

Every optional setting is safe to omit:

- `read_pool_enabled` omitted: enabled. Set `false` to run emulation inline on the async
  workers.
- `solana.confirm` omitted: poll every 200 ms. `on_ws_fail` only applies to `ws` mode.
- `max_connections` omitted: derived from the fd limit.
- `batching`, `jito_bundler`, `get_logs_max_block_range`, `metrics_host` omitted: off.

When the read pool is enabled the Proxy logs `read pool enabled: <N> workers` at startup.

## Deployment

- Run public endpoints behind a reverse proxy (for example nginx) with TLS, per-client rate
  limits (`limit_req` / `limit_conn`), request body size limits and timeouts. The Proxy does
  not rate-limit per client.
- Set `max_batch_size` (for example `100`) and consider `get_logs_max_block_range` on public
  endpoints.
- Keep `metrics_host` on a private interface.
- Run the container with CPU shares rather than a hard CPU quota so read/write
  prioritization is effective.

## Telemetry and logging

| Variable | Description |
|---|---|
| `ENABLE_OTEL_TRACING` | `true` to export OpenTelemetry traces. |
| `ENABLE_OTEL_METRICS` | `true` to export OpenTelemetry metrics. |
| `OTLP_RECEIVER_URL` | OTLP receiver endpoint, e.g. `http://otel-collector:4317`. |
| `ENABLE_STDOUT_LOGGING_ENV` | `false` to disable JSON logging on stdout (enabled by default). |

```yaml
environment:
  ENABLE_OTEL_TRACING: "true"
  ENABLE_OTEL_METRICS: "true"
  OTLP_RECEIVER_URL: "http://otel-collector:4317"
  ENABLE_STDOUT_LOGGING_ENV: "true"
```

With `metrics_host` set, Prometheus metrics are served at `GET /metrics`:
`rome_proxy_rpc_requests_total{method,status}`, `rome_proxy_rpc_duration_seconds{method}`
and `rome_proxy_rpc_in_flight{method}`.

# Proxy — architecture, configuration & tuning

The operational reference for the Rome proxy (`rome-apps/proxy`): the JSON-RPC endpoint
that fronts a Rome chain. This document explains how the proxy behaves under load (the
read/write paths, the write-lane pool, the mempool batcher, the confirmation backend) and
how to size and tune it.

Source references are given as `file:line`; line numbers drift, so confirm against the
current tree.

> For the multi-DoTx envelope on the SDK side see `rome-sdk` `rome-evm-client::tx::batch`.
> Sender version (SIMD-0385 `v1`/`legacy`) is boot-gate-selected — not a proxy config knob;
> see `tx_v1_gate_active` in `proxy/src/config.rs`.

---

## 1. What the proxy does

Users point standard Ethereum tooling (`ethers`, `viem`, Hardhat, Foundry) at the proxy (`:9090`) and
it speaks plain `eth_*` JSON-RPC. Internally it serves two kinds of work that **must not
contend**:

- **Reads** (`eth_call`, `eth_estimateGas`, `rome_emulateCallAccounts`, `eth_getLogs`, …) —
  synchronous SVM emulation against Solana account state, or Postgres reads for logs/blocks.
- **Writes** (`eth_sendRawTransaction`) — lease a payer, emulate to resolve the Solana account
  list, submit to Solana, and **wait for confirmation** before returning the tx hash.

**Invariant:** a hash returned from `eth_sendRawTransaction` means the tx is already
Solana-confirmed, never "accepted but pending". No code path may return a hash before
confirmation.

---

## 2. Request paths and isolation

| | Read path | Write path |
|---|---|---|
| Methods | `eth_call`, `eth_estimateGas`, `rome_emulate*`, logs/blocks | `eth_sendRawTransaction` |
| Work | SVM emulation / Postgres | emulate → submit → confirm |
| Resource pool | `resource_factory_fake` — **non-draining**, leases nothing | `resource_factory` — **the write lanes** (drains) |
| Connection | fresh RPC client per call | shared submit client (`solana.rpc_url`(+`rpc_urls`)) |
| Optional pool | `read_pool_enabled` (default **on**) → dedicated low-priority thread pool | async runtime workers |

Isolation guarantees (`proxy/README.md` "How the read and write lanes work"):

- Read and write resource pools are **distinct mutexes** — a read can never exhaust or poison
  the write lanes.
- With `read_pool_enabled: true` (the default), read emulation runs on a dedicated pool niced
  low, so a read burst can't starve submit/confirm. Writes get CPU right-of-way via `nice`
  (effective on Linux under CPU **shares**, not a hard quota).
- **Fresh client per read** (`proxy/src/config.rs`): `get_account_storage()` builds a new
  `RpcClient` per account fetch. This is deliberate — a prior change cached one client and the
  RPC dropped it on idle, so the next emulation reused a dead socket and hung. Submit/confirm
  was never on that cached client.

---

## 3. Write lanes (the payer pool)

The write pool is the `ResourceFactory` (`rome-sdk` `rome-evm-client/src/resources.rs:116`):

```
ResourceFactory(Arc<Mutex<Vec<ResourceItem>>>)
```

It is built from the `payers` config (`resources.rs:119` `from_payers`). For a payer with a
`fee_recipients` list, `ResourceItem::from_payer` (`resources.rs:89-99`) produces **one
`ResourceItem` per fee_recipient**, all sharing that payer's keypair. So:

> **lanes = number of payers × fee_recipients per payer**

Example: 16 payers × 32 fee_recipients = **512 lanes**, drawing Solana fees from **16
keypairs**. (`number_holders` is the alternative `ResourceType` — N holder lanes, no
fee_recipient.) A payer entry must set **exactly one** of `fee_recipients` / `number_holders`
(`resources.rs:49-55`).

**`get()` busy-yields when the pool is empty** (`resources.rs:136-156`):

```rust
pub async fn get(&self) -> ProgramResult<Resource> {
    loop {
        { let mut lock = self.0.lock()?;
          if !lock.is_empty() { /* take one at random, drain it, return */ } }
        tracing::debug!("no available resourced, yielding");
        tokio::task::yield_now().await;   // ← spin while exhausted
    }
}
```

A leased lane is returned to the pool when its `Resource` drops. **One lane is held for the
full duration of a write** — emulate → submit → confirm — see `client_state_advance.rs:139`
(`send_transaction`) and `:191` (`send_pack`). This is the load-bearing fact for tuning §6.

---

## 4. Mempool batching (multi-DoTx packing)

Default-off. Configured by the top-level `batching` block (`config.rs:73-74`); absent ⇒
`None` ⇒ the per-tx path only. When present, concurrent `eth_sendRawTransaction` calls that
are already blocked awaiting confirmation are **coalesced** into one Solana tx (a `DoTxBatch`
carrying up to `max_pack_size` EVM txs). It never returns a hash early — it only changes
*how* concurrently-waiting txs reach Solana (`batcher.rs:9-12`).

### 4.1 Knobs (`batching.rs`)

| Key | Default | Cap | Meaning |
|---|---|---|---|
| `max_pack_size` | **4** (`batching.rs:39`) | — | max EVM txs per Solana pack |
| `max_pack_bytes` | **1232** (`:42`) | 1232 | composed Solana-tx size cap; clamped to protocol 1232 |
| `max_pack_cu` | **1_400_000** (`:45`) | 1.4M | compute units per pack; clamped to protocol 1.4M |
| `pack_concurrency` (**PC**) | **1** (`:48`) | — | number of packer threads draining the intake queue |
| `pack_fill_timeout_ms` | **0** (`:51`) | — | backlogged pack's wait-to-gather window; 0 = submit on drain |
| `intake_capacity` | **1024** (`:54`) | — | bounded intake queue; full ⇒ **backpressures** (never drops) |

`to_pack_limits()` (`batching.rs:66-73`) clamps `max_bytes`/`max_cu` to `PackLimits::solana()`
so an operator misconfig can't exceed the protocol caps; `max_count` passes through.

### 4.2 How the coalescer works (`batcher.rs`)

- `Batcher::new(...)` spawns **`pack_concurrency` packer threads** over one shared
  `Arc<Mutex<receiver>>` intake queue (`batcher.rs:82-97`).
- Each thread loops: lock the receiver → `collect_group` → **unlock** → `submit_group`
  (the slow Solana submit+confirm runs without the lock, so PC packs confirm in parallel)
  (`batcher.rs:152-171`).
- `collect_group` (`batcher.rs:117-144`) is **backlog-driven**: block for the first job,
  instant-drain whatever is already queued (up to `max_pack_size`), and **only** wait the
  fill window if the instant-drain already found **≥ 2** jobs (`group.len() >= 2`,
  `batcher.rs:134`). A lone tx never waits → idle traffic pays no added latency.
- `submit_group` → `RealBatchBackend` → `RomeEVMClient::send_pack` (`batcher.rs:45-52`),
  which **leases one lane** for the pack (`client_state_advance.rs:191`).

**Consequence:** packs only form under genuine intake backlog. If the packer threads drain the
queue faster than requests arrive, every group is size 1 and there is no packing — regardless
of `max_pack_size`.

### 4.3 The PC ↔ lanes coupling (the thing to get right)

`PC` (drain threads) and `lanes` (the payer pool) are **distinct knobs**, but they meet at
`get()`: each active packer thread leases one lane for its whole pack. So when **more than
`lanes` packer threads are concurrently submitting**, the excess threads find the pool empty
and busy-spin in `get()` (`resources.rs:153`) → a futex / `__pv_queued_spin_lock` storm that
burns CPU instead of doing useful emulation.

> **Sizing rule: keep `pack_concurrency` ≤ lanes.** To run more packer threads, grow lanes by
> adding fee_recipients (e.g. 16 payers × 48 = 768) — **not** more payers. fee_recipients cost
> nothing on Solana (the payer pays the fee) and the gas they accumulate funnels back to driver
> funding. Adding payers only matters if you need more *SOL-paying* capacity, which is rarely
> the binding constraint.

Running more packer threads than lanes therefore lowers throughput rather than raising it:
the surplus threads spin in the pool lock instead of emulating.

---

## 5. Confirmation backend (`solana.confirm`)

`eth_sendRawTransaction` blocks until the tx confirms. Backend selected by `solana.confirm.mode`
(`confirm.rs`):

| Field | Default | Meaning |
|---|---|---|
| `mode` | `poll` (`confirm.rs:103`) | `poll` \| `ws` \| `hybrid` |
| `poll_interval_ms` | 200 | poll cadence |
| `timeout_ms` | 30000 | per-confirm deadline |
| `ws_url` / `ws_urls` | — / `[]` | ws endpoint(s); non-empty `ws_urls` wins (`confirm.rs:146`) |
| `ws_connections` | 1 | concurrent ws conns per endpoint |
| `on_ws_fail` | `poll` | ws mode: fall back to batched poll while ws is down, or `error` |
| `race` / `route.{single,parallel,iterative,default}` | `false` / `None` | hybrid tuning |

**`ws` mode** uses a single shared `logsSubscribe` stream — steady-state it issues **zero**
poll RPCs and falls back to batched poll only if the stream drops (`on_ws_fail: poll`).

**Confirmed-commitment floor.** With `commitment: confirmed`, confirmation lands about two
slots after submission, so a single `eth_sendRawTransaction` round-trip is roughly
emulation (a few hundred ms) plus two slot times, on the order of one second on a typical
cluster. This floor is a *chain* property: it cannot be lowered without a weaker commitment
(which would break the hash-means-confirmed invariant) or faster slots. Latency above the
floor under load is queue wait (§6).

---

## 6. Sizing & tuning

**Throughput is the product of two rates:** `EVM TPS ≈ Solana-tx rate × pack factor`. With a
**blocking** driver (each client waits for its receipt before sending the next), these trade
off and the total is bounded by the closed loop:

> `EVM TPS ≈ concurrent clients ÷ round-trip latency`

What this means in practice:

- **`PC > lanes` is the cliff** — it doesn't add throughput, it adds spin. Keep `PC ≤ lanes`.
- **More packing ≠ more TPS under a blocking driver.** Lowering `PC` deepens the backlog so
  packs fill, but fewer concurrent confirms lower TPS and raise p50 latency (each pack also
  emulates its members in sequence before submit). Packing reduces
  the *Solana-tx count* (great for validator load / cost), but to raise *TPS* you must raise
  the Solana-tx rate — i.e. more concurrent clients and/or more nodes (`solana.rpc_urls`), not
  a `PC` tweak.
- The healthy regime is **`PC = lanes`** (max concurrent confirms with zero spin) on enough
  Solana nodes to carry the Solana-tx rate.

**Multi-node submission:** list extra nodes in `solana.rpc_urls` (`config.rs:124-129`) —
the tower round-robins submission across them, lifting the Solana-tx ceiling.

---

## 7. Measuring the pack factor (do this on-chain)

The pack factor is **how many EVM txs ride in one Solana tx**. Measure it from chain, not from
derived tables or logs:

```bash
# 1. pick busy slots in the run window
psql ... -c "SELECT slot_number, COUNT(*) FROM evm_tx_sol_tx
             WHERE slot_number > <start> GROUP BY slot_number ORDER BY 2 DESC LIMIT 5;"
# 2. for sigs in those slots, count rome-evm (DoTx) instructions per Solana tx
getTransaction <sig> {encoding: jsonParsed, maxSupportedTransactionVersion: 0}
  → count message.instructions where programId == <rome-evm program id>
# pack factor = mean(DoTx instructions per Solana tx)
```

**Two metrics that will mislead you:**

- **`evm_tx_sol_tx` shows 1:1 even when packing happens.** The indexer maps only the *first*
  DoTx of a multi-DoTx Solana tx, so the DB structurally cannot show pack > 1. Do not infer "no packing" from `maxpack = 1` in the DB.
- **`accepted_EVM / grep -c "Sending tx"` overstates.** The proxy log line under-counts real
  submits, inflating the ratio relative to the on-chain value.

The on-chain DoTx-per-tx count is the only reliable measure.

---

## 8. Tuning checklist

- **Lanes** = payers × fee_recipients. Check the live pool with `rome_getResources`.
- **Keep `pack_concurrency` ≤ lanes.** Grow lanes by adding fee_recipients, not payers.
- **Expect a latency floor** of roughly emulation plus two slots (§5); anything above it
  under load is queueing.
- **Raise throughput** with more concurrent clients and more Solana submission nodes
  (`solana.rpc_urls`), not by lowering `pack_concurrency`.
- **Measure the pack factor on-chain** (§7), not from the database or logs.
- **Funding:** Solana fees come from the payer keypairs (associated-token-account creation
  is the main SOL consumer); gas collected by the fee_recipients is separate.
- Re-measure after every image upgrade; behavior depends on the SDK and cluster.

---

## 9. Full configuration reference

One config file (YAML/JSON) via `-c <path>` or `PROXY_CONFIG`. Telemetry is configured
separately by env vars (see `proxy/README.md`).

### Top-level `ProxyConfig` (`proxy/src/config.rs`)

| Key | Type | Default | Purpose |
|---|---|---|---|
| `solana` | object | — | Solana connectivity + confirm (below) |
| `program_id` | base58 | — | rome-evm program id |
| `chain_id` | u64 | — | Rome chain id |
| `payers` | list | — | **write lanes** — see §3 |
| `proxy_host` | socket | — | JSON-RPC bind (`:9090`) |
| `ethereum_storage` | object | — | indexer storage (`pg_storage` / `in_memory`) |
| `gas_price_mul` | f64 | — | gas-price multiplier |
| `track_gas` | bool | `false` | gas-consumption tracking |
| `price_manager` | object | none | Meteora / Hermes(Pyth) gas pricing |
| `priority_fee` | object | absent ⇒ **on** with defaults (quiet-cluster bid 0; `enabled: false` to opt out) | Solana priority-fee (cu_price) bid — see below |
| `max_connections` | u32 | **derived** | inbound jsonrpsee cap; omit ⇒ half the `RLIMIT_NOFILE` soft limit, floored at 100 |
| `max_batch_size` | u32 | `1000` | max calls per JSON-RPC batch; over-limit batches are rejected whole. Use a lower value (e.g. `100`) on public endpoints |
| `read_pool_enabled` | bool | **`true`** (`read_pool_on()` → `unwrap_or(true)`) | dedicated read-emulation pool; set `false` to run inline |
| `metrics_host` | socket | none | Prometheus `/metrics` sidecar |
| `jito_bundler` | object | none (off) | Jito bundle plumbing (default-off) |
| `batching` | object | none (off) | mempool batching — §4 |
| `get_logs_max_block_range` | u64 | none (uncapped) | `eth_getLogs` span cap → `-32005` when exceeded |

### `priority_fee` — Solana priority fee (cu_price)

> The fee model (units, how `cu_price` relates to EVM tips, estimate versus actual
> charge) is documented in
> [`rome-sdk/docs/PRIORITY_FEE.md`](https://github.com/rome-protocol/rome-sdk/blob/main/docs/PRIORITY_FEE.md).
> This section is the configuration reference.

Default **on**. With no `priority_fee` block the proxy polls
`getRecentPrioritizationFees` and bids the p90 value of the recent fee window as
`cu_price`. On a quiet cluster that value is **0** (no charge, as on Ethereum without
contention); it rises under real contention. Set **`enabled: false`** to turn the feature
off entirely (no bid, no charge). The real-CU gas estimate is independent of this block.

| Key | Type | Default | Purpose |
|---|---|---|---|
| `enabled` | bool | `true` | Master switch. `false` disables the priority-fee bid entirely. |
| `cu_price_percentile` | u8 | `90` | Percentile of the recent cluster fee window used as the bid. |
| `max_microlamports` | u64 | `0` | Cap on the bid in µlamports per CU. `0` means uncapped; a value above 0 clamps the polled `cu_price`. |
| `min_microlamports` | u64 | `1` | Fallback bid when the recent-fee window is empty (for example, RPC unavailable). |
| `poll_interval_ms` | u64 | `1000` | Poll cadence for the recent-fee window (about one slot). |

Example with a cap that bounds operator exposure:

```yaml
priority_fee:
  enabled: true
  cu_price_percentile: 90
  max_microlamports: 1000
  min_microlamports: 1000
  poll_interval_ms: 1000
```

At startup the proxy logs the effective settings
(`priority-fee: enabled (bid p90, cap … µlam/CU [0=uncapped], min floor … µlam/CU, …ms poll)`).
`eth_maxPriorityFeePerGas` and `eth_feeHistory` reflect the live bid and return `0` when
`cu_price` is 0 or the feature is disabled.

### `solana` + `solana.confirm` — §5. `payers[]` (`PayerConfig`, `resources.rs:24-29`):
`payer_keypair` (path), and exactly one of `fee_recipients` (list) **or** `number_holders` (u64).

---

## 10. References

| Topic | Source |
|---|---|
| ProxyConfig struct + init wiring | `proxy/src/config.rs:28-99`, `:115-` |
| Batching config + clamping | `proxy/src/batching.rs` |
| Coalescer (collect_group, packer_loop) | `proxy/src/batcher.rs` |
| Confirm backend | `proxy/src/api/.../confirm.rs` |
| Write lanes / payer pool | `rome-sdk` `rome-evm-client/src/resources.rs:116-156` |
| Lane lease per write | `rome-sdk` `rome-evm-client/src/client_state_advance.rs:139,191` |
| Config field quickstart | [`../README.md`](../README.md) |
| Annotated example config | [`../proxy-config.example.yml`](../proxy-config.example.yml) |

**Verify before assuming:** config defaults drift — reconfirm against `config.rs` /
`batching.rs` at HEAD, and re-measure on the current image before relying on numbers.

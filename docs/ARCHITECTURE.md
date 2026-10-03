# Rome Via — the indexer pipeline, end to end

**What this is.** An operator walkthrough of how a Solana transaction against a
rome-evm program becomes a row on the Rome Via block explorer. It traces the five
stages — **Hercules → rome-via-sync → rome-via-enrich → rome-via-api → rome-via UI** —
what each consumes and produces, the two databases, the two independent read paths, and
the correctness invariants that make the explorer's numbers true.

**What this is *not*.**
- **Service overview, build and deployment** are in [`../README.md`](../README.md). This doc
  starts where indexing writes rows.
- **The proxy `eth_*` parity half** — how Solana-origin txs surface on
  `eth_getTransactionByHash` / `eth_getLogs` — is in
  [`SOLANA_ORIGIN_TX_PARITY.md`](SOLANA_ORIGIN_TX_PARITY.md). That path and this one read
  *different databases*; see [§2](#2-two-databases-two-read-paths).
- **Build, test and the change-impact map** are in [`../CONTRIBUTING.md`](../CONTRIBUTING.md).

> **Source references.** Citations are `path:line` or `file (symbol)`; line numbers drift,
> so confirm against the current tree. The Hercules indexing *engine* lives in the
> `rome-sdk` `rome-evm-client` crate (Hercules is a thin binary over it); those are cited by
> `file (symbol)`.

---

## 1. The pipeline at a glance

```
   Solana (rome-evm program txs)
            │
            │  poll blocks by program_id (interval ~400ms)
            ▼
┌───────────────────────┐
│  HERCULES             │  binary: hercules/  ·  engine: rome-sdk/rome-evm-client/indexer
│  (Solana→EVM indexer) │  decode rome-evm instrs → EVM txs+results; produce EVM blocks
└───────────┬───────────┘
            │ writes
            ▼
   ╔═══════════════════╗      eth_block, eth_block_txs, evm_tx,
   ║  HERCULES DB      ║      evm_tx_result {tx_result, receipt_params}, evm_tx_sol_tx
   ╚═════════╤═════════╝
             │                         ┌──────────────────────────────────────────────┐
   ┌─────────┴─────────┐   READ PATH A │ PROXY  eth_*  reads Hercules DB directly       │
   │  rome-via-sync    │  (proxy/)     │ (receipt_params, tx_result.logs) — eth_getLogs │
   │  mirror + denorm  │               │  etc. SEE SOLANA_ORIGIN_TX_PARITY.md           │
   │  + self-heal      │               └──────────────────────────────────────────────┘
   └─────────┬─────────┘
             │ mirrors (cursor-based, chain_id-scoped)
             ▼
   ╔═══════════════════╗   rome_via.{eth_block, eth_block_txs, evm_tx, evm_tx_result,
   ║  rome_via_db      ║   evm_tx_sol_tx, sol_slot}              ← base tables (sync)
   ║  (shared, multi-  ║   rome_via.{cross_chain_correlations, cross_vm_seams,
   ║   chain)          ║   token_metadata, token_holders, token_transfers, address_stats,
   ╚═════════╤═════════╝   search_index, contract_labels, hook_*, batch_traces,
             │             throughput_*}                         ← derived (enrich)
             │
   ┌─────────┴─────────┐
   │  rome-via-enrich  │  supervised polling workers → derived tables
   │  (derived tables) │  (each eth_calls the PROXY for on-chain reads)
   └─────────┬─────────┘
             │
   ┌─────────┴─────────┐   READ PATH B (the explorer): reads rome_via_db only
   │  rome-via-api     │  REST /api/v1/* (axum), optional Redis, SSE
   └─────────┬─────────┘
             │ HTTP + /config.json (runtime chain selection)
             ▼
   ┌───────────────────┐
   │  rome-via UI      │  Vite/React — one chain-agnostic image, configured at runtime
   └───────────────────┘
```

| Stage | Crate / repo | Role | Port |
|---|---|---|---|
| Hercules | `hercules/` (engine: `rome-sdk/rome-evm-client`) | Solana→EVM block indexer | :8000 admin |
| rome-via-sync | `rome-via-sync/` | Mirror Hercules DB → `rome_via_db` (+ RLP denorm + self-heal) | :8091 health |
| rome-via-enrich | `rome-via-enrich/` | Supervised workers compute derived tables | :8092 health |
| rome-via-api | `rome-via-api/` | REST `/api/v1` over `rome_via_db` | :8090 |
| rome-via-classify | `rome-via-classify/` | Shared classification library used by enrich and api | — |
| rome-audit | `rome-audit/` | Compliance audit trail from finalized Hercules data into the `audit` schema | :8093 health |
| rome-via UI | separate frontend repository | Block-explorer frontend | — |

---

## 2. Two databases, two read paths

This is the single most important mental model, and the one most likely to trip up a fix.

- **Hercules DB** — written by Hercules, the production indexer output.
- **`rome_via_db`** — the explorer's own DB: base tables *mirrored* from Hercules by sync,
  plus *derived* tables computed by enrich. It can be **shared by several chains**, so
  every table is keyed by `chain_id`.

Two consumers read two different stores:

| | **Read path A — Proxy `eth_*`** | **Read path B — Explorer** |
|---|---|---|
| Reads | **Hercules DB** directly (`receipt_params`, `tx_result.logs`) | **`rome_via_db`** (mirrored + enriched) |
| Serves | `eth_getTransactionByHash`, `eth_getLogs`, `eth_getTransactionReceipt`, … | `GET /api/v1/*` for the UI |
| Owned by | `proxy/` + `rome-sdk` storage | `rome-via-{sync,enrich,api}` |
| Doc | [`SOLANA_ORIGIN_TX_PARITY.md`](SOLANA_ORIGIN_TX_PARITY.md) | this doc |

**Consequence:** fixing one path does **not** fix the other. A Solana-origin (DoTxUnsigned)
tx that's invisible to `eth_getLogs` is a path-A bug (the `receipt_params` keying in
rome-sdk); the same tx not appearing on the explorer's Tokens page is a path-B bug (an enrich
worker). They share the upstream Hercules row but nothing downstream.

---

## 3. Stage 1 — Hercules (Solana → EVM)

Hercules is the binary (`hercules/src/{main,config,cli}.rs`); the indexing engine is the
`rome-sdk` `rome-evm-client` `indexer` module. `main.rs` loads config and starts a
`StandaloneIndexer`, which runs two cooperating loops: a `SolanaBlockLoader` (fetch +
cache Solana blocks) and a `RollupIndexer` (parse → produce).

### 3.1 Consume

The block loader polls confirmed/finalized Solana slots and loads full blocks **filtered
by the rome-evm `program_id`** (`SolanaBlockLoaderConfig.program_id`), at
`indexing_interval_ms` (default ~400ms). The block parser
(`rome-evm-client/src/indexer/parsers/block_parser.rs`) walks each block's transactions,
keeps only successful ones (`meta.err.is_none()`), finds instructions whose program is the
rome-evm program, and decodes the rome-evm instruction:

- **`DoTx`** (signed) — RLP carries a full signed EVM tx → `decode_transaction_from_rlp`.
  `origination = ecdsa`.
- **`DoTxUnsigned`** (Solana-native) — a bare EIP-1559 RLP with **no signature**. The
  sender is **never** `ecrecover`'d; it's a *synthetic* address derived from the Solana
  fee-payer (`derive_synthetic_sender(signer)`). `origination = solana_unsigned`,
  `solana_signer = <fee-payer pubkey>`.
- **Iterative VM** (`DoTxIterative` / holder-account flows) — a multi-step EVM tx whose
  results accumulate across slot boundaries; the parser reassembles them into one logical
  tx keyed by the canonical hash.

The execution **result** (logs, exit reason, gas, footprint, slot, timestamp) is parsed
from the Solana **log messages** into a `TxResult`.

### 3.2 Produce — EVM blocks

In single-state mode the `SingleStateBlockProducer` derives block numbers and hashes
deterministically from the Solana block. The block header lands in `eth_block`.

### 3.3 Produce — DB writes (the shapes downstream depends on)

Hercules writes five tables. The two JSONB shapes are load-bearing for the whole pipeline:

| Table | Key | Notable columns |
|---|---|---|
| `evm_tx` | `tx_hash` | `rlp`, **`origination`** (`'ecdsa'`\|`'solana_unsigned'`), **`solana_signer`** (base58, NULL for ecdsa) |
| `evm_tx_result` | `(slot_number, tx_hash)` | **`tx_result` JSONB**, **`receipt_params` JSONB** |
| `eth_block` | `(slot_number, slot_block_idx)` | flattened `BlockParams` (hash/parent/number/timestamp), gas, recipient |
| `eth_block_txs` | `(slot_number, slot_block_idx, tx_hash)` | `tx_idx` |
| `evm_tx_sol_tx` | `(evm_tx_hash, sol_signature)` | the settling Solana signature(s) |

```
tx_result      = { exit_reason{code,reason,return_value}, logs[], gas_report{gas_value,
                   gas_price, gas_recipient}, footprint, slot_number, timestamp }   ← NOT NULL
receipt_params = { blockhash, block_number, tx_index, block_gas_used, first_log_index }
```

**Two-phase write — remember this.** Rows are written in **two passes**:
1. **Parse time** (`transaction_storage.rs` `register_parse_results`) — INSERT `evm_tx` +
   `evm_tx_result` with **`receipt_params = NULL`** (the block isn't produced yet).
2. **Block-production time** (`blocks_produced`) — UPDATE `evm_tx_result.receipt_params`,
   keyed on the **canonical tx hash**.

So `tx_result` (and its `logs`) is present from parse time; `receipt_params` arrives only
after block production. **This is why enrich's holders worker keys off `tx_result.logs`,
not `receipt_params`** (see [§8 keystone](#8-correctness-invariants)).

### 3.4 The canonical tx hash

| Origination | Hash |
|---|---|
| `ecdsa` | `keccak(rlp)` — standard Ethereum |
| `solana_unsigned` | **`keccak(sol_sig ‖ instr_idx)`** — `rome-evm-client/.../parsers/unsigned_tx.rs` (`unsigned_tx_hash`) |

A DoTxUnsigned RLP has no signature, so its RLP-derived hash is non-canonical and can
collide; the canonical identity is minted off the Solana signature + instruction index.
The block-production UPDATE keys `receipt_params` on this canonical hash so the row is
discoverable (the keystone for path-A `eth_*` parity). On read, the
canonical hash is re-injected for `solana_unsigned` (`resolve_tx_identity`) because an RLP
round-trip would otherwise recompute the non-canonical hash.

### 3.5 Config

`HERCULES_CONFIG` (YAML): Postgres pool, multiplexed Solana RPC (providers + emergency
fallback), `program_id`, commitment, `parse_mode: single_state`, `indexing_interval_ms`,
`mode` (`Indexer`\|`Recovery` backfill). See [`../hercules/README.md`](../hercules/README.md).

---

## 4. Stage 2 — rome-via-sync (mirror + denormalize + self-heal)

`rome-via-sync` is a cursor-based polling daemon: read FROM the Hercules DB
(`source_db_url`), write TO `rome_via_db` (`target_db_url`). The base interval is
`poll_interval_ms` (or `poll_interval_seconds`, default 2s) with a geometric idle backoff up
to `max_idle_poll_interval_seconds`. It runs the **base** migrations
(`rome-via-sync/migrations/`, `0001`–`0099` range).

**Base tables**, synced in dependency order (`sync.rs`): `sol_slot`, `eth_block`,
`eth_block_txs`, `evm_tx`, `evm_tx_result`, `evm_tx_sol_tx`. (The mirrored Solana block
payload table was dropped; the Hercules DB's own `sol_block` is unaffected.) Per-table
watermarks live in `rome_via.sync_cursors`. Each table pages by a keyset bound on its own
source slot range, inserts with `ON CONFLICT DO NOTHING` (batched with `UNNEST`), and
advances the cursor. Cursors start at the chain's first block.

Run-loop behavior:

- **Drain to empty on each wake.** Full cycles repeat until no cursor advances, then the
  loop sleeps. `batch_size` (default 5000) is a per-pass chunk, not a throughput cap.
- **Slot-boundary batching** for `evm_tx` / `evm_tx_result` / `evm_tx_sol_tx`, so a large
  block is never split by a batch cut.
- **Source tip tracking.** Each cycle records the source maximum slot on
  `sync_cursors`, so the API can expose sync lag. An absent value stays absent (never 0).
- **Live notifications.** Sync emits Postgres `NOTIFY` events for new blocks and
  transactions. The payload carries `fromBlock`, `toBlock`, `blockCount` and `txCount`
  (oracle-keeper transactions excluded); older `blockNumber` / `txHash` fields are kept for
  existing clients.
- `chain_counters.total_txs` is maintained incrementally so `/stats/overview` reads one row.

Three deliberate transformations on the way in:

1. **`chain_id` prepended to every primary key** — that's what makes `rome_via_db` safely
   multi-chain (`0002`–`0011`).
2. **RLP decoded at sync time** (`rlp_decode.rs`, migration `0010_evm_tx_denorm`) into
   denormalized columns (`from_addr`, `to_addr`, `value_wei`, `nonce`, `gas_price`,
   `gas_limit`, `method_id`, `input_len`, `tx_type_byte`). For `solana_unsigned`, sender
   recovery is **skipped** (no signature) — the synthetic `from` from Hercules is used.
3. **Composite flatten** — Hercules' `BlockParams` composite type becomes
   `params_blockhash` / `params_parent_hash` / `params_number` /
   `params_block_timestamp` (a NUMERIC epoch — relevant in §6).

**The self-heal** (migration `0011_origination`, `sync.rs`): `origination` /
`solana_signer` were added to `evm_tx` *after* rows had been mirrored. Unlike the other
append-only tables, the `evm_tx` upsert uses
`ON CONFLICT (chain_id, tx_hash) DO UPDATE … WHERE … IS DISTINCT FROM …` for *just those
two columns* (the immutable RLP/to/value fields are intentionally excluded). A one-time
cursor reset re-reads history and back-tags any row Hercules has since classified as
`solana_unsigned` — a no-op for the ecdsa majority, no hand-backfill.

---

## 5. Stage 3 — rome-via-enrich (derived tables)

`rome-via-enrich` computes the derived tables the explorer actually queries. It runs the
**derived** migrations (`rome-via-enrich/migrations/`, `0100`–`0899` band; gaps in the
sequence are intentional). `ROME_VIA_ENRICH_CONFIG` (TOML) **requires** `chain_id`,
`db_url` and `rome_evm_program_id` and fails at startup on a missing or empty value: a
wrong or empty program id would misclassify every tx.

### 5.1 Supervisor + cursors

`main.rs` launches every worker wrapped by `supervise(name, factory)`
(`supervisor.rs`): on `Err`, restart with bounded exponential backoff
(1s→2s→…→cap `MAX_BACKOFF = 60s`); a successful `HEALTHY_RUN_THRESHOLD = 60s` run resets
the schedule; restarts increment `worker_restart_total`. A panicking worker no longer
takes the daemon down. Each worker tracks progress in a `rome_via.enrich_cursors`
`(chain_id, worker)` row (`SELECT last_processed` → process a slot-ordered batch → advance
to `max_slot` → UPSERT).

**Propagate, don't swallow.** A per-row write failure must **hold** the cursor at the
earliest failed slot rather than advance past it; advancing would lose that row's
enrichment permanently. The shared helper is `workers/rpc_verdict.rs`
(`valve_persist_cursor`, `HoldState`, `ValveOutcome`):

- no miss: advance;
- **transient** miss (transport error, database unavailable): hold indefinitely, so an
  outage stalls visibly instead of losing data;
- **terminal** miss (deterministic: pruned RPC history, empty result, constraint
  violation): give up after a bounded hold and advance past **only** the stuck slot.

Database errors are classified by determinism (constraint violations are terminal,
everything else transient). Verdicts from a failed Solana RPC read are never persisted.
Patterns such as `let _ = …`, `if let Err(..) { warn!(..) }` or `.unwrap_or_default()` on a
write are bugs in this crate.

### 5.2 The proxy dependency (a silent-failure surface)

Several workers `eth_call` a **proxy** (`config.proxy_url`) for on-chain reads
(`name()` / `symbol()` / `decimals()` / `mint_id()` / `eth_getCode`). The helpers return
`Option` — if the proxy is **unreachable**, they return `None`, the row is written with
NULLs, and the next poll re-probes. There's no fatal error, so **a wrong or down
`proxy_url` degrades enrichment silently** (the row exists but stays unlabeled /
unclassified). Monitor enrichment freshness and verify `proxy_url` on every deployment.

### 5.3 The workers

Launch order is `main.rs`; "source → target" is the column the worker reads and the table
it writes.

| Worker | Derives | Source → Target | Notes |
|---|---|---|---|
| `method_decoder` | 4-byte selector → signature | `evm_tx.method_id` → `method_signatures` | Seeds canonical Rome + EVM selectors at startup (UPSERT, oldest-wins); 4byte fallback. |
| `holders` | balances + transfers | **`evm_tx_result.tx_result.logs`** → `token_transfers`, `token_holders`, `token_holder_counts` | **Keystone:** logs are in `tx_result`, NOT `receipt_params`. Debit/credit; delete on 0; pre-aggregate WHERE balance>0. |
| `gate_events` | transfer-restriction changes | `evm_tx_result` logs (`SpecificRestrictionModuleSet`, `TransfersRestrictionToggled`) → `token_gate_events` | Invalidates `token_metadata.gated` so `metadata` re-derives it. Backs `GET /tokens/:address/gate-events`. |
| `contract_creation` | creator + creation tx | direct-deploy txs (`to` NULL, init code) + receipts → `contract_labels` | Direct deploys only; factory-deployed contracts come from `factory_tokens`. |
| `factory_tokens` | token provenance | `evm_tx_result.tx_result.logs` (`TokenCreated`) → `token_metadata{mint,factory,creator}` | Inserts with `name=NULL` for `metadata` to fill. |
| `metadata` | name/symbol/decimals/**kind**/**gated** | proxy `eth_call` + Solana mint-owner → `token_metadata` | `kind ∈ {ERC-20, SPL, Token-2022}` via `mint_id()`; `kind` NULLABLE (unclassified sentinel, `0213`). Gated-token detection reads the transfer-restriction module. |
| `contract_labels` | protocol display label | **configured map FIRST**, then `eth_getCode`+`name()` → `contract_labels` | See §5.4. |
| `verified_labels` | verified contract names | Sourcify-compatible verifier (`verifier_url`) → `contract_labels` | Disabled unless `verifier_url` is set. |
| `address_stats` | tx_count, first/last_seen, **is_contract** | `evm_tx` (+fee recipient from `gas_report`) → `address_stats` | See §5.5. |
| `search_indexer` | fuzzy search rows | txs/blocks/`token_metadata`/`address_stats` → `search_index` | pg_trgm GIN. |
| `cross_chain` | **Rhea/Remus/Romulus** | `evm_tx.origination` + Solana tx logs → `cross_chain_correlations` | See §5.6. |
| `hooks_registry` | observed hooks | `hook_executions` DISTINCT → `hooks_registry` | |
| `batch_trace` | DoTxBatch breakdown | `evm_tx_sol_tx` logs → `batch_traces` | Feeds proxy `debug_traceRomeTransaction`. |
| `hook_executions` | per-hook pass/reject/CU | Meta-Hook Router logs → `hook_executions` | |
| `meta_hook_indexer` | native router txs | `getSignaturesForAddress(router)` → `meta_hook_invocations` + `hook_executions` | |
| `hook_metadata` | hook contract names | proxy `name()` / bytecode → `hooks_registry.name` | |
| `throughput_record` | all-time throughput records | `eth_block` / `evm_tx` → `throughput_peak_windows`, `throughput_busiest_blocks`, `throughput_histogram` | Incremental from its cursor: resumes on restart and reads at most 50,000 blocks per poll. A full paged rebuild runs only on first start (no cursor, or a cursor with an empty record); it pages 50,000 blocks at a time (memory is one page plus the fixed-size record) and writes the record and cursor once at the end. There is no periodic recompute. To force a rebuild after a backfill or a rule change, delete the cursor row — `DELETE FROM rome_via.enrich_cursors WHERE chain_id = <chain> AND worker = 'throughput_record';` — and the next poll rebuilds. Every page ends on a complete block number: a full page drops the rows of its last block number and the next page re-reads it whole. Oracle selectors excluded from application counts. |
| `cross_vm_seams` | EVM↔Solana crossings feed | correlations + CPI targets → `cross_vm_seams` | One row only per tx on a seam (`evm_to_sol`, `sol_to_evm`, `bridge`); `is_oracle` flag; a trailing recheck window. Backs `GET /cross-vm`. |

### 5.4 contract_labels — registry-first, then on-chain (`contract_labels.rs`)

Resolution tiers, in order:
1. **Configured tier (FIRST)** — `registry_label(map, addr)`. The map
   (`address → label`) comes from the deployment config as `[[contract_labels]]` TOML and
   is built once at worker start. This is how
   protocol infra that *reverts* `name()`/`symbol()` (UniswapV2Router02, AavePool,
   Multicall3, factories, the faucet, all of V3/V4) still gets a clean label — **no
   on-chain read**.
2. **On-chain, `getCode`-first** — `eth_getCode` first: empty code → cache as
   "seen, EOA" (1 call, no label); bytecode → `name()` + `symbol()` (3 calls). Most
   `to_addr` are EOAs, so the getCode short-circuit cuts proxy load ~10×.
3. **Normalize** (`normalize_label`) — `NAME_FINGERPRINTS` substring rules
   (`compound`→"Compound", `comet`→"Comet", `aave`→"Aave V3", `uniswap v3`→"Uniswap V3", …;
   order is load-bearing) then `CODE_FINGERPRINTS` (e.g. UV3 pool `swap` selector
   `128acb08`) then symbol/raw-name fallback.

Upsert is incremental (row appears ~1–3s after resolution, restart-safe), trigger is
"distinct `to_addr` absent from `contract_labels`" (auto-backfills first deploy, idempotent
after).

### 5.5 address_stats — is_contract + first_seen (`address_stats.rs`)

- **is_contract** — an address is a contract if it (1) emitted a Transfer log,
  **OR** (2) is in `token_metadata`, **OR** (3) is in `contract_labels`. Was emitter-only
  → under-counted to "CONTRACTS 1". A **chain-wide sweep gated `is_contract = false`**, so
  broadening only ever flips false→true on the next poll — self-heals, **no migration**.
  `contract_labels` also records `has_code` from `eth_getCode`, so EOAs are not
  misflagged as contracts.
- **first_seen/last_seen** (`epoch_to_dt`) —
  `params_block_timestamp` is a NUMERIC **epoch**, read as `::FLOAT8` →
  `DateTime::from_timestamp`. The old `::TEXT` + RFC3339 parse always failed → both columns
  were NULL for every address (migration `0216` cursor-reset backfills).

### 5.6 cross_chain — the Rhea/Remus/Romulus classifier (`cross_chain.rs`)

The sole surface for tx-type. The rule is **submission-path + depth-aware**
(`classify`):

```
Romulus  IFF  origination != "solana_unsigned"            (a signed, SDK-self-submitted RLP)
         AND  ≥1 TOP-LEVEL (depth-1) program is neither infra nor the rome-evm program
Rhea     otherwise (the default)
```

- **Origination gate** — `solana_unsigned` (DoTxUnsigned, synthetic sender)
  is a *regular* single-chain EVM tx → Rhea up front, regardless of any top-level program.
- **Depth-1 only** — only programs invoked directly by the tx's own
  instructions count (parsed from `Program <ID> invoke [N]` lines). A cached wrapper's
  inner SPL `transfer` CPI fires at **depth ≥ 2**, so it does **not** flip single-chain
  DeFi (Comet supply/withdraw, Uniswap swaps) to Romulus — the key property the
  depth-aware fix restored.
- **Infra set** — `DEFAULT_INFRA` (ComputeBudget, System, **ATA**, BPF/Native
  loaders, Sysvar) ∪ rome-evm program ∪ meta-hook program ∪ `extra_infra_programs`. The
  **ATA program is infra**: a top-level `createIdempotent` is the
  SDK/proxy execution prelude, not a composed native leg.
- **"CPI" = a real `Program X invoke [N]` log line** — precompile read shortcuts on
  `0xff..08` (`account_info`, `account_data_at`, …) dispatch as `CrossStateEthCall`, make
  no syscall, emit no invoke line, and **do not affect classification** (see the
  `CrossStateEthCall` handling in `rome-evm`).
- **Remus is NOT produced here** — Remus = ≥2 EVM RLP legs across ≥2 chains;
  sister-chain legs live in *other* rollups' DBs, unobservable in this single-chain pass.
  Structurally unproduced by design; the UI hides the Remus tile.

When a tx is Romulus, the worker logs the exact `flipping_programs` so a false positive can
be tuned away via `extra_infra_programs`. A missing Solana signature list is treated as
sync lag (a transient miss), not filed as Rhea. `solChain` on correlation rows comes from
the `solana_cluster` config key.

### 5.7 rome-via-classify — one home for classification rules

Action tags (`classify.rs`), cross-VM seams (`seams.rs`), status and revert reasons
(`status.rs`) and the oracle-keeper selector list (`ORACLE_SELECTORS`, `is_oracle_method`)
live in the `rome-via-classify` library. Both `rome-via-api` (read path) and
`rome-via-enrich` (persisted feeds) call it, so the API and the stored tables cannot drift.
Add or change a rule there, with tests, and never inline a duplicate in either consumer.

### 5.8 Maintenance operations

`rome-via-enrich maintenance <op>` runs a bounded one-shot operation and exits, before
migrations, the health server or any worker start. Every op is a dry run unless `--apply`:

- `reextract-transfers` — re-extracts ERC-20 `Transfer` events from
  `evm_tx_result.tx_result` and reconciles `token_transfers`. Bounded to slots at or below
  the `holders` cursor so it cannot collide with the live worker's balance accounting.
- `backfill-balances` — heals `token_holders.balance` to on-chain `balanceOf()` with
  per-pair optimistic concurrency, safe against the live worker (`--token`,
  `--concurrency`).
- `backfill-oracle-flag` — marks existing oracle-keeper crossings in `cross_vm_seams`.
  Idempotent. Historical data is changed only through opt-in operations like this one, never
  through an automatic migration.

---

## 6. Stage 4 — rome-via-api (REST over rome_via_db)

`rome-via-api` is an axum service (`ROME_VIA_API_CONFIG` TOML: `chain_id`, `db_url`,
`bind_addr` :8090, `proxy_url`, optional `redis_url`, `[foreign_proxies]`). It reads
`rome_via_db` (both base + derived tables) and serves `/api/v1/*`. OpenAPI is at
`/api/v1/docs` and is authoritative for the full surface. Read caching uses Redis with
stale-while-revalidate (an aged entry is served immediately while one background task
refreshes it, guarded by a lock key). Over-limit pagination requests are rejected with 400,
never silently clamped. The endpoints that carry the explorer's correctness facts:

### `/api/v1/stats/overview` — chain-wide aggregates (`stats.rs`)

The summary-tile source. **All counts are `chain_id`-scoped**, not page-derived (a list
page filtering its fetched 50 rows is the bug this replaced). `StatsOverview` returns
`latest_block_number`, `latest_slot`, `source_max_slot`, `total_txs`/`tx_count_total`,
`tps_60s_estimate`, `active_addresses`, `token_count_total`, and two breakdowns:

- `token_kind_counts{erc20,spl,token2022}` — `COUNT(*) FILTER` over `token_metadata.kind`.
- `address_type_counts{contracts,eoas,synthetics}` where:
  - `contracts` = `COUNT WHERE address_stats.is_contract`,
  - `synthetics` = `COUNT(DISTINCT COALESCE(from_addr,from_address))` over `evm_tx` with
    `origination <> 'ecdsa' AND solana_signer IS NOT NULL`,
  - `eoas` = `eoa_count(active − contracts − synthetics)`, **clamped ≥ 0** (unit-tested) so independent racing counts never render a negative tile.

### `/api/v1/tokens` + `/tokens/:address` — derived circulating supply (`tokens.rs`)

For **wrapper kinds** (`SPL`, `Token-2022`; `WRAPPER_KINDS`) the on-chain
`total_supply()` is decimal-misaligned or literally 0, so the API derives
`circulating_supply = SUM(token_holders.balance > 0)`, on the same base-unit scale as
balances, so `holder_share = balance / total` is decimal-agnostic. Plain ERC-20s keep their
self-consistent raw `supply` (field omitted). `kind` is read as **`Option<String>`**; a
NULL (unclassified) token would otherwise fail the whole list. `GET /tokens?factory=0x…`
filters by deploying factory (a malformed filter is a 400).

### `/api/v1/addresses/:address` — synthetic + label (`addresses.rs`)

`controlled_by_solana` / `solana_pubkey` are derived **directly from `evm_tx`** — any row
with `origination <> 'ecdsa'` and a `solana_signer` whose `from` matches reveals the
controlling Solana account. **No reverse-map table** — the indexer-populated columns *are*
the reverse map. Live balance is an `eth_getBalance` to the proxy. The tx list LEFT JOINs
`contract_labels` → `to_label`/`to_label_detail` and `cross_chain_correlations` →
`tx_type`/`solana_legs`.

The response also carries a server-side `kind` (`burn`, `precompile_rome`,
`precompile_eth`, `token`, `contract`, `sol_account`, `eoa`) from `api::address_kind`. The
explorer frontend mirrors this precedence; change both together. `GET
/addresses/:address/tokens` lists the address's token positions.

### `/api/v1/txs/:hash` — settlement + status (`txs.rs`)

Returns the decoded tx: `status` from `tx_result.exit_reason.code` (0 = success),
`gas_*` from `tx_result.gas_report`, `tx_type` from `cross_chain_correlations`,
`origination`/`solana_signer`, decoded `transfers[]`, and a `solana_settlement_sig`
(scalar subquery on `evm_tx_sol_tx`). Action tags come from `rome-via-classify`
(§5.7); for example, `contract_creation` requires init-code evidence (no `to` **and**
non-empty calldata), and gas-wrapper precompile legs classify as `wrap` / `unwrap`.

### `/api/v1/search` — `search.rs`

pg_trgm `similarity()` over `search_index`, ordered by similarity then weight, up to 20
results, wrapped in a `statement_timeout` (timeout → `partial=true`).

### Other surfaces

- `/throughput/*` — current and peak TPS, timeseries, histogram, slot cadence and the
  persisted all-time record (`throughput_record` worker). Counts include oracle refresh
  transactions; `current-tps` also reports the application-only split.
- `/cross-vm` — the EVM↔Solana crossings feed; oracle-keeper rows hidden unless
  `include_oracle=true`.
- `/audit/events`, `/audit/event-counts` — read-only browse over `audit.chain_event`
  written by rome-audit.
- `/stream/*` — server-sent events backed by Postgres `LISTEN`, with a per-IP connection
  limit.

---

## 7. Stage 5 — rome-via UI

The UI (a separate Vite/React repository) ships as **one chain-agnostic image** and
reads `/config.json` at runtime — the chain id, RPC, explorer URL, and registry-projected
config are injected per deployment, never baked into the bundle. **List-page summary tiles
read `/api/v1/stats/overview`** (chain-wide), never a count over the fetched page. Number
columns format base-units by decimals (BigInt-exact); "—" for 0.

> **Verify by rendering.** Summary tiles and number formatting are computed client-side, so
> they do not appear in any API response and an API/SQL audit cannot see them. Verify a UI
> surface by **rendering the page** (for example in a headless browser) and reading the
> tiles as a user would.

---

## 8. Correctness invariants

*Read before touching the pipeline.*

1. **The keystone wrong-column class.** Logs live in `evm_tx_result.tx_result.logs`, NOT
   `receipt_params.logs` (`holders.rs`). `receipt_params` is also NULL until block
   production and historically NULL for `solana_unsigned`, so reading it drops the entire
   token surface. The general bug class: a worker reading the wrong column / a stuck cursor
   / a never-populated derived table presents as "empty/0/null", which *looks* like "no
   data yet," not "broken code." Trace each field production → render.
2. **`chain_id` is load-bearing in every enrich query**: `rome_via_db` can be shared
   across chains; a missing `chain_id` predicate pollutes one chain's derived
   data with another's.
3. **tx-type is derived, submission-path + depth-aware** — Romulus only for signed RLPs
   that compose a top-level native leg; `solana_unsigned` and proxy-relayed are always
   Rhea; cached-wrapper inner CPIs are depth ≥ 2 → Rhea. Remus is structurally unproduced.
4. **token `kind` is a nullable sentinel** (`0213`) — read as `Option` everywhere.
5. **is_contract is a union** (emitter ∪ token_metadata ∪ contract_labels), self-healing.
6. **Wrapper supply is derived** (SUM of holders), never the on-chain `total_supply()`.
7. **Canonical hash for `solana_unsigned`** is `keccak(sol_sig‖idx)`; `receipt_params` and
   reads must key on it, or the tx is invisible to path A.
8. **Cursors never skip a failed write** (§5.1). A swallowed write is data loss.

---

## 9. Structural — every new chain gets this for free

No per-chain hardcoding. Two mechanisms make a freshly brought-up chain correct from its
first poll:

- **Image behaviors compute from `chain_id`-scoped data.** Breakdowns, is_contract,
  derived supply, classification, kind/provenance all live in the chain-agnostic service
  images. A new chain is correct immediately; one-time backfills (cursor resets, the
  is_contract sweep) only catch up data indexed under *older* code.
- **Deployment-provided config** (verified tokens, canonical factories,
  `[[contract_labels]]`, `[[program_labels]]`): generate these per chain from a single
  source of truth in your deployment tooling, into both the enrich TOML and the UI
  `/config.json`. The `contract_labels` worker consults that map **first** (§5.4). Never
  hardcode per-chain values in code.

---

## 10. Operating it — quick reference

| Service | Config env | Reads | Writes |
|---|---|---|---|
| Hercules | `HERCULES_CONFIG` (YAML) | Solana RPC | Hercules DB |
| rome-via-sync | `ROME_VIA_SYNC_CONFIG` (TOML) | Hercules DB | `rome_via_db` (base) |
| rome-via-enrich | `ROME_VIA_ENRICH_CONFIG` (TOML) | `rome_via_db` + proxy `eth_call` | `rome_via_db` (derived) |
| rome-via-api | `ROME_VIA_API_CONFIG` (TOML) | `rome_via_db` + proxy + Redis | — |
| rome-audit | `ROME_AUDIT_CONFIG` (TOML) | Hercules DB (finalized slots) | `audit` schema |

**Shared-DB migrations:** rome-via-sync (`0001`–`0099`), rome-via-enrich (`0100`–`0899`)
and rome-audit (`0901` and up, `audit` schema) can all run against the same database and
share one `_sqlx_migrations` table, each with `ignore_missing = true`. Version numbers must
therefore be unique **across all three crates**: a new migration takes the next free number
in its crate's band. A collision aborts startup with "migration N was previously applied
but has been modified". See
[`../CONTRIBUTING.md`](../CONTRIBUTING.md#database-migrations).

**When a surface is wrong — where to look:**

| Symptom | Likely stage / cause |
|---|---|
| Tokens/holders empty across the chain | enrich `holders` cursor stuck, or proxy unreachable (§5.2), or wrong-column regression (§8.1) |
| Contracts/labels missing or "CONTRACTS N" too low | `contract_labels` proxy down, or `address_stats` is_contract union (§5.5) |
| Single-chain DeFi tagged Romulus | `cross_chain` infra set / depth (§5.6); check `extra_infra_programs` + `rome_evm_program_id` |
| first_seen/last_seen NULL | `address_stats` epoch decode (§5.5) |
| Solana-origin tx missing from `eth_getLogs` | **path A**, not this pipeline — see `SOLANA_ORIGIN_TX_PARITY.md` |
| Summary tiles wrong but rows fine | UI reads `/stats`; confirm `stats.rs` aggregates + **render the page** (§7) |
| Enrichment silently stale | a worker erroring + backoff-restarting (`worker_restart_total`), a cursor held on a transient miss (§5.1), or `proxy_url` unreachable |

---

## 11. Cross-references

- [`../README.md`](../README.md) — services, build, Docker and configuration.
- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — layout, test selection and the change-impact map.
- [`SOLANA_ORIGIN_TX_PARITY.md`](SOLANA_ORIGIN_TX_PARITY.md) — read path A (proxy `eth_*` for Solana-origin txs).
- [`../hercules/README.md`](../hercules/README.md) — Hercules configuration.
- `rome-via-enrich/src/workers/cross_chain.rs` (module doc) — the CPI-depth classification rule.

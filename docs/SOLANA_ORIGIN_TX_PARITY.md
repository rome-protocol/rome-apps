# Solana-origin EVM transactions — how they surface like a regular Ethereum transaction

**Audience:** anyone working on Hercules (the indexer), the proxy, or the Rome Via explorer who needs to understand how a transaction that *originates from a Solana wallet* is made indistinguishable from an ordinary MetaMask/ethers transaction across the JSON-RPC API and the explorer.

**TL;DR:** A Solana-origin EVM transaction (`DoTxUnsigned`) is **not a new transaction type**. It is a canonical EIP-1559 (type-`0x02`) transaction whose sender happens to be authenticated by a *Solana* signature instead of an ECDSA signature. Every layer that already understands a 1559 transaction is reused unchanged; the only Solana-specific steps are (a) deriving the sender from the Solana pubkey instead of `ecrecover`, and (b) minting a deterministic transaction hash off-chain. Get those two right at the **origin** (the indexer), and the receipt, logs, `from`/`to`, explorer rows, and protocol labels all fall out for free — for Compound today and any app tomorrow, with no per-app band-aids.

---

## 0. The problem this solves

A `DoTxUnsigned` is submitted as a **Solana instruction**: a Solana wallet (Phantom) signs a Solana transaction that carries an RLP-encoded EVM call as instruction data. There is no ECDSA signature over the EVM payload. Historically these transactions surfaced as second-class citizens:

- `eth_getTransactionByHash` returned the tx but with **no `blockNumber`**; `eth_getTransactionReceipt` / `eth_getLogs` / `eth_getBlockReceipts` returned **nothing**. A Comet liquidator bot (or viem, or The Graph) watching `eth_getLogs` could not see a Solana-origin borrow → it looked like the position never changed → **bad debt risk**.
- The explorer showed **"contract creation"** with a blank method and a `from` of `0x0000…`.

Both are now fixed at the source. The rest of this document explains exactly how.

---

## 1. The transaction shape — a canonical EIP-1559 RLP

The EVM payload inside the Solana instruction is a standard **type-`0x02` (EIP-1559) RLP**, byte-for-byte the same envelope a MetaMask transaction uses — the same `chainId, nonce, maxPriorityFeePerGas, maxFeePerGas, gasLimit, to, value, data, accessList` fields. The only difference from an ECDSA transaction is that the signature fields (`v, r, s`) are **not a valid secp256k1 signature** — for a Solana-origin tx they are zeroed/garbage, because the authentication happened on the Solana side.

**Why this matters:** because the envelope is canonical 1559, *every* downstream decoder is reused unchanged. The indexer, the proxy, and `rome-via-sync` all call the **same** RLP decoder they use for ECDSA transactions; the only branch is "for a Solana-origin tx, skip `ecrecover` and substitute the synthetic sender (§2)." We did **not** invent a 9-field legacy shape or a bespoke type. This is the design decision that makes Ethereum-equivalence cheap.

- Decoder (explorer side): `rome-via-sync/src/rlp_decode.rs` — `decode_signed_tx(rlp_bytes, recover_sender: bool)`. For `origination == "solana_unsigned"` the caller passes `recover_sender = false`, so the decoder parses every 1559 field but does **not** run `ecrecover` (which would yield garbage from the zeroed signature).
- Decoder (indexer side, rome-sdk): the same canonical decode path is used in `rome-evm-client`'s block parser; the inner call's `to`/`input` are available exactly as for an ECDSA tx.

---

## 2. `from` — the synthetic sender

The EVM-level sender of a Solana-origin tx is **derived deterministically from the Solana signer's public key**:

```
from = keccak256(solana_pubkey)[12:32]      // last 20 bytes of the keccak hash
```

This is the same derivation the on-chain program and the off-chain emulator use (see `derive_sender` in `rome-evm`'s `aux.rs`), so the EVM `from` an app observes is stable and reproducible from the Solana pubkey alone. The originating Solana pubkey is also surfaced verbatim (the explorer shows a **"Controlled by Solana `<pubkey>`"** banner and a ◎ badge).

**`from` on the explorer — the `COALESCE` rule (and a fixed pitfall).** `rome-via-api` returns the sender as:

```sql
COALESCE(et.from_addr, et.from_address) AS effective_from   -- rome-via-api/src/api/txs.rs
```

`from_address` is Hercules' authoritative synthetic sender; `from_addr` is a denormalized column written by `rome-via-sync`. Because `from_addr` *wins* the `COALESCE`, it must hold the resolved synthetic sender — **not** the zero-address sentinel that `decode_signed_tx(recover_sender = false)` returns. `rome-via-sync` routes the denorm bind through `denorm_from_addr(...)` (a wrapper over `resolve_sync_from`) so both columns carry the synthetic for `solana_unsigned` and the locally-recovered sender for ECDSA. (Regression history: a zero sentinel briefly leaked into `from_addr` and shadowed the synthetic — fixed; covered by `denorm_from_addr_solana_unsigned_is_synthetic_not_zero`.)

---

## 3. The transaction hash — how it is calculated

A Solana-origin tx needs a **stable, content-derived EVM transaction hash** so it can be looked up by `eth_getTransactionByHash` and joined across tables. Two facts force the design:

1. There is no ECDSA signature, so the usual `keccak(signed_rlp)` Ethereum tx-hash isn't meaningful.
2. The hash the on-chain program produces is slot-derived, can collide, and is never logged — so it cannot be the canonical identifier.

Therefore the indexer **mints the canonical hash off-chain**, deterministically, from the confirmed Solana transaction:

```
unsigned_tx_hash(sol_signature, instr_idx) = keccak256( sol_signature[64 bytes] ‖ instr_idx_u8 )
```

- Implementation + golden test: `rome-evm-client/src/indexer/parsers/unsigned_tx.rs` (rome-sdk). The output is **golden-pinned** so the value never drifts.
- It is **not** `keccak(rlp)` (that distinction is load-bearing — see §4).
- It is reproducible by all three parties — the submitter, the indexer, and the proxy — from the confirmed Solana signature + instruction index, so everyone agrees on the same hash for the same on-chain event.

---

## 4. `eth_*` parity — the `receipt_params` keystone (Hercules)

Every standard read method — `eth_getTransactionByHash.blockNumber`, `eth_getTransactionReceipt`, `eth_getLogs`, `eth_getBlockReceipts` — derives its answer from one stored record: **`evm_tx_result.receipt_params`**, written when a block is produced.

`receipt_params` is written by `blocks_produced` in `rome-evm-client/src/indexer/pg_storage/transaction_storage.rs` (rome-sdk) via:

```sql
UPDATE evm_tx_result SET receipt_params = $… WHERE tx_hash = $3
```

An earlier bug here was a **hash-key mismatch**: `$3` was bound from `Transaction::decode(rlp).hash`, which ethers recomputes as `keccak(rlp)`. For an ECDSA tx that equals the canonical hash, so the `UPDATE` matched. For a `solana_unsigned` tx the canonical hash is `keccak(sol_sig ‖ idx)` (§3) — **not** `keccak(rlp)` — so the `UPDATE` matched **no row**, `receipt_params` stayed `NULL`, and every `eth_*` method returned null/empty. (The *read* side's canonical-hash precedence had already been fixed; this *block-production write* had been missed.)

The fix threads the canonical stored `tx_hash` (`eth_block_txs.tx_hash`, already joined by the `pending_transactions_with_slot_statuses2` view) through the pending path and overrides the decoded `.hash` in `append_pending_tx`:

- `rome-evm-client/src/indexer/pg_storage/ethereum_block_storage.rs` — `append_pending_tx` sets `decoded.hash = canonical tx_hash`.
- migration `…/migrations/2026-06-03-000000_pending_view_canonical_hash` — adds `ebt.tx_hash` to the view.
- `blocks_produced` itself is unchanged; the change is a no-op for ECDSA.

**Result:** a Solana-origin Comet `supply`/`borrow`/`withdraw` now resolves `blockNumber` + receipt + emits its `Transfer`/Comet logs through `eth_getLogs` — exactly like an ECDSA tx. Liquidator bots, viem, and The Graph consume it with zero Solana awareness.

---

## 5. `to` and `method` — decoded from the inner call

The `to` address and the method selector come straight from the inner 1559 RLP (§1), decoded by the **same** path ECDSA uses (skipping `ecrecover`):

- Explorer: `rome-via-sync/src/sync.rs::sync_evm_tx` calls `decode_signed_tx(rlp, recover_sender = origination != "solana_unsigned")` and writes the denormalized `to_addr` / `method_id`. Before this, Solana-origin rows had `NULL` `to_addr`/`method_id` and rendered as "contract creation"; now they carry the real target + selector.
- The action taxonomy is **origination-agnostic**: `rome-via-classify/src/classify.rs` (shared by `rome-via-api` and `rome-via-enrich`) maps a method selector/name to a protocol action (`supply → lend_supply`, `withdrawTo → lend_withdraw`, `borrow → lend_borrow`, `liquidationCall/absorb → liquidate`, `refresh → oracle_refresh`, etc.). Because it keys off the *method* (now populated), Solana-origin and ECDSA transactions classify identically with no special-casing.

---

## 6. Contract / protocol labels — how the names are obtained

The explorer shows a clean protocol label next to the `to` contract — "Compound", "Aave V3", "Uniswap V3", or a token symbol. Labels come from two tiers: a deployment-provided `[[contract_labels]]` map is consulted first (for protocol infrastructure that reverts `name()`/`symbol()`), and otherwise the label is derived from on-chain data. On-chain, the contract self-identifies: a Comet deployment returns `name()` = `"Compound cached 9-asset"`, `symbol()` = `"cwUSDC-9"`.

### 6.1 How it's resolved (and why it does NOT stress the API)

This is the standard explorer pattern (and the same one `rome-via-enrich` already uses for token and hook names): **resolve once per distinct contract, in a background worker, and cache in the DB.** The read/API path is pure SQL — **zero `eth_call`s per page view.**

- Worker: `rome-via-enrich/src/workers/contract_labels.rs`. It polls for distinct `evm_tx.to_addr` values absent from the cache, resolves each, and upserts. The eth_call volume is bounded by the number of **distinct contracts** ever seen (dozens), not by transaction count; and `name()`/`symbol()` are immutable for a contract, so each is resolved exactly once.
- **`getCode` first.** Each `eth_call` on Rome runs a full emulation (about 1 s), so the worker calls `eth_getCode` first: an address with no code is cached as an EOA after one call, and only contracts get the `name()` + `symbol()` reads. Bytecode selector fingerprinting is the fallback when `name()` is empty. (Batching through Multicall3 `aggregate3` does not work on Rome, so reads are per contract; each contract is resolved once and cached.)
- **Failure handling.** On a total RPC failure the worker writes **nothing** (a `should_upsert` guard skips the all-`None` shape), so the address stays absent and is re-probed after recovery — it never caches a permanent `NULL`. A reachable-but-unnamed contract *is* cached (so it isn't re-probed every tick).
- **Normalization.** A `const` fingerprint table (mirroring the hook-selector table) turns the raw on-chain string into a clean label: `name()` containing "Compound" → "Compound" (ordered so it wins over a "Comet" substring), "Aave" → "Aave V3", "Uniswap V3" / the UV3 `swap` selector → "Uniswap V3", otherwise a short `symbol()` → the token symbol. Raw `name()`/`symbol()` are also stored for transparency.
- Storage: `rome_via.contract_labels (chain_id, address, display_label, display_label_detail, raw_name, raw_symbol, updated_at, …)` — created by migration `rome-via-enrich/migrations/0211_contract_labels.up.sql` and extended by later migrations.

### 6.2 How it reaches the UI

- API: `rome-via-api` JOINs `contract_labels` by `(chain_id, to_addr)` and returns `toLabel` / `toLabelDetail` (camelCase, omitted when absent) on every tx — list, single-tx, and address-txs queries (`src/api/txs.rs`, `src/api/addresses.rs`, struct in `src/api/models.rs`). The join key matches the worker's stored casing exactly (no `LOWER()`).
- UI: the Rome Via frontend renders the label as a chip in the transaction list and detail views.

---

## 7. The two read paths (the mental model)

There are **two independent consumers** of Solana-origin transactions; fixing one does not fix the other, and both now handle `solana_unsigned`:

| Path | Serves | Reads from | Made Solana-aware by |
|---|---|---|---|
| **Proxy `eth_*`** | viem, The Graph, liquidator bots | Hercules-produced DB (`receipt_params`, logs) — `rome-evm-client` | canonical-hash `receipt_params` keying (§4) |
| **Explorer (Rome Via)** | humans in the block explorer | `rome_via_db` denormalized rows + `contract_labels` — `rome-via-{sync,enrich,api}` | inner-call denormalization and labels (§5, §6) + the synthetic `from` (§2) |

The proxy reads the Hercules-produced store directly, which is why §4's `receipt_params` fix is sufficient for full `eth_getLogs` parity.

---

## 8. The pipeline end-to-end (who does what)

```
Solana wallet (Phantom) signs a Solana instruction carrying a 1559 RLP EVM call
        │
        ▼
Rome EVM Solana program executes the EVM call atomically on-chain
        │
        ▼
HERCULES (rome-apps/hercules → rome-sdk/rome-evm-client)
  • parses the DoTxUnsigned event
  • derives synthetic from = keccak(pubkey)[12:]                       (§2)
  • mints canonical hash = keccak(sol_sig ‖ idx)                        (§3)
  • produces the EVM block and writes receipt_params keyed on that hash (§4)
  → serves eth_getTransactionByHash / Receipt / getLogs / getBlockReceipts
        │
        ├──────────────────────────────► PROXY eth_* (viem / liquidators / The Graph)
        │
        ▼
rome-via-sync  (rome-apps/rome-via-sync)
  • mirrors evm_tx into rome_via_db
  • decode_signed_tx(recover_sender=false) → denorm to_addr / method_id (§5)
  • denorm_from_addr → synthetic from                                   (§2)
  • skips the SSE "new tx" NOTIFY for oracle-only batches
        │
        ▼
rome-via-enrich (rome-apps/rome-via-enrich)
  • contract_labels worker: configured labels, else getCode + name()/symbol(),
    resolve-once, cache in rome_via.contract_labels                     (§6)
        │
        ▼
rome-via-api → Rome Via frontend
  • rome-via-classify action tags (origination-agnostic)                (§5)
  • toLabel / toLabelDetail JOIN → label chip                           (§6)
  • ◎ Solana badge + "Controlled by Solana <pubkey>"                    (§2)
```

---

## 9. Why this is the right design (Ethereum-equivalence)

A Solana-origin transaction **is** a normal EIP-1559 transaction. The *only* substantive difference is that its sender is authenticated by a Solana signature rather than ECDSA — so the sender is *derived* (§2) rather than *recovered*, and the hash is *minted* (§3) rather than computed from a signature. Everything after that — RLP decode, block production, receipt, logs, explorer rows, action classification, contract labels — runs through the **identical** code paths as an ECDSA transaction.

That is why the fixes live at the **origin** (the indexer + the canonical hash) and not as read-time patches in the explorer or the API: fix it once where the transaction enters the system, and every current and future consumer — Compound's liquidator today, any app tomorrow — sees a first-class Ethereum transaction with no per-app special-casing.

---

## Reference — source map

| Concern | Repo / path |
|---|---|
| Synthetic sender derivation | `rome-evm` `aux.rs::derive_sender` |
| Canonical hash (off-chain mint, golden-pinned) | rome-sdk `rome-evm-client/src/indexer/parsers/unsigned_tx.rs` |
| `receipt_params` write / `eth_*` parity | rome-sdk `rome-evm-client/src/indexer/pg_storage/{transaction_storage.rs, ethereum_block_storage.rs, migrations/2026-06-03-000000_pending_view_canonical_hash}` |
| RLP decode (skip ecrecover) | rome-apps `rome-via-sync/src/rlp_decode.rs::decode_signed_tx` |
| `to`/`method`/`from_addr` denorm + SSE de-noise | rome-apps `rome-via-sync/src/sync.rs` |
| Action taxonomy (origination-agnostic) | rome-apps `rome-via-classify/src/classify.rs` |
| Contract-label worker (getCode-first, cache) | rome-apps `rome-via-enrich/src/workers/contract_labels.rs` + `migrations/0211_contract_labels.up.sql` |
| `toLabel` API surface | rome-apps `rome-via-api/src/api/{txs.rs, addresses.rs, models.rs}` |

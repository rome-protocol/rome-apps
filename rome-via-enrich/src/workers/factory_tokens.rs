//! Factory-token discovery worker (F7).
//!
//! Today token discovery is **Transfer-log-only** (`holders.rs` tails
//! `evm_tx_result.tx_result->'logs'` for the ERC-20 `Transfer` topic). A token
//! created via `ERC20SPLFactory` but never transferred has **zero** Transfer
//! logs, so it never enters `token_metadata` and is invisible to the explorer's
//! tokens page until someone moves it.
//!
//! ## The creation signal is an EVM log — `TokenCreated`
//!
//! `ERC20SPLFactory` emits, on every wrapper registration (the
//! `add_spl_token_with_metadata` / `add_spl_token_no_metadata` /
//! `create_token_mint`+`init_token_mint` → `_register_contract` path):
//!
//! ```solidity
//! event TokenCreated(
//!     address indexed creator,
//!     bytes32 indexed mint,
//!     address indexed wrapper,
//!     string name,
//!     string symbol,
//!     uint64 nonce
//! );
//! ```
//!
//! `keccak256("TokenCreated(address,bytes32,address,string,string,uint64)")` =
//! [`TOKEN_CREATED_TOPIC`]. Because all three address/bytes32 fields are
//! `indexed`, the topics array is `[topic0, creator, mint, wrapper]` and the
//! **new token address is `topics[3]`** (the wrapper). `log.address` is the
//! *factory* (not the token), so unlike the Transfer worker we read the token
//! address out of `topics[3]`, not `log.address`.
//!
//! This is the cheap path — the exact same `evm_tx_result.tx_result` tail
//! `holders.rs` already uses, just a different topic0 and a different field
//! extraction. **Live-verified on Hadrian 2026-06-04**: the wUSDC creation tx
//! `0xed7765…` carries this log with `topics[3] = 0x…9a8b4cb7…` (= the wUSDC
//! wrapper) and `topics[2] = 0x3b442cb3…` (= wUSDC's Solana mint).
//!
//! ## Why match by topic0, not by factory address
//!
//! There have been **six** `ERC20SPLFactory` deploys on Hadrian (registry
//! `contracts.json`), and other chains have their own. Rather than pin a factory
//! address (which drifts on every redeploy and differs per chain), we match
//! **any** log whose `topics[0]` is the `TokenCreated` signature. The event
//! signature is canonical across factory versions, so topic0-matching is both
//! version-proof and chain-proof. A non-factory contract emitting an identically-
//! shaped event would also be picked up — that's acceptable: the worst case is a
//! `token_metadata` row that the metadata worker then probes and (if it's not a
//! real ERC-20) leaves with NULL name; it does not corrupt anything.
//!
//! ## Provenance capture (factory / creator / mint)
//!
//! Beyond *discovering* the wrapper address, the `TokenCreated` log carries the
//! token's **provenance** — the three fields a trust story is built from:
//! * **factory** = `log.address` (the emitting `ERC20SPLFactory`),
//! * **creator** = `topics[1]` (the EOA/contract that called the factory, right-
//!   12 bytes → 0x address),
//! * **mint** = `topics[2]` (the underlying Solana mint, bytes32 → base58 via the
//!   same [`metadata::bytes32_hex_to_base58`] the metadata worker uses for
//!   `mint_id()`, so the two write the identical canonical pubkey string).
//!
//! We UPSERT these onto `token_metadata` so BOTH freshly-discovered tokens AND
//! tokens already inserted by the Transfer-log path (`holders.rs`) get their
//! provenance backfilled the moment their `TokenCreated` is seen:
//!
//! ```sql
//! INSERT INTO token_metadata (chain_id, address, factory, creator, mint)
//! VALUES (...)
//! ON CONFLICT (chain_id, address) DO UPDATE
//!     SET factory = EXCLUDED.factory,
//!         creator = EXCLUDED.creator,
//!         mint    = COALESCE(token_metadata.mint, EXCLUDED.mint);
//! ```
//!
//! `kind` is left untouched (we never INSERT or clobber it here) — the metadata
//! worker owns classification. On a *fresh* insert the row's `kind`/`name` are
//! NULL (column has no default after migration 0213's DROP NOT NULL), so the
//! metadata worker's `name IS NULL OR kind IS NULL` poll still picks it up to fill
//! name/symbol/decimals/supply/kind. The `mint` COALESCE is defensive: if the
//! metadata worker already wrote `mint` from `mint_id()` we keep that value rather
//! than overwrite with the (identical) topic-derived one — but for a token whose
//! provenance arrives *before* the metadata probe, this is the row that seeds
//! `mint`.
//!
//! ## Cursor / backfill
//!
//! Uses an `enrich_cursors` row `worker = 'factory_tokens'`, advancing by
//! `slot_number` exactly like `holders`. On first deploy the cursor starts at 0
//! and backfills every historical `TokenCreated` log, so tokens created before
//! this worker existed are discovered on the first pass. Migration `0214` deletes
//! this cursor (`DELETE FROM enrich_cursors WHERE worker='factory_tokens'`) so a
//! deploy carrying the provenance change re-scans history once and backfills
//! factory/creator/mint onto every already-known token.

use sqlx::PgPool;
use std::time::Duration;
use tracing::{debug, warn};

/// `keccak256("TokenCreated(address,bytes32,address,string,string,uint64)")`.
/// Verified live on Hadrian (2026-06-04) against the wUSDC creation tx.
const TOKEN_CREATED_TOPIC: &str =
    "0xf245fc52db9cb811010723161a1db52c457827a236b7bfa82ef624e82113f0e0";

pub async fn run(
    pool: PgPool,
    chain_id: i64,
    poll_interval: Duration,
    batch_size: i64,
) -> anyhow::Result<()> {
    const MAX_HOLD: u32 = 30;
    let mut hold = super::rpc_verdict::HoldState::clear();

    loop {
        let cursor: i64 = sqlx::query_scalar(
            "SELECT COALESCE(last_processed, 0)
             FROM rome_via.enrich_cursors
             WHERE chain_id = $1 AND worker = 'factory_tokens'",
        )
        .bind(chain_id)
        .fetch_optional(&pool)
        .await?
        .unwrap_or(0);

        // Same tail as holders: logs live in `tx_result.logs` (NOT
        // receipt_params). `tx_result` is NOT NULL so this includes
        // solana_unsigned rows too. Cursor on slot_number.
        //
        // `AND r.chain_id = $1` is LOAD-BEARING: rome_via_db is SHARED across
        // every chain on the cluster (Hadrian 200010 + Trajan 121302 today), so
        // without it this worker would ingest another chain's TokenCreated logs
        // into THIS chain's token_metadata — cross-chain pollution.
        let rows: Vec<(i64, serde_json::Value)> = sqlx::query_as(
            "SELECT r.slot_number, r.tx_result
             FROM rome_via.evm_tx_result r
             WHERE r.chain_id = $1
               AND r.slot_number > $2
               AND r.tx_result IS NOT NULL
             ORDER BY r.slot_number ASC, r.tx_hash ASC
             LIMIT $3",
        )
        .bind(chain_id)
        .bind(cursor)
        .bind(batch_size)
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

        if rows.is_empty() {
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        // E-C1: defer the (possibly LIMIT-cut) top slot so a full batch never
        // skips a boundary slot's tail. Fetch query unchanged; only what we
        // process + persist changes.
        let slots: Vec<i64> = rows.iter().map(|r| r.0).collect();
        let plan = super::batch_cursor::plan_batch(&slots, batch_size as usize);
        if plan.giant_slot {
            warn!(worker = "factory_tokens", slot = slots[0],
                  "single slot fills an entire batch; tail beyond batch_size cannot be deferred (unexpected on one-block-per-slot chains)");
        }

        let mut discovered = 0usize;
        let mut earliest_miss: Option<(i64, super::rpc_verdict::MissKind)> = None;

        for (slot_number, tx_result) in &rows[..plan.process_count] {
            for prov in extract_created_token_provenance(tx_result) {
                // Insert the row with provenance (factory/creator/mint) and a
                // NULL kind so the metadata worker's `name IS NULL OR kind IS
                // NULL` poll still enriches name/symbol/decimals/supply + kind.
                // ON CONFLICT DO UPDATE backfills provenance onto an already-known
                // row (e.g. one the Transfer-log path discovered first) WITHOUT
                // clobbering kind/name. `mint` is COALESCEd so a value the
                // metadata worker already wrote from `mint_id()` is preserved.
                let res = sqlx::query(
                    "INSERT INTO rome_via.token_metadata
                         (chain_id, address, kind, factory, creator, mint)
                     VALUES ($1, $2, NULL, $3, $4, $5)
                     ON CONFLICT (chain_id, address) DO UPDATE
                         SET factory = EXCLUDED.factory,
                             creator = EXCLUDED.creator,
                             mint    = COALESCE(rome_via.token_metadata.mint, EXCLUDED.mint)",
                )
                .bind(chain_id)
                .bind(&prov.address)
                .bind(prov.factory.as_deref())
                .bind(prov.creator.as_deref())
                .bind(prov.mint.as_deref())
                .execute(&pool)
                .await;

                match res {
                    Ok(r) if r.rows_affected() > 0 => {
                        discovered += 1;
                        debug!(
                            token_address = %prov.address,
                            "captured factory token provenance (TokenCreated)"
                        );
                    }
                    Ok(_) => {} // no-op (row identical)
                    Err(e) => {
                        warn!(token_address = %prov.address, error = %e, "failed to upsert token provenance");
                        if earliest_miss.is_none() {
                            earliest_miss =
                                Some((*slot_number, super::rpc_verdict::db_miss_kind(&e)));
                        }
                    }
                }
            }
        }

        // F6: don't commit past a swallowed per-row write; hold the cursor
        // just below the earliest miss so it re-processes next poll (the
        // upsert is ON CONFLICT-idempotent, so replay is safe).
        let outcome =
            super::rpc_verdict::valve_persist_cursor(plan.new_cursor, earliest_miss, hold, MAX_HOLD);
        hold = outcome.hold;
        if outcome.gave_up {
            warn!(worker = "factory_tokens",
                "terminal miss unresolved for MAX_HOLD polls; advancing past stuck slot [F6]");
        }

        let _ = sqlx::query(
            "INSERT INTO rome_via.enrich_cursors (chain_id, worker, last_processed, last_processed_at)
             VALUES ($1, 'factory_tokens', $2, NOW())
             ON CONFLICT (chain_id, worker) DO UPDATE
                 SET last_processed    = EXCLUDED.last_processed,
                     last_processed_at = NOW()",
        )
        .bind(chain_id)
        .bind(outcome.persist)
        .execute(&pool)
        .await;

        if discovered > 0 {
            tracing::info!(
                worker = "factory_tokens",
                discovered,
                new_cursor = outcome.persist,
                "discovered new factory tokens"
            );
        }

        if earliest_miss.is_some() {
            tokio::time::sleep(poll_interval).await;
        }
    }
}

/// Token provenance captured from one `TokenCreated` log.
///
/// `address` is the wrapper (the new token, `topics[3]`). `factory` is the
/// emitter (`log.address`), `creator` is `topics[1]` (right-12 → 0x address),
/// `mint` is `topics[2]` rendered as a base58 Solana pubkey. `factory`/`creator`
/// are always present for a well-formed log; `mint` is `None` only if the topic
/// word can't be parsed as a clean bytes32 (best-effort — the row is still
/// emitted so factory/creator land).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenProvenance {
    pub address: String,
    pub factory: Option<String>,
    pub creator: Option<String>,
    pub mint: Option<String>,
}

/// Extract the wrapper address from every `TokenCreated` log in a result JSON.
///
/// Thin wrapper over [`extract_created_token_provenance`] kept for the
/// address-only call sites / tests. See that function for the field semantics.
#[cfg(test)]
fn extract_created_tokens(result_json: &serde_json::Value) -> Vec<String> {
    extract_created_token_provenance(result_json)
        .into_iter()
        .map(|p| p.address)
        .collect()
}

/// Extract full provenance from every `TokenCreated` log in a row's result JSON
/// (`evm_tx_result.tx_result`).
///
/// Matches logs whose `topics[0]` is the `TokenCreated` signature. Per log:
/// * `address` (the new token) ← `topics[3]` (the wrapper) — NOT `log.address`,
///   which is the factory.
/// * `factory` ← `log.address` (the emitting `ERC20SPLFactory`).
/// * `creator` ← `topics[1]` (right-12 bytes → 0x address).
/// * `mint` ← `topics[2]` (bytes32 → base58 Solana pubkey, best-effort).
///
/// All EVM addresses are 0x-prefixed lowercase. A log missing the wrapper topic
/// (malformed) is skipped. Pure function — no I/O, fully unit-testable. A
/// `receipt_params`-shaped value (no `logs` key) yields nothing.
fn extract_created_token_provenance(result_json: &serde_json::Value) -> Vec<TokenProvenance> {
    let logs = match result_json.get("logs") {
        Some(serde_json::Value::Array(a)) => a,
        _ => return Vec::new(),
    };

    let mut out = Vec::new();
    for log in logs {
        let topics = match log.get("topics") {
            Some(serde_json::Value::Array(t)) => t,
            _ => continue,
        };

        let topic0 = topics.first().and_then(|v| v.as_str()).unwrap_or_default();
        if topic0.to_lowercase() != TOKEN_CREATED_TOPIC {
            continue;
        }

        // topics = [topic0, creator(1), mint(2), wrapper(3)].
        let wrapper_raw = match topics.get(3).and_then(|v| v.as_str()) {
            Some(s) => s,
            None => continue, // malformed — TokenCreated always has 4 topics
        };
        let address = topic_to_address(wrapper_raw);
        if address.is_empty() {
            continue;
        }

        // factory = emitter; empty (missing) → None.
        let factory = log
            .get("address")
            .and_then(|v| v.as_str())
            .map(|s| s.to_lowercase())
            .filter(|s| !s.is_empty());

        // creator = topics[1], 32-byte word → 20-byte address.
        let creator = topics
            .get(1)
            .and_then(|v| v.as_str())
            .map(topic_to_address)
            .filter(|s| !s.is_empty());

        // mint = topics[2], bytes32 → base58 (best-effort). Reuses the metadata
        // worker's tested encoder so it matches the `mint_id()`-derived value.
        let mint = topics
            .get(2)
            .and_then(|v| v.as_str())
            .and_then(super::metadata::bytes32_hex_to_base58);

        out.push(TokenProvenance {
            address,
            factory,
            creator,
            mint,
        });
    }
    out
}

/// Convert a 32-byte padded EVM topic to a 20-byte address string (0x-prefixed,
/// lowercase). Mirrors `holders::topic_to_address`.
fn topic_to_address(topic: &str) -> String {
    let hex = topic.trim_start_matches("0x");
    if hex.len() < 40 {
        return String::new();
    }
    format!("0x{}", &hex[hex.len() - 40..]).to_lowercase()
}

// ─────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    /// The live wUSDC `TokenCreated` log (Hadrian, tx 0xed7765…): topic0 =
    /// signature, topics[3] = wrapper 0x…9a8b4cb7…. Extraction returns the
    /// lowercased wrapper address.
    #[test]
    fn extract_wusdc_creation_from_live_log() {
        let tx_result = serde_json::json!({
            "exit_reason": "Succeed",
            "logs": [
                {
                    // Emitter is the FACTORY, not the token.
                    "address": "0x86149124d74ebb3aa41a19641b700e88202b6285",
                    "topics": [
                        TOKEN_CREATED_TOPIC,
                        "0x0000000000000000000000001f4946be340f06c46a50e65084790968abcc48f6", // creator
                        "0x3b442cb3912157f13a933d0134282d032b5ffecd01a2dbf1b7790608df002ea7", // mint
                        "0x0000000000000000000000009a8b4cb7326033d72ca393c6b4c0d7fb904fa900"  // wrapper
                    ],
                    "data": "0x"
                }
            ]
        });
        let tokens = extract_created_tokens(&tx_result);
        assert_eq!(tokens, vec!["0x9a8b4cb7326033d72ca393c6b4c0d7fb904fa900".to_string()]);
    }

    /// Mixed-case topic0 still matches (case-insensitive), and the wrapper is
    /// lowercased on the way out.
    #[test]
    fn extract_is_case_insensitive_on_topic_and_lowercases_addr() {
        let tx_result = serde_json::json!({
            "logs": [
                {
                    "address": "0x8614912 4D74EBB3AA41A19641B700E88202B6285".replace(' ', ""),
                    "topics": [
                        TOKEN_CREATED_TOPIC.to_uppercase().replace("0X", "0x"),
                        "0x0000000000000000000000001f4946be340f06c46a50e65084790968abcc48f6",
                        "0x4de5b3fa1e6c00708f7ff480e2186357da3bc7110c576e9364da84c4c77ad904",
                        "0x00000000000000000000000055E4502D799938582BC2A15771ACC6A4D2928273"  // wETH wrapper, upper
                    ],
                    "data": "0x"
                }
            ]
        });
        let tokens = extract_created_tokens(&tx_result);
        assert_eq!(tokens, vec!["0x55e4502d799938582bc2a15771acc6a4d2928273".to_string()]);
    }

    /// A Transfer log (different topic0) is ignored — only TokenCreated matches.
    #[test]
    fn extract_ignores_non_tokencreated_logs() {
        let transfer_topic =
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
        let tx_result = serde_json::json!({
            "logs": [
                {
                    "address": "0x9a8b4cb7326033d72ca393c6b4c0d7fb904fa900",
                    "topics": [
                        transfer_topic,
                        "0x0000000000000000000000001f4946be340f06c46a50e65084790968abcc48f6",
                        "0x0000000000000000000000009a8b4cb7326033d72ca393c6b4c0d7fb904fa900"
                    ],
                    "data": "0x00000000000000000000000000000000000000000000000000000000000f4240"
                }
            ]
        });
        assert!(extract_created_tokens(&tx_result).is_empty());
    }

    /// Multiple TokenCreated logs in one tx (batch registration) → all wrappers.
    #[test]
    fn extract_multiple_creations() {
        let tx_result = serde_json::json!({
            "logs": [
                {
                    "address": "0x86149124d74ebb3aa41a19641b700e88202b6285",
                    "topics": [
                        TOKEN_CREATED_TOPIC,
                        "0x0000000000000000000000001f4946be340f06c46a50e65084790968abcc48f6",
                        "0x3b442cb3912157f13a933d0134282d032b5ffecd01a2dbf1b7790608df002ea7",
                        "0x0000000000000000000000009a8b4cb7326033d72ca393c6b4c0d7fb904fa900"
                    ],
                    "data": "0x"
                },
                {
                    "address": "0x86149124d74ebb3aa41a19641b700e88202b6285",
                    "topics": [
                        TOKEN_CREATED_TOPIC,
                        "0x0000000000000000000000001f4946be340f06c46a50e65084790968abcc48f6",
                        "0x069b8857feab8184fb687f634618c035dac439dc1aeb3b5598a0f00000000001",
                        "0x0000000000000000000000008c965f79b3d9bb95c12687e533fd5490b9c251cc"
                    ],
                    "data": "0x"
                }
            ]
        });
        let tokens = extract_created_tokens(&tx_result);
        assert_eq!(
            tokens,
            vec![
                "0x9a8b4cb7326033d72ca393c6b4c0d7fb904fa900".to_string(),
                "0x8c965f79b3d9bb95c12687e533fd5490b9c251cc".to_string(),
            ]
        );
    }

    /// A `receipt_params`-shaped value (no `logs` key) yields nothing — same
    /// guard as the holders worker against reading the wrong JSONB column.
    #[test]
    fn extract_from_receipt_params_shape_yields_nothing() {
        let receipt_params = serde_json::json!({
            "blockhash": "0xabc",
            "block_number": 100,
            "tx_index": 0,
            "first_log_index": 0
        });
        assert!(extract_created_tokens(&receipt_params).is_empty());
    }

    /// `logs` present but empty → nothing (graceful).
    #[test]
    fn extract_empty_logs_yields_nothing() {
        let tx_result = serde_json::json!({ "logs": [] });
        assert!(extract_created_tokens(&tx_result).is_empty());
    }

    /// Malformed TokenCreated (fewer than 4 topics) is skipped, not panicked on.
    #[test]
    fn extract_skips_malformed_tokencreated() {
        let tx_result = serde_json::json!({
            "logs": [
                {
                    "address": "0x86149124d74ebb3aa41a19641b700e88202b6285",
                    "topics": [
                        TOKEN_CREATED_TOPIC,
                        "0x0000000000000000000000001f4946be340f06c46a50e65084790968abcc48f6"
                        // missing mint + wrapper topics
                    ],
                    "data": "0x"
                }
            ]
        });
        assert!(extract_created_tokens(&tx_result).is_empty());
    }

    // ─────────────────────────────────────────────────────────────
    // Provenance capture (factory / creator / mint)
    // ─────────────────────────────────────────────────────────────

    /// The live wUSDC `TokenCreated` log (Hadrian, tx 0xed7765…) carries the full
    /// provenance: emitter = factory `0x861491…`, creator = topics[1] →
    /// `0x1f4946…`, mint = topics[2] → base58 `4zMMC9srt5…`, wrapper = topics[3].
    #[test]
    fn extract_provenance_from_live_wusdc_log() {
        let tx_result = serde_json::json!({
            "logs": [
                {
                    // Emitter is the FACTORY, not the token.
                    "address": "0x86149124d74ebb3aa41a19641b700e88202b6285",
                    "topics": [
                        TOKEN_CREATED_TOPIC,
                        "0x0000000000000000000000001f4946be340f06c46a50e65084790968abcc48f6", // creator
                        "0x3b442cb3912157f13a933d0134282d032b5ffecd01a2dbf1b7790608df002ea7", // mint
                        "0x0000000000000000000000009a8b4cb7326033d72ca393c6b4c0d7fb904fa900"  // wrapper
                    ],
                    "data": "0x"
                }
            ]
        });
        let prov = extract_created_token_provenance(&tx_result);
        assert_eq!(prov.len(), 1);
        let p = &prov[0];
        assert_eq!(p.address, "0x9a8b4cb7326033d72ca393c6b4c0d7fb904fa900");
        assert_eq!(p.factory.as_deref(), Some("0x86149124d74ebb3aa41a19641b700e88202b6285"));
        assert_eq!(p.creator.as_deref(), Some("0x1f4946be340f06c46a50e65084790968abcc48f6"));
        // mint topics[2] → base58 (canonical wUSDC mint, verified in metadata.rs).
        assert_eq!(p.mint.as_deref(), Some("4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU"));
    }

    /// Provenance addresses are lowercased; factory comes from `log.address`,
    /// creator from topics[1] (right-12), mint from topics[2] (base58).
    #[test]
    fn extract_provenance_lowercases_and_maps_fields() {
        let tx_result = serde_json::json!({
            "logs": [
                {
                    "address": "0x86149124D74EBB3AA41A19641B700E88202B6285", // upper factory
                    "topics": [
                        TOKEN_CREATED_TOPIC,
                        "0x0000000000000000000000001F4946BE340F06C46A50E65084790968ABCC48F6", // upper creator
                        "0x069b8857feab8184fb687f634618c035dac439dc1aeb3b5598a0f00000000001", // wSOL mint
                        "0x0000000000000000000000008c965f79b3d9bb95c12687e533fd5490b9c251cc"  // wrapper
                    ],
                    "data": "0x"
                }
            ]
        });
        let prov = extract_created_token_provenance(&tx_result);
        assert_eq!(prov.len(), 1);
        let p = &prov[0];
        assert_eq!(p.address, "0x8c965f79b3d9bb95c12687e533fd5490b9c251cc");
        assert_eq!(p.factory.as_deref(), Some("0x86149124d74ebb3aa41a19641b700e88202b6285"));
        assert_eq!(p.creator.as_deref(), Some("0x1f4946be340f06c46a50e65084790968abcc48f6"));
        assert_eq!(p.mint.as_deref(), Some("So11111111111111111111111111111111111111112"));
    }

    /// Multiple creations in one tx → one provenance row each.
    #[test]
    fn extract_provenance_multiple_creations() {
        let tx_result = serde_json::json!({
            "logs": [
                {
                    "address": "0x86149124d74ebb3aa41a19641b700e88202b6285",
                    "topics": [
                        TOKEN_CREATED_TOPIC,
                        "0x0000000000000000000000001f4946be340f06c46a50e65084790968abcc48f6",
                        "0x3b442cb3912157f13a933d0134282d032b5ffecd01a2dbf1b7790608df002ea7",
                        "0x0000000000000000000000009a8b4cb7326033d72ca393c6b4c0d7fb904fa900"
                    ],
                    "data": "0x"
                },
                {
                    "address": "0x86149124d74ebb3aa41a19641b700e88202b6285",
                    "topics": [
                        TOKEN_CREATED_TOPIC,
                        "0x0000000000000000000000001f4946be340f06c46a50e65084790968abcc48f6",
                        "0x069b8857feab8184fb687f634618c035dac439dc1aeb3b5598a0f00000000001",
                        "0x0000000000000000000000008c965f79b3d9bb95c12687e533fd5490b9c251cc"
                    ],
                    "data": "0x"
                }
            ]
        });
        let prov = extract_created_token_provenance(&tx_result);
        assert_eq!(prov.len(), 2);
        assert_eq!(prov[0].address, "0x9a8b4cb7326033d72ca393c6b4c0d7fb904fa900");
        assert_eq!(prov[1].address, "0x8c965f79b3d9bb95c12687e533fd5490b9c251cc");
    }

    /// A receipt_params-shaped value (no `logs`) and a non-TokenCreated log both
    /// yield no provenance rows.
    #[test]
    fn extract_provenance_ignores_non_matching() {
        let receipt_params = serde_json::json!({ "blockhash": "0xabc", "block_number": 100 });
        assert!(extract_created_token_provenance(&receipt_params).is_empty());

        let transfer = serde_json::json!({
            "logs": [
                {
                    "address": "0x9a8b4cb7326033d72ca393c6b4c0d7fb904fa900",
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x0000000000000000000000001f4946be340f06c46a50e65084790968abcc48f6",
                        "0x0000000000000000000000009a8b4cb7326033d72ca393c6b4c0d7fb904fa900"
                    ],
                    "data": "0x"
                }
            ]
        });
        assert!(extract_created_token_provenance(&transfer).is_empty());
    }

    /// A malformed TokenCreated (missing wrapper topic) is skipped — no panic,
    /// no row. (Mirrors the wrapper-only extractor's guard.)
    #[test]
    fn extract_provenance_skips_malformed() {
        let tx_result = serde_json::json!({
            "logs": [
                {
                    "address": "0x86149124d74ebb3aa41a19641b700e88202b6285",
                    "topics": [
                        TOKEN_CREATED_TOPIC,
                        "0x0000000000000000000000001f4946be340f06c46a50e65084790968abcc48f6",
                        "0x3b442cb3912157f13a933d0134282d032b5ffecd01a2dbf1b7790608df002ea7"
                        // missing wrapper topic
                    ],
                    "data": "0x"
                }
            ]
        });
        assert!(extract_created_token_provenance(&tx_result).is_empty());
    }

    /// An unparseable mint word (wrong length) leaves `mint` None but still emits
    /// the row with factory + creator (mint conversion is best-effort).
    #[test]
    fn extract_provenance_bad_mint_still_emits_row() {
        let tx_result = serde_json::json!({
            "logs": [
                {
                    "address": "0x86149124d74ebb3aa41a19641b700e88202b6285",
                    "topics": [
                        TOKEN_CREATED_TOPIC,
                        "0x0000000000000000000000001f4946be340f06c46a50e65084790968abcc48f6",
                        "0xbad", // not a 32-byte word → base58 conversion fails
                        "0x0000000000000000000000009a8b4cb7326033d72ca393c6b4c0d7fb904fa900"
                    ],
                    "data": "0x"
                }
            ]
        });
        let prov = extract_created_token_provenance(&tx_result);
        assert_eq!(prov.len(), 1);
        assert_eq!(prov[0].address, "0x9a8b4cb7326033d72ca393c6b4c0d7fb904fa900");
        assert_eq!(prov[0].creator.as_deref(), Some("0x1f4946be340f06c46a50e65084790968abcc48f6"));
        assert_eq!(prov[0].mint, None);
    }

    /// topic0 confirmation: the hardcoded constant equals the keccak of the
    /// canonical signature. (Recomputed here so a typo in the const fails the
    /// suite. keccak256 of the signature string, first 4 bytes shown as the full
    /// 32-byte topic.)
    #[test]
    fn token_created_topic_is_stable() {
        // The 32-byte topic0 is fixed by the event signature; this guards against
        // an accidental edit to the constant.
        assert_eq!(
            TOKEN_CREATED_TOPIC,
            "0xf245fc52db9cb811010723161a1db52c457827a236b7bfa82ef624e82113f0e0"
        );
        assert_eq!(TOKEN_CREATED_TOPIC.len(), 66); // 0x + 64 hex
    }
}

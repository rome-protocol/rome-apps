//! Gate-events worker — freshness + history for gated (Arc-style) tokens.
//!
//! ## The staleness the metadata worker can't fix on its own
//!
//! [`super::metadata`] reads a token's gate state exactly ONCE: its poll
//! predicate is `name IS NULL OR kind IS NULL OR gated IS NULL`, so the moment
//! it writes a definitive `gated` the row drops out and is never re-read. That
//! is correct for name/symbol/decimals (immutable) but WRONG for the gate,
//! which is mutable and — critically — is usually wired/toggled a beat AFTER the
//! token first appears. Live example (Hadrian, 2026-08-04): `MBS Prime`
//! (`0xb519a2cb…`) was created one block before its WhitelistRestrictions module
//! (`0x94a5389a…`) was attached, so the metadata worker's single probe saw no
//! module and cached `gated=false, restriction_module=NULL` permanently — even
//! though the module is wired on-chain.
//!
//! ## The fix: index the two gate events, invalidate, let metadata re-derive
//!
//! This worker tails the same `evm_tx_result.tx_result` logs as
//! [`super::factory_tokens`] (a DB keyset scan by `slot_number`, NOT an
//! `eth_getLogs` call) and matches two topic0s:
//!
//! * **`SpecificRestrictionModuleSet(bytes32,address)`** — emitted by the
//!   ArcToken (`log.address` = token). Both args indexed → `topics = [topic0,
//!   typeId, module]`. We only act when `topics[1] ==
//!   keccak256("TRANSFER_RESTRICTION")` (the yield module reuses the same event
//!   with a different typeId). This is the canonical, deploy-path-independent
//!   token→module link.
//! * **`TransfersRestrictionToggled(bool)`** — emitted by the module
//!   (`log.address` = module) when the gate opens/closes. `bool` is NOT indexed,
//!   so it lives in `data` (a 32-byte word: 0 = open, 1 = restricted). We map
//!   module→token via `token_metadata.restriction_module`.
//!
//! For every matched event we (a) record it in `token_gate_events` (the history
//! feed) and (b) set the affected token's `token_metadata.gated = NULL`. That
//! re-enqueues it into the metadata worker's existing poll, which re-runs the
//! authoritative `getRestrictionModule` + `transfersAllowed()` read and writes
//! the fresh verdict. **The gate derivation stays in ONE place (the metadata
//! worker); this worker only invalidates.** The rome-via badge already reads
//! `gated`, so it self-freshes with no UI change.
//!
//! ## Cursor / backfill
//!
//! `enrich_cursors` row `worker = 'gate_events'`, advancing by `slot_number`
//! like `factory_tokens`. On first deploy the cursor starts at 0 and backfills
//! every historical gate event in slot order — so wiring events (earlier slot)
//! are always processed before their toggles (later slot), guaranteeing the
//! module→token link exists by the time a toggle needs it. This one pass fixes
//! every already-issued token whose gate the single metadata probe missed.

use sqlx::PgPool;
use std::time::Duration;
use tracing::{debug, warn};

/// `keccak256("SpecificRestrictionModuleSet(bytes32,address)")` — emitted by the
/// ArcToken on `setRestrictionModule`. topics = [topic0, typeId, module].
const SPECIFIC_RESTRICTION_MODULE_SET_TOPIC: &str =
    "0xe5d41ffde12968cb577153d35b775280c377967d3bcb834268d42e7e687ca312";

/// `keccak256("TransfersRestrictionToggled(bool)")` — emitted by the
/// WhitelistRestrictions module on `setTransfersAllowed`. bool is in `data`.
const TRANSFERS_RESTRICTION_TOGGLED_TOPIC: &str =
    "0x70a23fe37c63b4aecb5c585cbcc7e044a1601e38986a22eaf1932b87030ba097";

/// `keccak256("TRANSFER_RESTRICTION")` — the module-type id that gates transfers.
/// A `SpecificRestrictionModuleSet` whose `topics[1]` is anything else (e.g. the
/// yield module) is ignored.
const TRANSFER_RESTRICTION_TYPE: &str =
    "0x16d3efd52fe4afa679136c32a17cfe3bac40019518e3dd5b5d42aeb676bcb941";

/// A gate event parsed from one log, tagged with its position in the tx's `logs`
/// array (the stable `log_index` used for idempotent inserts).
#[derive(Debug, Clone, PartialEq, Eq)]
struct GateEventAt {
    log_index: i32,
    event: GateEvent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum GateEvent {
    /// The token wired a TRANSFER_RESTRICTION module. `token` = emitter,
    /// `module` = `topics[2]`.
    ModuleWired { token: String, module: String },
    /// The module's gate was toggled. `module` = emitter, `transfers_allowed` is
    /// the new value (true = open / ungated, false = restricted / gated).
    TransfersToggled {
        module: String,
        transfers_allowed: bool,
    },
}

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
             WHERE chain_id = $1 AND worker = 'gate_events'",
        )
        .bind(chain_id)
        .fetch_optional(&pool)
        .await?
        .unwrap_or(0);

        // Same tail as factory_tokens: logs live in `tx_result.logs`. `AND
        // chain_id = $1` is LOAD-BEARING — rome_via_db is shared across chains.
        let rows: Vec<(i64, String, serde_json::Value)> = sqlx::query_as(
            "SELECT r.slot_number, r.tx_hash, r.tx_result
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
            warn!(worker = "gate_events", slot = slots[0],
                  "single slot fills an entire batch; tail beyond batch_size cannot be deferred (unexpected on one-block-per-slot chains)");
        }

        let mut invalidated = 0usize;
        let mut earliest_miss: Option<(i64, super::rpc_verdict::MissKind)> = None;

        for (slot_number, tx_hash, tx_result) in &rows[..plan.process_count] {
            for GateEventAt { log_index, event } in extract_gate_events(tx_result) {
                match event {
                    GateEvent::ModuleWired { token, module } => {
                        // UPSERT the link + invalidate. Insert (not just update)
                        // so a token whose row hasn't been created yet still gets
                        // one — kind NULL keeps it in the metadata worker's poll,
                        // gated NULL forces the fresh gate read. Latest wiring
                        // wins (setRestrictionModule overwrites unconditionally).
                        let res = sqlx::query(
                            "INSERT INTO rome_via.token_metadata
                                 (chain_id, address, kind, restriction_module, gated)
                             VALUES ($1, $2, NULL, $3, NULL)
                             ON CONFLICT (chain_id, address) DO UPDATE
                                 SET restriction_module = EXCLUDED.restriction_module,
                                     gated              = NULL",
                        )
                        .bind(chain_id)
                        .bind(&token)
                        .bind(&module)
                        .execute(&pool)
                        .await;
                        if let Err(e) = res {
                            warn!(token = %token, error = %e, "gate_events: failed to link/invalidate module_wired");
                            if earliest_miss.is_none() {
                                earliest_miss =
                                    Some((*slot_number, super::rpc_verdict::db_miss_kind(&e)));
                            }
                        } else {
                            invalidated += 1;
                        }

                        if let Err(e) = record_event(
                            &pool, chain_id, Some(&token), &module, "module_wired", None,
                            *slot_number, tx_hash, log_index,
                        )
                        .await
                        {
                            if earliest_miss.is_none() {
                                earliest_miss =
                                    Some((*slot_number, super::rpc_verdict::db_miss_kind(&e)));
                            }
                        }
                    }
                    GateEvent::TransfersToggled {
                        module,
                        transfers_allowed,
                    } => {
                        // Map module→token via the recorded link, then invalidate
                        // so the metadata worker re-derives gated from head state.
                        let tokens: Vec<(String,)> = match sqlx::query_as(
                            "SELECT address FROM rome_via.token_metadata
                             WHERE chain_id = $1 AND LOWER(restriction_module) = LOWER($2)",
                        )
                        .bind(chain_id)
                        .bind(&module)
                        .fetch_all(&pool)
                        .await
                        {
                            Ok(t) => t,
                            Err(e) => {
                                warn!(module = %module, error = %e, "gate_events: token lookup failed on toggle");
                                if earliest_miss.is_none() {
                                    earliest_miss =
                                        Some((*slot_number, super::rpc_verdict::db_miss_kind(&e)));
                                }
                                continue;
                            }
                        };

                        for (token,) in &tokens {
                            let res = sqlx::query(
                                "UPDATE rome_via.token_metadata
                                 SET gated = NULL
                                 WHERE chain_id = $1 AND address = $2",
                            )
                            .bind(chain_id)
                            .bind(token)
                            .execute(&pool)
                            .await;
                            if let Err(e) = res {
                                warn!(token = %token, error = %e, "gate_events: failed to invalidate on toggle");
                                if earliest_miss.is_none() {
                                    earliest_miss =
                                        Some((*slot_number, super::rpc_verdict::db_miss_kind(&e)));
                                }
                            } else {
                                invalidated += 1;
                            }
                        }

                        if let Err(e) = record_event(
                            &pool,
                            chain_id,
                            tokens.first().map(|(t,)| t.as_str()),
                            &module,
                            "transfers_toggled",
                            Some(transfers_allowed),
                            *slot_number,
                            tx_hash,
                            log_index,
                        )
                        .await
                        {
                            if earliest_miss.is_none() {
                                earliest_miss =
                                    Some((*slot_number, super::rpc_verdict::db_miss_kind(&e)));
                            }
                        }

                        debug!(module = %module, transfers_allowed, matched = tokens.len(), "gate_events: toggle");
                    }
                }
            }
        }

        // F6: don't commit past a swallowed per-event write; hold the cursor
        // just below the earliest miss so it re-processes next poll (the
        // writes above are ON CONFLICT-idempotent, so replay is safe).
        let outcome =
            super::rpc_verdict::valve_persist_cursor(plan.new_cursor, earliest_miss, hold, MAX_HOLD);
        hold = outcome.hold;
        if outcome.gave_up {
            warn!(worker = "gate_events",
                "terminal miss unresolved for MAX_HOLD polls; advancing past stuck slot [F6]");
        }

        let _ = sqlx::query(
            "INSERT INTO rome_via.enrich_cursors (chain_id, worker, last_processed, last_processed_at)
             VALUES ($1, 'gate_events', $2, NOW())
             ON CONFLICT (chain_id, worker) DO UPDATE
                 SET last_processed    = EXCLUDED.last_processed,
                     last_processed_at = NOW()",
        )
        .bind(chain_id)
        .bind(outcome.persist)
        .execute(&pool)
        .await;

        if invalidated > 0 {
            tracing::info!(
                worker = "gate_events",
                invalidated,
                new_cursor = outcome.persist,
                "recorded gate events + invalidated gated cache"
            );
        }

        if earliest_miss.is_some() {
            tokio::time::sleep(poll_interval).await;
        }
    }
}

/// Insert one history row (idempotent on `(chain_id, tx_hash, log_index)`).
#[allow(clippy::too_many_arguments)]
async fn record_event(
    pool: &PgPool,
    chain_id: i64,
    token: Option<&str>,
    module: &str,
    event_type: &str,
    transfers_allowed: Option<bool>,
    slot_number: i64,
    tx_hash: &str,
    log_index: i32,
) -> Result<(), sqlx::Error> {
    let res = sqlx::query(
        "INSERT INTO rome_via.token_gate_events
             (chain_id, token_address, module_address, event_type,
              transfers_allowed, slot_number, tx_hash, log_index)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (chain_id, tx_hash, log_index) DO NOTHING",
    )
    .bind(chain_id)
    .bind(token)
    .bind(module)
    .bind(event_type)
    .bind(transfers_allowed)
    .bind(slot_number)
    .bind(tx_hash)
    .bind(log_index)
    .execute(pool)
    .await;
    if let Err(e) = &res {
        warn!(module = %module, event_type, error = %e, "gate_events: failed to record history row");
    }
    // F6: propagate (rather than swallow) so the caller can hold the cursor —
    // a lost audit row would otherwise be gone forever once the slot passes.
    res.map(|_| ())
}

/// Extract every TRANSFER_RESTRICTION gate event from a row's `tx_result` JSON,
/// tagged with its `logs`-array position. Pure — no I/O. A `receipt_params`-
/// shaped value (no `logs`) or an unrelated log yields nothing.
fn extract_gate_events(result_json: &serde_json::Value) -> Vec<GateEventAt> {
    let logs = match result_json.get("logs") {
        Some(serde_json::Value::Array(a)) => a,
        _ => return Vec::new(),
    };

    let mut out = Vec::new();
    for (idx, log) in logs.iter().enumerate() {
        let topics = match log.get("topics") {
            Some(serde_json::Value::Array(t)) => t,
            _ => continue,
        };
        let topic0 = topics
            .first()
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_lowercase();

        if topic0 == SPECIFIC_RESTRICTION_MODULE_SET_TOPIC {
            // topics = [topic0, typeId(1), module(2)]. Only the transfer gate.
            let type_id = topics.get(1).and_then(|v| v.as_str()).unwrap_or_default();
            if type_id.to_lowercase() != TRANSFER_RESTRICTION_TYPE {
                continue;
            }
            let token = match log.get("address").and_then(|v| v.as_str()) {
                Some(s) if !s.is_empty() => s.to_lowercase(),
                _ => continue, // emitter is the token — a missing address is unusable
            };
            let module = match topics.get(2).and_then(|v| v.as_str()).map(topic_to_address) {
                Some(m) if !m.is_empty() => m,
                _ => continue,
            };
            out.push(GateEventAt {
                log_index: idx as i32,
                event: GateEvent::ModuleWired { token, module },
            });
        } else if topic0 == TRANSFERS_RESTRICTION_TOGGLED_TOPIC {
            // Emitter is the module; bool `transfers_allowed` is in `data`.
            let module = match log.get("address").and_then(|v| v.as_str()) {
                Some(s) if !s.is_empty() => s.to_lowercase(),
                _ => continue,
            };
            let transfers_allowed = match log
                .get("data")
                .and_then(|v| v.as_str())
                .and_then(parse_bool_word)
            {
                Some(b) => b,
                None => continue, // malformed data word — skip rather than guess
            };
            out.push(GateEventAt {
                log_index: idx as i32,
                event: GateEvent::TransfersToggled {
                    module,
                    transfers_allowed,
                },
            });
        }
    }
    out
}

/// Parse an ABI bool word from an event `data` hex string. A 32-byte word encodes
/// `false` as all-zero and `true` as `…01`; we accept any non-zero as true.
/// Empty / `0x` (no data) → `None`.
fn parse_bool_word(data: &str) -> Option<bool> {
    let hex = data.trim_start_matches("0x").trim_start_matches("0X");
    if hex.is_empty() {
        return None;
    }
    Some(hex.chars().any(|c| c != '0'))
}

/// 32-byte padded EVM topic → 20-byte 0x address (lowercase). Mirrors
/// `factory_tokens::topic_to_address`.
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

    fn word(addr40: &str) -> String {
        format!("0x{:0>64}", addr40.trim_start_matches("0x"))
    }

    /// A TRANSFER_RESTRICTION wiring log → ModuleWired{token=emitter, module=topics[2]}.
    /// Uses MBS Prime's real addresses (Hadrian, 2026-08-04).
    #[test]
    fn extracts_module_wired_for_transfer_restriction() {
        let tx = serde_json::json!({
            "logs": [{
                "address": "0xb519a2cb728197f47b58fbada2f14ce074d32dac", // token (emitter)
                "topics": [
                    SPECIFIC_RESTRICTION_MODULE_SET_TOPIC,
                    TRANSFER_RESTRICTION_TYPE,                         // typeId
                    word("0x94a5389ac23de33eb240c8b874b7e19cd57c2788") // module
                ],
                "data": "0x"
            }]
        });
        assert_eq!(
            extract_gate_events(&tx),
            vec![GateEventAt {
                log_index: 0,
                event: GateEvent::ModuleWired {
                    token: "0xb519a2cb728197f47b58fbada2f14ce074d32dac".into(),
                    module: "0x94a5389ac23de33eb240c8b874b7e19cd57c2788".into(),
                }
            }]
        );
    }

    /// A SpecificRestrictionModuleSet for a DIFFERENT typeId (e.g. the yield
    /// module) is ignored — only the transfer gate matters here.
    #[test]
    fn ignores_non_transfer_restriction_module_set() {
        let yield_type = "0x0000000000000000000000000000000000000000000000000000000000000abc";
        let tx = serde_json::json!({
            "logs": [{
                "address": "0xb519a2cb728197f47b58fbada2f14ce074d32dac",
                "topics": [
                    SPECIFIC_RESTRICTION_MODULE_SET_TOPIC,
                    yield_type,
                    word("0x94a5389ac23de33eb240c8b874b7e19cd57c2788")
                ],
                "data": "0x"
            }]
        });
        assert!(extract_gate_events(&tx).is_empty());
    }

    /// TransfersRestrictionToggled(false) = restricted/gated → transfers_allowed false.
    #[test]
    fn extracts_toggle_restricted() {
        let tx = serde_json::json!({
            "logs": [{
                "address": "0x94a5389ac23de33eb240c8b874b7e19cd57c2788", // module (emitter)
                "topics": [TRANSFERS_RESTRICTION_TOGGLED_TOPIC],
                "data": "0x0000000000000000000000000000000000000000000000000000000000000000"
            }]
        });
        assert_eq!(
            extract_gate_events(&tx),
            vec![GateEventAt {
                log_index: 0,
                event: GateEvent::TransfersToggled {
                    module: "0x94a5389ac23de33eb240c8b874b7e19cd57c2788".into(),
                    transfers_allowed: false,
                }
            }]
        );
    }

    /// TransfersRestrictionToggled(true) = open/ungated → transfers_allowed true.
    #[test]
    fn extracts_toggle_open() {
        let tx = serde_json::json!({
            "logs": [{
                "address": "0x94a5389ac23de33eb240c8b874b7e19cd57c2788",
                "topics": [TRANSFERS_RESTRICTION_TOGGLED_TOPIC],
                "data": "0x0000000000000000000000000000000000000000000000000000000000000001"
            }]
        });
        match &extract_gate_events(&tx)[0].event {
            GateEvent::TransfersToggled { transfers_allowed, .. } => assert!(*transfers_allowed),
            other => panic!("expected toggle, got {other:?}"),
        }
    }

    /// A toggle with no `data` word is skipped (we don't guess the gate state).
    #[test]
    fn skips_toggle_with_empty_data() {
        let tx = serde_json::json!({
            "logs": [{
                "address": "0x94a5389ac23de33eb240c8b874b7e19cd57c2788",
                "topics": [TRANSFERS_RESTRICTION_TOGGLED_TOPIC],
                "data": "0x"
            }]
        });
        assert!(extract_gate_events(&tx).is_empty());
    }

    /// Unrelated logs (e.g. an ERC-20 Transfer) yield nothing; log_index tracks
    /// the real array position of the matched event.
    #[test]
    fn ignores_unrelated_and_tracks_log_index() {
        let transfer = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
        let tx = serde_json::json!({
            "logs": [
                { "address": "0xaaaa", "topics": [transfer], "data": "0x" },
                {
                    "address": "0x94a5389ac23de33eb240c8b874b7e19cd57c2788",
                    "topics": [TRANSFERS_RESTRICTION_TOGGLED_TOPIC],
                    "data": "0x0000000000000000000000000000000000000000000000000000000000000000"
                }
            ]
        });
        let out = extract_gate_events(&tx);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].log_index, 1);
    }

    /// A `receipt_params`-shaped value (no `logs`) yields nothing.
    #[test]
    fn receipt_params_shape_yields_nothing() {
        let rp = serde_json::json!({ "blockhash": "0xabc", "block_number": 100 });
        assert!(extract_gate_events(&rp).is_empty());
    }

    #[test]
    fn parse_bool_word_cases() {
        assert_eq!(parse_bool_word("0x0000000000000000000000000000000000000000000000000000000000000001"), Some(true));
        assert_eq!(parse_bool_word("0x0000000000000000000000000000000000000000000000000000000000000000"), Some(false));
        assert_eq!(parse_bool_word("0x"), None);
        assert_eq!(parse_bool_word(""), None);
    }

    /// The hardcoded topic0 constants must equal keccak256 of their signatures —
    /// a typo silently stops matching, so guard it in CI.
    #[test]
    fn gate_event_topics_match_keccak256_of_signatures() {
        use sha3::{Digest, Keccak256};
        let topic = |sig: &str| {
            let h = Keccak256::digest(sig.as_bytes());
            format!("0x{}", hex_lower(&h))
        };
        assert_eq!(
            topic("SpecificRestrictionModuleSet(bytes32,address)"),
            SPECIFIC_RESTRICTION_MODULE_SET_TOPIC
        );
        assert_eq!(
            topic("TransfersRestrictionToggled(bool)"),
            TRANSFERS_RESTRICTION_TOGGLED_TOPIC
        );
        // topic1 filter = keccak256 of the plain type string.
        assert_eq!(
            format!("0x{}", hex_lower(&Keccak256::digest(b"TRANSFER_RESTRICTION"))),
            TRANSFER_RESTRICTION_TYPE
        );
    }

    fn hex_lower(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}

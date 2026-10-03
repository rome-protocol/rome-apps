//! Reads `audit.chain_event` rows in the total order (IMPL-PLAN §"H2":
//! `block_number, tx_index, log_index`) that every builder in this module
//! depends on for determinism, plus the small `args` JSONB arg-extraction
//! helpers every builder needs. `event_id` is read back too (it's the
//! `opened_by_event`/`closed_by_event` foreign key each interval carries)
//! but never used to ORDER anything — only `(block_number, tx_index,
//! log_index)` is the natural order (C1).
//!
//! Takes an open `&mut Transaction`, not a bare `&PgPool` (H3, P2
//! review): [`super::rebuild::rebuild_tier2`] runs the whole TRUNCATE +
//! recompute inside ONE transaction, so every read here must go through
//! that same transaction — reading from the pool directly would see
//! whatever isolation level gives it, not necessarily the in-progress
//! rebuild's own writes-so-far (irrelevant here since reads precede writes
//! per table, but the shared executor is what makes the whole rebuild
//! atomic to any concurrent reader).

use std::str::FromStr;

use bigdecimal::BigDecimal;
use sqlx::{Postgres, Transaction};

/// One `audit.chain_event` row, as read back for Tier-2 derivation. `args`
/// stays a raw `serde_json::Value` — no need for `ArgValue: Deserialize`
/// (which P0/P1 never added); the small `arg_*` helpers below read the
/// exact JSON shapes `ingest::pipeline` writes (untagged `ArgValue`
/// serialization: `Address`/`Bytes32` as bare lowercase hex strings,
/// `Uint256` as a bare decimal string, `Bool` as a bare JSON bool).
///
/// `tx_signer` (P4a, additive): `audit.chain_event.tx_signer` itself has
/// been non-NULL since P1 (populated from `evm_tx.from_address` at ingest —
/// see `ingest::pipeline::try_ingest_log`/`resolve_tx_receipt`'s
/// `MissingSigner` guard). Carried here as `Option<[u8; 20]>`, not a bare
/// array, because a row ingested by a hypothetical PRE-signer-guard build
/// (or any future relaxation of that guard) is a real possibility this
/// reader must not silently coerce — `None` for anything that doesn't
/// parse to exactly 20 bytes. P4b's code_change builder needs this to
/// attribute a state-changing tx to its signer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainEventRow {
    pub event_id: i64,
    pub block_number: i64,
    pub tx_index: i32,
    pub log_index: i32,
    pub event_name: String,
    pub args: serde_json::Value,
    pub tx_signer: Option<[u8; 20]>,
}

/// One asset's resolved sources for the allowlist/gate/exposure builders.
/// Caller-supplied (P2 scope note in the module doc) — never hardcoded.
#[derive(Debug, Clone)]
pub struct AssetSources {
    pub asset_id: String,
    /// The resolved Axis-1 module's `source_contract` address —
    /// `WhitelistStatusChanged`/`TransfersRestrictionToggled` are emitted
    /// from here (IMPL-PLAN §"conventions": capture the ABI of the
    /// *resolved* module).
    pub axis1_module: [u8; 20],
    /// The `ArcToken` contract's own address — `Transfer` events for
    /// exposure-window transfer-shape classification.
    pub token_address: [u8; 20],
    /// This asset's `RestrictionsRouter` (P4b-i, `screening_gap`'s epoch
    /// scoping) — `None` when the asset has no router configured, which
    /// means no epochs, which means [`super::screening::build_transfer_screening_and_gaps`]
    /// never reports a gap (honest — never a fabricated alarm from a router
    /// this asset doesn't have).
    pub router_address: Option<[u8; 20]>,
    /// Every yield-token ERC-20 this asset has ever pointed at (P4b-i
    /// `yield_run`/`yield_credit` — capture §1.2's resolution HISTORY, not
    /// just the current pointer: an asset that re-pointed via
    /// `YieldTokenUpdated` gets one run/credit series per yield token it
    /// ever used, keyed by yield_token in the PK). Empty ⇒ no yield rows.
    pub yield_tokens: Vec<[u8; 20]>,
    /// Every `YieldBlacklistRestrictions` module this asset has ever routed
    /// through (P4b-i `yield_blacklist_interval`). Merged by position — see
    /// [`merge_events_by_position`] — since more than one module's history
    /// can apply across a module swap. Empty ⇒ no yield-blacklist rows.
    pub yield_blacklist_modules: Vec<[u8; 20]>,
    /// This asset's `ArcTokenPurchase` storefront (P4b-ii `sale` /
    /// `code_change`'s `STOREFRONT_UPGRADE`), if this asset is ever sold
    /// through one. `None` ⇒ no sale rows and no storefront-upgrade rows for
    /// this asset — never fabricated from an address it doesn't have.
    pub storefront: Option<[u8; 20]>,
    /// Every purchase-token ERC-20 this asset's storefront has ever pointed
    /// at (P4b-ii `sale`'s payment leg — capture §1.2's resolution HISTORY,
    /// same discipline as `yield_tokens`: an old, no-longer-current purchase
    /// token can still be the payment leg for a historical sale). Empty ⇒
    /// `sale`'s payment leg is always `None` for this asset.
    pub purchase_tokens: Vec<[u8; 20]>,
    /// This asset's `ArcTokenFactoryV2` (P4b-ii `code_change`'s
    /// `UPGRADE`-via-factory / `MODULE_LINKED`). `None` ⇒ only the token's
    /// OWN `Upgraded`/`SpecificRestrictionModuleSet` contribute code_change
    /// rows for this asset — never a fabricated factory-mediated row.
    pub factory: Option<[u8; 20]>,
}

/// Full rebuild input: one chain, N assets (allowlist/gate/exposure are
/// asset-scoped), plus the chain-global sanctions modules and routers
/// (`sanction_denyset_interval`/`router_sanctions_epoch` are NOT per-asset —
/// IMPL-PLAN §2 H1 sweep note).
#[derive(Debug, Clone)]
pub struct Tier2Config {
    pub chain_id: i64,
    pub assets: Vec<AssetSources>,
    pub sanctions_modules: Vec<[u8; 20]>,
    pub routers: Vec<[u8; 20]>,
    /// The Rome protocol multisig address (P4b-ii `code_change`'s
    /// `signer_attribution` — checked BEFORE any role lookup). `None` ⇒
    /// `ROME_MULTISIG` is structurally unreachable — multisig attribution
    /// is strictly opt-in, never inferred.
    pub multisig: Option<[u8; 20]>,
}

/// The raw positional row `sqlx` decodes below — `(event_id, block_number,
/// tx_index, log_index, event_name, args, tx_signer)` — before it is mapped
/// into the [`ChainEventRow`] struct. `tx_signer` is `Option` for
/// NULL-tolerance (see the decode note at the query).
type RawEventRow = (i64, i64, i32, i32, String, serde_json::Value, Option<Vec<u8>>);

/// Reads every `audit.chain_event` row for `(chain_id, source_contract)`
/// whose `event_name` is in `event_names`, ordered `(block_number, tx_index,
/// log_index)` — the total order every builder assumes its input arrives in.
pub async fn fetch_events(
    tx: &mut Transaction<'_, Postgres>,
    chain_id: i64,
    source_contract: &[u8; 20],
    event_names: &[&str],
) -> Result<Vec<ChainEventRow>, sqlx::Error> {
    // P4a NIT fix: `tx_signer` decodes as `Option<Vec<u8>>`, not a bare
    // `Vec<u8>` — the column is NOT NULL today (so this is unreachable in
    // practice), but decoding it as non-nullable would make a real NULL
    // FAIL the whole `fetch_all` with a decode error rather than mapping to
    // `None`, contradicting this fn's own NULL-tolerance doc above.
    let rows: Vec<RawEventRow> = sqlx::query_as(
        r#"
        SELECT event_id, block_number, tx_index, log_index, event_name, args, tx_signer
        FROM audit.chain_event
        WHERE chain_id = $1 AND source_contract = $2 AND event_name = ANY($3)
        ORDER BY block_number, tx_index, log_index
        "#,
    )
    .bind(chain_id)
    .bind(source_contract.as_slice())
    .bind(event_names)
    .fetch_all(&mut **tx)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(event_id, block_number, tx_index, log_index, event_name, args, tx_signer)| {
                ChainEventRow {
                    event_id,
                    block_number,
                    tx_index,
                    log_index,
                    event_name,
                    args,
                    tx_signer: tx_signer.and_then(|bytes| bytes.as_slice().try_into().ok()),
                }
            },
        )
        .collect())
}

/// Reads an `Address` arg: a bare lowercase `0x`-prefixed hex string in the
/// JSON, decoded to exactly 20 bytes. Returns `None` on a missing key or a
/// malformed/wrong-width value — callers treat that as "skip this event",
/// matching the rest of this module's "never invent, never coerce" discipline.
/// (For a `Bytes32` typeId arg, see [`arg_bytes32`] below instead.)
pub(super) fn arg_address(args: &serde_json::Value, key: &str) -> Option<[u8; 20]> {
    let s = args.get(key)?.as_str()?;
    let bytes = hex::decode(s.trim_start_matches("0x")).ok()?;
    bytes.try_into().ok()
}

/// Reads a `Bytes32` arg (e.g. `typeId`) as a fixed 32-byte array.
pub(super) fn arg_bytes32(args: &serde_json::Value, key: &str) -> Option<[u8; 32]> {
    let s = args.get(key)?.as_str()?;
    let bytes = hex::decode(s.trim_start_matches("0x")).ok()?;
    bytes.try_into().ok()
}

pub(super) fn arg_bool(args: &serde_json::Value, key: &str) -> Option<bool> {
    args.get(key)?.as_bool()
}

pub(super) const ZERO_ADDRESS: [u8; 20] = [0u8; 20];

/// Reads a `Uint256` arg as a full-width [`BigDecimal`] — never `i64`/`u128`,
/// both of which overflow a real 256-bit amount (P4b-i's yield-amount paths:
/// `Transfer.value`, `YieldDistributed.amount`). The JSON value is the bare
/// decimal string `ingest::pipeline`'s `ArgValue::Uint256` writes; anything
/// that doesn't match `^(0|[1-9][0-9]*)$` — missing key, non-string, leading
/// zero, a sign, non-digit — is malformed and returns `None`, matching this
/// module's "never invent, never coerce" discipline for every other `arg_*`
/// helper (callers treat `None` as skip-this-event).
pub(super) fn arg_uint256(args: &serde_json::Value, key: &str) -> Option<BigDecimal> {
    let s = args.get(key)?.as_str()?;
    if !is_bare_decimal(s) {
        return None;
    }
    BigDecimal::from_str(s).ok()
}

/// `^(0|[1-9][0-9]*)$` without pulling in the `regex` crate (not a workspace
/// dep here): exactly `"0"`, or a non-zero-leading run of ASCII digits.
fn is_bare_decimal(s: &str) -> bool {
    match s.as_bytes() {
        [b'0'] => true,
        [first, rest @ ..] if first.is_ascii_digit() && *first != b'0' => {
            rest.iter().all(u8::is_ascii_digit)
        }
        _ => false,
    }
}

/// Merges N already `(block_number, tx_index, log_index)`-ordered sources
/// (each individually guaranteed by [`fetch_events`]) into ONE stream in
/// that same total order — used wherever a builder's input spans more than
/// one `source_contract` (P4b-i: multiple `yield_blacklist_modules` history,
/// or a yield-token's `Transfer`s merged against its `ArcToken`'s
/// `YieldDistributed`s). Never sorts by `event_id` (C1). A position tie
/// (not expected on a real chain — two distinct logs can't share a
/// `log_index` within one tx) breaks on `event_name` so the result is fully
/// deterministic regardless of which source list was passed first.
pub(super) fn merge_events_by_position(sources: Vec<Vec<ChainEventRow>>) -> Vec<ChainEventRow> {
    let mut merged: Vec<ChainEventRow> = sources.into_iter().flatten().collect();
    merged.sort_by(|a, b| {
        (a.block_number, a.tx_index, a.log_index, a.event_name.as_str()).cmp(&(
            b.block_number,
            b.tx_index,
            b.log_index,
            b.event_name.as_str(),
        ))
    });
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arg_uint256_round_trips_a_bare_decimal_string() {
        let args = serde_json::json!({ "value": "123456789012345678901234567890" });
        assert_eq!(
            arg_uint256(&args, "value"),
            Some(BigDecimal::from_str("123456789012345678901234567890").unwrap())
        );
    }

    #[test]
    fn arg_uint256_accepts_zero() {
        let args = serde_json::json!({ "value": "0" });
        assert_eq!(arg_uint256(&args, "value"), Some(BigDecimal::from(0)));
    }

    #[test]
    fn arg_uint256_rejects_leading_zero() {
        let args = serde_json::json!({ "value": "0123" });
        assert_eq!(arg_uint256(&args, "value"), None);
    }

    #[test]
    fn arg_uint256_rejects_non_digit_and_missing_key() {
        assert_eq!(
            arg_uint256(&serde_json::json!({ "value": "-5" }), "value"),
            None
        );
        assert_eq!(
            arg_uint256(&serde_json::json!({ "value": "12.5" }), "value"),
            None
        );
        assert_eq!(arg_uint256(&serde_json::json!({}), "value"), None);
    }

    #[test]
    fn merge_events_by_position_orders_across_sources_never_by_insertion() {
        let ev = |id: i64, block: i64, tx: i32, log: i32, name: &str| ChainEventRow {
            event_id: id,
            block_number: block,
            tx_index: tx,
            log_index: log,
            event_name: name.to_string(),
            args: serde_json::json!({}),
            tx_signer: None,
        };
        // Source B is passed FIRST but its events sort AFTER source A's.
        let source_a = vec![ev(1, 100, 0, 0, "A")];
        let source_b = vec![ev(2, 100, 0, 1, "B")];
        let merged = merge_events_by_position(vec![source_b, source_a]);
        assert_eq!(
            merged.iter().map(|e| e.event_id).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }
}

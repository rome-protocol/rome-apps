//! `transfer_screening` + `screening_gap` (P4b-i) — capture §4 rule 2
//! ("Sanctions clearance ↔ token — log-order adjacency"): `TransferScreened`
//! is emitted inside the token's `_update`, so it **precedes** the
//! resulting `Transfer` log in the same tx. Pairing is **purely positional**
//! — nearest preceding unmatched `TransferScreened` in the SAME tx — never
//! by argument tuple (two identical `(from,to,amount)` pairs in one tx must
//! still pair by position, not collide on equal args).
//!
//! **Why a stack, not a scan-with-flags:** "nearest preceding unmatched" in
//! a stream walked in increasing log-index order is exactly LIFO — each
//! `TransferScreened` is pushed as it's seen; each `Transfer` pops the most
//! recently pushed (nearest, still-unmatched) screening. A tx logging
//! `TS@0, TS@2, T@3, T@5` pairs `T@3↔TS@2` (pop first) then `T@5↔TS@0` (pop
//! remaining) — exactly the spec's own worked example.
//!
//! **The stack resets at every tx boundary** — pairing (and leftover
//! bookkeeping) never crosses transactions; a `TransferScreened` unmatched
//! at the end of ITS tx is simply a module screening a foreign token
//! (bidirectional screening covers `from` AND `to`, so a module watching
//! this asset can screen a transfer that belongs to a different token
//! entirely) — **no row, no alarm**, by construction (nothing pushes an
//! "unmatched screening" fact anywhere).
//!
//! **HIGH-1 (P4b-i review): the pairing walk is CHAIN-GLOBAL, not
//! per-asset — this is load-bearing, not an optimization.** `TransferScreened`
//! carries no token/contract identity at all (verified against real
//! on-chain logs — `GlobalSanctions.TransferScreened(address indexed from, address indexed
//! to, uint256 amount)`, exactly 3 topics + 1 data word, no fourth arg and
//! no way to recover the screened token from `msg.sender` either since the
//! event is emitted BY the sanctions module, not the token). One physical
//! `GlobalSanctions` module instance can be the router-registered global
//! implementation for MANY tokens (spec: "router-scoped, not per-token"), so
//! a per-asset pairing walk that only sees ITS OWN transfers can be handed a
//! stack containing another asset's still-unmatched screening and wrongly
//! consume it — masking a real `screening_gap` on THIS asset's unscreened
//! transfer and fabricating a false "cleared" `transfer_screening` row
//! pointing at the wrong screening event. The fix: **exactly ONE pairing
//! walk per rebuild**, over the UNION of every configured asset's token
//! `Transfer`s (each tagged with its owning `asset_id`) merged against the
//! full chain-global `TransferScreened` set — the SAME LIFO/tx-reset/
//! positional algorithm, just fed the true on-chain consumption order
//! instead of an artificially asset-scoped slice of it. Each resulting pair
//! or gap is then attributed to the transfer's own `asset_id`, and a gap is
//! scoped to THAT asset's own router epochs (`router_epochs_by_asset`).
//! **Residual, stated honestly:** a `GlobalSanctions` module screening a
//! token that is NOT in `config.assets` at all still leaves an unmatched
//! screening this walk can't attribute anywhere — unknowable without that
//! token's own Transfer stream, which is exactly the P2-era scope note this
//! whole crate already operates under (caller-supplied config, never a
//! chain-wide scan). Configuring every real asset that shares a sanctions
//! module removes the residual; an unconfigured foreign token does not
//! corrupt anything (it just contributes an unattributed, silently-dropped
//! unmatched screening — never a false pair on a KNOWN asset).
//!
//! **`screening_gap`** is the mirror case: an unpaired `Transfer` (empty
//! stack) is a gap **iff** its full `(block, tx_index, log_index)` position
//! sits inside ITS OWN asset's router's active sanctions epoch
//! ([`super::router_epoch::RouterEpochRow::contains_position`], the same M2
//! half-open convention `gate_interval` uses) — pre-epoch, post-removal, and
//! same-block-before-registration transfers are never gaps. No router
//! configured for that asset ⇒ never a gap — honest, never a fabricated
//! alarm from an epoch this asset doesn't have.

use std::collections::HashMap;

use super::fetch::ChainEventRow;
use super::router_epoch::RouterEpochRow;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferScreeningRow {
    pub asset_id: String,
    pub block_number: i64,
    pub tx_index: i32,
    pub screening_log_index: i32,
    pub transfer_log_index: i32,
    pub module_address: [u8; 20],
    pub screening_event: i64,
    pub transfer_event: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScreeningGapRow {
    pub asset_id: String,
    pub block_number: i64,
    pub tx_index: i32,
    pub log_index: i32,
    pub transfer_event: i64,
    pub epoch_module: [u8; 20],
}

/// One event in the merged screening/transfer stream, tagged by kind so the
/// walk below never has to re-inspect `event_name` — `module_address` only
/// exists on a `TransferScreened` item (it's the `source_contract` the
/// event was fetched from; [`super::fetch::ChainEventRow`] carries no
/// `source_contract` field, so callers tag it at fetch time, one module at
/// a time, before merging). `Transfer` carries its owning `asset_id` (HIGH-1
/// — the union walk needs to know which asset each transfer belongs to
/// AFTER pairing, never before: pairing itself is asset-blind).
enum StreamItem<'a> {
    Screening {
        module_address: [u8; 20],
        event: &'a ChainEventRow,
    },
    Transfer {
        asset_id: &'a str,
        event: &'a ChainEventRow,
    },
}

impl StreamItem<'_> {
    fn position(&self) -> (i64, i32, i32) {
        let ev = match self {
            StreamItem::Screening { event, .. } => event,
            StreamItem::Transfer { event, .. } => event,
        };
        (ev.block_number, ev.tx_index, ev.log_index)
    }
}

/// `transfer_events` = the UNION of EVERY configured asset's own token
/// `Transfer` rows, each tagged `(asset_id, event)` — HIGH-1: this MUST be
/// the union across all assets sharing a sanctions module, never one
/// asset's slice, or a screening consumed by another asset's transfer can
/// mask this asset's real gap (see module doc).
/// `screening_events` = `(module_address, event)` pairs across EVERY
/// configured sanctions module's `TransferScreened` rows — chain-global.
/// `router_epochs_by_asset` = each asset's OWN router's epochs, keyed by
/// `asset_id` (missing/empty ⇒ that asset never reports a gap).
pub fn build_transfer_screening_and_gaps(
    transfer_events: &[(String, ChainEventRow)],
    screening_events: &[([u8; 20], ChainEventRow)],
    router_epochs_by_asset: &HashMap<String, Vec<RouterEpochRow>>,
) -> (Vec<TransferScreeningRow>, Vec<ScreeningGapRow>) {
    let mut items: Vec<StreamItem> = Vec::new();
    for (asset_id, ev) in transfer_events {
        if ev.event_name == "Transfer" {
            items.push(StreamItem::Transfer {
                asset_id,
                event: ev,
            });
        }
    }
    for (module_address, ev) in screening_events {
        if ev.event_name == "TransferScreened" {
            items.push(StreamItem::Screening {
                module_address: *module_address,
                event: ev,
            });
        }
    }
    items.sort_by_key(|it| it.position());

    let mut screenings = Vec::new();
    let mut gaps = Vec::new();
    // LIFO of unmatched TransferScreened in the CURRENT tx only — GLOBAL
    // across every asset's transfers (HIGH-1: this is the whole point —
    // a screening consumed by asset B's transfer is REMOVED from this
    // stack and can never spuriously pair with asset A's transfer later
    // in the same tx).
    let mut stack: Vec<([u8; 20], &ChainEventRow)> = Vec::new();
    let mut current_tx: Option<(i64, i32)> = None;
    let empty_epochs: Vec<RouterEpochRow> = Vec::new();

    for item in &items {
        let (block, tx_index, _) = item.position();
        let tx_key = (block, tx_index);
        if current_tx != Some(tx_key) {
            stack.clear(); // never pair/leak across a tx boundary
            current_tx = Some(tx_key);
        }

        match item {
            StreamItem::Screening {
                module_address,
                event,
            } => {
                stack.push((*module_address, event));
            }
            StreamItem::Transfer { asset_id, event: ev } => {
                if let Some((module_address, screening_ev)) = stack.pop() {
                    screenings.push(TransferScreeningRow {
                        asset_id: asset_id.to_string(),
                        block_number: ev.block_number,
                        tx_index: ev.tx_index,
                        screening_log_index: screening_ev.log_index,
                        transfer_log_index: ev.log_index,
                        module_address,
                        screening_event: screening_ev.event_id,
                        transfer_event: ev.event_id,
                    });
                } else {
                    let pos = (ev.block_number, ev.tx_index, ev.log_index);
                    let epochs = router_epochs_by_asset
                        .get(*asset_id)
                        .unwrap_or(&empty_epochs);
                    if let Some(epoch) = epochs.iter().find(|e| e.contains_position(pos)) {
                        gaps.push(ScreeningGapRow {
                            asset_id: asset_id.to_string(),
                            block_number: ev.block_number,
                            tx_index: ev.tx_index,
                            log_index: ev.log_index,
                            transfer_event: ev.event_id,
                            epoch_module: epoch.module_address,
                        });
                    }
                    // No epoch covers this position (or no router at all
                    // for this asset) — an unscreened transfer outside
                    // enforcement is expected, never a gap.
                }
            }
        }
    }

    (screenings, gaps)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(b: u8) -> [u8; 20] {
        [b; 20]
    }
    fn addr_hex(b: u8) -> String {
        format!("0x{}", hex::encode(addr(b)))
    }

    fn transfer_ev(event_id: i64, block: i64, tx_index: i32, log_index: i32) -> ChainEventRow {
        ChainEventRow {
            event_id,
            block_number: block,
            tx_index,
            log_index,
            event_name: "Transfer".to_string(),
            args: serde_json::json!({ "from": addr_hex(1), "to": addr_hex(2), "value": "1000" }),
            tx_signer: None,
        }
    }

    fn tagged(asset_id: &str, ev: ChainEventRow) -> (String, ChainEventRow) {
        (asset_id.to_string(), ev)
    }

    fn screening_ev(event_id: i64, block: i64, tx_index: i32, log_index: i32) -> ChainEventRow {
        ChainEventRow {
            event_id,
            block_number: block,
            tx_index,
            log_index,
            event_name: "TransferScreened".to_string(),
            args: serde_json::json!({ "from": addr_hex(1), "to": addr_hex(2), "amount": "1000" }),
            tx_signer: None,
        }
    }

    const MODULE: [u8; 20] = [0x11; 20];

    fn no_epochs() -> HashMap<String, Vec<RouterEpochRow>> {
        HashMap::new()
    }

    #[test]
    fn screening_pairs_nearest_preceding_unmatched_in_same_tx() {
        // TS@0, TS@2, T@3, T@5 ⇒ T@3↔TS@2, T@5↔TS@0
        let transfers = vec![
            tagged("A", transfer_ev(10, 100, 0, 3)),
            tagged("A", transfer_ev(11, 100, 0, 5)),
        ];
        let screenings_in = vec![
            (MODULE, screening_ev(20, 100, 0, 0)),
            (MODULE, screening_ev(21, 100, 0, 2)),
        ];
        let (pairs, gaps) = build_transfer_screening_and_gaps(&transfers, &screenings_in, &no_epochs());
        assert_eq!(gaps, vec![]);
        assert_eq!(pairs.len(), 2);
        let by_transfer: std::collections::HashMap<i64, i64> = pairs
            .iter()
            .map(|p| (p.transfer_event, p.screening_event))
            .collect();
        assert_eq!(by_transfer.get(&10), Some(&21), "T@3 must pair with TS@2 (nearest)");
        assert_eq!(by_transfer.get(&11), Some(&20), "T@5 must pair with TS@0 (remaining)");
    }

    #[test]
    fn screening_collision_fixture_identical_arg_tuples_still_pairs_by_position() {
        // TWO TransferScreened with the exact same (from,to,amount) are
        // BOTH still on the stack when the first Transfer arrives (TS@0,
        // TS@2, T@3, T@5 — same layout as the nearest-preceding test) — an
        // argument-tuple matcher has no basis to disambiguate and would
        // pick an arbitrary (e.g. first-pushed) candidate; positional
        // pairing must still pick the NEAREST one (T@3↔TS@2, T@5↔TS@0),
        // never collide on equal args.
        let transfers = vec![
            tagged("A", transfer_ev(10, 100, 0, 3)),
            tagged("A", transfer_ev(11, 100, 0, 5)),
        ];
        let screenings_in = vec![
            (MODULE, screening_ev(20, 100, 0, 0)),
            (MODULE, screening_ev(21, 100, 0, 2)),
        ];
        let (pairs, _gaps) = build_transfer_screening_and_gaps(&transfers, &screenings_in, &no_epochs());
        let by_transfer: std::collections::HashMap<i64, i64> = pairs
            .iter()
            .map(|p| (p.transfer_event, p.screening_event))
            .collect();
        assert_eq!(by_transfer.get(&10), Some(&21), "T@3 must pair with the NEAREST (TS@2), not TS@0");
        assert_eq!(by_transfer.get(&11), Some(&20), "T@5 must pair with the remaining TS@0");
    }

    #[test]
    fn screening_never_pairs_across_txs() {
        // TS in tx 0, unpaired T in tx 1 — must NOT pair (and must not gap
        // without a router).
        let transfers = vec![tagged("A", transfer_ev(10, 100, 1, 0))];
        let screenings_in = vec![(MODULE, screening_ev(20, 100, 0, 5))];
        let (pairs, gaps) = build_transfer_screening_and_gaps(&transfers, &screenings_in, &no_epochs());
        assert_eq!(pairs, vec![]);
        assert_eq!(gaps, vec![]);
    }

    #[test]
    fn unmatched_screening_is_expected_not_an_alarm() {
        // A leftover TransferScreened (foreign-token screening) at the end
        // of its tx produces zero rows in BOTH tables.
        let transfers: Vec<(String, ChainEventRow)> = vec![];
        let screenings_in = vec![(MODULE, screening_ev(20, 100, 0, 0))];
        let (pairs, gaps) = build_transfer_screening_and_gaps(&transfers, &screenings_in, &no_epochs());
        assert_eq!(pairs, vec![]);
        assert_eq!(gaps, vec![]);
    }

    /// L4 (P4b-i review): MORE transfers than screenings in a tx —
    /// TS@0, T@1, T@2 — the first transfer consumes the only screening; the
    /// second is unpaired and (with an in-epoch router configured) a gap.
    #[test]
    fn more_transfers_than_screenings_second_transfer_is_a_gap() {
        let transfers = vec![
            tagged("A", transfer_ev(10, 100, 0, 1)),
            tagged("A", transfer_ev(11, 100, 0, 2)),
        ];
        let screenings_in = vec![(MODULE, screening_ev(20, 100, 0, 0))];
        let mut epochs_by_asset = HashMap::new();
        epochs_by_asset.insert("A".to_string(), vec![epoch((100, 0, 0), None, MODULE)]);

        let (pairs, gaps) = build_transfer_screening_and_gaps(&transfers, &screenings_in, &epochs_by_asset);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].transfer_event, 10, "T@1 pairs with the only screening");
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].transfer_event, 11, "T@2 is unpaired and in-epoch — a real gap");
    }

    /// HIGH-1 (P4b-i review) — THE union-walk test. Two DIFFERENT
    /// configured assets share one sanctions module + one router. One tx:
    /// `TS(for B)@0, T_B@1, T_A@2` — T_A is genuinely UNSCREENED (the only
    /// screening in the tx belongs to B's transfer). A per-asset walk that
    /// only sees asset A's own transfers would find its stack still holding
    /// the unconsumed screening (never having seen B's transfer at all) and
    /// WRONGLY pair T_A with it — masking A's real gap and fabricating a
    /// false "cleared" transfer_screening row. The union walk sees BOTH
    /// transfers in true log order, correctly lets B consume the screening,
    /// and correctly leaves A's transfer unpaired → a real gap.
    #[test]
    fn cross_asset_screening_consumption_never_masks_another_assets_gap() {
        let transfer_b = transfer_ev(11, 100, 0, 1); // T_B@1
        let transfer_a = transfer_ev(12, 100, 0, 2); // T_A@2, genuinely unscreened
        let transfers = vec![tagged("B", transfer_b), tagged("A", transfer_a)];
        let screenings_in = vec![(MODULE, screening_ev(20, 100, 0, 0))]; // TS(for B)@0

        let mut epochs_by_asset = HashMap::new();
        epochs_by_asset.insert("A".to_string(), vec![epoch((100, 0, 0), None, MODULE)]);
        epochs_by_asset.insert("B".to_string(), vec![epoch((100, 0, 0), None, MODULE)]);

        let (pairs, gaps) =
            build_transfer_screening_and_gaps(&transfers, &screenings_in, &epochs_by_asset);

        assert_eq!(pairs.len(), 1, "exactly one real pair: B's transfer with the screening");
        assert_eq!(pairs[0].asset_id, "B");
        assert_eq!(pairs[0].transfer_event, 11);
        assert_eq!(pairs[0].screening_event, 20);

        assert_eq!(gaps.len(), 1, "asset A's transfer must surface as a REAL gap, never a false pair");
        assert_eq!(gaps[0].asset_id, "A");
        assert_eq!(gaps[0].transfer_event, 12);
    }

    fn epoch(open: (i64, i32, i32), close: Option<(i64, i32, i32)>, module: [u8; 20]) -> RouterEpochRow {
        RouterEpochRow {
            module_address: module,
            from_block: open.0,
            to_block: close.map(|c| c.0),
            open_tx_index: open.1,
            open_log_index: open.2,
            close_tx_index: close.map(|c| c.1),
            close_log_index: close.map(|c| c.2),
            opened_by_event: 900,
            closed_by_event: close.map(|_| 901),
        }
    }

    #[test]
    fn in_epoch_unscreened_transfer_is_a_gap() {
        let mut epochs_by_asset = HashMap::new();
        epochs_by_asset.insert("A".to_string(), vec![epoch((100, 0, 0), None, MODULE)]);
        let transfers = vec![tagged("A", transfer_ev(10, 150, 0, 5))];
        let (pairs, gaps) = build_transfer_screening_and_gaps(&transfers, &[], &epochs_by_asset);
        assert_eq!(pairs, vec![]);
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].transfer_event, 10);
        assert_eq!(gaps[0].epoch_module, MODULE);
    }

    #[test]
    fn pre_epoch_and_post_epoch_transfers_are_no_gap() {
        let mut epochs_by_asset = HashMap::new();
        epochs_by_asset.insert(
            "A".to_string(),
            vec![epoch((100, 0, 0), Some((200, 0, 0)), MODULE)],
        );
        let before = tagged("A", transfer_ev(10, 50, 0, 0));
        let after = tagged("A", transfer_ev(11, 250, 0, 0));
        let (_pairs, gaps) =
            build_transfer_screening_and_gaps(&[before, after], &[], &epochs_by_asset);
        assert_eq!(gaps, vec![], "outside the epoch window — never a gap");
    }

    #[test]
    fn same_block_pre_registration_position_is_no_gap() {
        // Registration lands at (100, tx=1, log=0); an unscreened transfer
        // in the SAME BLOCK but an EARLIER tx (100, tx=0, log=0) predates
        // the registration and must not be flagged.
        let mut epochs_by_asset = HashMap::new();
        epochs_by_asset.insert("A".to_string(), vec![epoch((100, 1, 0), None, MODULE)]);
        let transfers = vec![tagged("A", transfer_ev(10, 100, 0, 0))];
        let (_pairs, gaps) = build_transfer_screening_and_gaps(&transfers, &[], &epochs_by_asset);
        assert_eq!(
            gaps,
            vec![],
            "block-granular scoping would wrongly flag this; tuple-granular must not"
        );
    }

    /// L3 (P4b-i review) — the mirror of the pre-registration test:
    /// a `ModuleTypeRemoved` closes the epoch at (100, tx=5, log=2); an
    /// unscreened transfer in the SAME BLOCK, AFTER the close position
    /// (100, tx=7, log=0), is genuinely OUTSIDE the epoch and must not gap.
    #[test]
    fn same_block_after_epoch_close_position_is_no_gap() {
        let mut epochs_by_asset = HashMap::new();
        epochs_by_asset.insert(
            "A".to_string(),
            vec![epoch((100, 0, 0), Some((100, 5, 2)), MODULE)],
        );
        let transfers = vec![tagged("A", transfer_ev(10, 100, 7, 0))];
        let (_pairs, gaps) = build_transfer_screening_and_gaps(&transfers, &[], &epochs_by_asset);
        assert_eq!(
            gaps,
            vec![],
            "same block, but AFTER the close position — already outside the epoch"
        );
    }

    #[test]
    fn no_router_configured_means_no_gaps() {
        let transfers = vec![tagged("A", transfer_ev(10, 500, 0, 0))];
        let (_pairs, gaps) = build_transfer_screening_and_gaps(&transfers, &[], &no_epochs());
        assert_eq!(gaps, vec![]);
    }
}

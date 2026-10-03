//! `exposure_window` — spec §7's single most audit-critical sequence: any
//! UNGATED [`gate::GateIntervalRow`] on a previously-gated asset. `pattern`
//! is a **projection classification** over the `ArcToken.Transfer` shape
//! inside the window — never a new on-chain fact (IMPL-PLAN §7 impl-plan
//! note) — because gated mint/burn revert (spec §4.1), so routine issuance
//! *also* requires an un-gate and would otherwise misread as an incident.
//!
//! **Classification rule (P2 scope decision — flagged in the P2 report as
//! the subtlest ambiguity in the plan):** the spec's prose names the
//! ISSUANCE/CLAWBACK patterns by *signer role* ("mint by MINTER key",
//! "burn+mint by issuer keys") — but per-block role-holder attribution
//! (`signer_attribution`, spec §5.2) is Tier-3/`code_change` territory, not
//! built in P2. This builder classifies by **Transfer shape alone**, which
//! is sufficient to satisfy the two contrasting fixtures P2 needs to prove:
//! - **ISSUANCE** — every transfer in the window is a mint (`from == 0x0`,
//!   `to != 0x0`), no burn, no third-party (both-nonzero) transfer.
//! - **CLAWBACK** — the window has both a mint and a burn, no third-party
//!   transfer (recall-and-reissue, no non-custodial movement).
//! - **OTHER** — any third-party transfer occurred (both parties nonzero,
//!   including mint/burn-window edge cases with a real 2-party leg) — the
//!   high-severity default, per spec §7 "an exposure window with
//!   unexplained third-party transfers can never sit under ENFORCED_CLEAN".
//!   `flags.never_allowlisted_recipients` names every third-party
//!   recipient that never held an (open OR closed) allowlist interval —
//!   spec §7(a)'s specific "transfers to addresses never on the allowlist" flag.
//!
//! A later phase that adds real signer-role attribution can tighten
//! ISSUANCE/CLAWBACK to also check the signer without changing this
//! module's table shape — `flags` already carries the transfer positions
//! for that follow-up to join against.
//!
//! **Window membership is TUPLE-granular, not block-granular (H1, P2
//! review).** A transfer is "inside" an exposure window iff its
//! `(block_number, tx_index, log_index)` position sits inside the
//! UNGATED [`gate::GateIntervalRow`]'s half-open `[open_position,
//! close_position)` — via [`gate::GateIntervalRow::contains_position`], the
//! single shared implementation of the M2 boundary convention (module doc,
//! `gate.rs`). A block-only filter is wrong at both boundaries: a
//! third-party transfer in the RE-GATE block but positioned BEFORE the
//! re-gate toggle was still ungated on-chain and belongs in the window
//! (missing it would misclassify an OTHER as ISSUANCE/CLAWBACK); a transfer
//! in the UN-GATE block but positioned BEFORE the un-gate toggle was still
//! gated and does NOT belong in the window (including it would falsely
//! inflate severity on a transfer that never actually happened while ungated).
//!
//! **`flags` carries NATURAL KEYS, never `event_id` (H2, P2 review /
//! C1).** `event_id` is a DB surrogate — `GENERATED ALWAYS AS IDENTITY`,
//! assigned by insertion order on one instance (IMPL-PLAN C1) — so it must
//! never enter anything whose determinism is asserted across independent
//! re-indexes (exactly the rebuild-determinism cross-DB test in
//! `tests/tier2_db.rs`). `third_party_transfer_events` therefore stores
//! `[block_number, tx_index, log_index]` triples, which resolve to the
//! event just as well as an `event_id` would, but are instance-independent.

use std::collections::BTreeSet;

use super::allowlist::AllowlistIntervalRow;
use super::fetch::{arg_address, ChainEventRow, ZERO_ADDRESS};
use super::gate::GateIntervalRow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExposurePattern {
    Clawback,
    Issuance,
    Other,
}

impl ExposurePattern {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            ExposurePattern::Clawback => "CLAWBACK",
            ExposurePattern::Issuance => "ISSUANCE",
            ExposurePattern::Other => "OTHER",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExposureWindowRow {
    pub open_block: i64,
    pub close_block: Option<i64>,
    pub pattern: ExposurePattern,
    pub flags: serde_json::Value,
    pub opened_by_event: i64,
    pub closed_by_event: Option<i64>,
}

/// `gate_intervals` = this asset's already-built [`GateIntervalRow`]s (any
/// order — sorted internally by `from_block`, since [`super::gate::build_gate_intervals`]
/// already returns them in that order but this function doesn't assume it).
/// `transfer_events` = this asset's `ArcToken.Transfer` chain_event rows,
/// `(block_number, tx_index, log_index)`-ordered. `allowlist_intervals` =
/// this asset's already-built [`AllowlistIntervalRow`]s, used only to build
/// the "ever allowlisted" set for the never-allowlisted flag (open AND
/// closed intervals both count — "ever", not "currently").
pub fn build_exposure_windows(
    gate_intervals: &[GateIntervalRow],
    transfer_events: &[ChainEventRow],
    allowlist_intervals: &[AllowlistIntervalRow],
) -> Vec<ExposureWindowRow> {
    let ever_allowlisted: BTreeSet<[u8; 20]> =
        allowlist_intervals.iter().map(|r| r.address).collect();

    let mut sorted_gates = gate_intervals.to_vec();
    sorted_gates.sort_by_key(|r| r.open_position());

    let mut result = Vec::new();
    let mut ever_gated = false;

    for gi in &sorted_gates {
        if gi.gated {
            ever_gated = true;
            continue;
        }
        if !ever_gated {
            // An ungated interval with no gated interval before it is NOT
            // "on a previously gated asset" (spec §7) — the asset simply
            // hasn't been gated yet (or never will be); not an exposure window.
            continue;
        }

        // H1: tuple-granular membership via the shared boundary check —
        // never block-only (see module doc).
        let in_window: Vec<&ChainEventRow> = transfer_events
            .iter()
            .filter(|ev| gi.contains_position((ev.block_number, ev.tx_index, ev.log_index)))
            .collect();

        let mut has_mint = false;
        let mut has_burn = false;
        let mut third_party_positions: Vec<serde_json::Value> = Vec::new();
        let mut never_allowlisted_recipients: BTreeSet<String> = BTreeSet::new();

        for ev in &in_window {
            let from = arg_address(&ev.args, "from");
            let to = arg_address(&ev.args, "to");
            let from_zero = from == Some(ZERO_ADDRESS);
            let to_zero = to == Some(ZERO_ADDRESS);
            match (from_zero, to_zero) {
                (true, false) => has_mint = true,
                (false, true) => has_burn = true,
                (false, false) => {
                    // H2: natural-key triple, never `event_id`.
                    third_party_positions.push(serde_json::json!([
                        ev.block_number,
                        ev.tx_index,
                        ev.log_index
                    ]));
                    if let Some(to_addr) = to {
                        if !ever_allowlisted.contains(&to_addr) {
                            never_allowlisted_recipients
                                .insert(format!("0x{}", hex::encode(to_addr)));
                        }
                    }
                }
                (true, true) => {} // from==to==0x0 can't happen on a real ERC-20; ignore defensively
            }
        }

        let pattern = if !third_party_positions.is_empty() {
            ExposurePattern::Other
        } else if has_mint && has_burn {
            ExposurePattern::Clawback
        } else if has_mint {
            ExposurePattern::Issuance
        } else {
            // No mint, no burn, no third-party transfer at all inside the
            // window — an un-gate with zero token movement. Not a
            // recognized issuance/clawback shape; falls to the OTHER
            // catch-all (spec §7: "or was something else"), with empty flags.
            ExposurePattern::Other
        };

        let flags = serde_json::json!({
            "third_party_transfer_events": third_party_positions,
            "never_allowlisted_recipients": never_allowlisted_recipients.into_iter().collect::<Vec<_>>(),
        });

        result.push(ExposureWindowRow {
            open_block: gi.from_block,
            close_block: gi.to_block,
            pattern,
            flags,
            opened_by_event: gi.opened_by_event,
            closed_by_event: gi.closed_by_event,
        });
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr_hex(b: u8) -> String {
        format!("0x{}", hex::encode([b; 20]))
    }
    fn zero_hex() -> String {
        addr_hex(0)
    }

    fn transfer_ev_at(
        event_id: i64,
        block: i64,
        tx_index: i32,
        log_index: i32,
        from: String,
        to: String,
    ) -> ChainEventRow {
        ChainEventRow {
            event_id,
            block_number: block,
            tx_index,
            log_index,
            event_name: "Transfer".to_string(),
            args: serde_json::json!({ "from": from, "to": to, "value": "1000" }),
            tx_signer: None,
        }
    }

    fn transfer_ev(event_id: i64, block: i64, from: String, to: String) -> ChainEventRow {
        transfer_ev_at(event_id, block, 0, 0, from, to)
    }

    fn gated_then_ungated(open_block: i64, close_block: Option<i64>) -> Vec<GateIntervalRow> {
        vec![
            GateIntervalRow {
                gated: true,
                from_block: 0,
                to_block: Some(open_block),
                open_tx_index: 0,
                open_log_index: 0,
                close_tx_index: Some(0),
                close_log_index: Some(0),
                opened_by_event: 100,
                closed_by_event: Some(101),
            },
            GateIntervalRow {
                gated: false,
                from_block: open_block,
                to_block: close_block,
                open_tx_index: 0,
                open_log_index: 0,
                close_tx_index: close_block.map(|_| 0),
                close_log_index: close_block.map(|_| 0),
                opened_by_event: 101,
                closed_by_event: close_block.map(|_| 102),
            },
        ]
    }

    #[test]
    fn mint_only_window_classifies_issuance() {
        let gates = gated_then_ungated(100, Some(200));
        let transfers = vec![transfer_ev(1, 150, zero_hex(), addr_hex(0xAA))];
        let rows = build_exposure_windows(&gates, &transfers, &[]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pattern, ExposurePattern::Issuance);
        assert_eq!(rows[0].open_block, 100);
        assert_eq!(rows[0].close_block, Some(200));
    }

    #[test]
    fn stranger_transfer_window_classifies_other_with_flag() {
        let gates = gated_then_ungated(100, Some(200));
        // A never-allowlisted address receiving a real (non-mint) transfer.
        let transfers = vec![transfer_ev(1, 150, addr_hex(0x01), addr_hex(0xFF))];
        let rows = build_exposure_windows(&gates, &transfers, &[]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pattern, ExposurePattern::Other);
        let flagged = rows[0].flags["never_allowlisted_recipients"]
            .as_array()
            .unwrap();
        assert_eq!(flagged.len(), 1);
        assert_eq!(flagged[0].as_str().unwrap(), addr_hex(0xFF));
    }

    #[test]
    fn flags_carry_natural_key_positions_not_event_id() {
        let gates = gated_then_ungated(100, Some(200));
        // event_id deliberately large/unusual (9999) — if the implementation
        // ever leaked it into flags, this value would show up verbatim.
        let transfers = vec![transfer_ev_at(
            9999,
            150,
            3,
            7,
            addr_hex(0x01),
            addr_hex(0xFF),
        )];
        let rows = build_exposure_windows(&gates, &transfers, &[]);
        let positions = rows[0].flags["third_party_transfer_events"]
            .as_array()
            .unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0], serde_json::json!([150, 3, 7]));
        // The surrogate event_id (9999) must not appear anywhere in flags.
        assert!(!rows[0].flags.to_string().contains("9999"));
    }

    #[test]
    fn issuance_and_other_are_distinguished() {
        let gates_a = gated_then_ungated(100, Some(200));
        let issuance = build_exposure_windows(
            &gates_a,
            &[transfer_ev(1, 150, zero_hex(), addr_hex(0xAA))],
            &[],
        );
        let gates_b = gated_then_ungated(100, Some(200));
        let other = build_exposure_windows(
            &gates_b,
            &[transfer_ev(1, 150, addr_hex(0x01), addr_hex(0xFF))],
            &[],
        );
        assert_ne!(issuance[0].pattern, other[0].pattern);
        assert_eq!(issuance[0].pattern, ExposurePattern::Issuance);
        assert_eq!(other[0].pattern, ExposurePattern::Other);
    }

    #[test]
    fn mint_and_burn_no_third_party_classifies_clawback() {
        let gates = gated_then_ungated(100, Some(200));
        let transfers = vec![
            transfer_ev(1, 140, addr_hex(0xAA), zero_hex()), // burn
            transfer_ev(2, 150, zero_hex(), addr_hex(0xBB)), // mint
        ];
        let rows = build_exposure_windows(&gates, &transfers, &[]);
        assert_eq!(rows[0].pattern, ExposurePattern::Clawback);
    }

    #[test]
    fn ungated_interval_before_ever_gated_is_not_an_exposure_window() {
        let gates = vec![GateIntervalRow {
            gated: false,
            from_block: 50,
            to_block: None,
            open_tx_index: 0,
            open_log_index: 0,
            close_tx_index: None,
            close_log_index: None,
            opened_by_event: 1,
            closed_by_event: None,
        }];
        let rows = build_exposure_windows(&gates, &[], &[]);
        assert_eq!(rows, vec![]);
    }

    // ---- H1 boundary fixtures (P2 review), BOTH directions ---------

    /// A gate that opens (ungates) at (block, tx_index=5, log_index=2) — a
    /// transfer EARLIER in that same block (tx_index=1) is still GATED
    /// on-chain and must be EXCLUDED from the window.
    #[test]
    fn same_block_transfer_before_the_ungate_toggle_is_excluded() {
        let gates = vec![
            GateIntervalRow {
                gated: true,
                from_block: 0,
                to_block: Some(100),
                open_tx_index: 0,
                open_log_index: 0,
                close_tx_index: Some(0),
                close_log_index: Some(0),
                opened_by_event: 1,
                closed_by_event: Some(2),
            },
            GateIntervalRow {
                gated: false,
                from_block: 100,
                to_block: None,
                open_tx_index: 5,
                open_log_index: 2,
                close_tx_index: None,
                close_log_index: None,
                opened_by_event: 2,
                closed_by_event: None,
            },
        ];
        // Same block as the un-gate toggle, but BEFORE it (tx_index=1 < 5).
        let transfers = vec![transfer_ev_at(1, 100, 1, 0, addr_hex(0x01), addr_hex(0xFF))];
        let rows = build_exposure_windows(&gates, &transfers, &[]);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].pattern,
            ExposurePattern::Other,
            "with the buggy block-only filter this transfer would wrongly land IN the window; \
             correctly excluded, there's no third-party activity so it's the empty-window OTHER default"
        );
        assert_eq!(
            rows[0].flags["third_party_transfer_events"]
                .as_array()
                .unwrap()
                .len(),
            0,
            "the same-block pre-toggle transfer must be EXCLUDED — it happened while still gated"
        );
    }

    /// A gate that closes (re-gates) at (block, tx_index=5, log_index=2) —
    /// a transfer EARLIER in that same block (tx_index=1) was still
    /// UNGATED on-chain and must be INCLUDED in the window.
    #[test]
    fn same_block_transfer_before_the_regate_toggle_is_included() {
        let gates = vec![
            GateIntervalRow {
                gated: true,
                from_block: 0,
                to_block: Some(50),
                open_tx_index: 0,
                open_log_index: 0,
                close_tx_index: Some(0),
                close_log_index: Some(0),
                opened_by_event: 1,
                closed_by_event: Some(2),
            },
            GateIntervalRow {
                gated: false,
                from_block: 50,
                to_block: Some(200),
                open_tx_index: 0,
                open_log_index: 0,
                close_tx_index: Some(5),
                close_log_index: Some(2),
                opened_by_event: 2,
                closed_by_event: Some(3),
            },
        ];
        // Same block as the re-gate toggle, but BEFORE it (tx_index=1 < 5) —
        // a never-allowlisted stranger transfer, still ungated at that point.
        let transfers = vec![transfer_ev_at(1, 200, 1, 0, addr_hex(0x01), addr_hex(0xFF))];
        let rows = build_exposure_windows(&gates, &transfers, &[]);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].pattern,
            ExposurePattern::Other,
            "with the buggy block-only filter this transfer would be wrongly EXCLUDED \
             (block == to_block fails `< to_block`), missing a real third-party leg — \
             correctly included, it's an audit-critical OTHER, not a false ISSUANCE/empty result"
        );
        let flagged = rows[0].flags["never_allowlisted_recipients"]
            .as_array()
            .unwrap();
        assert_eq!(
            flagged.len(),
            1,
            "the same-block pre-re-gate transfer must be INCLUDED — it happened while still ungated"
        );
        assert_eq!(flagged[0].as_str().unwrap(), addr_hex(0xFF));
    }

    #[test]
    fn same_block_transfer_at_or_after_the_regate_toggle_is_excluded() {
        let gates = vec![
            GateIntervalRow {
                gated: true,
                from_block: 0,
                to_block: Some(50),
                open_tx_index: 0,
                open_log_index: 0,
                close_tx_index: Some(0),
                close_log_index: Some(0),
                opened_by_event: 1,
                closed_by_event: Some(2),
            },
            GateIntervalRow {
                gated: false,
                from_block: 50,
                to_block: Some(200),
                open_tx_index: 0,
                open_log_index: 0,
                close_tx_index: Some(5),
                close_log_index: Some(2),
                opened_by_event: 2,
                closed_by_event: Some(3),
            },
        ];
        // AT the re-gate toggle's own position — already gated again by then.
        let transfers = vec![transfer_ev_at(1, 200, 5, 2, addr_hex(0x01), addr_hex(0xFF))];
        let rows = build_exposure_windows(&gates, &transfers, &[]);
        assert_eq!(
            rows[0].flags["third_party_transfer_events"]
                .as_array()
                .unwrap()
                .len(),
            0,
            "a transfer AT the re-gate toggle's own position is already gated — excluded"
        );
    }
}

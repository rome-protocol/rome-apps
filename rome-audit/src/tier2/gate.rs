//! `gate_interval` — ← `TransfersRestrictionToggled(transfersAllowed)`,
//! COALESCED (IMPL-PLAN §2/§3 P3(b), "Q9"): the setter has no changed-guard,
//! so two toggles landing on the same resulting `gated` value must collapse
//! into ONE interval, never a zero-width open/close pair.
//!
//! `gated = !transfersAllowed` — `transfersAllowed = true` means Axis-1 is
//! off (anyone can transfer to anyone); `false` restricts to
//! whitelisted↔whitelisted pairs (spec §4.1).
//!
//! **Interval boundary convention (M2, P2 review):** every interval
//! here is half-open on the FULL `(block_number, tx_index, log_index)`
//! position of its opening/closing toggle event — `[open_position,
//! close_position)` — never block-granular. `from_block`/`to_block` name the
//! interval for humans and for other block-granular tables, but a caller
//! resolving "what was the state at this exact log position" (e.g.
//! [`super::exposure`]'s same-block transfer classification) MUST compare
//! against `open_tx_index`/`open_log_index` and `close_tx_index`/
//! `close_log_index`, not `from_block`/`to_block` alone — two events in the
//! SAME block on either side of a toggle are on opposite sides of the
//! interval boundary.
//!
//! **Pre-first-toggle state (M1, P2 review, source-verified):**
//! `WhitelistRestrictions.initialize` sets `transfersAllowed = true` and
//! emits NO `TransfersRestrictionToggled` for it — so the interval
//! `[deploy, first-toggle)` has no row in this table at all, and the
//! NORMATIVE reading of that absence is UNGATED (the module's real,
//! unemitted `initialize` default), never "unknown" or "assume gated".
//! Every caller of this table (exposure-window's `ever_gated` walk
//! included) must treat "no gate_interval row covers this block" as
//! ungated, not as a data gap.

use super::fetch::{arg_bool, ChainEventRow};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GateIntervalRow {
    pub gated: bool,
    pub from_block: i64,
    pub to_block: Option<i64>,
    /// Full position of the opening toggle event — see the module doc's
    /// boundary convention.
    pub open_tx_index: i32,
    pub open_log_index: i32,
    /// Full position of the closing toggle event, `None` iff the interval
    /// is still open (`to_block` is also `None` in that case).
    pub close_tx_index: Option<i32>,
    pub close_log_index: Option<i32>,
    pub opened_by_event: i64,
    pub closed_by_event: Option<i64>,
}

/// `events` MUST already be `TransfersRestrictionToggled` rows from one
/// Axis-1 module, in `(block_number, tx_index, log_index)` order.
pub fn build_gate_intervals(events: &[ChainEventRow]) -> Vec<GateIntervalRow> {
    let mut result = Vec::new();
    // (gated, opening event) of the currently-open interval.
    let mut current: Option<(bool, &ChainEventRow)> = None;

    for ev in events {
        if ev.event_name != "TransfersRestrictionToggled" {
            continue;
        }
        let Some(transfers_allowed) = arg_bool(&ev.args, "transfersAllowed") else {
            continue;
        };
        let new_gated = !transfers_allowed;

        match current {
            None => current = Some((new_gated, ev)),
            Some((gated, _)) if gated == new_gated => {
                // Redundant toggle to the same value — coalesce, no-op.
            }
            Some((gated, open_ev)) => {
                result.push(GateIntervalRow {
                    gated,
                    from_block: open_ev.block_number,
                    to_block: Some(ev.block_number),
                    open_tx_index: open_ev.tx_index,
                    open_log_index: open_ev.log_index,
                    close_tx_index: Some(ev.tx_index),
                    close_log_index: Some(ev.log_index),
                    opened_by_event: open_ev.event_id,
                    closed_by_event: Some(ev.event_id),
                });
                current = Some((new_gated, ev));
            }
        }
    }

    if let Some((gated, open_ev)) = current {
        result.push(GateIntervalRow {
            gated,
            from_block: open_ev.block_number,
            to_block: None,
            open_tx_index: open_ev.tx_index,
            open_log_index: open_ev.log_index,
            close_tx_index: None,
            close_log_index: None,
            opened_by_event: open_ev.event_id,
            closed_by_event: None,
        });
    }
    result
}

/// `(block_number, tx_index, log_index)` — the total-order position type
/// shared with [`super::fetch::ChainEventRow`]'s ordering columns.
pub type Position = (i64, i32, i32);

impl GateIntervalRow {
    pub fn open_position(&self) -> Position {
        (self.from_block, self.open_tx_index, self.open_log_index)
    }

    pub fn close_position(&self) -> Option<Position> {
        match (self.to_block, self.close_tx_index, self.close_log_index) {
            (Some(b), Some(t), Some(l)) => Some((b, t, l)),
            _ => None,
        }
    }

    /// True iff `pos` sits inside this interval's half-open
    /// `[open_position, close_position)` — the M2 boundary convention
    /// (module doc). This is the ONE place that implements the
    /// tuple-granular check; [`super::exposure`]'s transfer filter and
    /// [`is_gated_at`] both go through it so the boundary semantics can't drift.
    pub fn contains_position(&self, pos: Position) -> bool {
        pos >= self.open_position() && self.close_position().is_none_or(|cp| pos < cp)
    }
}

/// M1 pin: a position with NO covering `gate_interval` row (before the
/// first-ever toggle, or in a gap this table simply never populated) reads
/// UNGATED — never "unknown", never "assume gated". Encoded structurally:
/// this returns `false` both when an explicit ungated interval covers
/// `pos` AND when nothing covers it at all — the same answer either way,
/// by construction, not by a special-cased branch.
pub fn is_gated_at(intervals: &[GateIntervalRow], pos: Position) -> bool {
    intervals
        .iter()
        .any(|gi| gi.gated && gi.contains_position(pos))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(event_id: i64, block: i64, transfers_allowed: bool) -> ChainEventRow {
        ChainEventRow {
            event_id,
            block_number: block,
            tx_index: 0,
            log_index: 0,
            event_name: "TransfersRestrictionToggled".to_string(),
            args: serde_json::json!({ "transfersAllowed": transfers_allowed }),
            tx_signer: None,
        }
    }

    #[test]
    fn redundant_toggles_coalesce_to_one_interval() {
        // false, false, true → gated,gated (coalesced),ungated
        let events = vec![ev(1, 100, false), ev(2, 150, false), ev(3, 200, true)];
        let rows = build_gate_intervals(&events);
        assert_eq!(
            rows,
            vec![
                GateIntervalRow {
                    gated: true,
                    from_block: 100,
                    to_block: Some(200),
                    open_tx_index: 0,
                    open_log_index: 0,
                    close_tx_index: Some(0),
                    close_log_index: Some(0),
                    opened_by_event: 1,
                    closed_by_event: Some(3),
                },
                GateIntervalRow {
                    gated: false,
                    from_block: 200,
                    to_block: None,
                    open_tx_index: 0,
                    open_log_index: 0,
                    close_tx_index: None,
                    close_log_index: None,
                    opened_by_event: 3,
                    closed_by_event: None,
                },
            ]
        );
    }

    #[test]
    fn simple_gate_then_ungate() {
        let events = vec![ev(1, 100, false), ev(2, 200, true)];
        let rows = build_gate_intervals(&events);
        assert_eq!(
            rows,
            vec![
                GateIntervalRow {
                    gated: true,
                    from_block: 100,
                    to_block: Some(200),
                    open_tx_index: 0,
                    open_log_index: 0,
                    close_tx_index: Some(0),
                    close_log_index: Some(0),
                    opened_by_event: 1,
                    closed_by_event: Some(2),
                },
                GateIntervalRow {
                    gated: false,
                    from_block: 200,
                    to_block: None,
                    open_tx_index: 0,
                    open_log_index: 0,
                    close_tx_index: None,
                    close_log_index: None,
                    opened_by_event: 2,
                    closed_by_event: None,
                },
            ]
        );
    }

    /// M1 pin (P2 review): `WhitelistRestrictions.initialize` sets
    /// `transfersAllowed = true` with NO emitted toggle — a position
    /// before the first-ever toggle has no covering row here, and the
    /// normative reading of that absence is UNGATED.
    #[test]
    fn absence_before_first_toggle_reads_ungated() {
        // The asset's first-ever toggle GATES it at block 100 — nothing
        // before that exists in this table at all.
        let events = vec![ev(1, 100, false)];
        let rows = build_gate_intervals(&events);

        assert!(
            !is_gated_at(&rows, (50, 0, 0)),
            "no gate_interval row covers block 50 — must read ungated, not unknown/gated"
        );
        assert!(
            is_gated_at(&rows, (100, 0, 0)),
            "the gated interval itself must read gated once it starts"
        );
    }

    #[test]
    fn contains_position_is_half_open_on_the_full_tuple_not_just_block() {
        // SAME block open and close — only tx_index/log_index distinguish
        // "before the open toggle" / "inside" / "at-or-after the close toggle".
        let closed = GateIntervalRow {
            gated: true,
            from_block: 100,
            to_block: Some(100),
            open_tx_index: 1,
            open_log_index: 0,
            close_tx_index: Some(5),
            close_log_index: Some(2),
            opened_by_event: 1,
            closed_by_event: Some(2),
        };
        // At/after the open position, before the close position — inside,
        // even though `from_block == to_block`.
        assert!(closed.contains_position((100, 1, 0)));
        assert!(closed.contains_position((100, 3, 9)));
        // At/after the close position — outside.
        assert!(!closed.contains_position((100, 5, 2)));
        assert!(!closed.contains_position((100, 9, 9)));
        // Before the open position, same block — outside.
        assert!(!closed.contains_position((100, 0, 5)));
    }
}

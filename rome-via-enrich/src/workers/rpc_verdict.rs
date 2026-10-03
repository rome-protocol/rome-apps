//! Pure helpers for the "never persist a verdict derived from a failed/absent
//! Solana RPC read" fixes:
//!
//! - **E-N7** (`cross_chain`, `hook_executions`): these workers call
//!   `getTransaction` at index time to classify a tx. On an `Err`/`None`/truncated
//!   response they used to classify from absent data AND advance the cursor past
//!   the row — so an RPC-outage window was mis-filed forever, unrecoverable
//!   because the cursor had moved. The fix holds the cursor just below the
//!   earliest miss so the slot re-processes when the RPC recovers, with a bounded
//!   valve so a genuinely-unretrievable tx (pruned beyond RPC retention during a
//!   deep backfill) can't wedge the whole worker.
//! - **E-N1** (`batch_trace`): it wrote a permanent `{"not_a_batch": true}`
//!   sentinel whenever the log set lacked the batch marker — including when the
//!   logs were pruned/truncated/absent, which permanently blocked the anti-join
//!   from ever re-tracing a real batch. The fix only sentinels a COMPLETE,
//!   retrievable, non-truncated log set.
//!
//! All logic is pure so it can be unit-tested + mutation-checked directly; the
//! stateful glue (per-poll hold streak, DB writes) stays in the workers.

use sqlx::error::DatabaseError;

/// Solana RPC emits a literal `Log truncated` line when it cuts a tx's
/// `logMessages`. Truncated logs are incomplete, so any verdict derived from
/// them (batch-ness, top-level programs, CPI targets, hook tallies) is
/// untrustworthy and must not be persisted.
pub(crate) fn logs_truncated(logs: &[String]) -> bool {
    logs.iter().any(|l| l.contains("Log truncated"))
}

/// E-N1: whether a `not_a_batch` sentinel is a trustworthy verdict for this
/// fetch. Only a COMPLETE, retrievable, non-truncated, non-empty log set that
/// simply lacks the batch marker qualifies. `None` (result null/pruned/absent or
/// a JSON-RPC error body) → not trustworthy. An empty log set → not trustworthy
/// (a real confirmed tx always emits at least invoke/success lines, so empty
/// means an odd/incomplete response, not a genuine non-batch). Truncated → not
/// trustworthy. Sentineling any of these would permanently block re-tracing.
pub(crate) fn may_sentinel_not_a_batch(fetched: &Option<Vec<String>>) -> bool {
    matches!(fetched, Some(logs) if !logs.is_empty() && !logs_truncated(logs))
}

/// E-N7: the cursor value to persist given the batch's proposed `new_cursor`
/// (from `batch_cursor::plan_batch`) and the slot of the EARLIEST RPC miss in
/// the batch, if any. A miss means we could not reliably classify a row, so we
/// must not commit past it: cap the cursor just below the earliest miss so that
/// slot (and everything after it) re-processes on the next poll. No miss → the
/// plan's cursor is returned unchanged.
///
/// A miss is always on a processed row, whose slot is ≤ `plan_new_cursor`, so
/// the `min` returns `earliest_miss_slot - 1`. (The `-1` can be below 0 only for
/// a miss at slot 0 — genesis, not real — and `slot > -1` still correctly
/// re-fetches slot 0; do NOT clamp to 0, which would skip the missed slot.)
pub(crate) fn cursor_capped_by_miss(plan_new_cursor: i64, earliest_miss_slot: Option<i64>) -> i64 {
    match earliest_miss_slot {
        None => plan_new_cursor,
        Some(s) => plan_new_cursor.min(s - 1),
    }
}

/// E-N7 safety valve: once the cursor has been held at the same capped value for
/// `max_hold` consecutive polls, the missing tx is likely permanently
/// unretrievable (pruned beyond RPC retention during a deep backfill).
/// Force-advance past it (logged loud) so one dead tx never wedges the worker.
pub(crate) fn should_force_advance(hold_streak: u32, max_hold: u32) -> bool {
    hold_streak >= max_hold
}

/// Why a getTransaction read is unusable for classification. A **Terminal** miss
/// (result null/pruned/absent, `meta` absent → empty logs, or truncated logs)
/// will never resolve — it may be given up on after a bounded number of retries.
/// A **Transient** miss (transport `Err` — timeout / connection-refused / 5xx /
/// 429) recovers, so it is held/retried indefinitely; force-advancing past it
/// would drop recoverable data. A total-RPC outage therefore STALLS the worker
/// (visible as a stale cursor) rather than mis-filing thousands of txs. [E-N7 F2]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MissKind {
    Transient,
    Terminal,
}

/// Give up on (advance past / sentinel) the earliest miss ONLY when it is
/// TERMINAL and has been held/retried `max_hold` consecutive polls. A transient
/// miss is never given up on. Composed from [`should_force_advance`] so the
/// threshold stays one primitive. [E-N7 F2 / E-N1 F1]
pub(crate) fn should_give_up(kind: MissKind, held: u32, max_hold: u32) -> bool {
    matches!(kind, MissKind::Terminal) && should_force_advance(held, max_hold)
}

/// F6: classify a DB write/read failure for the miss valve. A constraint
/// violation is DETERMINISTIC — it fails identically on every replay — so it is
/// TERMINAL (given up on after MAX_HOLD, never wedges the worker forever). Any
/// other error (pool timeout, connection reset, IO) is TRANSIENT: retried
/// indefinitely since it may succeed once the DB recovers. [F6 poison policy]
pub(crate) fn miss_kind_from_constraint(is_constraint: bool) -> MissKind {
    if is_constraint {
        MissKind::Terminal
    } else {
        MissKind::Transient
    }
}

/// F6: map a `sqlx::Error` to a [`MissKind`] via [`miss_kind_from_constraint`].
/// A unique- or check-constraint violation is the deterministic (Terminal) case.
pub(crate) fn db_miss_kind(err: &sqlx::Error) -> MissKind {
    let is_constraint = matches!(
        err.as_database_error(),
        Some(db) if DatabaseError::is_unique_violation(db) || DatabaseError::is_check_violation(db)
    );
    miss_kind_from_constraint(is_constraint)
}

/// F6: per-poll cursor-hold state for the shared miss valve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HoldState {
    pub cursor: i64,
    pub streak: u32,
}

impl HoldState {
    /// Fresh state — no active hold.
    pub(crate) fn clear() -> Self {
        HoldState {
            cursor: i64::MIN,
            streak: 0,
        }
    }
}

/// F6: outcome of one poll's cursor decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ValveOutcome {
    pub persist: i64,
    pub hold: HoldState,
    pub gave_up: bool,
}

/// F6: the E-N7 cursor valve as ONE pure function shared by every worker (no
/// inline twins — see the drift risk flagged in review). Given the batch's
/// planned cursor, the earliest miss (if any), the prior hold state, and
/// `max_hold`, returns the cursor to persist + the next hold state + whether a
/// terminal miss was given up on. No miss → advance to the plan cursor and clear
/// the hold. A miss → hold just below it ([`cursor_capped_by_miss`]), counting
/// consecutive identical holds; a TERMINAL miss held `max_hold` polls is given up
/// on (advance past ONLY the stuck slot); a TRANSIENT miss holds indefinitely.
pub(crate) fn valve_persist_cursor(
    plan_new_cursor: i64,
    earliest_miss: Option<(i64, MissKind)>,
    hold: HoldState,
    max_hold: u32,
) -> ValveOutcome {
    match earliest_miss {
        None => ValveOutcome {
            persist: plan_new_cursor,
            hold: HoldState::clear(),
            gave_up: false,
        },
        Some((miss_slot, kind)) => {
            let capped = cursor_capped_by_miss(plan_new_cursor, Some(miss_slot));
            let streak = if capped == hold.cursor {
                hold.streak.saturating_add(1)
            } else {
                1
            };
            if should_give_up(kind, streak, max_hold) {
                // Terminal miss held MAX_HOLD polls: advance past ONLY the stuck
                // slot (not the whole plan cursor) so one dead slot can't wedge
                // the worker while later slots still re-process.
                ValveOutcome {
                    persist: miss_slot,
                    hold: HoldState::clear(),
                    gave_up: true,
                }
            } else {
                ValveOutcome {
                    persist: capped,
                    hold: HoldState {
                        cursor: capped,
                        streak,
                    },
                    gave_up: false,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── F6 poison policy: miss_kind_from_constraint / db_miss_kind ──
    #[test]
    fn constraint_is_terminal() {
        assert_eq!(miss_kind_from_constraint(true), MissKind::Terminal);
    }
    #[test]
    fn non_constraint_is_transient() {
        assert_eq!(miss_kind_from_constraint(false), MissKind::Transient);
    }
    #[test]
    fn pool_timeout_is_transient() {
        // A non-DatabaseError sqlx error (no constraint) → Transient (retry).
        assert_eq!(db_miss_kind(&sqlx::Error::PoolTimedOut), MissKind::Transient);
    }

    // ── F6 shared valve: valve_persist_cursor ──
    #[test]
    fn valve_no_miss_advances_and_clears() {
        let out = valve_persist_cursor(100, None, HoldState { cursor: 50, streak: 5 }, 30);
        assert_eq!(out.persist, 100);
        assert_eq!(out.hold, HoldState::clear());
        assert!(!out.gave_up);
    }
    #[test]
    fn valve_first_miss_holds_below() {
        let out =
            valve_persist_cursor(100, Some((60, MissKind::Transient)), HoldState::clear(), 30);
        assert_eq!(out.persist, 59); // cursor_capped_by_miss(100, Some(60))
        assert_eq!(out.hold, HoldState { cursor: 59, streak: 1 });
        assert!(!out.gave_up);
    }
    #[test]
    fn valve_repeated_hold_increments_streak() {
        let out = valve_persist_cursor(
            100,
            Some((60, MissKind::Terminal)),
            HoldState { cursor: 59, streak: 4 },
            30,
        );
        assert_eq!(out.persist, 59);
        assert_eq!(out.hold, HoldState { cursor: 59, streak: 5 });
        assert!(!out.gave_up);
    }
    #[test]
    fn valve_terminal_gives_up_at_max_hold_past_stuck_slot() {
        // streak reaches max_hold → give up, advance PAST ONLY the stuck slot.
        let out = valve_persist_cursor(
            100,
            Some((60, MissKind::Terminal)),
            HoldState { cursor: 59, streak: 29 },
            30,
        );
        assert_eq!(out.persist, 60); // miss_slot, NOT the plan cursor 100
        assert_eq!(out.hold, HoldState::clear());
        assert!(out.gave_up);
    }
    #[test]
    fn valve_transient_never_gives_up() {
        let out = valve_persist_cursor(
            100,
            Some((60, MissKind::Transient)),
            HoldState { cursor: 59, streak: 999 },
            30,
        );
        assert_eq!(out.persist, 59);
        assert!(!out.gave_up);
    }

    // ── logs_truncated ──
    #[test]
    fn truncated_detected() {
        let logs = vec!["Program X invoke [1]".to_string(), "Log truncated".to_string()];
        assert!(logs_truncated(&logs));
    }
    #[test]
    fn complete_not_truncated() {
        let logs = vec!["Program X invoke [1]".to_string(), "Program X success".to_string()];
        assert!(!logs_truncated(&logs));
    }
    #[test]
    fn empty_not_truncated() {
        assert!(!logs_truncated(&[]));
    }

    // ── may_sentinel_not_a_batch (E-N1) ──
    #[test]
    fn no_sentinel_on_none() {
        // pruned / null result / JSON-RPC error body → never sentinel.
        assert!(!may_sentinel_not_a_batch(&None));
    }
    #[test]
    fn no_sentinel_on_empty_logs() {
        // odd/incomplete response (no invoke/success lines) → never sentinel.
        assert!(!may_sentinel_not_a_batch(&Some(vec![])));
    }
    #[test]
    fn no_sentinel_on_truncated_logs() {
        // a big real batch whose marker got cut → never sentinel.
        let logs = vec!["Program data: ...".to_string(), "Log truncated".to_string()];
        assert!(!may_sentinel_not_a_batch(&Some(logs)));
    }
    #[test]
    fn sentinel_on_complete_non_batch_logs() {
        // the ONLY case that may be sentineled: a complete, non-truncated log
        // set that genuinely lacks the batch marker.
        let logs = vec!["Program X invoke [1]".to_string(), "Program X success".to_string()];
        assert!(may_sentinel_not_a_batch(&Some(logs)));
    }

    // ── cursor_capped_by_miss (E-N7) ──
    #[test]
    fn no_miss_keeps_plan_cursor() {
        assert_eq!(cursor_capped_by_miss(100, None), 100);
    }
    #[test]
    fn miss_caps_just_below_earliest() {
        // earliest miss at slot 50 → cursor 49 so `slot > 49` re-fetches slot 50.
        assert_eq!(cursor_capped_by_miss(100, Some(50)), 49);
    }
    #[test]
    fn miss_at_top_slot_caps_below_it() {
        // miss on the last processed slot (== plan cursor) → hold one below it.
        assert_eq!(cursor_capped_by_miss(100, Some(100)), 99);
    }

    // ── should_force_advance (E-N7 valve) ──
    #[test]
    fn holds_below_max() {
        assert!(!should_force_advance(0, 30));
        assert!(!should_force_advance(29, 30));
    }
    #[test]
    fn force_advances_at_and_above_max() {
        assert!(should_force_advance(30, 30));
        assert!(should_force_advance(31, 30));
    }

    // ── should_give_up (E-N7 F2 / E-N1 F1) ──
    /// A TERMINAL miss (pruned/absent/truncated — never resolves) is given up on
    /// once it has been held max_hold polls.
    #[test]
    fn give_up_on_terminal_at_and_above_max() {
        assert!(should_give_up(MissKind::Terminal, 30, 30));
        assert!(should_give_up(MissKind::Terminal, 31, 30));
    }
    /// A terminal miss below the threshold is still held (keep retrying).
    #[test]
    fn hold_terminal_below_max() {
        assert!(!should_give_up(MissKind::Terminal, 0, 30));
        assert!(!should_give_up(MissKind::Terminal, 29, 30));
    }
    /// A TRANSIENT miss (transport Err — recovers) is NEVER given up on, even far
    /// past the threshold: force-advancing past a recoverable outage drops data.
    #[test]
    fn never_give_up_on_transient() {
        assert!(!should_give_up(MissKind::Transient, 30, 30));
        assert!(!should_give_up(MissKind::Transient, 9999, 30));
    }
}

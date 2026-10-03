//! Shared slot-batch cursor planning for the enrich workers (E-C1).
//!
//! Every polling worker fetches `WHERE slot_number > cursor ORDER BY slot_number
//! LIMIT n` and used to advance the persisted cursor to the batch's max slot.
//! When the `LIMIT` cut a slot mid-way (guaranteed during catch-up, when every
//! batch is full), the next `slot > cursor` query permanently SKIPPED that
//! boundary slot's remaining rows — silently losing balances / TokenCreated /
//! gate / hook / creator rows.
//!
//! [`plan_batch`] fixes this uniformly WITHOUT a schema change: when a batch is
//! full it defers the top (possibly-cut) slot — process only the slots strictly
//! below it (which are fully present, since rows arrive slot-ordered), and set
//! the cursor to the last fully-included slot so the deferred slot is re-fetched
//! whole next batch. A partial batch means the worker is caught up, so every
//! slot (including the top) is complete and all rows are processed. The one
//! residual case — a single slot that fills an entire batch (≥ `batch_size`
//! rows) — cannot occur on one-block-per-slot Rome chains at any realistic
//! throughput; it is processed and flagged (`giant_slot`) so the caller logs it
//! loudly rather than silently cutting the tail.

/// The decision for one fetched batch: how many leading rows to process now and
/// what cursor to persist. See [`plan_batch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BatchPlan {
    /// Process `rows[0..process_count]`; the rest (the deferred top slot) are
    /// re-fetched next batch.
    pub process_count: usize,
    /// The value to persist as `enrich_cursors.last_processed`.
    pub new_cursor: i64,
    /// True iff the whole batch is a single slot ≥ `batch_size` (impossible on
    /// one-block-per-slot chains). Caller should `warn!` — the tail beyond the
    /// batch is not deferrable, so it would be cut. Never true in practice.
    pub giant_slot: bool,
}

/// Plan one fetched batch. `slots` = the slot number of each row, ascending
/// (the workers' `ORDER BY slot_number` guarantees this). `batch_size` = the
/// fetch LIMIT. Callers MUST guard against an empty batch before calling (all
/// 7 do: `if rows.is_empty() { continue }`). For non-empty input `process_count`
/// is always ≥ 1. See the module docs for the defer-the-top-slot rationale.
pub(crate) fn plan_batch(slots: &[i64], batch_size: usize) -> BatchPlan {
    let n = slots.len();
    if n == 0 {
        // Unreachable (callers guard empty); defensive, never persisted.
        return BatchPlan { process_count: 0, new_cursor: 0, giant_slot: false };
    }
    let top = slots[n - 1]; // ascending ⇒ max slot
    if n < batch_size {
        // Partial batch ⇒ caught up; every slot (incl. top) is complete.
        return BatchPlan { process_count: n, new_cursor: top, giant_slot: false };
    }
    // Full batch: the top slot may have been cut by LIMIT — defer it.
    if slots[0] == top {
        // Entire batch is one slot ≥ batch_size — cannot occur on Rome.
        return BatchPlan { process_count: n, new_cursor: top, giant_slot: true };
    }
    let cut = slots.partition_point(|&s| s < top); // rows with slot < top (≥ 1 here)
    BatchPlan { process_count: cut, new_cursor: slots[cut - 1], giant_slot: false }
}

#[cfg(test)]
mod tests {
    use super::{plan_batch, BatchPlan};

    /// Partial batch (len < batch_size) ⇒ caught up: process everything, cursor
    /// = last (highest) slot.
    #[test]
    fn partial_batch_processes_all() {
        let slots = [10i64, 10, 11, 12];
        assert_eq!(
            plan_batch(&slots, 8),
            BatchPlan { process_count: 4, new_cursor: 12, giant_slot: false }
        );
    }

    /// Full batch whose top slot is (possibly) cut mid-way ⇒ defer the whole top
    /// slot: process only the rows below it, cursor = last fully-included slot,
    /// so `slot > cursor` re-fetches the top slot whole next batch. THIS is the
    /// E-C1 bug case — the old code processed all 5 and set cursor=11, skipping
    /// slot 11's tail forever.
    #[test]
    fn full_batch_defers_cut_top_slot() {
        let slots = [10i64, 10, 11, 11, 11];
        assert_eq!(
            plan_batch(&slots, 5),
            BatchPlan { process_count: 2, new_cursor: 10, giant_slot: false }
        );
    }

    /// Full batch, top slot has a single row: still deferred conservatively
    /// (re-fetched + processed next batch — no loss, no double-process).
    #[test]
    fn full_batch_defers_even_single_row_top() {
        let slots = [10i64, 11, 12];
        assert_eq!(
            plan_batch(&slots, 3),
            BatchPlan { process_count: 2, new_cursor: 11, giant_slot: false }
        );
    }

    /// Full batch that is entirely ONE slot (≥ batch_size) ⇒ giant slot: process
    /// all, advance past it, flag for a loud log. Cannot happen on Rome.
    #[test]
    fn full_batch_single_giant_slot_flagged() {
        let slots = [7i64, 7, 7];
        assert_eq!(
            plan_batch(&slots, 3),
            BatchPlan { process_count: 3, new_cursor: 7, giant_slot: true }
        );
    }
}

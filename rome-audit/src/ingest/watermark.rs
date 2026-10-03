//! The verified-finality watermark decision (IMPL-PLAN §5.2) — pure, no I/O.
//!
//! P1 CONTRACT UNDER TEST: a Solana slot `S` is audit-final only when ALL
//! THREE conditions hold on a re-read: (i) `S` sits at least `lag` slots
//! below the finalized tip — finalized by the ROOTED PREFIX alone, STATUS-
//! FREE: neither `S`'s own per-row status label NOR the mere existence of a
//! `sol_slot` row for `S` is required. Solana finality is a prefix property
//! (Tower BFT rooting is prefix-monotone: a slot at or below a
//! rooted/finalized tip IS finalized on-chain, full stop, regardless of
//! what row Hercules has or hasn't written for it) — so once `finalized_tip`
//! has advanced `lag` slots past `S`, `S` is finalized no matter whether its
//! own `sol_slot.status` still reads `Confirmed`/`Processed`, or whether a
//! `sol_slot` row exists for it AT ALL. Trusting the per-row label instead
//! (`status == Finalized`) is exactly what wedged live ingest for 31h on
//! Hadrian: Hercules left a finality-PROMOTION gap where a contiguous run of
//! slots sat labeled `Confirmed` far behind an already-far-advanced
//! `finalized_tip`, and the old per-row conjunct never let the
//! contiguous-prefix walk (`pipeline::run_ingest_once`) step over the first
//! one. A LATER incident showed requiring `status.is_some()` (i.e. requiring
//! a row to exist at all) has the exact same wedging shape: a SKIPPED Solana
//! slot never gets a `sol_slot` row in the first place (the production
//! writer, `solana_block_storage.rs`, only rows slots that actually produced
//! a block), so a routine skip below the finalized tip is a finalized-EMPTY
//! slot — nothing to lose — and gating the whole audit's advance on its
//! absent row is the same class of bug as gating on a stale status label.
//! Below the rooted tip, a missing row simply means "no block, no events,
//! nothing to capture" — it advances. A missing row NEAR the tip
//! (`finalized_tip - S < lag`) still WAITS, because it fails condition (i)
//! on the lag arithmetic alone, same as any other near-tip slot — no
//! separate status gate is needed to get that right. **Coverage-hole
//! detection (a real block that Hercules failed to index at all) is
//! DECOUPLED from this path on purpose** — it belongs in an `eth_block`
//! block-number contiguity monitor (block numbers are strictly increasing;
//! a gap in the number sequence is a real un-indexed block, distinguishable
//! from a skip which leaves no number at all), never back in this guard;
//! (ii) production-head clearance —
//! `max_produced_slot ≥ S + lag` (this is the empty/gap discriminator, NOT
//! "S has rows": a transient `clean_from_slot` gap drops the head below S,
//! so this fails and the worker WAITS; a genuinely empty or skipped slot
//! leaves the head advancing past S normally, so this holds and S PASSES —
//! the tracker never stalls on legitimate emptiness); (iii) content-digest
//! stability — the slot's digest is unchanged since the tracker first
//! observed it (a changing digest is a reorg-in-progress signal and RESETS
//! first-seen).
//!
//! First-seen/digest state is deliberately IN-MEMORY only (IMPL-PLAN §6:
//! "in-flight first-seen/digest state may live in memory; (ii) covers the
//! dangerous window regardless") — a worker restart just re-observes from
//! scratch, which is safe because condition (ii) alone prevents a
//! transiently-gapped slot from passing regardless of what any stale
//! in-memory state might have claimed.

use std::collections::{BTreeMap, BTreeSet};

/// Mirrors Hercules' `SlotStatus` enum (`sol_slot.status`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotStatusKind {
    Processed,
    Confirmed,
    Finalized,
}

/// The content digest for one slot (§5.2-iii): `sol_slot.blockhash` + the
/// SET of produced `eth_block` hashes at that slot + a per-block-idx tx
/// count. A stably-empty/skipped slot has a well-defined digest too (its
/// blockhash/absence marker + an empty set + zero counts) — instability
/// means the digest CHANGES between reads, not that it's "non-empty".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotDigest {
    pub sol_blockhash: Option<String>,
    pub eth_block_hashes: BTreeSet<String>,
    pub tx_count_by_block_idx: BTreeMap<i32, i64>,
}

impl SlotDigest {
    /// A slot Hercules has no `sol_slot` row for yet (not seen at all).
    pub fn unknown() -> Self {
        Self {
            sol_blockhash: None,
            eth_block_hashes: BTreeSet::new(),
            tx_count_by_block_idx: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatermarkVerdict {
    /// All three §5.2 conditions held on this tick — the caller may advance
    /// `verified_through_slot` to (at least) this slot and ingest its logs.
    Pass,
    /// At least one condition failed (or this is the first observation, so
    /// digest-stability can't yet be proven) — do not advance; try again.
    Wait,
}

/// In-memory first-seen/digest tracker. One instance per chain; safe to
/// drop and recreate on restart (see module doc).
#[derive(Debug, Default)]
pub struct WatermarkTracker {
    first_seen: BTreeMap<i64, SlotDigest>,
}

impl WatermarkTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// One evaluation tick for `slot`. Pure given its inputs — the caller
    /// (the Postgres-backed pipeline) is responsible for reading
    /// `status`/`finalized_tip`/`max_produced_slot`/`digest` fresh from
    /// Hercules on every call; this function only carries the first-seen
    /// digest state across calls.
    pub fn evaluate(
        &mut self,
        slot: i64,
        status: Option<SlotStatusKind>,
        finalized_tip: Option<i64>,
        max_produced_slot: Option<i64>,
        digest: SlotDigest,
        lag: i64,
    ) -> WatermarkVerdict {
        // A (status-free fix): finalized by the ROOTED PREFIX
        // (`finalized_tip`) alone — not by this slot's own per-row status
        // label, and not gated on a `sol_slot` row existing at all. See the
        // module doc for why either gate wedges live ingest (a stale label
        // in #540's incident; a routine skipped slot's absent row in this
        // one). `status` is accepted but intentionally unused here —
        // coverage-hole detection lives in a decoupled `eth_block` number
        // -contiguity monitor, not in this guard.
        let _ = status;
        let finalized_with_lag = finalized_tip.is_some_and(|tip| tip.saturating_sub(slot) >= lag);
        let head_cleared = max_produced_slot.is_some_and(|max| max >= slot.saturating_add(lag));

        let digest_stable = match self.first_seen.get(&slot) {
            Some(prev) if *prev == digest => true,
            _ => {
                // Either never observed, or the digest CHANGED since last
                // observation — either way, restart the stability clock
                // from this read.
                self.first_seen.insert(slot, digest);
                false
            }
        };

        if finalized_with_lag && head_cleared && digest_stable {
            self.first_seen.remove(&slot);
            WatermarkVerdict::Pass
        } else {
            WatermarkVerdict::Wait
        }
    }
}

#[cfg(test)]
mod tests {
    //! P1 CONTRACT UNDER TEST (restated): `WatermarkTracker::evaluate`
    //! returns `Pass` iff rooted-by-tip (`slot` sits `lag` slots below
    //! `finalized_tip` — status-free: independent of both the row's own
    //! status label AND whether a `sol_slot` row exists for it at all) AND
    //! head-cleared AND a digest that reads IDENTICAL across two
    //! consecutive ticks — never on the first observation of a slot, and
    //! never while any one condition is false.

    use super::*;

    fn digest(sol_hash: &str, eth_hashes: &[&str], tx_counts: &[(i32, i64)]) -> SlotDigest {
        SlotDigest {
            sol_blockhash: Some(sol_hash.to_string()),
            eth_block_hashes: eth_hashes.iter().map(|s| s.to_string()).collect(),
            tx_count_by_block_idx: tx_counts.iter().copied().collect(),
        }
    }

    const LAG: i64 = 2;

    #[test]
    fn first_observation_never_passes_even_if_everything_else_is_ready() {
        let mut t = WatermarkTracker::new();
        let d = digest("solhash", &["ethhash"], &[(0, 3)]);
        let verdict = t.evaluate(
            100,
            Some(SlotStatusKind::Finalized),
            Some(110),
            Some(110),
            d,
            LAG,
        );
        assert_eq!(verdict, WatermarkVerdict::Wait);
    }

    #[test]
    fn stable_digest_across_two_ticks_with_everything_ready_passes() {
        let mut t = WatermarkTracker::new();
        let d = digest("solhash", &["ethhash"], &[(0, 3)]);
        let first = t.evaluate(
            100,
            Some(SlotStatusKind::Finalized),
            Some(110),
            Some(110),
            d.clone(),
            LAG,
        );
        assert_eq!(first, WatermarkVerdict::Wait);
        let second = t.evaluate(
            100,
            Some(SlotStatusKind::Finalized),
            Some(110),
            Some(110),
            d,
            LAG,
        );
        assert_eq!(second, WatermarkVerdict::Pass);
    }

    #[test]
    fn digest_change_between_ticks_resets_first_seen_and_waits() {
        // This is the C1-A / clean_from_slot-regenerate race: a slot
        // re-read with DIFFERENT content must never be treated as stable
        // just because it was "seen before".
        let mut t = WatermarkTracker::new();
        let d1 = digest("solhash", &["ethhash-v1"], &[(0, 3)]);
        let d2 = digest("solhash", &["ethhash-v2"], &[(0, 3)]); // content changed
        let first = t.evaluate(
            100,
            Some(SlotStatusKind::Finalized),
            Some(110),
            Some(110),
            d1,
            LAG,
        );
        assert_eq!(first, WatermarkVerdict::Wait);
        let second = t.evaluate(
            100,
            Some(SlotStatusKind::Finalized),
            Some(110),
            Some(110),
            d2.clone(),
            LAG,
        );
        assert_eq!(
            second,
            WatermarkVerdict::Wait,
            "changed digest must not pass on the very next tick"
        );
        // Now it needs its OWN two stable reads before passing.
        let third = t.evaluate(
            100,
            Some(SlotStatusKind::Finalized),
            Some(110),
            Some(110),
            d2,
            LAG,
        );
        assert_eq!(third, WatermarkVerdict::Pass);
    }

    #[test]
    fn finalized_status_but_lag_not_yet_met_waits() {
        let mut t = WatermarkTracker::new();
        let d = digest("solhash", &["ethhash"], &[(0, 3)]);
        // finalized_tip - slot = 1, lag requires 2.
        let first = t.evaluate(
            100,
            Some(SlotStatusKind::Finalized),
            Some(101),
            Some(110),
            d.clone(),
            LAG,
        );
        assert_eq!(first, WatermarkVerdict::Wait);
        let second = t.evaluate(
            100,
            Some(SlotStatusKind::Finalized),
            Some(101),
            Some(110),
            d,
            LAG,
        );
        assert_eq!(
            second,
            WatermarkVerdict::Wait,
            "lag not met — must never pass regardless of digest stability"
        );
    }

    #[test]
    fn confirmed_status_rooted_by_the_finalized_tip_passes_on_the_second_stable_read() {
        // THE incident regression (A′): a slot labeled `Confirmed` (never
        // promoted to `Finalized` in Hercules' `sol_slot` row — exactly the
        // finality-promotion gap that wedged live Hadrian ingest for 31h)
        // must still PASS once it sits `lag` slots below the finalized tip
        // and its digest reads stable across two ticks. Finality here comes
        // from the ROOTED PREFIX (`finalized_tip`), not from this slot's own
        // stale per-row label — see the module doc.
        let mut t = WatermarkTracker::new();
        let d = digest("solhash", &["ethhash"], &[(0, 3)]);
        let first = t.evaluate(
            100,
            Some(SlotStatusKind::Confirmed),
            Some(110),
            Some(110),
            d.clone(),
            LAG,
        );
        assert_eq!(
            first,
            WatermarkVerdict::Wait,
            "first observation never passes regardless of status"
        );
        let second = t.evaluate(
            100,
            Some(SlotStatusKind::Confirmed),
            Some(110),
            Some(110),
            d,
            LAG,
        );
        assert_eq!(
            second,
            WatermarkVerdict::Pass,
            "a Confirmed-but-rooted slot must pass — trusting the stale per-row \
             label over the finalized tip is exactly what wedged live ingest"
        );
    }

    #[test]
    fn head_not_cleared_waits_this_is_the_empty_gap_discriminator() {
        // A transient clean_from_slot gap drops MAX(eth_block.slot_number)
        // below S — this is exactly what must make the tracker WAIT, even
        // if the slot's own digest looks perfectly stable (a stale/cached
        // read of a row about to be deleted+regenerated would still look
        // "stable" by digest alone; head-clearance is the guard that catches it).
        let mut t = WatermarkTracker::new();
        let d = digest("solhash", &["ethhash"], &[(0, 3)]);
        t.evaluate(
            100,
            Some(SlotStatusKind::Finalized),
            Some(110),
            Some(99),
            d.clone(),
            LAG,
        );
        let verdict = t.evaluate(
            100,
            Some(SlotStatusKind::Finalized),
            Some(110),
            Some(99),
            d,
            LAG,
        );
        assert_eq!(verdict, WatermarkVerdict::Wait);
    }

    #[test]
    fn a_genuinely_empty_or_skipped_slot_can_still_pass() {
        // No eth_block row at this slot at all (skipped/empty) — but the
        // head has advanced well past it (later slots produced blocks), so
        // (ii) holds; the digest is the well-defined "empty" shape and is
        // stable across two ticks; (i) holds. Must PASS, storing nothing
        // (the pipeline layer is what "stores nothing" — this function
        // just proves the verdict itself isn't gated on non-emptiness).
        let mut t = WatermarkTracker::new();
        let empty = digest("solhash-for-empty-slot", &[], &[]);
        let first = t.evaluate(
            100,
            Some(SlotStatusKind::Finalized),
            Some(110),
            Some(110),
            empty.clone(),
            LAG,
        );
        assert_eq!(first, WatermarkVerdict::Wait);
        let second = t.evaluate(
            100,
            Some(SlotStatusKind::Finalized),
            Some(110),
            Some(110),
            empty,
            LAG,
        );
        assert_eq!(
            second,
            WatermarkVerdict::Pass,
            "a genuinely empty/skipped slot must not stall the watermark"
        );
    }

    #[test]
    fn unknown_slot_status_waits_on_first_observation() {
        // Renamed from `unknown_slot_status_waits` — this now Waits purely
        // because it's a FIRST observation (digest-stability can't yet be
        // proven), not because `status` is `None`. See
        // `missing_sol_slot_row_below_tip_passes_on_second_stable_read` for
        // the below-tip skip-advance case this used to conflate with.
        let mut t = WatermarkTracker::new();
        let d = SlotDigest::unknown();
        let verdict = t.evaluate(100, None, Some(110), Some(110), d, LAG);
        assert_eq!(verdict, WatermarkVerdict::Wait);
    }

    #[test]
    fn missing_sol_slot_row_below_tip_passes_on_second_stable_read() {
        // THE fix (A, status-free): a slot with NO `sol_slot` row at all
        // (`status: None`) — a normal SKIPPED Solana slot, since Hercules
        // only rows slots that produced a block — must still PASS once it
        // sits `lag` slots below the finalized tip and its (unknown) digest
        // reads stable across two ticks. This is the mutation-proof anchor:
        // re-inserting `status.is_some() &&` into `evaluate` must REDDEN
        // this test (it would wedge at Wait forever instead).
        let mut t = WatermarkTracker::new();
        let d = SlotDigest::unknown();
        let first = t.evaluate(100, None, Some(110), Some(110), d.clone(), LAG);
        assert_eq!(first, WatermarkVerdict::Wait, "first observation never passes regardless of status");
        let second = t.evaluate(100, None, Some(110), Some(110), d, LAG);
        assert_eq!(
            second,
            WatermarkVerdict::Pass,
            "a missing-row slot below the finalized tip is a finalized SKIP — \
             nothing to lose — and must not wedge the whole audit's advance"
        );
    }

    #[test]
    fn missing_sol_slot_row_near_the_tip_still_waits() {
        // The preserved discriminator: a missing row too close to the tip
        // (`tip - slot < lag`) could still receive a block later, so it must
        // keep waiting regardless of how "stable" its (unknown) digest
        // reads. `110 - 109 = 1 < lag(2)`.
        let mut t = WatermarkTracker::new();
        let d = SlotDigest::unknown();
        let first = t.evaluate(109, None, Some(110), Some(110), d.clone(), LAG);
        assert_eq!(first, WatermarkVerdict::Wait);
        let second = t.evaluate(109, None, Some(110), Some(110), d, LAG);
        assert_eq!(
            second,
            WatermarkVerdict::Wait,
            "near-tip missing row must wait — it hasn't cleared the lag window yet"
        );
    }

    #[test]
    fn two_slot_digests_with_identical_fields_are_equal() {
        // Sanity on the Eq derive itself — the whole tracker rests on this.
        let a = digest("h", &["a", "b"], &[(0, 1), (1, 2)]);
        let b = digest("h", &["b", "a"], &[(1, 2), (0, 1)]); // different insertion order
        assert_eq!(a, b, "BTreeSet/BTreeMap equality must be order-independent");
    }

    #[test]
    fn passed_slot_is_no_longer_tracked_so_a_later_recheck_treats_it_as_first_seen_again() {
        let mut t = WatermarkTracker::new();
        let d = digest("solhash", &["ethhash"], &[(0, 3)]);
        t.evaluate(
            100,
            Some(SlotStatusKind::Finalized),
            Some(110),
            Some(110),
            d.clone(),
            LAG,
        );
        let pass = t.evaluate(
            100,
            Some(SlotStatusKind::Finalized),
            Some(110),
            Some(110),
            d.clone(),
            LAG,
        );
        assert_eq!(pass, WatermarkVerdict::Pass);
        // The pipeline should never re-evaluate an already-passed slot, but
        // prove the tracker's OWN state doesn't wrongly latch "passed forever"
        // in a way that would mask a real future divergence check.
        let recheck = t.evaluate(
            100,
            Some(SlotStatusKind::Finalized),
            Some(110),
            Some(110),
            d,
            LAG,
        );
        assert_eq!(
            recheck,
            WatermarkVerdict::Wait,
            "post-Pass state was cleared, so this is a fresh first-observation"
        );
    }
}

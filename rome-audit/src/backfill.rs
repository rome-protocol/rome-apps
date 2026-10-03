//! P4a deliverable 2 — the backfill EXECUTOR: turns an honestly-recorded
//! `audit.backfill_gap` row (P3c a.6 / P4a §3's `resolve::pass::
//! detect_backfill_gaps`) into actually-landed `audit.chain_event` rows.
//!
//! **Reuses the ingest pipeline's OWN decode+insert path** — `ingest::
//! pipeline::ingest_slot`, `pub(crate)`-visible for exactly this — restricted
//! to the gap's single address and driven by the CALLER's CURRENT
//! `IngestConfig` (so the gapped address's real, live `IngestFilter` is
//! applied — never a second, forked decode path, and never an unfiltered
//! firehose against a chain-wide-scoped source; see `ingest::IngestFilter`'s
//! doc for THE landmine this guards).
//!
//! **Chunked + resumable (P4a HIGH-2).** A gap's `[from_block,
//! watermark_at_detection]` range can span a token's ENTIRE life —
//! potentially millions of Solana slots for a widening gap on a long-lived
//! chain-wide token. Walking it all in one `AuditWorker::tick` would stall
//! live ingest for hours; a transient error mid-range used to force a
//! from-scratch restart on the next call (a livelock under a persistently
//! flaky source). `run_backfill_once` instead walks AT MOST
//! `max_slots_per_call` slots per call, persisting progress in
//! `audit.backfill_gap.resume_from_slot` — the NEXT call resumes from that
//! cursor, never from `from_block`. `remediated_at` is set ONLY once the
//! cursor reaches the gap's `watermark_at_detection`.
//!
//! **Bounded to one gap per call, but skips non-ingestable ones (P4a
//! MED-1).** A pending gap whose address isn't in the CALLER's current
//! `IngestConfig.sources` must never head-of-line-block every OTHER pending
//! gap forever — `run_backfill_once` scans a bounded page of pending gaps
//! and picks the first one that's actually ingestable right now.
//!
//! **`remediated_at` is set ONLY after the WHOLE `[from_block,
//! watermark_at_detection]` range completes successfully** — every write
//! along the way is `ON CONFLICT DO NOTHING`-idempotent (same discipline as
//! live ingest), so a transient failure mid-chunk simply leaves
//! `resume_from_slot`/`remediated_at` at their last-persisted values and the
//! NEXT call resumes from there. Every row landed here is `≤
//! watermark_at_detection` — already inside the audit-final, append-only
//! window (the live watermark has long since passed it) — so this never
//! races the live ingest cursor.

use std::collections::{BTreeMap, BTreeSet};

use sqlx::PgPool;

use crate::ingest::pipeline::ingest_slot;
use crate::ingest::{hercules_reads, IngestConfig, IngestError, SourceSpec};
use crate::registry::AbiRegistry;

/// P4a HIGH-2 — the default per-call chunk cap. `run_backfill_once` takes
/// this as an explicit parameter (not a hardcoded const inside the
/// function) so tests can exercise the chunking/resume behavior with a tiny
/// value without needing a multi-million-row fixture; production callers
/// (`run::AuditWorker::tick`) pass this constant.
pub const DEFAULT_MAX_SLOTS_PER_BACKFILL_CALL: i64 = 20_000;

/// How many pending-gap rows [`run_backfill_once`] pages through (P4a
/// MED-1) looking for the first INGESTABLE one — bounded so a chain with
/// many simultaneously-pending, currently-non-ingestable gaps can't turn
/// one call into an unbounded table scan.
const PENDING_GAP_PAGE_SIZE: i64 = 50;

/// One outcome of [`run_backfill_once`] — every case a caller/test can
/// assert on directly, never by scraping log text (same convention as
/// `run::TickOutcome`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackfillOutcome {
    /// No `remediated_at IS NULL` row exists for this chain right now.
    NoGapsPending,
    /// Every pending gap in the scanned page is currently non-ingestable
    /// (its address isn't in the caller's `IngestConfig.sources`) —
    /// skipped this call; retried on the next call.
    GapNotIngestable { source_contract: [u8; 20] },
    /// This call walked a BOUNDED chunk of the gap's range and did NOT
    /// reach `watermark_at_detection` — `resume_from_slot` was persisted so
    /// the next call resumes from `resumed_through_slot + 1`, never from
    /// `from_block` (P4a HIGH-2).
    Progressed {
        source_contract: [u8; 20],
        from_block: i64,
        resumed_through_slot: i64,
        events_inserted: usize,
    },
    /// The gap's full `[from_block, watermark_at_detection]` range was
    /// walked to completion and `remediated_at` was set.
    Remediated {
        source_contract: [u8; 20],
        from_block: i64,
        events_inserted: usize,
    },
    /// H1 — the gap's `from_block` predates the RETAINED FLOOR
    /// (`hercules_reads::min_produced_block`): Hercules has pruned the blocks
    /// covering the low end of this gap, so it can NEVER be fully backfilled.
    /// Flagged `source_pruned = TRUE` (loud `tracing::error!` + a
    /// distinguishable marker) so the gap is VISIBLY unrecoverable rather
    /// than falsely marked clean-`remediated_at` with 0 events. `remediated_at`
    /// stays NULL; `source_pruned = TRUE` removes it from the pending queue
    /// (see `pending_gaps`) so it neither re-alarms every tick nor
    /// head-of-line-blocks other gaps.
    SourcePruned {
        source_contract: [u8; 20],
        from_block: i64,
        retained_floor: i64,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum BackfillError {
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    #[error(transparent)]
    Source(#[from] anyhow::Error),
    #[error(transparent)]
    Ingest(#[from] IngestError),
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

struct PendingGap {
    source_contract: [u8; 20],
    from_block: i64,
    watermark_at_detection: i64,
    resume_from_slot: Option<i64>,
}

/// A bounded page of pending gaps (P4a MED-1 — the caller picks the first
/// INGESTABLE one, never blindly the first row), ordered deterministically.
async fn pending_gaps(pool: &PgPool, chain_id: i64) -> Result<Vec<PendingGap>, sqlx::Error> {
    let rows: Vec<(Vec<u8>, i64, i64, Option<i64>)> = sqlx::query_as(
        r#"
        SELECT source_contract, from_block, watermark_at_detection, resume_from_slot
        FROM audit.backfill_gap
        WHERE chain_id = $1 AND remediated_at IS NULL AND NOT source_pruned
        ORDER BY source_contract, from_block
        LIMIT $2
        "#,
    )
    .bind(chain_id)
    .bind(PENDING_GAP_PAGE_SIZE)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(addr_bytes, from_block, watermark_at_detection, resume_from_slot)| PendingGap {
            source_contract: addr_bytes.try_into().expect(
                "audit.backfill_gap.source_contract is always 20 bytes (BYTEA written from a [u8; 20])",
            ),
            from_block,
            watermark_at_detection,
            resume_from_slot,
        })
        .collect())
}

async fn persist_backfill_progress(
    pool: &PgPool,
    chain_id: i64,
    source_contract: [u8; 20],
    from_block: i64,
    resume_from_slot: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE audit.backfill_gap
        SET resume_from_slot = $4
        WHERE chain_id = $1 AND source_contract = $2 AND from_block = $3 AND remediated_at IS NULL
        "#,
    )
    .bind(chain_id)
    .bind(source_contract.as_slice())
    .bind(from_block)
    .bind(resume_from_slot)
    .execute(pool)
    .await?;
    Ok(())
}

async fn mark_gap_remediated(
    pool: &PgPool,
    chain_id: i64,
    source_contract: [u8; 20],
    from_block: i64,
    remediated_at: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE audit.backfill_gap
        SET remediated_at = $4, resume_from_slot = NULL
        WHERE chain_id = $1 AND source_contract = $2 AND from_block = $3 AND remediated_at IS NULL
        "#,
    )
    .bind(chain_id)
    .bind(source_contract.as_slice())
    .bind(from_block)
    .bind(remediated_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// H1 — flags a gap whose low end is unrecoverable (its `from_block`
/// predates the source's retained floor). Sets `source_pruned = TRUE` and
/// leaves `remediated_at` NULL: the row stays out of `pending_gaps` (which
/// filters `NOT source_pruned`) yet is honestly distinguishable from a clean
/// remediation.
async fn mark_gap_source_pruned(
    pool: &PgPool,
    chain_id: i64,
    source_contract: [u8; 20],
    from_block: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE audit.backfill_gap
        SET source_pruned = TRUE
        WHERE chain_id = $1 AND source_contract = $2 AND from_block = $3 AND remediated_at IS NULL
        "#,
    )
    .bind(chain_id)
    .bind(source_contract.as_slice())
    .bind(from_block)
    .execute(pool)
    .await?;
    Ok(())
}

/// Processes AT MOST one BOUNDED chunk of one pending gap for
/// `config.chain_id`. `config` is the CALLER's current, live `IngestConfig`
/// (`AuditWorker`'s own `self.ingest_config` in production) — the gapped
/// address's `SourceSpec` (kind + `IngestFilter`) is read from it directly,
/// never re-derived, so backfill and live ingest can never disagree about
/// what's in scope for that address. `max_slots_per_call` bounds how many
/// Solana slots this ONE call walks (P4a HIGH-2) — production callers pass
/// [`DEFAULT_MAX_SLOTS_PER_BACKFILL_CALL`]; tests pass a small value to
/// exercise chunking without a multi-million-row fixture.
pub async fn run_backfill_once(
    source: &PgPool,
    target: &PgPool,
    registry: &AbiRegistry,
    config: &IngestConfig,
    max_slots_per_call: i64,
) -> Result<BackfillOutcome, BackfillError> {
    let gaps = pending_gaps(target, config.chain_id).await?;

    // P4a MED-1: the first INGESTABLE gap in the page, not blindly the
    // first row — a gap whose address the caller currently doesn't
    // recognize must never block every OTHER pending gap forever.
    let chosen = gaps
        .iter()
        .find_map(|gap| {
            let addr_hex = format!("0x{}", hex::encode(gap.source_contract));
            config.sources.get(&addr_hex).map(|spec| (gap, addr_hex, spec))
        });

    let Some((gap, addr_hex, spec)) = chosen else {
        return Ok(match gaps.first() {
            None => BackfillOutcome::NoGapsPending,
            Some(first) => BackfillOutcome::GapNotIngestable {
                source_contract: first.source_contract,
            },
        });
    };

    // H1 — pruned-history alarm. If the gap's `from_block` predates the
    // source's retained floor, Hercules has pruned the blocks covering its
    // low end; it can never be fully backfilled. Flag it loudly + mark
    // `source_pruned` rather than silently walking only the retained tail and
    // marking the whole gap clean-remediated with a falsely-complete count.
    if let Some(retained_floor) = hercules_reads::min_produced_block(source).await? {
        if gap.from_block < retained_floor {
            mark_gap_source_pruned(target, config.chain_id, gap.source_contract, gap.from_block)
                .await?;
            tracing::error!(
                chain_id = config.chain_id,
                address = %addr_hex,
                from_block = gap.from_block,
                watermark_at_detection = gap.watermark_at_detection,
                retained_floor,
                "P4a/H1: backfill gap's from_block predates the source's retained floor — the low \
                 end of this range was PRUNED and can never be captured; flagged source_pruned \
                 (NOT falsely marked clean-remediated)"
            );
            return Ok(BackfillOutcome::SourcePruned {
                source_contract: gap.source_contract,
                from_block: gap.from_block,
                retained_floor,
            });
        }
    }

    let Some((range_min_slot, max_slot)) =
        hercules_reads::slot_range_for_blocks(source, gap.from_block, gap.watermark_at_detection)
            .await?
    else {
        // No produced block in range at all — nothing to backfill. Still
        // audit-final: mark remediated (0 events) rather than leaving a
        // permanently-pending row for a range that will never gain blocks.
        mark_gap_remediated(target, config.chain_id, gap.source_contract, gap.from_block, now_unix())
            .await?;
        return Ok(BackfillOutcome::Remediated {
            source_contract: gap.source_contract,
            from_block: gap.from_block,
            events_inserted: 0,
        });
    };

    // P4a HIGH-2: resume from the persisted cursor, never from `from_block`
    // (or the range's own min) — `.max(range_min_slot)` is a defensive
    // floor in case a resume cursor somehow predates the real range.
    let walk_start = gap.resume_from_slot.unwrap_or(range_min_slot).max(range_min_slot);
    // P4a NIT: a non-positive `max_slots_per_call` would make `chunk_end <
    // walk_start` — an empty loop that persists NO progress, forever
    // re-`Progressed`-ing at the same cursor (a livelock, not reachable
    // today since the only real caller passes the fixed 20_000 const, but
    // sealed against a future misconfigured caller).
    let effective_chunk = max_slots_per_call.max(1);
    let chunk_end = (walk_start.saturating_add(effective_chunk - 1)).min(max_slot);

    // Restricted to exactly this ONE address — reuses `ingest_slot`
    // byte-for-byte (same decode, same `ON CONFLICT DO NOTHING` idempotent
    // write, same terminal-quarantine/transient-propagate classification),
    // with `spec`'s REAL `IngestFilter` still applied inside it (module doc:
    // never a firehose).
    let addresses: BTreeSet<String> = BTreeSet::from([addr_hex.clone()]);
    let topic0_set: BTreeSet<String> = registry
        .all_topic0s()
        .iter()
        .map(|t| format!("0x{}", hex::encode(t)))
        .collect();
    let restricted_sources: BTreeMap<String, SourceSpec> =
        BTreeMap::from([(addr_hex.clone(), spec.clone())]);
    let restricted_config = IngestConfig {
        chain_id: config.chain_id,
        confirmation_lag: config.confirmation_lag,
        sources: restricted_sources,
    };

    let mut events_inserted = 0usize;
    if walk_start <= chunk_end {
        // H2 — batch the log READ across the whole chunk in ONE ranged query,
        // then re-drive the SAME per-slot decode/insert path (`ingest_slot`)
        // only for the NON-EMPTY slots. A gap spanning millions of slots is
        // overwhelmingly empty; the old `for slot in walk_start..=chunk_end`
        // fired one `matched_logs_at_slot` point-query PER slot (empty ones
        // included), blocking the tick loop. This turns that into `1 + K`
        // queries (K = non-empty slots in the chunk). `ingest_slot` is reused
        // BYTE-FOR-BYTE (same decode, filter, quarantine, idempotent
        // `ON CONFLICT DO NOTHING`) — never a forked decode path.
        let non_empty = hercules_reads::matched_log_slots_in_range(
            source,
            walk_start,
            chunk_end,
            &addresses,
            &topic0_set,
        )
        .await?;
        for slot in non_empty {
            // Ascending (the query's `ORDER BY slot_number`). Any transient
            // Err propagates HERE — neither `resume_from_slot` nor
            // `remediated_at` is touched for THIS chunk, so a mid-chunk
            // failure leaves the cursor at its LAST successfully-persisted
            // value and the next call re-walks from there (idempotent),
            // never from `from_block` (P4a HIGH-2).
            events_inserted += ingest_slot(
                source,
                target,
                registry,
                &restricted_config,
                &addresses,
                &topic0_set,
                slot,
            )
            .await?;
        }
    }

    if chunk_end < max_slot {
        persist_backfill_progress(target, config.chain_id, gap.source_contract, gap.from_block, chunk_end + 1)
            .await?;
        Ok(BackfillOutcome::Progressed {
            source_contract: gap.source_contract,
            from_block: gap.from_block,
            resumed_through_slot: chunk_end,
            events_inserted,
        })
    } else {
        mark_gap_remediated(target, config.chain_id, gap.source_contract, gap.from_block, now_unix())
            .await?;
        Ok(BackfillOutcome::Remediated {
            source_contract: gap.source_contract,
            from_block: gap.from_block,
            events_inserted,
        })
    }
}

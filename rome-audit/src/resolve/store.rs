//! Persistence for `capture_manifest` (capture §1.4). Kept separate from
//! [`super::manifest`] (the pure hash/canonicalization logic) the same way
//! `tier2::fetch`/`tier2::rebuild` separate reads from writes — no `PgPool`
//! anywhere in `manifest.rs`.

use std::collections::BTreeSet;

use sqlx::PgPool;

use super::manifest::CaptureManifest;

/// Inserts one `capture_manifest` row, keyed by its own `manifest_hash`
/// (idempotent — re-resolving an unchanged graph at a later wall-clock time
/// produces the same hash, per the determinism guarantee, and this INSERT
/// is then a no-op).
///
/// Generic over the executor (P3c H2) — `run_resolve_pass` calls this
/// against an open `Transaction` so every manifest insert + the
/// `asset_event` rebuild + any gap row land atomically in ONE transaction;
/// a bare `&PgPool` (autocommitting per statement) still works for any
/// standalone caller (e.g. this module's own tests).
pub async fn insert_capture_manifest<'c, E>(
    executor: E,
    chain_id: i64,
    manifest: &CaptureManifest,
    generated_at: i64,
) -> Result<[u8; 32], sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let hash = manifest.manifest_hash();
    sqlx::query(
        r#"
        INSERT INTO audit.capture_manifest
            (manifest_hash, chain_id, asset_id, registry_commit_sha, resolved_sources, source_intervals, generated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT (manifest_hash) DO NOTHING
        "#,
    )
    .bind(hash.as_slice())
    .bind(chain_id)
    .bind(&manifest.asset_id)
    .bind(&manifest.registry_commit_sha)
    .bind(serde_json::to_value(&manifest.resolved_sources).expect("infallible"))
    .bind(serde_json::to_value(&manifest.source_intervals).expect("infallible"))
    .bind(generated_at)
    .execute(executor)
    .await?;
    Ok(hash)
}

/// The union of every address ever recorded in ANY prior `capture_manifest`
/// row for `chain_id` — the durable "previous pass(es)' map" [`super::pass`]
/// used to diff a fresh resolve pass against to find genuinely NEW sources
/// (a.6). Read BEFORE the current pass's own manifests are inserted.
///
/// **Dead since P4a §3** — `resolve::pass::detect_backfill_gaps` now keys
/// gap detection on `previously_known_ingest_scope` instead (manifest-known
/// address-only was the silent-gap bug that fix closes: this fn can't
/// distinguish "descriptor-less, never actually ingestable" from "genuinely
/// ingestable," and can't see filter-widening at all). Kept (not deleted)
/// as public API in case an external caller of this crate's lib surface
/// still depends on it; `#[deprecated]` documents that nothing in THIS
/// crate calls it anymore.
#[deprecated(
    note = "superseded by previously_known_ingest_scope (P4a §3) for gap-detection purposes; kept only as a public-API compatibility shim"
)]
pub async fn previously_known_addresses(
    pool: &PgPool,
    chain_id: i64,
) -> Result<std::collections::BTreeSet<String>, sqlx::Error> {
    let rows: Vec<(String,)> = sqlx::query_as(
        r#"
        SELECT DISTINCT elem ->> 'address' AS address
        FROM audit.capture_manifest, jsonb_array_elements(resolved_sources) AS elem
        WHERE chain_id = $1
        "#,
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(a,)| a).collect())
}

/// Inserts one `audit.backfill_gap` row (a.6's honesty mechanism), keyed by
/// the table's own `(chain_id, source_contract, from_block)` primary key.
/// Generic over the executor (P3c H2) — see [`insert_capture_manifest`].
///
/// **P4a HIGH-1 fix.** The storage PK is `(chain_id, source_contract,
/// from_block)` — but `from_block` anchors at the MIN across every
/// contributing asset's interval for that address, which stays essentially
/// CONSTANT as MORE assets widen the same address's filter later (a later
/// asset's own interval never starts BEFORE an earlier one already
/// captured). So a 2nd (or 3rd, …) widening of the SAME address re-detects
/// under the SAME `from_block` and used to hit `ON CONFLICT … DO NOTHING`:
/// silently no-op if the 1st gap had ALREADY been remediated — the 2nd
/// widening's own newly-exposed slice of history (between the two
/// watermarks) was then never recorded as needing backfill at all. Callers
/// (`resolve::pass::detect_backfill_gaps`) only ever call this when the
/// `(address, filter_fingerprint)` pair is GENUINELY new this pass — so on
/// a real conflict, re-arming unconditionally is always correct, never
/// spurious: extend the row's coverage to the newer (larger) watermark and
/// reset `remediated_at`/`resume_from_slot` to NULL so the backfill executor
/// re-walks the row's FULL `[from_block, watermark_at_detection]` range
/// under whatever the CURRENT (now-wider) filter is — safe and idempotent
/// (`ON CONFLICT DO NOTHING` on `chain_event` re-lands zero duplicates for
/// the slice already captured under the narrower filter).
#[allow(clippy::too_many_arguments)]
pub async fn insert_backfill_gap<'c, E>(
    executor: E,
    chain_id: i64,
    source_contract: [u8; 20],
    source_kind: &str,
    from_block: i64,
    watermark_at_detection: i64,
    manifest_hash: [u8; 32],
    detected_at: i64,
) -> Result<(), sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    sqlx::query(
        r#"
        INSERT INTO audit.backfill_gap
            (chain_id, source_contract, source_kind, from_block, watermark_at_detection, manifest_hash, detected_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT (chain_id, source_contract, from_block) DO UPDATE
        SET watermark_at_detection = GREATEST(audit.backfill_gap.watermark_at_detection, EXCLUDED.watermark_at_detection),
            remediated_at = NULL,
            resume_from_slot = NULL
        "#,
    )
    .bind(chain_id)
    .bind(source_contract.as_slice())
    .bind(source_kind)
    .bind(from_block)
    .bind(watermark_at_detection)
    .bind(manifest_hash.as_slice())
    .bind(detected_at)
    .execute(executor)
    .await?;
    Ok(())
}

/// The set of `(address, filter_fingerprint)` pairs EVER actually
/// ingestable for `chain_id`, as of before this pass's own upsert (P4a §3
/// — the gap-detection re-key; see `resolve::pass::detect_backfill_gaps`'s
/// doc for why `previously_known_addresses`, which is manifest-known-ever
/// rather than actually-ingestable, silently missed both a newly-descriptor'd
/// kind and filter-widening). Read BEFORE the current pass's own
/// `upsert_ingest_scope` calls — same read-before-write discipline as
/// [`previously_known_addresses`].
pub async fn previously_known_ingest_scope(
    pool: &PgPool,
    chain_id: i64,
) -> Result<BTreeSet<(String, String)>, sqlx::Error> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        r#"
        SELECT '0x' || encode(address, 'hex'), filter_fingerprint
        FROM audit.ingest_scope
        WHERE chain_id = $1
        "#,
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().collect())
}

/// Upserts one `(chain_id, address, filter_fingerprint)` ingest-scope row —
/// idempotent (`first_seen_at` is never overwritten once a pair has
/// landed). `source_kind` IS refreshed on conflict (P4a NIT fix — a
/// dual-role address whose merge-conflict WINNER flips between passes,
/// e.g. a newly-registered richer kind now outranking the one that won
/// last time, would otherwise leave this column showing a stale kind;
/// harmless to gap-detection logic itself, which always reads the kind
/// straight off the CURRENT pass's `sources` map, but this column is also
/// meant to be human/tooling-readable). Generic over the executor (P3c H2
/// discipline continues here) — `run_resolve_pass` calls this inside its
/// existing transaction, so a later statement's failure rolls this back
/// too; a phantom fingerprint surviving a rolled-back pass would otherwise
/// permanently suppress that pair's future gap detection.
pub async fn upsert_ingest_scope<'c, E>(
    executor: E,
    chain_id: i64,
    address: [u8; 20],
    source_kind: &str,
    filter_fingerprint: &str,
    first_seen_at: i64,
) -> Result<(), sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    sqlx::query(
        r#"
        INSERT INTO audit.ingest_scope (chain_id, address, source_kind, filter_fingerprint, first_seen_at)
        VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (chain_id, address, filter_fingerprint) DO UPDATE SET source_kind = EXCLUDED.source_kind
        "#,
    )
    .bind(chain_id)
    .bind(address.as_slice())
    .bind(source_kind)
    .bind(filter_fingerprint)
    .bind(first_seen_at)
    .execute(executor)
    .await?;
    Ok(())
}

/// Reads back a `capture_manifest` row's `generated_at` (for the
/// determinism test: two inserts at different wall-clock times must produce
/// the SAME `manifest_hash`, but the row's `generated_at` still records
/// whichever insert actually landed first — `ON CONFLICT DO NOTHING` keeps
/// the original).
pub async fn generated_at_for(pool: &PgPool, hash: [u8; 32]) -> Result<Option<i64>, sqlx::Error> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT generated_at FROM audit.capture_manifest WHERE manifest_hash = $1")
            .bind(hash.as_slice())
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(g,)| g))
}

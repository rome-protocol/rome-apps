-- B2 self-heal (live-DB latch fix). On the live Hadrian deploy, resolve ran
-- 3× BEFORE ingest ever captured anything: with no `audit.ingest_watermark`
-- row, `run::compute_resolve_frontier` returned frontier -1, so
-- `detect_backfill_gaps` recorded NO gaps and `upsert_ingest_scope` marked
-- every (address, filter) pair "previously known". That latch means a
-- start-fix alone (B1) would then find every pair already in
-- `ingest_scope` and silently skip the initial cohort's pre-boot history
-- FOREVER.
--
-- A scope row claiming a pair "was ingestable" on a chain where nothing was
-- ever ingested (no watermark row) is VOID by construction — delete it so
-- the NEXT resolve pass (now that B1 initializes a real watermark) re-records
-- the honest backfill gaps. `capture_manifest` is deliberately NOT touched:
-- it's append-only and does not gate gap detection (gap detection keys on
-- `ingest_scope`, per resolve/pass.rs::detect_backfill_gaps).
DELETE FROM audit.ingest_scope s
WHERE NOT EXISTS (
    SELECT 1 FROM audit.ingest_watermark w WHERE w.chain_id = s.chain_id
);

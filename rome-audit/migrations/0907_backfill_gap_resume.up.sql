-- P4a HIGH-2: a widening gap can span a token's ENTIRE life (millions of
-- slots) — walking it all in one `backfill::run_backfill_once` call would
-- stall live ingest for hours, and a transient failure mid-range would
-- otherwise force a from-scratch restart on retry (livelock under a
-- persistently-flaky source). `resume_from_slot` persists the chunked
-- walk's progress so a call resumes from its own cursor, never from
-- `from_block`. NULL = never started (or reset by a re-arm, see 0006's
-- `ON CONFLICT` — HIGH-1).
ALTER TABLE audit.backfill_gap ADD COLUMN resume_from_slot BIGINT;

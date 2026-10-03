-- 0213_token_metadata_kind_nullable.up.sql
-- `kind` was NOT NULL DEFAULT 'ERC-20' (0100) — fine when kind was unused, but it
-- is now a meaningful on-chain classification (ERC-20 | SPL | Token-2022) written
-- by the metadata worker. NULL is the "unclassified, needs probing" sentinel that
-- (a) the worker's `name IS NULL OR kind IS NULL` re-poll and (b) the holders /
-- factory_tokens seed inserts rely on. The NOT NULL constraint made those
-- kind=NULL inserts/updates fail at runtime (observed live: factory_tokens
-- "null value in column kind violates not-null constraint"). Drop it so NULL works
-- as the set-once sentinel (avoids the perpetual re-probe an 'ERC-20' sentinel
-- would force). DROP NOT NULL is a no-op if already dropped (idempotent-safe).
ALTER TABLE rome_via.token_metadata ALTER COLUMN kind DROP NOT NULL;

-- Reclassify backfill: null existing kinds so the metadata worker re-probes each
-- token's on-chain identity (mint_id() + Solana mint owner) and rewrites kind.
-- Existing rows were seeded 'ERC-20' before kind was meaningful. Runs once
-- (migration-tracked); no-op on a fresh DB.
UPDATE rome_via.token_metadata SET kind = NULL;

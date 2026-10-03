-- 0214_add_token_provenance.up.sql
--
-- Token PROVENANCE — surface, on token-detail, where a token came from so the UI
-- can render a trust story (Verified=registry / Rome-factory=canonical-factory /
-- User). Three nullable columns:
--   * mint    — the underlying Solana SPL mint (base58). Written by the metadata
--               worker from mint_id(); also seeded by factory_tokens from the
--               TokenCreated `mint` topic. NULL for plain ERC-20s (no mint).
--   * factory — the ERC20SPLFactory that created the token (TokenCreated emitter).
--   * creator — the account that created it (TokenCreated indexed `creator`).
-- factory/creator are written by the factory_tokens worker.
ALTER TABLE rome_via.token_metadata
    ADD COLUMN IF NOT EXISTS mint    TEXT,
    ADD COLUMN IF NOT EXISTS factory VARCHAR(42),
    ADD COLUMN IF NOT EXISTS creator VARCHAR(42);

-- One-time `mint` backfill for WRAPPERS ONLY. The metadata worker's re-poll
-- predicate is `name IS NULL OR kind IS NULL` (intentionally NOT `mint IS NULL`,
-- which would re-probe plain ERC-20s forever — their mint is correctly NULL).
-- NULLing kind for the two wrapper kinds makes exactly those rows match the
-- predicate; the worker then re-probes and writes `mint` (and rewrites kind).
-- Plain ERC-20s (kind='ERC-20') are untouched — they have no mint. The pass is
-- self-terminating: once a wrapper is re-probed, kind+mint are set and it stops
-- matching. (On a fresh DB where 0213 already NULLed every kind, this matches
-- nothing — harmless; the post-0213 re-probe already writes mint.)
UPDATE rome_via.token_metadata SET kind = NULL WHERE kind IN ('SPL', 'Token-2022');

-- Reset the factory_tokens cursor so it re-scans history once and UPSERTs
-- factory/creator (and seeds mint) onto every already-known token whose
-- TokenCreated event predates this change. The prior cursor value can't be
-- reconstructed; the worker re-establishes it on the next run (the same pattern
-- as 0212's cross_chain reclassify).
DELETE FROM rome_via.enrich_cursors WHERE worker = 'factory_tokens';

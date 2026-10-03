-- Restore the NOT NULL constraint. Per-row kind values are not retained across the
-- up-migration's reclassify, so backfill any remaining NULLs to the original
-- default before re-asserting NOT NULL. (The column keeps its DEFAULT 'ERC-20'.)
UPDATE rome_via.token_metadata SET kind = 'ERC-20' WHERE kind IS NULL;
ALTER TABLE rome_via.token_metadata ALTER COLUMN kind SET NOT NULL;

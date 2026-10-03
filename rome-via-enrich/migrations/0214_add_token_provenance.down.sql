-- 0214_add_token_provenance.down.sql
--
-- Drop the three provenance columns. The kind=NULL backfill and the
-- factory_tokens cursor delete from the up-migration are not reversible (the
-- prior values can't be reconstructed) — but they're harmless to leave: the
-- worker simply re-establishes its cursor and re-derives kind. Dropping the
-- columns is the meaningful inverse.
ALTER TABLE rome_via.token_metadata
    DROP COLUMN IF EXISTS mint,
    DROP COLUMN IF EXISTS factory,
    DROP COLUMN IF EXISTS creator;

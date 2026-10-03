-- 0001_enrich_tables.down.sql
-- Reverses 0001_enrich_tables.up.sql

DROP TABLE IF EXISTS rome_via.enrich_cursors;
DROP INDEX IF EXISTS rome_via.ix_search_index_trgm;
DROP TABLE IF EXISTS rome_via.search_index;
DROP EXTENSION IF EXISTS pg_trgm CASCADE;
DROP TABLE IF EXISTS rome_via.address_stats;
DROP TABLE IF EXISTS rome_via.token_holder_counts;
DROP INDEX IF EXISTS rome_via.ix_token_holders_token;
DROP TABLE IF EXISTS rome_via.token_holders;
DROP INDEX IF EXISTS rome_via.ix_token_transfers_to;
DROP INDEX IF EXISTS rome_via.ix_token_transfers_from;
DROP INDEX IF EXISTS rome_via.ix_token_transfers_token;
DROP TABLE IF EXISTS rome_via.token_transfers;
DROP TABLE IF EXISTS rome_via.token_metadata;
DROP TABLE IF EXISTS rome_via.method_signatures;

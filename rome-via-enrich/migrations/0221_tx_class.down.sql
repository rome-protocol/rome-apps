-- 0221_tx_class.down.sql
-- Drops the persisted classification; the API's seam/type/status filters lose their
-- backing table and must fall back to unfiltered reads.
DROP TABLE IF EXISTS rome_via.tx_class;

-- 0220_throughput_histogram.down.sql
-- Drops the persisted histogram; /throughput/histogram falls back to the live scan.
DROP TABLE IF EXISTS rome_via.throughput_histogram;

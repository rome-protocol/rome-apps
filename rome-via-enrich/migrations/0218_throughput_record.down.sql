-- 0218_throughput_record.down.sql
--
-- Drop throughput record tables in reverse order (safe, FK-free).
DROP TABLE IF EXISTS rome_via.throughput_busiest_blocks;
DROP TABLE IF EXISTS rome_via.throughput_peak_windows;

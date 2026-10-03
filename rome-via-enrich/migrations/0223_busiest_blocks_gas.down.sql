-- 0223_busiest_blocks_gas.down.sql
ALTER TABLE rome_via.throughput_busiest_blocks DROP COLUMN IF EXISTS gas_used;

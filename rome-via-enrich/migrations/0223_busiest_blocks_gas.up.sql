-- 0223_busiest_blocks_gas.up.sql
--
-- Adds gas_used to the busiest-blocks record so /throughput/top-blocks can be served
-- from it. TopBlock exposes gas_used; without the column the fast path would have to
-- emit a placeholder that silently differs from the live scan it replaces.
--
-- Backfilled by the throughput_record worker's next full recompute (existing rows read
-- 0 until then, which is why the API only serves from the record when the row is present
-- AND the worker has run at least one recompute pass on this schema).
ALTER TABLE rome_via.throughput_busiest_blocks
    ADD COLUMN IF NOT EXISTS gas_used BIGINT NOT NULL DEFAULT 0;

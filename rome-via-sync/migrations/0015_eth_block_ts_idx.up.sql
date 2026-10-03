-- Accelerate the recent-window aggregates that filter
-- eth_block.params_block_timestamp > NOW() - <window>:
--   stats/overview recent_txs, throughput current-tps / timeseries /
--   top-blocks / cadence.
-- The existing eth_block indexes cover (chain_id, params_number) and
-- (chain_id, params_blockhash) but NOT the timestamp, so every windowed
-- query was a range scan over the whole chain partition — the source of the
-- home-page slow-query pileup under load.
CREATE INDEX IF NOT EXISTS rome_via_eth_block_ts
  ON rome_via.eth_block (chain_id, params_block_timestamp);

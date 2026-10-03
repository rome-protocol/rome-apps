-- GET /stats computed total_txs as COUNT(*) over rome_via.evm_tx on every cache
-- miss. That table is 33M rows and growing ~1.5M/hour, so the cost of the headline
-- "all-time transactions" number scales with chain age — and because it sits behind
-- a short cache TTL, the displayed value does not tick, it STEPS by whatever
-- accumulated while the cache was warm. Compute and liveness are the same defect.
--
-- Maintained instead. rome-via-sync adds the number of rows it genuinely inserted,
-- in the SAME statement as the insert (see sync::evm_tx_insert_sql), so the counter
-- cannot drift from the rows it counts.
--
-- Seeded from the current COUNT(*): one scan, once, rather than one per cache miss.
CREATE TABLE IF NOT EXISTS rome_via.chain_counters (
    chain_id   BIGINT PRIMARY KEY,
    total_txs  BIGINT      NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO rome_via.chain_counters (chain_id, total_txs)
SELECT chain_id, COUNT(*) FROM rome_via.evm_tx GROUP BY chain_id
ON CONFLICT (chain_id) DO NOTHING;

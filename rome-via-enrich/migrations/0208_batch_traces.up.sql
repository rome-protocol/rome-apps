-- DoTxBatch per-sub-call + per-CPI trace storage.
--
-- Populated by the `batch_trace` enrich worker: poll each indexed Solana tx
-- via evm_tx_sol_tx, fetch meta.log_messages, call
-- rome_evm_client::indexer::parsers::parse_batch_trace, and persist the
-- structured trace as JSONB. Surfaced via rome-via-api at
-- GET /api/v1/txs/{hash}/batch-trace.
--
-- One row per (chain_id, tx_hash) batch tx. Non-batch txs (those whose
-- log_messages don't contain the "Atomic batch transaction" start marker)
-- are skipped during ingestion — they get no row.
CREATE TABLE IF NOT EXISTS rome_via.batch_traces (
    chain_id   BIGINT      NOT NULL,
    tx_hash    TEXT        NOT NULL,
    trace      JSONB       NOT NULL,
    indexed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, tx_hash)
);

CREATE INDEX IF NOT EXISTS idx_batch_traces_indexed_at
    ON rome_via.batch_traces (chain_id, indexed_at DESC);

-- 0221_tx_class.up.sql
--
-- rome_via.tx_class: persisted per-transaction classification (action tags, cross-VM
-- seams, status, rome tx type), so the explorer can FILTER and COUNT on them in SQL.
--
-- Until now action tags were computed at read time (per row, after pagination) and the
-- seam rule lived only in the frontend. Screens that wanted "only cross-VM" or "only
-- failed" therefore had to fetch a recency window and narrow it in the browser — under
-- load that window is all ordinary EVM traffic, so those screens rendered empty while
-- the chain held thousands of matching rows.
--
-- Maintained by the tx_class enrich worker. Because classification joins evm_tx,
-- evm_tx_result, cross_chain_correlations and method_signatures — the last two written
-- by OTHER workers — a row classified too early can be wrong; the worker therefore
-- re-processes a trailing slot window and runs a periodic full recompute.
--
-- slot_number/tx_idx are carried so a seam filter and the feed's
-- (slot_number DESC, tx_idx DESC) ordering can be served by one index.
--
-- No foreign-key constraints: rome_via schema design.
CREATE TABLE IF NOT EXISTS rome_via.tx_class (
    chain_id     BIGINT      NOT NULL,
    tx_hash      VARCHAR(66) NOT NULL,
    slot_number  BIGINT      NOT NULL,   -- keyset half 1, mirrors eth_block_txs
    tx_idx       INT         NOT NULL,   -- keyset half 2
    action_tags  TEXT[]      NOT NULL DEFAULT '{}',
    seams        TEXT[]      NOT NULL DEFAULT '{}',   -- evm_to_sol | sol_to_evm | bridge
    status       TEXT        NOT NULL,                -- success | failed
    rome_tx_type TEXT        NOT NULL DEFAULT 'Rhea', -- COALESCEd at WRITE time
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, tx_hash)
);

-- Partial btree per seam, NOT a GIN index on seams[]: the feed needs ORDERED access
-- (slot_number DESC, tx_idx DESC) which GIN cannot serve, and the seam set is closed.
CREATE INDEX IF NOT EXISTS ix_tx_class_seam_evm_to_sol
    ON rome_via.tx_class (chain_id, slot_number DESC, tx_idx DESC)
    WHERE 'evm_to_sol' = ANY(seams);
CREATE INDEX IF NOT EXISTS ix_tx_class_seam_sol_to_evm
    ON rome_via.tx_class (chain_id, slot_number DESC, tx_idx DESC)
    WHERE 'sol_to_evm' = ANY(seams);
CREATE INDEX IF NOT EXISTS ix_tx_class_seam_bridge
    ON rome_via.tx_class (chain_id, slot_number DESC, tx_idx DESC)
    WHERE 'bridge' = ANY(seams);
-- Failed is the selective half of status; success needs no index (it is the bulk).
CREATE INDEX IF NOT EXISTS ix_tx_class_status_failed
    ON rome_via.tx_class (chain_id, slot_number DESC, tx_idx DESC)
    WHERE status = 'failed';
CREATE INDEX IF NOT EXISTS ix_tx_class_type
    ON rome_via.tx_class (chain_id, rome_tx_type, slot_number DESC, tx_idx DESC);

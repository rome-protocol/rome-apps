-- 0224_cross_vm_seams.up.sql
--
-- rome_via.cross_vm_seams: the feed of EVM<->Solana crossings. One row per tx that
-- sits on at least one seam — and NO row for anything else.
--
-- This replaces rome_via.tx_class (0221/0222), which recorded a row for EVERY
-- transaction so that a query could filter down to the crossings. On hadrian that was
-- 3.09M rows (growing ~100/s) to locate 114 cross-VM txs — 1 in 27,000. Every problem
-- that table caused was a consequence of that ratio: the writer could not keep up with
-- the chain, which forced a drain loop and three cursors, and a needle-in-a-haystack
-- predicate over a full shadow table gave the planner a choice it kept getting wrong
-- (identical indexes, 233ms vs 20.7s depending only on which filter was set).
--
-- A feed of just the crossings is small and dense: the writer reads every tx to decide
-- but writes almost nothing, so it cannot fall behind, and every read is an ordered scan
-- of a tiny table. `seams` is non-empty by construction — membership IS the filter.
--
-- slot_number/tx_idx mirror the tx feed's keyset so this table can be paginated the
-- same way, and here they are the primary access path rather than join ballast.
--
-- No foreign-key constraints: rome_via schema design.
CREATE TABLE IF NOT EXISTS rome_via.cross_vm_seams (
    chain_id     BIGINT      NOT NULL,
    tx_hash      VARCHAR(66) NOT NULL,
    slot_number  BIGINT      NOT NULL,
    tx_idx       INT         NOT NULL,
    seams        TEXT[]      NOT NULL,   -- non-empty: evm_to_sol | sol_to_evm | bridge
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, tx_hash)
);

-- The feed's only access path: newest crossings first. No per-seam partial indexes —
-- the table holds hundreds of rows, so filtering within it is free, and one index that
-- always applies beats several the planner has to choose between.
CREATE INDEX IF NOT EXISTS ix_cross_vm_seams_feed
    ON rome_via.cross_vm_seams (chain_id, slot_number DESC, tx_idx DESC);

-- Retire the shadow table and its five indexes. Purely derived data: no chain state
-- lives here, and the new worker rebuilds the crossings from evm_tx on its first pass.
DROP TABLE IF EXISTS rome_via.tx_class;

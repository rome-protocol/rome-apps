-- Surface the priority portion of the fee on each tx result so the explorer can
-- show the base-vs-priority split (Phase 3). The on-chain program emits a
-- PRIORITY_FEE receipt-log marker (lamports); the SDK LogParser records it into
-- tx_result.gas_report.priority_fee, and sync denormalizes it into this column.
-- base = gas_value - priority_fee is derived API-side from the same gas_report.
--
-- Additive + defaulted: legacy mirrored rows (and pre-priority txs that never set
-- the marker) read as 0, which is correct — no priority means base == gas_value.
-- Lamport-scale value fits BIGINT (priority is a tiny per-tx bid, not the U256
-- wei total that gas_value/gas_price carry as NUMERIC).

ALTER TABLE rome_via.evm_tx_result
    ADD COLUMN IF NOT EXISTS priority_fee BIGINT NOT NULL DEFAULT 0;

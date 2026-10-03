-- 0211_contract_labels.up.sql
-- Per-contract on-chain identity cache for the contract-labeling feature.
--
-- Populated by the `contract_labels` enrich worker: for each distinct
-- evm_tx.to_addr (NOT NULL) absent from this table, it resolves the contract's
-- ON-CHAIN identity once — name()/symbol() eth_calls (batched via Multicall3
-- aggregate3) plus runtime-bytecode selector fingerprints — normalizes that to
-- a clean protocol display label, and upserts the row. The label source is the
-- CHAIN, not an off-chain registry.
--
-- Surfaced via rome-via-api (Layer 2), which JOINs display_label onto the tx
-- list/detail so e.g. the Compound v3 Comet (name() = "Compound cached
-- 9-asset") renders as "Compound".
--
-- One row per (chain_id, address). Re-running the worker is idempotent —
-- "absent from this table" is the trigger, so first deploy auto-backfills every
-- already-indexed contract.
CREATE TABLE IF NOT EXISTS rome_via.contract_labels (
    chain_id             BIGINT      NOT NULL,
    address              VARCHAR(42) NOT NULL,
    display_label        TEXT,                    -- clean protocol/token label, e.g. "Compound"
    display_label_detail TEXT,                    -- secondary detail, e.g. "cwUSDC-9"
    raw_name             TEXT,                    -- on-chain name() return (audit trail)
    raw_symbol           TEXT,                    -- on-chain symbol() return (audit trail)
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, address)
);

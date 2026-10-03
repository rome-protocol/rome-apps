-- 0201_hook_executions.up.sql
-- Hook execution tracking — one row per hook invocation per transaction.
-- Phase 4.5: populated by the hook_executions enrich worker.
CREATE SCHEMA IF NOT EXISTS rome_via;

CREATE TABLE IF NOT EXISTS rome_via.hook_executions (
    chain_id       BIGINT      NOT NULL,
    tx_hash        VARCHAR(66) NOT NULL,
    hook_address   VARCHAR(44) NOT NULL,  -- matches hooks_registry.hook_address
    hook_kind      TEXT        NOT NULL,  -- 'native' | 'evm_kyc' | 'evm_custom'
    result         TEXT        NOT NULL,  -- 'pass' | 'reject' | 'error' | 'skipped'
    reason         TEXT,                  -- human-readable reason (e.g. "sender not on whitelist")
    gas_used       BIGINT,               -- CU consumed by the hook invocation
    block_number   BIGINT,
    timestamp      TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, tx_hash, hook_address)
);

CREATE INDEX IF NOT EXISTS ix_hook_exec_hook
    ON rome_via.hook_executions (chain_id, hook_address, timestamp DESC);

CREATE INDEX IF NOT EXISTS ix_hook_exec_block
    ON rome_via.hook_executions (chain_id, block_number DESC);

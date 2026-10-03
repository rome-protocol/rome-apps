-- Meta-Hook Router invocations indexed directly from Solana RPC.
-- These represent Token-2022 transfers whose mint has the router attached as a
-- transfer hook. They are *not* EVM txs (no entry in evm_tx), so they need a
-- separate identity, but the router itself executes rome-evm bytecode for each
-- attached hook, so the hook outcomes live in the existing hook_executions
-- table using a synthetic `tx_hash = 'sol:<sol_signature>'`.
CREATE TABLE IF NOT EXISTS rome_via.meta_hook_invocations (
    chain_id      BIGINT      NOT NULL,
    sol_signature TEXT        NOT NULL,
    slot_number   BIGINT      NOT NULL,
    block_time    BIGINT,                   -- unix epoch seconds from Solana
    mint          TEXT,                     -- base58 mint parsed from router anchor log
    hook_count    INTEGER     NOT NULL DEFAULT 0,
    outcome       TEXT        NOT NULL,     -- 'success' | 'reject' | 'error' | 'unknown'
    fee_payer     TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, sol_signature)
);

CREATE INDEX IF NOT EXISTS meta_hook_invocations_slot_desc
    ON rome_via.meta_hook_invocations (chain_id, slot_number DESC);

-- 0200_cross_chain.up.sql
-- Cross-chain correlations and hooks registry tables.
-- Phase 4: cross-chain worker + hooks-registry worker.
CREATE SCHEMA IF NOT EXISTS rome_via;

-- ─────────────────────────────────────────────────────────────────────────────
-- Cross-chain correlations — one row per indexed tx with its rome_tx_type
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rome_via.cross_chain_correlations (
    chain_id       BIGINT      NOT NULL,
    tx_hash        VARCHAR(66) NOT NULL,
    rome_tx_type   TEXT        NOT NULL, -- 'Rhea' | 'Remus' | 'Romulus'
    evm_legs       JSONB       NOT NULL DEFAULT '[]'::jsonb,   -- [{chainId, blockNumber, txHash}]
    solana_legs    JSONB       NOT NULL DEFAULT '[]'::jsonb,   -- [{solChain, solSignature}]
    block_number   BIGINT      NOT NULL,
    timestamp      TIMESTAMPTZ,
    PRIMARY KEY (chain_id, tx_hash)
);

CREATE INDEX IF NOT EXISTS ix_cross_chain_type
    ON rome_via.cross_chain_correlations (chain_id, rome_tx_type, block_number DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- Hooks registry — token-2022 hooks and meta-hook router registrations
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rome_via.hooks_registry (
    chain_id           BIGINT      NOT NULL,
    token_address      VARCHAR(42) NOT NULL,
    hook_address       VARCHAR(44) NOT NULL, -- Solana pubkey (44 chars) or EVM address (42 chars)
    hook_kind          TEXT        NOT NULL, -- 'native' | 'evm_kyc' | 'evm_custom' | ...
    registered_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    registered_tx_hash VARCHAR(66),
    source             TEXT        NOT NULL DEFAULT 'manual', -- 'event' | 'manual'
    PRIMARY KEY (chain_id, token_address, hook_address)
);

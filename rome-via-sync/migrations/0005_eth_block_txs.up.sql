-- Mirror of Hercules' eth_block_txs table.
-- Divergence: chain_id prepended to PK; FK constraints omitted (cross-chain safe).
-- tx_idx is INTEGER in Hercules (not SMALLINT); keep as INTEGER.

CREATE TABLE rome_via.eth_block_txs (
    chain_id       BIGINT NOT NULL,
    slot_number    BIGINT NOT NULL,
    slot_block_idx INTEGER NOT NULL,
    tx_hash        VARCHAR(66) NOT NULL,
    tx_idx         INTEGER NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, slot_number, slot_block_idx, tx_hash)
);

-- Index for tx_hash lookups (single-tx queries)
CREATE INDEX rome_via_eth_block_txs_hash ON rome_via.eth_block_txs (chain_id, tx_hash);

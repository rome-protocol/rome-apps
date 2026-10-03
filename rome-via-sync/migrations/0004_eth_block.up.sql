-- Mirror of Hercules' eth_block table.
-- Divergence: chain_id prepended to PK; Hercules uses a BlockParams composite type;
--   we flatten all BlockParams fields into individual columns for query convenience
--   and to avoid Postgres composite type DDL in the mirror DB.
-- BlockParams = (blockhash VARCHAR(66), parent_hash VARCHAR(66), number BIGINT, block_timestamp NUMERIC)

CREATE TABLE rome_via.eth_block (
    chain_id          BIGINT NOT NULL,
    slot_number       BIGINT NOT NULL,
    slot_block_idx    INTEGER NOT NULL,
    block_gas_used    NUMERIC NOT NULL,
    gas_recipient     VARCHAR(42),
    slot_timestamp    BIGINT,
    -- Flattened BlockParams (nullable until block is produced):
    params_blockhash    VARCHAR(66),
    params_parent_hash  VARCHAR(66),
    params_number       BIGINT,
    params_block_timestamp NUMERIC,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, slot_number, slot_block_idx)
);

-- Equivalent of eth_block_hash HASH index (chain_id prefix makes HASH less useful; use BTREE)
CREATE INDEX rome_via_eth_block_blockhash  ON rome_via.eth_block (chain_id, params_blockhash);
-- Equivalent of eth_block_number index
CREATE INDEX rome_via_eth_block_number     ON rome_via.eth_block (chain_id, params_number);

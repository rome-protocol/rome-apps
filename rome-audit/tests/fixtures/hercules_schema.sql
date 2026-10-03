-- A Hercules-shaped test schema — the subset of
-- rome-sdk/rome-evm-client/src/indexer/pg_storage/migrations that the crate's
-- HerculesSource reads, made TYPE-FAITHFUL to the real migrations so a test
-- exercises the exact sqlx decode path production does. In particular
-- `sol_slot.status` is the real custom Postgres ENUM `slotstatus` (declared
-- in `2024-11-25-151837_sol_block/up.sql`), NOT a stand-in TEXT column: the
-- reader decodes it as `Option<String>` via an explicit `status::text` cast,
-- and a TEXT fixture could not have caught the enum→String decode error that
-- broke live ingest. The composite block-params type keeps its production
-- name `blockparams` (from `2024-11-26-124439_eth_block/up.sql`) with
-- identical field types.

CREATE TYPE slotstatus AS ENUM ('Processed', 'Confirmed', 'Finalized');

CREATE TYPE blockparams AS (
    blockhash VARCHAR(66),
    parent_hash VARCHAR(66),
    number BIGINT,
    block_timestamp NUMERIC
);

CREATE TABLE sol_slot (
    slot_number BIGINT PRIMARY KEY,
    parent_slot BIGINT NOT NULL,
    status slotstatus,
    blockhash TEXT,
    timestamp BIGINT
);

CREATE TABLE eth_block (
    slot_number BIGINT NOT NULL,
    slot_block_idx INTEGER NOT NULL,
    PRIMARY KEY (slot_number, slot_block_idx),
    block_gas_used NUMERIC NOT NULL DEFAULT 0,
    gas_recipient VARCHAR(42),
    slot_timestamp BIGINT,
    params blockparams
);

CREATE TABLE evm_tx (
    tx_hash VARCHAR(66) PRIMARY KEY,
    rlp BYTEA,
    origination VARCHAR(16) NOT NULL DEFAULT 'ecdsa',
    solana_signer VARCHAR(64),
    from_address VARCHAR(42)
);

CREATE TABLE evm_tx_result (
    slot_number BIGINT NOT NULL,
    tx_hash VARCHAR(66) NOT NULL,
    PRIMARY KEY (slot_number, tx_hash),
    tx_result JSONB NOT NULL,
    receipt_params JSONB
);

CREATE TABLE eth_block_txs (
    slot_number BIGINT NOT NULL,
    slot_block_idx INTEGER NOT NULL,
    tx_hash VARCHAR(66) NOT NULL,
    tx_idx INTEGER NOT NULL,
    PRIMARY KEY (slot_number, slot_block_idx, tx_hash)
);

CREATE TABLE evm_log (
    slot_number BIGINT NOT NULL,
    tx_hash VARCHAR(66) NOT NULL,
    log_ordinal INTEGER NOT NULL,
    address VARCHAR(42) NOT NULL,
    topic0 VARCHAR(66),
    topic1 VARCHAR(66),
    topic2 VARCHAR(66),
    topic3 VARCHAR(66),
    PRIMARY KEY (slot_number, tx_hash, log_ordinal)
);

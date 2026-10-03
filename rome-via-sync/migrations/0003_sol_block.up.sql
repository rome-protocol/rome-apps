-- Mirror of Hercules' sol_block table.
-- Divergence: chain_id prepended to PK; FK to sol_slot omitted (cross-chain safe).

CREATE TABLE rome_via.sol_block (
    chain_id     BIGINT NOT NULL,
    slot_number  BIGINT NOT NULL,
    block_data   BYTEA,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, slot_number)
);

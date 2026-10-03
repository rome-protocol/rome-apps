-- Recreates the table SHAPE only, matching 0003 exactly. The mirrored payloads are
-- not recoverable from here — re-populating means reverting this, re-enabling
-- mirroring, and letting it backfill from the source, which retains the same slot
-- range.
CREATE TABLE IF NOT EXISTS rome_via.sol_block (
    chain_id     BIGINT NOT NULL,
    slot_number  BIGINT NOT NULL,
    block_data   BYTEA,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, slot_number)
);

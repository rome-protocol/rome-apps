-- Sync cursor table: tracks how far each table has been synced per chain.
-- last_synced_slot is the max slot_number (or evm_tx_hash cursor for evm_tx/evm_tx_sol_tx)
-- successfully written to the target table.

CREATE TABLE rome_via.sync_cursors (
    chain_id         BIGINT NOT NULL,
    table_name       TEXT NOT NULL,
    last_synced_slot BIGINT NOT NULL DEFAULT 0,
    last_synced_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, table_name)
);

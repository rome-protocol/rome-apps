-- Mirror of Hercules' sol_slot table.
-- Divergence: chain_id prepended to PK; no enum type (stored as TEXT for portability);
--   no foreign keys across chain boundaries; parent_slot nullable (matches Hercules 2025-08-13 migration).
--   blockhash + timestamp columns included (added to Hercules 2025-03-04).

CREATE TABLE rome_via.sol_slot (
    chain_id     BIGINT NOT NULL,
    slot_number  BIGINT NOT NULL,
    parent_slot  BIGINT,
    -- Hercules SlotStatus enum stored as TEXT: 'Processed' | 'Confirmed' | 'Finalized'
    status       TEXT,
    blockhash    TEXT,
    timestamp    BIGINT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, slot_number)
);

-- Equivalent of Hercules' sol_slot_parent index
CREATE INDEX rome_via_sol_slot_parent ON rome_via.sol_slot (chain_id, parent_slot);

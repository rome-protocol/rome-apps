-- Mirror of Hercules' evm_tx_sol_tx table (added 2025-12-25).
-- Divergence: chain_id prepended to PK; FK constraints omitted; UNIQUE constraint
--   on (slot_number, sol_signature) becomes a unique index with chain_id prefix.

CREATE TABLE rome_via.evm_tx_sol_tx (
    chain_id      BIGINT NOT NULL,
    evm_tx_hash   VARCHAR(66) NOT NULL,
    sol_signature TEXT NOT NULL,
    slot_number   BIGINT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, evm_tx_hash, sol_signature)
);

-- Equivalent of Hercules' UNIQUE (slot_number, sol_signature)
CREATE UNIQUE INDEX rome_via_evm_tx_sol_tx_slot_sig
    ON rome_via.evm_tx_sol_tx (chain_id, slot_number, sol_signature);

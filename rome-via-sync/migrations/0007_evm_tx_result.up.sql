-- Mirror of Hercules' evm_tx_result table.
-- Divergence: chain_id prepended to PK; FK constraints omitted.

CREATE TABLE rome_via.evm_tx_result (
    chain_id        BIGINT NOT NULL,
    slot_number     BIGINT NOT NULL,
    tx_hash         VARCHAR(66) NOT NULL,
    tx_result       JSONB NOT NULL,
    receipt_params  JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, slot_number, tx_hash)
);

-- Equivalent of Hercules' evm_tx_result_tx_hash index
CREATE INDEX rome_via_evm_tx_result_tx_hash ON rome_via.evm_tx_result (chain_id, tx_hash);

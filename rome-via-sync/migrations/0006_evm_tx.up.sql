-- Mirror of Hercules' evm_tx table.
-- Divergence: chain_id prepended to PK; from_address included (added to Hercules 2026-03-13).

CREATE TABLE rome_via.evm_tx (
    chain_id      BIGINT NOT NULL,
    tx_hash       VARCHAR(66) NOT NULL,
    rlp           BYTEA,
    from_address  VARCHAR(42),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, tx_hash)
);

-- Equivalent of Hercules' idx_evm_tx_from_address index
CREATE INDEX rome_via_evm_tx_from_address ON rome_via.evm_tx (chain_id, from_address);

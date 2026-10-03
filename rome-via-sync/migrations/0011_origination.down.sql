-- Revert 0011: drop the Solana-gate indexes and columns from rome_via.evm_tx.

DROP INDEX IF EXISTS rome_via.idx_rv_evm_tx_solana_signer;
DROP INDEX IF EXISTS rome_via.idx_rv_evm_tx_origination;

ALTER TABLE rome_via.evm_tx
    DROP COLUMN IF EXISTS solana_signer,
    DROP COLUMN IF EXISTS origination;

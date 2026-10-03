-- Revert 0013: drop the denormalized priority_fee column from rome_via.evm_tx_result.

ALTER TABLE rome_via.evm_tx_result
    DROP COLUMN IF EXISTS priority_fee;

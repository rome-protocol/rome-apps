-- Revert 0010: drop denormalized columns and indexes from rome_via.evm_tx.

DROP INDEX IF EXISTS rome_via.ix_evm_tx_to;
DROP INDEX IF EXISTS rome_via.ix_evm_tx_from;

ALTER TABLE rome_via.evm_tx
  DROP COLUMN IF EXISTS tx_type_byte,
  DROP COLUMN IF EXISTS input_len,
  DROP COLUMN IF EXISTS method_id,
  DROP COLUMN IF EXISTS gas_limit,
  DROP COLUMN IF EXISTS gas_price,
  DROP COLUMN IF EXISTS nonce,
  DROP COLUMN IF EXISTS value_wei,
  DROP COLUMN IF EXISTS to_addr,
  DROP COLUMN IF EXISTS from_addr;

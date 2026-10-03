-- Denormalize decoded RLP fields into rome_via.evm_tx.
-- All columns are nullable so existing rows remain valid;
-- the sync loop populates them at insert time (decode happens
-- in rome-via-sync when RLP is available).
-- Operators can backfill old rows with: rome-via-enrich rebuild --table evm_tx (Phase 3+).

ALTER TABLE rome_via.evm_tx
  ADD COLUMN from_addr   VARCHAR(42),     -- 0x-prefixed, lowercase, recovered from signature
  ADD COLUMN to_addr     VARCHAR(42),     -- 0x-prefixed; NULL = contract creation
  ADD COLUMN value_wei   NUMERIC,         -- Wei amount (decimal, string-convertible)
  ADD COLUMN nonce       BIGINT,
  ADD COLUMN gas_price   NUMERIC,         -- legacy gas_price OR max_fee_per_gas for EIP-1559
  ADD COLUMN gas_limit   BIGINT,
  ADD COLUMN method_id   VARCHAR(10),     -- "0x" + first 4 data bytes hex; NULL for transfers
  ADD COLUMN input_len   INTEGER,         -- byte length of tx data (0 for plain transfers)
  ADD COLUMN tx_type_byte SMALLINT;       -- 0=legacy, 1=EIP-2930, 2=EIP-1559

-- Sender address index (explorer: all txs from an address)
CREATE INDEX ix_evm_tx_from ON rome_via.evm_tx(chain_id, from_addr);

-- Recipient address index (explorer: all txs to a contract/address)
CREATE INDEX ix_evm_tx_to ON rome_via.evm_tx(chain_id, to_addr);

-- Creator + creation transaction per contract, for the address page (B4).
-- Populated by the contract_creation worker from eth_getTransactionReceipt of
-- to=NULL deploy txs (contractAddress + from). Nullable — absent for
-- factory-deployed contracts (no top-level creation) and until backfilled.
ALTER TABLE rome_via.contract_labels ADD COLUMN IF NOT EXISTS creator     VARCHAR(42);
ALTER TABLE rome_via.contract_labels ADD COLUMN IF NOT EXISTS creation_tx VARCHAR(66);

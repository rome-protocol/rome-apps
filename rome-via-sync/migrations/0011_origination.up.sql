-- Mirror Hercules' Solana-gate columns onto rome_via.evm_tx.
-- origination distinguishes signature-recovered ('ecdsa') txs from Solana-native
-- DoTxUnsigned ('solana_unsigned') txs, whose `from` is a synthetic address
-- (= keccak(solana_signer)[12:]) carried over from Hercules — there is no
-- recoverable ECDSA signature for those. solana_signer is the base58 Solana
-- pubkey that authorized the tx; NULL for ecdsa.
-- Existing rows default to 'ecdsa' so they remain valid.

ALTER TABLE rome_via.evm_tx
    ADD COLUMN IF NOT EXISTS origination   VARCHAR(16) NOT NULL DEFAULT 'ecdsa',
    ADD COLUMN IF NOT EXISTS solana_signer VARCHAR(64);   -- base58 Solana pubkey; NULL for ecdsa

-- Explorer: list Solana-native txs for a chain (partial — excludes the common case).
CREATE INDEX IF NOT EXISTS idx_rv_evm_tx_origination
    ON rome_via.evm_tx (chain_id, origination) WHERE origination <> 'ecdsa';

-- Explorer: all txs authorized by a given Solana signer.
CREATE INDEX IF NOT EXISTS idx_rv_evm_tx_solana_signer
    ON rome_via.evm_tx (chain_id, solana_signer) WHERE solana_signer IS NOT NULL;

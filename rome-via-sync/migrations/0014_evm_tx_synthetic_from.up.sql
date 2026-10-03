-- GET /addresses (and address-detail) resolve each address's solana_signer via a
-- LATERAL probe on lower(COALESCE(from_addr, from_address)) with origination <>
-- 'ecdsa' AND solana_signer IS NOT NULL. Without an index covering that
-- expression every probe early-aborts into a full evm_tx scan (~50 probes ≈ 60s,
-- killed by the proxy timeout). This partial expression index matches the
-- probe's predicates verbatim, making each probe an index lookup.
CREATE INDEX IF NOT EXISTS ix_evm_tx_synthetic_from
    ON rome_via.evm_tx (chain_id, lower(COALESCE(from_addr, from_address)))
    WHERE origination <> 'ecdsa' AND solana_signer IS NOT NULL;

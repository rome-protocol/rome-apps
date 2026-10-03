-- Holder-first lookup for GET /addresses/:address/tokens (token holdings of
-- an address). The PK (chain_id, token_address, holder_address) and the
-- token-first partial index cannot serve a holder-keyed scan; without this,
-- every holdings request is a full scan of token_holders.
CREATE INDEX IF NOT EXISTS ix_token_holders_holder
    ON rome_via.token_holders (chain_id, holder_address)
    WHERE balance > 0;

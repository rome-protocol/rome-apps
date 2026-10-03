-- On-chain-derived gated-RWA flag for tokens. A permissioned (Arc-style) token
-- routes transfers through a WhitelistRestrictions module; the metadata worker
-- reads getRestrictionModule(TRANSFER_RESTRICTION) + the module's transfersAllowed()
-- and writes `gated` + `restriction_module` here. NULL = not yet probed (the
-- metadata worker's poll predicate now includes `gated IS NULL`, so existing rows
-- are re-probed exactly once, then the definitive verdict drops them out).
ALTER TABLE rome_via.token_metadata
    ADD COLUMN IF NOT EXISTS gated              BOOLEAN,
    ADD COLUMN IF NOT EXISTS restriction_module VARCHAR(42);

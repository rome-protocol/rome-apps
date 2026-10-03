-- Human-readable name for each registered hook, resolved lazily by the
-- hook_metadata worker via eth_call name() on the hook's EVM address.
-- NULL means "not yet fetched" or "contract has no name()".
ALTER TABLE rome_via.hooks_registry
    ADD COLUMN IF NOT EXISTS name TEXT;

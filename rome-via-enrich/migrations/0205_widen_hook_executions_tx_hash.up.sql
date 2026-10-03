-- hook_executions.tx_hash was sized for an Ethereum tx hash (VARCHAR(66)).
-- Meta-hook Solana invocations use synthetic keys `sol:<base58 sig>` which run
-- to ~92 characters. Widen to TEXT so both kinds coexist.
ALTER TABLE rome_via.hook_executions
    ALTER COLUMN tx_hash TYPE TEXT;

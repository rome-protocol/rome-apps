-- Provenance tier for contract_labels.display_label, so a higher-confidence
-- writer's label can't be silently clobbered by a lower-confidence writer's
-- later pass. Ranking (see src/workers/label_provenance.rs): registry (3,
-- curated from rome-protocol/registry) > verified (2, Sourcify-verified
-- compilation name) > onchain (1, best-effort name()/symbol()/bytecode
-- heuristic — the pre-existing single tier, kept as the default/floor so
-- every existing row is correctly the lowest tier and can be overwritten by
-- either higher tier).
ALTER TABLE rome_via.contract_labels ADD COLUMN IF NOT EXISTS provenance TEXT NOT NULL DEFAULT 'onchain';

-- Last time the verified_labels worker checked this address against the
-- Sourcify instance (whether or not it turned out verified). NULL = never
-- checked. Lets the worker re-poll a not-yet-verified contract after a TTL
-- (it may get verified after first-seen) without re-checking every poll.
ALTER TABLE rome_via.contract_labels ADD COLUMN IF NOT EXISTS verified_checked_at TIMESTAMPTZ;

-- Backs the verified_labels worker's candidate poll: contracts with code that
-- aren't already curated (registry) or confirmed-verified, ordered by
-- staleness so never-checked rows (NULL) surface first.
CREATE INDEX IF NOT EXISTS ix_contract_labels_verified_candidates
    ON rome_via.contract_labels (chain_id, verified_checked_at)
    WHERE has_code = true AND provenance NOT IN ('verified', 'registry');

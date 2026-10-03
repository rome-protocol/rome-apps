DROP INDEX IF EXISTS rome_via.ix_contract_labels_verified_candidates;
ALTER TABLE rome_via.contract_labels DROP COLUMN IF EXISTS verified_checked_at;
ALTER TABLE rome_via.contract_labels DROP COLUMN IF EXISTS provenance;

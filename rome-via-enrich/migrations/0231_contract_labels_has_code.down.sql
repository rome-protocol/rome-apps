DROP INDEX IF EXISTS rome_via.ix_contract_labels_has_code;
ALTER TABLE rome_via.contract_labels DROP COLUMN IF EXISTS has_code;

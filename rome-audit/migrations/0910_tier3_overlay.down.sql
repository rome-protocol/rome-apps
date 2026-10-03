DROP TRIGGER IF EXISTS evidence_record_append_only ON audit.evidence_record;
DROP FUNCTION IF EXISTS audit.reject_evidence_record_mutation();
DROP TRIGGER IF EXISTS identity_key_append_only ON audit.identity_key;
DROP FUNCTION IF EXISTS audit.reject_identity_key_mutation();
DROP TRIGGER IF EXISTS identity_append_only ON audit.identity;
DROP FUNCTION IF EXISTS audit.reject_identity_mutation();

DROP TABLE IF EXISTS audit.address_label;
DROP TABLE IF EXISTS audit.evidence_record;
DROP TABLE IF EXISTS audit.identity_key;
DROP TABLE IF EXISTS audit.identity;

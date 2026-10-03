DROP TRIGGER IF EXISTS quarantine_append_only ON audit.quarantine;
DROP FUNCTION IF EXISTS audit.reject_quarantine_mutation();
DROP TABLE IF EXISTS audit.quarantine;

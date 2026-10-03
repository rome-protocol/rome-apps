DROP TABLE IF EXISTS audit.ingest_watermark;
DROP TRIGGER IF EXISTS chain_event_append_only ON audit.chain_event;
DROP FUNCTION IF EXISTS audit.reject_chain_event_mutation();
DROP TABLE IF EXISTS audit.chain_event;
DROP SCHEMA IF EXISTS audit;

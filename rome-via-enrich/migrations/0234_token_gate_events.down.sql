DROP INDEX IF EXISTS rome_via.idx_token_gate_events_token;
DROP TABLE IF EXISTS rome_via.token_gate_events;
DELETE FROM rome_via.enrich_cursors WHERE worker = 'gate_events';

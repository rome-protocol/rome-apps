-- Reset hook_executions + cross_chain_correlations so workers re-classify with
-- the new log-based logic.
DELETE FROM rome_via.hook_executions;
DELETE FROM rome_via.cross_chain_correlations;
DELETE FROM rome_via.enrich_cursors WHERE worker IN ('hook_executions', 'cross_chain');

-- Restore the original 0901 index and drop the browse composite.
CREATE INDEX ce_name ON audit.chain_event (chain_id, event_name, block_number);

DROP INDEX audit.ce_name_order;

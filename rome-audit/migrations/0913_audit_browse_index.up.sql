-- Browse index for the gated Rome Via "Audit" tab.
--
-- The tab does a keyset-paginated, newest-first scan over audit.chain_event,
-- optionally narrowed to one event_name. This composite covers both the
-- (chain_id, event_name) equality and the (block_number, tx_index, log_index)
-- keyset ordering in a single backward index scan, so the filtered browse
-- never falls back to a filter-then-sort over ce_total_order.
CREATE INDEX ce_name_order
    ON audit.chain_event (chain_id, event_name, block_number, tx_index, log_index);

-- ce_name (chain_id, event_name, block_number) is a strict leading prefix of
-- ce_name_order, so every plan it could serve is served at least as well by the
-- new index. Keeping both would only add write amplification + storage.
DROP INDEX audit.ce_name;

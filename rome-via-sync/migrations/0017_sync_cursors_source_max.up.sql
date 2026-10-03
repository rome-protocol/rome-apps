-- How far the SOURCE (hercules) has been written, as last observed by the sync.
--
-- `last_synced_slot` says how far this mirror has got; nothing recorded how far
-- it had to go. The explorer could therefore not tell "live" from "hours behind"
-- and had to present itself as live either way — which is what makes a backlogged
-- explorer look broken rather than honest (design finding 9d).
--
-- Nullable: unknown until the first cycle records it, and consumers must treat
-- absence as "lag unknown" and stay silent rather than claim to be caught up.
ALTER TABLE rome_via.sync_cursors
    ADD COLUMN IF NOT EXISTS source_max_slot BIGINT;

-- 0225_cross_vm_seams_oracle.up.sql
--
-- Flag oracle-keeper crossings in the seam feed, and index the default read around them.
--
-- The keeper's refresh() is authored from the Solana side (origination = solana_unsigned),
-- so it genuinely IS a sol_to_evm crossing — it is not misclassified. But it runs ~180/hr
-- (~1.58M/year) against 114 real crossings all-time on hadrian, so recording it unflagged
-- makes the feed **99.99% keeper traffic** and reproduces, inside the feed, exactly the
-- needle-in-a-haystack problem the feed was built to remove.
--
-- The rows are kept rather than dropped: they are true crossings, and discarding them
-- would bake a product judgement into the data and make "was the keeper crossing?"
-- unanswerable. Instead the default read excludes them, served by a partial index so it
-- stays an ordered scan of the dense (non-keeper) subset.
ALTER TABLE rome_via.cross_vm_seams
    ADD COLUMN IF NOT EXISTS is_oracle BOOLEAN NOT NULL DEFAULT false;

-- Default read: crossings that are not keeper traffic, newest first.
CREATE INDEX IF NOT EXISTS ix_cross_vm_seams_feed_no_oracle
    ON rome_via.cross_vm_seams (chain_id, slot_number DESC, tx_idx DESC)
    WHERE NOT is_oracle;

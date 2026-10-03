-- 0226_backfill_is_oracle.up.sql
--
-- Backfill cross_vm_seams.is_oracle for rows written before 0225 added the column.
--
-- 0225 added `is_oracle BOOLEAN NOT NULL DEFAULT false` and relied on the worker's
-- recheck sweep to correct existing rows. That was wrong: the sweep is a SLIDING window
-- over the most recent RECHECK_SLOTS, so it structurally cannot reach older history —
-- it corrected 56 of 128,479 rows on hadrian-lt and would never touch the rest. The
-- filter therefore appeared to do nothing, because ~everything stayed flagged
-- not-oracle.
--
-- A derived column added to an existing table has to be backfilled with the column, not
-- left to a forward-only worker. This sets the flag from the same source of truth the
-- worker uses: evm_tx.method_id = the oracle keeper's refresh() selector.
UPDATE rome_via.cross_vm_seams cvs
SET is_oracle = true
FROM rome_via.evm_tx et
WHERE et.chain_id = cvs.chain_id
  AND et.tx_hash  = cvs.tx_hash
  AND et.method_id = '0xf8ac93e8'
  AND NOT cvs.is_oracle;

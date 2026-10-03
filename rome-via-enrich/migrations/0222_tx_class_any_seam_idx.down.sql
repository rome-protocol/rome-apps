-- 0222_tx_class_any_seam_idx.down.sql
-- Drops the any-seam index; ?seam=any degrades to a full backwards scan.
DROP INDEX IF EXISTS rome_via.ix_tx_class_seam_any;

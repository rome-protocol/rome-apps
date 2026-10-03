-- 0225_cross_vm_seams_oracle.down.sql
DROP INDEX IF EXISTS rome_via.ix_cross_vm_seams_feed_no_oracle;
ALTER TABLE rome_via.cross_vm_seams DROP COLUMN IF EXISTS is_oracle;

-- 0224_cross_vm_seams.down.sql
-- Drops the seam feed. tx_class is NOT recreated: 0221/0222 own that, and reverting to
-- a full per-tx shadow table is a deliberate decision, not an automatic rollback.
DROP TABLE IF EXISTS rome_via.cross_vm_seams;

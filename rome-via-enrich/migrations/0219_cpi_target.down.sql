-- 0219_cpi_target.down.sql
ALTER TABLE rome_via.cross_chain_correlations
    DROP COLUMN IF EXISTS cpi_program,
    DROP COLUMN IF EXISTS cpi_program_label,
    DROP COLUMN IF EXISTS cpi_instruction;

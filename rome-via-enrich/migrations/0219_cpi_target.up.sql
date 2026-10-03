-- 0219_cpi_target.up.sql
-- CPI legibility: capture the depth >= 2 Solana program a Rome tx invoked via the
-- CpiProgram precompile (0xff..08), from the settling tx's logs in the
-- cross_chain worker. All nullable — set only for txs that invoke a
-- registry-labeled CPI target (most txs have none, so the column stays NULL).
--   cpi_program       base58 program id of the depth>=2 inner program
--   cpi_program_label registry-curated human label (e.g. "mangoV4")
--   cpi_instruction   the program's Anchor "Instruction: <Name>" log, when emitted
ALTER TABLE rome_via.cross_chain_correlations
    ADD COLUMN IF NOT EXISTS cpi_program       VARCHAR(44),
    ADD COLUMN IF NOT EXISTS cpi_program_label TEXT,
    ADD COLUMN IF NOT EXISTS cpi_instruction   TEXT;

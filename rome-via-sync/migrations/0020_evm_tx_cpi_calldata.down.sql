ALTER TABLE rome_via.evm_tx
    DROP COLUMN IF EXISTS cpi_program_calldata,
    DROP COLUMN IF EXISTS cpi_program_label_calldata,
    DROP COLUMN IF EXISTS cpi_instruction_calldata;

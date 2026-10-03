-- Mirror hercules's per-leg intra-block ordinal (rome-sdk #454) into the
-- explorer DB so an iterative EVM tx's Solana legs render in execution order
-- (slot_number, tx_idx, instr_idx). Legacy mirrored rows default to 0/0 until
-- re-synced.
ALTER TABLE rome_via.evm_tx_sol_tx
    ADD COLUMN tx_idx INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN instr_idx INTEGER NOT NULL DEFAULT 0;

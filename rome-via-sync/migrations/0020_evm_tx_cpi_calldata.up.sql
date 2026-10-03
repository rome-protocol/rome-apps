-- Depth-1 CPI target decoded straight from calldata (see rome-via-sync's
-- cpi_calldata module) — a deterministic source for CpiProgram.invoke /
-- invoke_signed targets, independent of the cross_chain worker's log-scrape
-- classification (which misses direct invokes of unmatched/filtered programs).
-- Immutable per-tx derivation, populated at sync time from the RLP decode.
ALTER TABLE rome_via.evm_tx
    ADD COLUMN cpi_program_calldata       varchar,
    ADD COLUMN cpi_program_label_calldata varchar,
    ADD COLUMN cpi_instruction_calldata   varchar;

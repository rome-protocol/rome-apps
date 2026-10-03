-- Ground-truth code presence per address, so is_contract stops guessing from
-- label presence. The contract_labels worker already calls eth_getCode first
-- (to skip name()/symbol() on EOAs); it now persists that bit here. NULL = not
-- yet probed — the worker backfills existing rows by re-resolving them.
--
-- Why it matters: a nameless contract (no name()/symbol(), e.g. an Arc
-- allowlist module) is otherwise stored identically to an EOA (raw_name = ''),
-- so is_contract could not tell them apart — measured residual on hadrian
-- 2026-07-31: ~18% of sampled low-traffic flipped addresses were real
-- contracts wrongly shown as EOA.
ALTER TABLE rome_via.contract_labels ADD COLUMN IF NOT EXISTS has_code BOOLEAN;

-- Partial index for the address_stats is_contract branch (EXISTS ... has_code)
-- and the worker's backfill scan (has_code IS NULL).
CREATE INDEX IF NOT EXISTS ix_contract_labels_has_code
    ON rome_via.contract_labels (chain_id, address)
    WHERE has_code = true;

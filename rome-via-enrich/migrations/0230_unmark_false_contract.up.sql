-- One-time correction for the is_contract false-positive bug.
--
-- The address_stats worker used to flip is_contract=true on a bare
-- EXISTS(contract_labels) — which matched the worker's "seen, not a contract"
-- NULL-label marker rows (written for EOAs examined as tx recipients). The
-- predicate is fixed in the worker (branch 3 now requires display_label IS NOT
-- NULL), but that only ever ADDS (WHERE is_contract=false), so the already
-- wrongly-marked rows need a one-time true->false backfill.
--
-- Measured on hadrian 2026-07-31: 3,357 of 3,462 marked-contract addresses had
-- no positive signal (all plain EOAs). This un-marks exactly those; a real
-- contract (token, token-transfer emitter, or resolved label) is untouched.
UPDATE rome_via.address_stats a
   SET is_contract = false
 WHERE a.is_contract = true
   AND NOT (
       EXISTS (
           SELECT 1 FROM rome_via.token_transfers tt
           WHERE tt.chain_id = a.chain_id AND tt.token_address = a.address
       )
       OR EXISTS (
           SELECT 1 FROM rome_via.token_metadata tm
           WHERE tm.chain_id = a.chain_id AND tm.address = a.address
       )
       OR EXISTS (
           SELECT 1 FROM rome_via.contract_labels cl
           WHERE cl.chain_id = a.chain_id AND cl.address = a.address
             AND cl.display_label IS NOT NULL
       )
   );

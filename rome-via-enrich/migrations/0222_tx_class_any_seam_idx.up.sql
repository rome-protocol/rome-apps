-- 0222_tx_class_any_seam_idx.up.sql
--
-- Index for `GET /txs?seam=any` — "sits on ANY cross-VM seam", which is what the
-- Cross-chain screen actually asks for.
--
-- 0221 added one partial index per named seam, but the `any` predicate
-- (`array_length(seams,1) > 0`) matches none of them, so that query degraded to a
-- backwards scan over the whole chain: measured 19.3s on hadrian-lt while the
-- single-seam variants served from their partial indexes in ~250-390ms.
--
-- `seams <> '{}'` is the indexable spelling of the same condition and is what the API
-- now emits.
CREATE INDEX IF NOT EXISTS ix_tx_class_seam_any
    ON rome_via.tx_class (chain_id, slot_number DESC, tx_idx DESC)
    WHERE seams <> '{}';

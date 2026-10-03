-- 0227_gas_recipient_idx.up.sql
--
-- Index the third way an address appears on a transaction: as the fee recipient.
--
-- GET /addresses/:addr/txs matches an address three ways — sender, recipient, and fee
-- recipient. The first two are indexed columns on evm_tx; the third lives inside a JSONB
-- blob on evm_tx_result, so it could not be indexed at all. Because all three were ORed
-- together in one predicate, the unindexable branch dragged the whole query down: the
-- planner had to hash-join the ENTIRE evm_tx_result table before it could test the
-- address, reading ~3.7M rows and spilling 365k blocks to temp to return 15 rows.
-- Measured on hadrian-lt: 23-26s, with no index scan anywhere in the plan.
--
-- This expression index makes the fee-recipient branch indexable, so the query can be
-- split into three index scans instead of one full scan (see the UNION rewrite in
-- rome-via-api/src/api/addresses.rs). The expression must match the query's spelling
-- exactly or the planner will not use it.
CREATE INDEX IF NOT EXISTS ix_evm_tx_result_gas_recipient
    ON rome_via.evm_tx_result (
        chain_id,
        (lower((tx_result -> 'gas_report'::text) ->> 'gas_recipient'::text))
    )
    WHERE tx_result -> 'gas_report' ->> 'gas_recipient' IS NOT NULL;

-- Covering index for the ordering join.
--
-- With the three branches indexed, the remaining cost was decorating the matched set:
-- the feed must know each tx's block to sort by it, and on a 30k-tx address that was
-- 30,100 random heap lookups (0.497ms each, ~15s). INCLUDE-ing the keyset columns makes
-- that an Index Only Scan. Measured on the same address: 21.2s -> 4.4s.
CREATE INDEX IF NOT EXISTS ix_eth_block_txs_hash_covering
    ON rome_via.eth_block_txs (chain_id, tx_hash)
    INCLUDE (slot_number, slot_block_idx, tx_idx);

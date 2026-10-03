-- 0218_throughput_record.up.sql
--
-- Throughput record tables: persist per-chain top-10 sustained-TPS windows and
-- top-10 busiest blocks so all-time peak reads are O(1).
--
-- rome_via.throughput_peak_windows: top-10 deduped W=10 sustained-TPS bursts.
-- Indexed by (chain_id, rank). W (window size) = 10 consecutive blocks
-- (bounded by TPS thresholds to exclude noise). Updated by the maintainer
-- worker on every enrich pass.
--
-- rome_via.throughput_busiest_blocks: top-10 single blocks per chain,
-- ranked by app_txs DESC. Indexed by (chain_id, rank). Also maintained by
-- the maintainer worker.
--
-- No foreign-key constraints: rome_via schema design.
CREATE TABLE IF NOT EXISTS rome_via.throughput_peak_windows (
    chain_id        BIGINT  NOT NULL,
    rank            INT     NOT NULL,              -- 1..10, ranked by app_tps DESC
    from_block      BIGINT  NOT NULL,
    to_block        BIGINT  NOT NULL,
    from_slot       BIGINT  NOT NULL,
    to_slot         BIGINT  NOT NULL,
    elapsed_seconds BIGINT  NOT NULL,
    total_txs       BIGINT  NOT NULL,
    app_txs         BIGINT  NOT NULL,
    total_tps       DOUBLE PRECISION NOT NULL,
    app_tps         DOUBLE PRECISION NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, rank)
);

CREATE TABLE IF NOT EXISTS rome_via.throughput_busiest_blocks (
    chain_id        BIGINT  NOT NULL,
    rank            INT     NOT NULL,              -- 1..10, ranked by app_txs DESC
    block_number    BIGINT  NOT NULL,
    slot_number     BIGINT  NOT NULL,
    total_txs       BIGINT  NOT NULL,
    app_txs         BIGINT  NOT NULL,
    block_timestamp BIGINT  NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, rank)
);

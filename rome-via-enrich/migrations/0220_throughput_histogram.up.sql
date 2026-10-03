-- 0220_throughput_histogram.up.sql
--
-- rome_via.throughput_histogram: persisted block-size distribution (blocks bucketed
-- by tx count) per chain, so /throughput/histogram?range=all is a 6-row table read
-- instead of a per-block CTE over the entire chain. The live scan measured 18.7s
-- under sustained load on a busy chain — no index removes it, because the query
-- aggregates every block before bucketing.
--
-- Maintained by the throughput_record enrich worker in the SAME transaction as the
-- peak-window / busiest-block tables: full recompute on seed + hourly backstop,
-- cumulative delta-merge on each incremental pass (overlap blocks are excluded so
-- they are never double-counted). Six rows per chain, one per bucket.
--
-- bucket_idx is the ordinal (0..5) and the sort key; bucket_label is the display
-- string and must match the labels rome-via-api's live-scan fallback emits, so both
-- the fast path and the fallback render identically.
--
-- No foreign-key constraints: rome_via schema design.
CREATE TABLE IF NOT EXISTS rome_via.throughput_histogram (
    chain_id     BIGINT      NOT NULL,
    bucket_idx   INT         NOT NULL,           -- 0..5, render order
    bucket_label TEXT        NOT NULL,           -- "0" | "1" | "2-5" | "6-20" | "21-50" | "50+"
    blocks       BIGINT      NOT NULL,           -- block count in this bucket, all-time
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, bucket_idx)
);

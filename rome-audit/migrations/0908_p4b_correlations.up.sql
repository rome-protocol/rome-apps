-- P4b-i (rome-arc AUDIT-TRAIL-CAPTURE-SPEC.md §4 correlations + IMPL-PLAN
-- §5's yield-blacklist axis): five more Tier-2 derived tables, same
-- doctrine as migration 0003 — pure functions of `audit.chain_event`,
-- REBUILDABLE (`tier2::rebuild::rebuild_tier2`), chain-scoped DELETE, never
-- a global TRUNCATE (P4a M3).

-- ---- yield_blacklist_interval — ← YieldBlacklistUpdated, same open/close
-- shape as allowlist_interval (migration 0003).
CREATE TABLE audit.yield_blacklist_interval (
    chain_id        BIGINT NOT NULL,
    asset_id        TEXT   NOT NULL,
    address         BYTEA  NOT NULL,
    from_block      BIGINT NOT NULL,
    to_block        BIGINT,
    opened_by_event BIGINT NOT NULL REFERENCES audit.chain_event (event_id),
    closed_by_event BIGINT REFERENCES audit.chain_event (event_id),
    PRIMARY KEY (chain_id, asset_id, address, from_block)
);
CREATE INDEX yield_blacklist_interval_open ON audit.yield_blacklist_interval (chain_id, asset_id, address)
    WHERE to_block IS NULL;

-- ---- transfer_screening — ← paired (TransferScreened, Transfer), nearest
-- preceding unmatched within the same tx (capture §4 rule 2). Positional
-- key (block_number, tx_index, screening_log_index): a screening event can
-- pair with at most one transfer.
CREATE TABLE audit.transfer_screening (
    chain_id             BIGINT NOT NULL,
    asset_id             TEXT   NOT NULL,
    block_number         BIGINT NOT NULL,
    tx_index             INT    NOT NULL,
    screening_log_index  INT    NOT NULL,
    transfer_log_index   INT    NOT NULL,
    module_address       BYTEA  NOT NULL,
    screening_event      BIGINT NOT NULL REFERENCES audit.chain_event (event_id),
    transfer_event       BIGINT NOT NULL REFERENCES audit.chain_event (event_id),
    PRIMARY KEY (chain_id, asset_id, block_number, tx_index, screening_log_index)
);

-- ---- screening_gap — an unpaired Transfer whose position sits inside the
-- asset's router's active sanctions epoch (never fabricated without a
-- configured router — see tier2::screening module doc).
CREATE TABLE audit.screening_gap (
    chain_id        BIGINT NOT NULL,
    asset_id        TEXT   NOT NULL,
    block_number    BIGINT NOT NULL,
    tx_index        INT    NOT NULL,
    log_index       INT    NOT NULL,
    transfer_event  BIGINT NOT NULL REFERENCES audit.chain_event (event_id),
    epoch_module    BYTEA  NOT NULL,
    PRIMARY KEY (chain_id, asset_id, block_number, tx_index, log_index)
);

-- ---- yield_run — a distribution RUN (capture §4 rule 1): credits since
-- the previous completion, closed by the next YieldDistributed.
-- `run_seq` is a DENSE 0..N index in TOTAL ORDER, deliberately NOT an
-- IDENTITY/serial surrogate (deviates from the plan's literal DDL sketch) —
-- an insertion-order id would break cross-DB rebuild determinism (C1); it
-- is computed in `tier2::yield_run::build_yield_runs` from the naturally-
-- ordered merged event stream and inserted as a plain BIGINT.
CREATE TABLE audit.yield_run (
    chain_id            BIGINT  NOT NULL,
    asset_id            TEXT    NOT NULL,
    yield_token         BYTEA   NOT NULL,
    run_seq             BIGINT  NOT NULL,
    total_amount        NUMERIC(78,0),
    credited_total       NUMERIC(78,0) NOT NULL,
    withheld_remainder  NUMERIC(78,0),
    over_credit         BOOLEAN NOT NULL DEFAULT FALSE,
    closed_by_event     BIGINT REFERENCES audit.chain_event (event_id),
    PRIMARY KEY (chain_id, asset_id, yield_token, run_seq)
);

-- ---- yield_credit — one credit (yield-token Transfer(from=ArcToken))
-- belonging to a yield_run. No FK to yield_run itself (run_seq is assigned
-- by the same builder pass in the same transaction, not a separately
-- persisted surrogate the DB could enforce ahead of the run row landing).
CREATE TABLE audit.yield_credit (
    chain_id      BIGINT  NOT NULL,
    asset_id      TEXT    NOT NULL,
    yield_token   BYTEA   NOT NULL,
    run_seq       BIGINT  NOT NULL,
    block_number  BIGINT  NOT NULL,
    tx_index      INT     NOT NULL,
    log_index     INT     NOT NULL,
    holder        BYTEA   NOT NULL,
    share         NUMERIC(78,0) NOT NULL,
    credit_event  BIGINT  NOT NULL REFERENCES audit.chain_event (event_id),
    PRIMARY KEY (chain_id, asset_id, yield_token, run_seq, block_number, tx_index, log_index)
);

-- Gate-change history + freshness for gated (Arc-style) tokens.
--
-- The metadata worker reads a token's gate state ONCE (poll predicate
-- `gated IS NULL`), so a gate wired or toggled AFTER that single read is never
-- reflected — the common case, since issuance wires/toggles the module a beat
-- after the token first appears (e.g. MBS Prime: module wired one block after
-- creation, so its lone probe saw no module and cached gated=false forever).
--
-- The `gate_events` worker tails evm_tx_result for two events and records them
-- here AND invalidates the token's cached verdict (`gated = NULL`) so the
-- metadata worker re-derives it:
--   * SpecificRestrictionModuleSet(bytes32 typeId, address module) — emitted by
--     the ArcToken; topic1 = keccak256("TRANSFER_RESTRICTION"). Links token→module.
--   * TransfersRestrictionToggled(bool) — emitted by the WhitelistRestrictions
--     module when the gate is opened/closed.
CREATE TABLE IF NOT EXISTS rome_via.token_gate_events (
    chain_id          BIGINT      NOT NULL,
    token_address     VARCHAR(42),            -- resolved ArcToken; NULL if module not yet linked
    module_address    VARCHAR(42) NOT NULL,   -- the WhitelistRestrictions module
    event_type        VARCHAR(20) NOT NULL,   -- 'module_wired' | 'transfers_toggled'
    transfers_allowed BOOLEAN,                 -- toggle: the new value; NULL for module_wired
    slot_number       BIGINT      NOT NULL,
    tx_hash           VARCHAR(66) NOT NULL,
    log_index         INTEGER     NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, tx_hash, log_index)
);

-- Per-token history feed, newest first (backs GET /tokens/:addr/gate-events).
CREATE INDEX IF NOT EXISTS idx_token_gate_events_token
    ON rome_via.token_gate_events (chain_id, token_address, slot_number DESC, log_index DESC);

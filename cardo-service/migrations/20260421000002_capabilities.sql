CREATE TABLE app_capabilities (
    app_id          TEXT NOT NULL REFERENCES apps(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    kind            TEXT NOT NULL CHECK (kind IN ('query','quote','execute')),
    description     TEXT NOT NULL,
    inputs          JSONB NOT NULL,
    outputs         JSONB NOT NULL,
    abi             TEXT NOT NULL,
    abi_hash        TEXT NOT NULL,
    cu_estimate     BIGINT NOT NULL,
    example_call    JSONB,
    PRIMARY KEY (app_id, name)
);

CREATE INDEX app_capabilities_kind_idx ON app_capabilities(kind);

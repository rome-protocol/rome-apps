CREATE TABLE apps (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    description     TEXT NOT NULL,
    icon_url        TEXT,
    tier            TEXT NOT NULL CHECK (tier IN ('featured','long-tail')),
    uniqueness      TEXT NOT NULL CHECK (uniqueness IN ('only-on-rome','better-on-rome','parity')),
    why_rome        TEXT NOT NULL,
    categories      TEXT[] NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('live','coming-soon','deprecated')),
    owner_team      TEXT NOT NULL,
    owner_contact   TEXT NOT NULL,
    rome_chain_id   BIGINT NOT NULL,
    contract_addr   TEXT NOT NULL,
    solana_programs TEXT[] NOT NULL DEFAULT '{}',
    url_app         TEXT NOT NULL,
    url_docs        TEXT NOT NULL,
    url_source      TEXT NOT NULL,
    mcp_tools       TEXT[] NOT NULL,
    manifest_url    TEXT NOT NULL,
    rest_base       TEXT NOT NULL,
    tags            TEXT[] NOT NULL DEFAULT '{}',
    signature_pub   TEXT NOT NULL,
    signature_val   TEXT NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    manifest_raw    JSONB NOT NULL
);

CREATE INDEX apps_tier_idx ON apps(tier);
CREATE INDEX apps_status_idx ON apps(status);
CREATE INDEX apps_categories_gin ON apps USING GIN(categories);

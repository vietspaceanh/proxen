CREATE TABLE IF NOT EXISTS request_tags (
    tag  INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS request_contexts (
    id           INTEGER PRIMARY KEY,
    model_tag    INTEGER NOT NULL REFERENCES request_tags(tag),
    upstream_tag INTEGER REFERENCES request_tags(tag),
    key_tag      INTEGER REFERENCES request_tags(tag),
    UNIQUE(model_tag, upstream_tag, key_tag)
);

CREATE TABLE IF NOT EXISTS requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    ctx_id INTEGER NOT NULL REFERENCES request_contexts(id),
    ttft_ms INTEGER DEFAULT 0,
    tps_centi INTEGER DEFAULT 0,
    input_tokens INTEGER DEFAULT 0,
    cached_input_tokens INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0,
    status INTEGER,
    duration_ms INTEGER DEFAULT 0,
    flags INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_requests_ts_ctx ON requests(timestamp, ctx_id);

CREATE TABLE IF NOT EXISTS models_cache (
    upstream TEXT NOT NULL,
    id TEXT NOT NULL,
    object TEXT,
    created INTEGER,
    owned_by TEXT,
    fetched_meta TEXT,
    fetched_at REAL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (upstream, id)
);

CREATE TABLE IF NOT EXISTS upstreams (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    base_url TEXT NOT NULL,
    api_key TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL DEFAULT 1,
    max_inflight INTEGER,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL DEFAULT '',
    active INTEGER NOT NULL DEFAULT 1,
    created_at REAL NOT NULL,
    last_used_at REAL
);

CREATE TABLE IF NOT EXISTS models (
    id TEXT PRIMARY KEY,
    enabled INTEGER NOT NULL DEFAULT 1,
    input_per_1m REAL NOT NULL DEFAULT 0,
    cached_input_per_1m REAL NOT NULL DEFAULT 0,
    output_per_1m REAL NOT NULL DEFAULT 0,
    max_input_tokens INTEGER,
    max_output_tokens INTEGER,
    fallback_strategy TEXT NOT NULL DEFAULT 'manual',
    extra_body TEXT,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS model_routes (
    model_id TEXT NOT NULL REFERENCES models(id) ON DELETE CASCADE ON UPDATE CASCADE,
    upstream_name TEXT NOT NULL,
    upstream_model_id TEXT NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (model_id, upstream_name)
);

CREATE INDEX IF NOT EXISTS idx_routes_sort ON model_routes(model_id, sort_order);

CREATE TABLE IF NOT EXISTS gate_limits (
    key TEXT PRIMARY KEY,
    max_inflight INTEGER NOT NULL,
    max_waiting INTEGER NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS key_limits (
    key_id INTEGER PRIMARY KEY REFERENCES keys(id) ON DELETE CASCADE,
    max_inflight INTEGER,
    max_requests INTEGER,
    max_requests_window_s REAL,
    max_tokens INTEGER,
    max_tokens_window_s REAL,
    updated_at REAL NOT NULL
);

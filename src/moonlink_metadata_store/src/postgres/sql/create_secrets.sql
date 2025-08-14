-- SQL statements to store moonlink secret related fields.
BEGIN;

CREATE TABLE secrets (
    id SERIAL PRIMARY KEY,          -- Unique row identifier.
    "schema" TEXT,                  -- Column store schema name.
    "table" TEXT,                   -- Column store table name.
    secret_type TEXT,               -- One of (S3, GCS)
    key_id TEXT,
    secret TEXT,
    project TEXT,          -- (optional)
    endpoint TEXT,         -- (optional)
    region TEXT            -- (optional)
);

-- Index to enable query on ("schema", "table").
CREATE INDEX idx_secrets_uid_oid ON secrets ("schema", "table");

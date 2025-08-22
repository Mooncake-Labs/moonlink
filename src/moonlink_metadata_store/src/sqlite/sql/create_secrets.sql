-- SQL statements to store moonlink secret related fields.
CREATE TABLE secrets (
    id SERIAL PRIMARY KEY,      -- Unique row identifier
    "database" TEXT,            -- column store database name
    "table" TEXT,               -- column store table name
    purpose TEXT,                -- Purpose of secret: 'iceberg' or 'wal'.
    secret_type TEXT,           -- One of (S3, GCS)
    key_id TEXT,
    secret TEXT,
    project TEXT,          -- (optional)  
    endpoint TEXT,         -- (optional)
    region TEXT            -- (optional)
);

-- Index to enable query on ("database", "table").
CREATE INDEX idx_secrets_uid_oid ON secrets ("database", "table");
-- Ensure at most one secret per purpose per table
CREATE UNIQUE INDEX IF NOT EXISTS idx_secrets_db_table_purpose ON secrets ("database", "table", purpose);

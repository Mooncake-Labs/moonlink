-- SQL statements to store moonlink secret related fields.
BEGIN;

CREATE TABLE secrets (
    id SERIAL PRIMARY KEY,   -- Unique row identifier.
    "database" TEXT,         -- Column store database name.
    "table" TEXT,            -- Column store table name.
    usage_type TEXT CHECK (usage_type IN ('iceberg', 'wal')),         -- Purpose of secret: 'iceberg' or 'wal'.
    storage_provider TEXT CHECK (storage_provider IN ('s3', 'gcs')),  -- One of ('s3', 'gcs')
    key_id TEXT,
    secret TEXT,
    project TEXT,          -- (optional)
    endpoint TEXT,         -- (optional)
    region TEXT            -- (optional)
);

-- Ensure at most one secret per usage type per table
CREATE UNIQUE INDEX IF NOT EXISTS idx_secrets_db_table_usage_type ON secrets ("database", "table", usage_type);

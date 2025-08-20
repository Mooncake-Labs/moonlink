-- SQL statement(s) to store moonlink managed column store tables.
CREATE TABLE tables (
    "database" TEXT,         -- column store database name
    "table" TEXT,            -- column store table name
    src_table_name TEXT NOT NULL,   -- source table name
    src_table_uri TEXT,             -- source URI
    config JSON,                    -- mooncake and persistence configurations
    PRIMARY KEY ("database", "table")
);

-- SQL statement(s) to store moonlink managed column store tables.
CREATE TABLE tables (
    mooncake_database TEXT,         -- column store database name
    mooncake_table TEXT,            -- column store table name
    src_table_name TEXT NOT NULL,   -- source table name
    src_table_uri TEXT,             -- source table URI
    config TEXT,                    -- mooncake and persistence configurations
    PRIMARY KEY (mooncake_database, mooncake_table)
);

use async_trait::async_trait;
use sqlx::Row;

use crate::base_metadata_store::TableMetadataEntry;
use crate::base_metadata_store::{MetadataStoreTrait, MOONLINK_METADATA_TABLE, MOONLINK_SCHEMA};
use crate::config_utils;
use crate::error::Error;
use crate::error::Result;
use crate::sqlite::sqlite_conn_wrapper::SqliteConnWrapper;
use crate::sqlite::utils;
use moonlink::MoonlinkTableConfig;
use moonlink_error::{ErrorStatus, ErrorStruct};

/// Default sqlite database filename.
const METADATA_DATABASE_FILENAME: &str = "moonlink_metadata_store.sqlite";
/// SQL statements for moonlink metadata table database.
const CREATE_TABLE_SCHEMA_SQL: &str = include_str!("sql/create_tables.sql");

pub struct SqliteMetadataStore {
    /// Database uri.
    database_uri: String,
}

#[async_trait]
impl MetadataStoreTrait for SqliteMetadataStore {
    async fn metadata_table_exists(&self) -> Result<bool> {
        let sqlite_conn = SqliteConnWrapper::new(&self.database_uri).await?;
        utils::table_exists(&sqlite_conn.pool, MOONLINK_SCHEMA, MOONLINK_METADATA_TABLE).await
    }

    async fn get_all_table_metadata_entries(&self) -> Result<Vec<TableMetadataEntry>> {
        let sqlite_conn = SqliteConnWrapper::new(&self.database_uri).await?;
        let rows = sqlx::query(
            r#"
            SELECT 
                t."database",
                t."table",
                t.src_table_name,
                t.src_table_uri,
                t.config
            FROM tables t
            "#,
        )
        .fetch_all(&sqlite_conn.pool)
        .await?;

        let mut metadata_entries = Vec::with_capacity(rows.len());
        for row in rows {
            let database: String = row.get("database");
            let table: String = row.get("table");
            let src_table_name: String = row.get("src_table_name");
            let src_table_uri: String = row.get("src_table_uri");
            let serialized_config: String = row.get("config");
            let json_value: serde_json::Value = serde_json::from_str(&serialized_config)?;

            let moonlink_table_config =
                config_utils::deserialize_moonlink_table_config(json_value)?;
            metadata_entries.push(TableMetadataEntry {
                database,
                table,
                src_table_name,
                src_table_uri,
                moonlink_table_config,
            });
        }

        Ok(metadata_entries)
    }

    async fn store_table_metadata(
        &self,
        database: &str,
        table: &str,
        src_table_name: &str,
        src_table_uri: &str,
        moonlink_table_config: MoonlinkTableConfig,
    ) -> Result<()> {
        let serialized_config = config_utils::parse_moonlink_table_config(moonlink_table_config)?;
        let serialized_config = serde_json::to_string(&serialized_config)?;

        // Create metadata tables if it doesn't exist.
        let sqlite_conn = SqliteConnWrapper::new(&self.database_uri).await?;
        utils::create_table_if_non_existent(
            &sqlite_conn.pool,
            MOONLINK_SCHEMA,
            MOONLINK_METADATA_TABLE,
            CREATE_TABLE_SCHEMA_SQL,
        )
        .await?;

        // Start a transaction.
        let mut tx = sqlite_conn.pool.begin().await?;
        // Insert into tables.
        let rows_affected = sqlx::query(
            r#"
            INSERT INTO tables ("database", "table", src_table_name, src_table_uri, config)
            VALUES (?, ?, ?, ?, ?);
            "#,
        )
        .bind(database)
        .bind(table)
        .bind(src_table_name)
        .bind(src_table_uri)
        .bind(serialized_config)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if rows_affected != 1 {
            return Err(Error::SqliteRowCountError(ErrorStruct::new(
                format!("expected 1 row affected, but got {rows_affected}"),
                ErrorStatus::Permanent,
            )));
        }

        tx.commit().await?;

        Ok(())
    }

    async fn delete_table_metadata(&self, database: &str, table: &str) -> Result<()> {
        let sqlite_conn = SqliteConnWrapper::new(&self.database_uri).await?;
        let mut tx = sqlite_conn.pool.begin().await?;

        // Delete from metadata table.
        let rows_affected =
            sqlx::query(r#"DELETE FROM tables  WHERE "database" = ? AND "table" = ?"#)
                .bind(database)
                .bind(table)
                .execute(&mut *tx)
                .await?
                .rows_affected();
        if rows_affected != 1 {
            return Err(Error::SqliteRowCountError(ErrorStruct::new(
                format!("expected 1 row affected, but got {rows_affected}"),
                ErrorStatus::Permanent,
            )));
        }

        tx.commit().await?;

        Ok(())
    }
}

impl SqliteMetadataStore {
    /// Create the database file if it doesn't exist.
    async fn create_database_file_if_non_existent(location: &str) -> Result<()> {
        let path = std::path::Path::new(&location);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(&parent).await.map_err(|e| {
                std::io::Error::new(e.kind(), format!("Failed to create directory {parent:?}"))
            })?;
        }

        tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&location)
            .await
            .map_err(|e| {
                std::io::Error::new(e.kind(), format!("Failed to open file {location:?}"))
            })?;
        Ok(())
    }

    pub async fn new(location: String) -> Result<Self> {
        // Get database filepath and uri.
        let (database_filepath, database_uri) = utils::get_database_uri_and_filepath(&location);

        // [`sqlx`] requires database file to exist before access.
        Self::create_database_file_if_non_existent(&database_filepath).await?;

        Ok(Self { database_uri })
    }

    pub async fn new_with_directory(directory: &str) -> Result<Self> {
        let path = std::path::Path::new(directory);
        let location = path
            .join(METADATA_DATABASE_FILENAME)
            .as_path()
            .to_str()
            .unwrap()
            .to_string();
        Self::new(location).await
    }
}

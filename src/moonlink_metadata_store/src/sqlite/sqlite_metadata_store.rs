use async_trait::async_trait;
use sqlx::Row;

use crate::base_metadata_store::TableMetadataEntry;
use crate::base_metadata_store::{
    MetadataStoreTrait, MOONLINK_METADATA_TABLE, MOONLINK_SCHEMA, MOONLINK_SECRET_TABLE,
};
use crate::config_utils;
use crate::error::Error;
use crate::error::Result;
use crate::sqlite::sqlite_conn_wrapper::SqliteConnWrapper;
use crate::sqlite::utils;
use moonlink::{MoonlinkTableConfig, MoonlinkTableSecret};

/// Default sqlite database filename.
const METADATA_DATABASE_FILENAME: &str = "moonlink_metadata_store.sqlite";
/// SQL statements for moonlink metadata table schema.
const CREATE_TABLE_SCHEMA_SQL: &str = include_str!("sql/create_tables.sql");
/// SQL statements for moonlink secret table schema.
const CREATE_SECRET_SCHEMA_SQL: &str = include_str!("sql/create_secrets.sql");

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
                t."schema",
                t."table",
                t.src_table_name,
                t.src_table_uri,
                t.config,
                s.secret_type,
                s.key_id,
                s.secret,
                s.endpoint,
                s.region,
                s.project
            FROM tables t
            LEFT JOIN secrets s
                ON t."schema" = s."schema"
                AND t."table" = s."table"
            "#,
        )
        .fetch_all(&sqlite_conn.pool)
        .await?;

        let mut metadata_entries = Vec::with_capacity(rows.len());
        for row in rows {
            let schema: String = row.get("schema");
            let table: String = row.get("table");
            let src_table_name: String = row.get("src_table_name");
            let src_table_uri: String = row.get("src_table_uri");
            let serialized_config: String = row.get("config");
            let json_value: serde_json::Value = serde_json::from_str(&serialized_config)?;

            let secret_type: Option<String> = row.get("secret_type");
            let secret_entry: Option<MoonlinkTableSecret> =
                secret_type.map(|secret_type| MoonlinkTableSecret {
                    secret_type: MoonlinkTableSecret::convert_secret_type(&secret_type),
                    key_id: row.get("key_id"),
                    secret: row.get("secret"),
                    endpoint: row.get("endpoint"),
                    region: row.get("region"),
                    project: row.get("project"),
                });

            let moonlink_table_config =
                config_utils::deserialize_moonlink_table_config(json_value, secret_entry)?;

            metadata_entries.push(TableMetadataEntry {
                schema,
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
        schema: &str,
        table: &str,
        src_table_name: &str,
        src_table_uri: &str,
        moonlink_table_config: MoonlinkTableConfig,
    ) -> Result<()> {
        let (serialized_config, moonlink_table_secret) =
            config_utils::parse_moonlink_table_config(moonlink_table_config)?;
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

        // Create secrets table if it doesn't exist.
        utils::create_table_if_non_existent(
            &sqlite_conn.pool,
            MOONLINK_SCHEMA,
            MOONLINK_SECRET_TABLE,
            CREATE_SECRET_SCHEMA_SQL,
        )
        .await?;

        // Start a transaction.
        let mut tx = sqlite_conn.pool.begin().await?;
        // Insert into tables.
        let rows_affected = sqlx::query(
            r#"
            INSERT INTO tables ("schema", "table", src_table_name, src_table_uri, config)
            VALUES (?, ?, ?, ?, ?);
            "#,
        )
        .bind(schema)
        .bind(table)
        .bind(src_table_name)
        .bind(src_table_uri)
        .bind(serialized_config)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if rows_affected != 1 {
            return Err(Error::SqliteRowCountError(1, rows_affected as u32));
        }

        // Insert into mooncake_secrets if present
        if let Some(secret) = moonlink_table_secret {
            let rows_affected = sqlx::query(
                r#"
                INSERT INTO secrets ("schema", "table", secret_type, key_id, secret, endpoint, region, project)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                "#,
            )
            .bind(schema)
            .bind(table)
            .bind(secret.get_secret_type())
            .bind(secret.key_id)
            .bind(secret.secret)
            .bind(secret.endpoint)
            .bind(secret.region)
            .bind(secret.project)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if rows_affected != 1 {
                return Err(Error::SqliteRowCountError(1, rows_affected as u32));
            }
        }

        tx.commit().await?;

        Ok(())
    }

    async fn delete_table_metadata(&self, schema: &str, table: &str) -> Result<()> {
        let sqlite_conn = SqliteConnWrapper::new(&self.database_uri).await?;
        let mut tx = sqlite_conn.pool.begin().await?;

        // Delete from metadata table.
        let rows_affected =
            sqlx::query(r#"DELETE FROM tables  WHERE "schema" = ? AND "table" = ?"#)
                .bind(schema)
                .bind(table)
                .execute(&mut *tx)
                .await?
                .rows_affected();
        if rows_affected != 1 {
            return Err(Error::SqliteRowCountError(1, rows_affected as u32));
        }

        // Delete from secret table.
        sqlx::query(r#"DELETE FROM secrets WHERE "schema" = ? AND "table" = ?"#)
            .bind(schema)
            .bind(table)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

        Ok(())
    }
}

impl SqliteMetadataStore {
    /// Create the database file if it doesn't exist.
    async fn create_database_file_if_non_existent(location: &str) -> Result<()> {
        let path = std::path::Path::new(&location);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(location)
            .await?;
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

use crate::base_metadata_store::MetadataStoreTrait;
use crate::base_metadata_store::TableMetadataEntry;
use crate::base_metadata_store::MOONLINK_METADATA_TABLE;
use crate::config_utils;
use crate::error::{Error, Result};
use crate::postgres::pg_client_wrapper::PgClientWrapper;
use crate::postgres::utils;
use moonlink::MoonlinkTableConfig;
use moonlink_error::{ErrorStatus, ErrorStruct};

use async_trait::async_trait;
use postgres_types::Json as PgJson;

/// SQL statements for moonlink metadata table database.
const CREATE_TABLE_SCHEMA_SQL: &str = include_str!("sql/create_tables.sql");

pub struct PgMetadataStore {
    /// Database connection string.
    uri: String,
}

#[async_trait]
impl MetadataStoreTrait for PgMetadataStore {
    async fn metadata_table_exists(&self) -> Result<bool> {
        let pg_client = PgClientWrapper::new(&self.uri).await?;
        utils::table_exists(&pg_client.postgres_client, MOONLINK_METADATA_TABLE).await
    }

    async fn get_all_table_metadata_entries(&self) -> Result<Vec<TableMetadataEntry>> {
        let pg_client = PgClientWrapper::new(&self.uri).await?;
        let rows = pg_client
            .postgres_client
            .query(
                r#"
                SELECT 
                    t."database",
                    t."table",
                    t.src_table_name,
                    t.src_table_uri,
                    t.config
                FROM tables t
                "#,
                &[],
            )
            .await?;

        let mut metadata_entries = Vec::with_capacity(rows.len());
        for row in rows {
            let database: String = row.get("database");
            let table: String = row.get("table");
            let src_table_name: String = row.get("src_table_name");
            let src_table_uri: String = row.get("src_table_uri");
            let serialized_config: serde_json::Value = row.get("config");
            let moonlink_table_config =
                config_utils::deserialize_moonlink_table_config(serialized_config)?;

            let metadata_entry = TableMetadataEntry {
                database,
                table,
                src_table_name,
                src_table_uri,
                moonlink_table_config,
            };
            metadata_entries.push(metadata_entry);
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
        let pg_client = PgClientWrapper::new(&self.uri).await?;
        let serialized_config = config_utils::parse_moonlink_table_config(moonlink_table_config)?;

        // Create metadata table if not exist.
        utils::create_table_if_non_existent(
            &pg_client.postgres_client,
            MOONLINK_METADATA_TABLE,
            CREATE_TABLE_SCHEMA_SQL,
        )
        .await?;

        // Start a transaction to insert rows into metadata table and secret table.
        pg_client.postgres_client.execute("BEGIN", &[]).await?;

        // Persist table metadata.
        // TODO(hjiang): Fill in other fields as well.
        let rows_affected = pg_client
            .postgres_client
            .execute(
                r#"INSERT INTO tables ("database", "table", src_table_name, src_table_uri, config)
                VALUES ($1, $2, $3, $4, $5)"#,
                &[
                    &database,
                    &table,
                    &src_table_name,
                    &src_table_uri,
                    &PgJson(&serialized_config),
                ],
            )
            .await?;
        if rows_affected != 1 {
            return Err(Error::PostgresRowCountError(ErrorStruct::new(
                format!("expected 1 row affected, but got {rows_affected}"),
                ErrorStatus::Permanent,
            )));
        }

        // Commit the transaction.
        pg_client.postgres_client.execute("COMMIT", &[]).await?;

        Ok(())
    }

    async fn delete_table_metadata(&self, database: &str, table: &str) -> Result<()> {
        let pg_client = PgClientWrapper::new(&self.uri).await?;

        // Start a transaction to insert rows into metadata table and secret table.
        pg_client.postgres_client.execute("BEGIN", &[]).await?;

        // Delete rows for metadata table.
        let rows_affected = pg_client
            .postgres_client
            .execute(
                r#"DELETE FROM tables WHERE "database" = $1 AND "table" = $2"#,
                &[&database, &table],
            )
            .await?;
        if rows_affected != 1 {
            return Err(Error::PostgresRowCountError(ErrorStruct::new(
                format!("expected 1 row affected, but got {rows_affected}"),
                ErrorStatus::Permanent,
            )));
        }

        // Commit the transaction.
        pg_client.postgres_client.execute("COMMIT", &[]).await?;

        Ok(())
    }
}

impl PgMetadataStore {
    /// Attempt to create a metadata storage; if [`mooncake`] database doesn't exist, current database is not managed by moonlink, return None.
    pub fn new(uri: String) -> Result<Self> {
        Ok(Self { uri })
    }
}

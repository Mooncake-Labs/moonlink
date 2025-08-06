use crate::base_metadata_store::MetadataStoreTrait;
use crate::base_metadata_store::TableMetadataEntry;
use crate::base_metadata_store::MOONLINK_METADATA_TABLE;
use crate::base_metadata_store::MOONLINK_SECRET_TABLE;
use crate::config_utils;
use crate::error::{Error, Result};
use crate::postgres::pg_client_wrapper::PgClientWrapper;
use crate::postgres::utils;
use moonlink::MoonlinkTableConfig;
use moonlink::MoonlinkTableSecret;

use async_trait::async_trait;
use postgres_types::Json as PgJson;

/// SQL statements for moonlink metadata table schema.
const CREATE_TABLE_SCHEMA_SQL: &str = include_str!("sql/create_tables.sql");
/// SQL statements for moonlink secret table schema.
const CREATE_SECRET_SCHEMA_SQL: &str = include_str!("sql/create_secrets.sql");

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
                "
                SELECT 
                    t.database_id,
                    t.table_id,
                    t.table_name,
                    t.uri,
                    t.config,
                    s.secret_type,
                    s.key_id,
                    s.secret,
                    s.endpoint,
                    s.region,
                    s.project
                FROM tables t
                LEFT JOIN secrets s
                    ON t.database_id = s.database_id
                    AND t.table_id = s.table_id
                ",
                &[],
            )
            .await?;

        let mut metadata_entries = Vec::with_capacity(rows.len());
        for row in rows {
            let database_id: u32 = row.get("database_id");
            let table_id: u32 = row.get("table_id");
            let src_table_name: String = row.get("table_name");
            let src_table_uri: String = row.get("uri");
            let serialized_config: serde_json::Value = row.get("config");
            let secret_type: Option<String> = row.get("secret_type");
            let secret_entry: Option<MoonlinkTableSecret> = {
                secret_type.map(|secret_type| MoonlinkTableSecret {
                    secret_type: MoonlinkTableSecret::convert_secret_type(&secret_type),
                    key_id: row.get("key_id"),
                    secret: row.get("secret"),
                    endpoint: row.get("endpoint"),
                    region: row.get("region"),
                    project: row.get("project"),
                })
            };
            let moonlink_table_config =
                config_utils::deserialize_moonlink_table_config(serialized_config, secret_entry)?;

            let metadata_entry = TableMetadataEntry {
                database_id,
                table_id,
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
        database_id: u32,
        table_id: u32,
        table_name: &str,
        table_uri: &str,
        moonlink_table_config: MoonlinkTableConfig,
    ) -> Result<()> {
        let pg_client = PgClientWrapper::new(&self.uri).await?;
        let (serialized_config, moonlink_table_secret) =
            config_utils::parse_moonlink_table_config(moonlink_table_config)?;

        // Create metadata table if not exist.
        utils::create_table_if_non_existent(
            &pg_client.postgres_client,
            MOONLINK_METADATA_TABLE,
            CREATE_TABLE_SCHEMA_SQL,
        )
        .await?;

        // Create secret table if not exist.
        utils::create_table_if_non_existent(
            &pg_client.postgres_client,
            MOONLINK_SECRET_TABLE,
            CREATE_SECRET_SCHEMA_SQL,
        )
        .await?;

        // Start a transaction to insert rows into metadata table and secret table.
        pg_client.postgres_client.execute("BEGIN", &[]).await?;

        // Persist table metadata.
        // TODO(hjiang): Fill in other fields as well.
        let rows_affected = pg_client
            .postgres_client
            .execute(
                "INSERT INTO tables (database_id, table_id, table_name, uri, config)
                VALUES ($1, $2, $3, $4, $5)",
                &[
                    &database_id,
                    &table_id,
                    &table_name,
                    &table_uri,
                    &PgJson(&serialized_config),
                ],
            )
            .await?;
        if rows_affected != 1 {
            return Err(Error::PostgresRowCountError(1, rows_affected as u32));
        }

        // Persist table secrets.
        if let Some(table_secret) = moonlink_table_secret {
            let rows_affected = pg_client
                .postgres_client
                .execute(
                    "INSERT INTO secrets (database_id, table_id, secret_type, key_id, secret, endpoint, region, project)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                    &[
                        &database_id,
                        &table_id,
                        &table_secret.get_secret_type(),
                        &table_secret.key_id,
                        &table_secret.secret,
                        &table_secret.endpoint.as_deref(),
                        &table_secret.region.as_deref(),
                        &table_secret.project.as_deref(),
                    ],
                )
                .await?;
            if rows_affected != 1 {
                return Err(Error::PostgresRowCountError(1, rows_affected as u32));
            }
        }

        // Commit the transaction.
        pg_client.postgres_client.execute("COMMIT", &[]).await?;

        Ok(())
    }

    async fn delete_table_metadata(&self, database_id: u32, table_id: u32) -> Result<()> {
        let pg_client = PgClientWrapper::new(&self.uri).await?;

        // Start a transaction to insert rows into metadata table and secret table.
        pg_client.postgres_client.execute("BEGIN", &[]).await?;

        // Delete rows for metadata table.
        let rows_affected = pg_client
            .postgres_client
            .execute(
                "DELETE FROM tables WHERE database_id = $1 AND table_id = $2",
                &[&database_id, &table_id],
            )
            .await?;
        if rows_affected != 1 {
            return Err(Error::PostgresRowCountError(1, rows_affected as u32));
        }

        // Delete rows for secret table, intentionally no check affected row counts.
        pg_client
            .postgres_client
            .execute(
                "DELETE FROM secrets WHERE database_id = $1 AND table_id = $2",
                &[&database_id, &table_id],
            )
            .await?;

        // Commit the transaction.
        pg_client.postgres_client.execute("COMMIT", &[]).await?;

        Ok(())
    }
}

impl PgMetadataStore {
    /// Attempt to create a metadata storage; if [`mooncake`] schema doesn't exist, current database is not managed by moonlink, return None.
    pub fn new(uri: String) -> Result<Self> {
        Ok(Self { uri })
    }
}

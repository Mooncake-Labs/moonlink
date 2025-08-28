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
use moonlink_error::{ErrorStatus, ErrorStruct};

use async_trait::async_trait;
use postgres_types::Json as PgJson;

/// SQL statements for moonlink metadata table database.
const CREATE_TABLE_SCHEMA_SQL: &str = include_str!("sql/create_tables.sql");
/// SQL statements for moonlink secret table database.
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
                r#"
                SELECT 
                    t."database",
                    t."table",
                    t.src_table_name,
                    t.src_table_uri,
                    t.config,
                    s_ice.storage_provider  AS iceberg_storage_provider,
                    s_ice.key_id            AS iceberg_key_id,
                    s_ice.secret            AS iceberg_secret,
                    s_ice.endpoint          AS iceberg_endpoint,
                    s_ice.region            AS iceberg_region,
                    s_ice.project           AS iceberg_project,
                    s_wal.storage_provider  AS wal_storage_provider,
                    s_wal.key_id            AS wal_key_id,
                    s_wal.secret            AS wal_secret,
                    s_wal.endpoint          AS wal_endpoint,
                    s_wal.region            AS wal_region,
                    s_wal.project           AS wal_project
                FROM tables t
                LEFT JOIN secrets s_ice
                    ON t."database" = s_ice."database"
                    AND t."table" = s_ice."table"
                    AND s_ice.usage_type = 'iceberg'
                LEFT JOIN secrets s_wal
                    ON t."database" = s_wal."database"
                    AND t."table" = s_wal."table"
                    AND s_wal.usage_type = 'wal'
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
            let iceberg_storage_provider: Option<String> = row.get("iceberg_storage_provider");
            let iceberg_secret: Option<MoonlinkTableSecret> =
                iceberg_storage_provider.map(|t| MoonlinkTableSecret {
                    secret_type: MoonlinkTableSecret::convert_secret_type(&t),
                    key_id: row.get("iceberg_key_id"),
                    secret: row.get("iceberg_secret"),
                    endpoint: row.get("iceberg_endpoint"),
                    region: row.get("iceberg_region"),
                    project: row.get("iceberg_project"),
                });
            let wal_storage_provider: Option<String> = row.get("wal_storage_provider");
            let wal_secret: Option<MoonlinkTableSecret> =
                wal_storage_provider.map(|t| MoonlinkTableSecret {
                    secret_type: MoonlinkTableSecret::convert_secret_type(&t),
                    key_id: row.get("wal_key_id"),
                    secret: row.get("wal_secret"),
                    endpoint: row.get("wal_endpoint"),
                    region: row.get("wal_region"),
                    project: row.get("wal_project"),
                });
            let moonlink_table_config = config_utils::deserialize_moonlink_table_config(
                serialized_config,
                iceberg_secret,
                wal_secret,
                &database,
                &table,
            )?;

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
        let (serialized_config, iceberg_secret, wal_secret) =
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

        // Persist table secrets.
        if let Some(table_secret) = iceberg_secret {
            let rows_affected = pg_client
                .postgres_client
                .execute(
                    r#"INSERT INTO secrets ("database", "table", usage_type, storage_provider, key_id, secret, endpoint, region, project)
                    VALUES ($1, $2, 'iceberg', $3, $4, $5, $6, $7, $8)"#,
                    &[
                        &database,
                        &table,
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
                return Err(Error::PostgresRowCountError(ErrorStruct::new(
                    format!("expected 1 row affected, but got {rows_affected}"),
                    ErrorStatus::Permanent,
                )));
            }
        }
        if let Some(table_secret) = wal_secret {
            let rows_affected = pg_client
                .postgres_client
                .execute(
                    r#"INSERT INTO secrets ("database", "table", usage_type, storage_provider, key_id, secret, endpoint, region, project)
                    VALUES ($1, $2, 'wal', $3, $4, $5, $6, $7, $8)"#,
                    &[
                        &database,
                        &table,
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
                return Err(Error::PostgresRowCountError(ErrorStruct::new(
                    format!("expected 1 row affected, but got {rows_affected}"),
                    ErrorStatus::Permanent,
                )));
            }
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

        // Delete rows for secret table, intentionally no check affected row counts.
        pg_client
            .postgres_client
            .execute(
                r#"DELETE FROM secrets WHERE "database" = $1 AND "table" = $2"#,
                &[&database, &table],
            )
            .await?;

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

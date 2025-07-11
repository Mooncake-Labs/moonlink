use crate::base_metadata_store::MetadataStoreTrait;
use crate::base_metadata_store::TableMetadataEntry;
use crate::base_metadata_store::{MOONLINK_METADATA_TABLE, MOONLINK_SCHEMA};
use crate::error::{Error, Result};
use crate::postgres::config_utils;
use crate::postgres::pg_client_wrapper::PgClientWrapper;
use crate::postgres::utils;
use moonlink::MoonlinkTableConfig;

use async_trait::async_trait;
use postgres_types::Json as PgJson;

/// SQL statements for moonlink metadata table schema.
const CREATE_TABLE_SCHEMA_SQL: &str = include_str!("sql/create_tables.sql");

pub struct PgMetadataStore {
    /// Database connection string.
    uri: String,
}

#[async_trait]
impl MetadataStoreTrait for PgMetadataStore {
    async fn schema_exists(&self) -> Result<bool> {
        let pg_client = PgClientWrapper::new(&self.uri).await?;
        utils::schema_exists(&pg_client.postgres_client, MOONLINK_SCHEMA).await
    }

    async fn metadata_table_exists(&self) -> Result<bool> {
        let pg_client = PgClientWrapper::new(&self.uri).await?;
        if !utils::table_exists(
            &pg_client.postgres_client,
            MOONLINK_SCHEMA,
            MOONLINK_METADATA_TABLE,
        )
        .await?
        {
            return Ok(false);
        }
        Ok(true)
    }

    async fn get_database_id(&self) -> Result<u32> {
        let pg_client = PgClientWrapper::new(&self.uri).await?;
        let row = pg_client
            .postgres_client
            .query_one(
                "SELECT oid FROM pg_database WHERE datname = current_database()",
                &[],
            )
            .await?;
        let oid = row.get("oid");
        Ok(oid)
    }

    async fn get_all_table_metadata_entries(&self) -> Result<Vec<TableMetadataEntry>> {
        let pg_client = PgClientWrapper::new(&self.uri).await?;
        let rows = pg_client
            .postgres_client
            .query(
                "SELECT oid, table_name, uri, config FROM mooncake.tables",
                &[],
            )
            .await?;

        let mut metadata_entries = Vec::with_capacity(rows.len());
        for cur_row in rows.into_iter() {
            assert_eq!(cur_row.len(), 4);
            let table_id = cur_row.get("oid");
            let src_table_name = cur_row.get("table_name");
            let src_table_uri = cur_row.get("uri");
            let serialized_config = cur_row.get("config");
            let moonlink_table_config =
                config_utils::deserialze_moonlink_table_config(serialized_config)?;
            metadata_entries.push(TableMetadataEntry {
                table_id,
                src_table_name,
                src_table_uri,
                moonlink_table_config,
            });
        }
        Ok(metadata_entries)
    }

    async fn store_table_metadata(
        &self,
        table_id: u32,
        table_name: &str,
        table_uri: &str,
        moonlink_table_config: MoonlinkTableConfig,
    ) -> Result<()> {
        let pg_client = PgClientWrapper::new(&self.uri).await?;
        let serialized_config =
            config_utils::serialize_moonlink_table_config(moonlink_table_config)?;

        if !utils::schema_exists(&pg_client.postgres_client, MOONLINK_SCHEMA).await? {
            return Err(Error::MetadataStoreFailedPrecondition(format!(
                "Schema {MOONLINK_SCHEMA} doesn't exist when store table metadata"
            )));
        }
        if !utils::table_exists(
            &pg_client.postgres_client,
            MOONLINK_SCHEMA,
            MOONLINK_METADATA_TABLE,
        )
        .await?
        {
            utils::create_table(&pg_client.postgres_client, CREATE_TABLE_SCHEMA_SQL).await?;
        }

        // TODO(hjiang): Fill in other fields as well.
        let rows_affected = pg_client
            .postgres_client
            .execute(
                "INSERT INTO mooncake.tables (oid, table_name, uri, config)
                VALUES ($1, $2, $3, $4)",
                &[
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

        Ok(())
    }

    async fn delete_table_metadata(&self, table_id: u32) -> Result<()> {
        let pg_client = PgClientWrapper::new(&self.uri).await?;
        let rows_affected = pg_client
            .postgres_client
            .execute("DELETE FROM mooncake.tables WHERE oid = $1", &[&table_id])
            .await?;

        if rows_affected != 1 {
            return Err(Error::PostgresRowCountError(1, rows_affected as u32));
        }
        Ok(())
    }
}

impl PgMetadataStore {
    /// Attempt to create a metadata storage; if [`mooncake`] schema doesn't exist, current database is not managed by moonlink, return None.
    pub fn new(uri: String) -> Result<Self> {
        Ok(Self { uri })
    }
}

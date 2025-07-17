/// This trait provides the interface for moonlink table metadata storage.
use async_trait::async_trait;

use crate::error::Result;
use moonlink::MoonlinkTableConfig;

/// Constants for moonlink metadata storage.
///
/// Database schema for moonlink.
pub const MOONLINK_SCHEMA: &str = "mooncake";
/// Metadata table name for moonlink.
pub const MOONLINK_METADATA_TABLE: &str = "tables";
/// Secret table name for moonlink.
pub const MOONLINK_SECRET_TABLE: &str = "secrets";

/// Metadata entry for each table.
#[derive(Clone, Debug)]
pub struct TableMetadataEntry {
    /// Database id.
    pub database_id: u32,
    /// Table id.
    pub table_id: u32,
    /// Src table name.
    pub src_table_name: String,
    /// Src table connection string.
    pub src_table_uri: String,
    /// Moonlink table config, including mooncake and iceberg table config.
    pub moonlink_table_config: MoonlinkTableConfig,
}

#[async_trait]
pub trait MetadataStoreTrait: Send + Sync {
    /// Return whether metadata table exists.
    #[allow(async_fn_in_trait)]
    async fn metadata_table_exists(&self) -> Result<bool>;

    /// Get all mooncake table metadata and secret entries in the metadata storage table.
    ///
    /// Precondition:
    /// - moonlink schema already exists;
    /// - metadata table already exists.
    #[allow(async_fn_in_trait)]
    async fn get_all_table_metadata_entries(&self) -> Result<Vec<TableMetadataEntry>>;

    /// Store table metadata and secret for the given mooncake table.
    /// Metadata table will be created if it doesn't exists.
    ///
    /// # Arguments
    ///
    /// * moonlink_table_config: contains both mooncake table config and iceberg table config;
    /// meanwhile iceberg table config contains necessary security entry to access object storage.
    ///
    /// Precondition:
    /// - moonlink schema already exists;
    /// - the requested table id hasn't been recorded in the metadata storage.
    #[allow(async_fn_in_trait)]
    async fn store_table_metadata(
        &self,
        database_id: u32,
        table_id: u32,
        table_name: &str,
        table_uri: &str,
        moonlink_table_config: MoonlinkTableConfig,
    ) -> Result<()>;

    /// Delete table config for the given table.
    /// Precondition: the requested table id has been record in the metadata storage.
    #[allow(async_fn_in_trait)]
    async fn delete_table_metadata(&self, database_id: u32, table_id: u32) -> Result<()>;
}

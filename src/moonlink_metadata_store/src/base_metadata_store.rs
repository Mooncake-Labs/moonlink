/// This trait provides the interface for moonlink table metadata storage.
use async_trait::async_trait;

use crate::error::Result;
use moonlink::MoonlinkTableConfig;

#[async_trait]
pub trait MetadataStoreTrait: Send {
    /// Load configuration for the given table.
    /// Precondition: the requested table id has been record in the metadata storage.
    #[allow(async_fn_in_trait)]
    async fn load_table_config(&self, table_id: u32) -> Result<MoonlinkTableConfig>;

    /// Store table config for the given table.
    /// Precondition: the requested table id hasn't been recorded in the metadata storage.
    #[allow(async_fn_in_trait)]
    async fn store_table_config(
        &self,
        table_id: u32,
        table_name: &str,
        moonlink_table_config: MoonlinkTableConfig,
    ) -> Result<()>;

    /// Delete table config for the given table.
    /// Precondition: the requested table id has been record in the metadata storage.
    #[allow(async_fn_in_trait)]
    async fn delete_table_config(&self, table_id: u32) -> Result<()>;
}

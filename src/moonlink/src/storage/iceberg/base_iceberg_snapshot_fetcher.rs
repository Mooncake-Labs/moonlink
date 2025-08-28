use arrow_schema::Schema as ArrowSchema;
/// Base class for iceberg schema fetching.
use async_trait::async_trait;

use crate::Result;

#[cfg(test)]
use mockall::*;

#[async_trait]
#[cfg_attr(test, automock)]
pub trait BaseIcebergSnapshotFetcher {
    /// Get the latest iceberg table schema.
    async fn fetch_table_schema(&self) -> Result<Option<ArrowSchema>>;
    /// Get the latest flush LSN for the latest snapshot.
    async fn get_flush_lsn(&self) -> Result<Option<u64>>;
}

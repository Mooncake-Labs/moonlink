use arrow_schema::Schema as ArrowSchema;
/// Base class for iceberg schema fetching.
use async_trait::async_trait;

use crate::Result;

#[cfg(test)]
use mockall::*;

#[async_trait]
#[cfg_attr(test, automock)]
pub trait BaseIcebergSchemaFetcher {
    /// Get the latest iceberg table schema.
    #[allow(dead_code)]
    async fn fetch_table_schema(&self) -> Result<Option<ArrowSchema>>;
}

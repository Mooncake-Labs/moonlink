use crate::storage::iceberg::base_iceberg_schema_fetcher::BaseIcebergSchemaFetcher;
use crate::storage::iceberg::catalog_utils;
use crate::storage::iceberg::iceberg_table_config::IcebergTableConfig;
use crate::storage::iceberg::moonlink_catalog::MoonlinkCatalog;
use crate::storage::iceberg::utils;
use crate::Result;

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use iceberg::arrow as IcebergArrow;
use iceberg::Result as IcebergResult;

#[allow(dead_code)]
pub struct IcebergSchemaFetcher {
    /// Iceberg table configuration.
    config: IcebergTableConfig,
    /// Iceberg catalog, which interacts with the iceberg table.
    catalog: Box<dyn MoonlinkCatalog>,
}

impl IcebergSchemaFetcher {
    #[allow(dead_code)]
    pub fn new(config: IcebergTableConfig) -> IcebergResult<Self> {
        let catalog = catalog_utils::create_catalog_without_schema(config.accessor_config.clone())?;
        Ok(Self { config, catalog })
    }
}

#[async_trait]
impl BaseIcebergSchemaFetcher for IcebergSchemaFetcher {
    async fn fetch_table_schema(&self) -> Result<Option<ArrowSchema>> {
        let table = utils::get_table_if_exists(
            &*self.catalog,
            &self.config.namespace,
            &self.config.table_name,
        )
        .await?;
        if let Some(table) = table {
            let iceberg_schema = table.metadata().current_schema();
            let arrow_schema = IcebergArrow::schema_to_arrow_schema(iceberg_schema)?;
            return Ok(Some(arrow_schema));
        }
        Ok(None)
    }
}

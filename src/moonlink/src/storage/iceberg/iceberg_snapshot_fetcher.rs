use crate::storage::iceberg::base_iceberg_snapshot_fetcher::BaseIcebergSnapshotFetcher;
use crate::storage::iceberg::catalog_utils;
use crate::storage::iceberg::iceberg_table_config::IcebergTableConfig;
use crate::storage::iceberg::iceberg_table_manager::MOONCAKE_TABLE_FLUSH_LSN;
use crate::storage::iceberg::moonlink_catalog::MoonlinkCatalog;
use crate::storage::iceberg::utils;
use crate::Result;

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use iceberg::arrow as IcebergArrow;

#[allow(dead_code)]
pub struct IcebergSnapshotFetcher {
    /// Iceberg table configuration.
    config: IcebergTableConfig,
    /// Iceberg catalog, which interacts with the iceberg table.
    catalog: Box<dyn MoonlinkCatalog>,
}

impl IcebergSnapshotFetcher {
    pub async fn new(config: IcebergTableConfig) -> Result<Self> {
        let catalog = catalog_utils::create_catalog_without_schema(config.clone()).await?;
        Ok(Self { config, catalog })
    }
}

#[async_trait]
impl BaseIcebergSnapshotFetcher for IcebergSnapshotFetcher {
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

    async fn get_flush_lsn(&self) -> Result<Option<u64>> {
        let table = utils::get_table_if_exists(
            &*self.catalog,
            &self.config.namespace,
            &self.config.table_name,
        )
        .await?;
        if let Some(table) = table {
            if let Some(iceberg_snapshot) = table.metadata().current_snapshot() {
                let flush_lsn = iceberg_snapshot
                    .summary()
                    .additional_properties
                    .get(MOONCAKE_TABLE_FLUSH_LSN)
                    .map(|s| s.parse::<u64>())
                    .unwrap_or_else(|| Ok(0))
                    .unwrap();
                return Ok(Some(flush_lsn));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::iceberg::file_catalog::FileCatalog;
use crate::storage::iceberg::iceberg_table_config::IcebergCatalogConfig;
use crate::storage::iceberg::moonlink_catalog::MoonlinkCatalog;

use iceberg::spec::Schema as IcebergSchema;
use iceberg::{spec::TableMetadataBuilder, Result as IcebergResult, TableUpdate};

/// Create a catelog based on the provided type.
///
/// It's worth noting catalog and warehouse uri are not 1-1 mapping; for example, rest catalog could handle warehouse.
/// Here we simply deduce catalog type from warehouse because both filesystem and object storage catalog are only able to handle certain scheme.
pub fn create_catalog(
    catalog: IcebergCatalogConfig,
    iceberg_schema: IcebergSchema,
) -> IcebergResult<Box<dyn MoonlinkCatalog>> {
    match catalog {
        IcebergCatalogConfig::File { accessor_config } => {
            Ok(Box::new(FileCatalog::new(accessor_config, iceberg_schema)?))
        }
        #[cfg(feature = "catalog-rest")]
        IcebergCatalogConfig::Rest { .. } => Err(iceberg::Error::new(
            iceberg::ErrorKind::FeatureUnsupported,
            "Only File catalog is supported currently",
        )),
        #[cfg(feature = "catalog-glue")]
        IcebergCatalogConfig::Glue { .. } => Err(iceberg::Error::new(
            iceberg::ErrorKind::FeatureUnsupported,
            "Only File catalog is supported currently",
        )),
    }
}

/// Create a catalog with no schema provided.
pub fn create_catalog_without_schema(
    catalog: IcebergCatalogConfig,
) -> IcebergResult<Box<dyn MoonlinkCatalog>> {
    match catalog {
        IcebergCatalogConfig::File { accessor_config } => {
            Ok(Box::new(FileCatalog::new_without_schema(accessor_config)?))
        }
        #[cfg(feature = "catalog-rest")]
        IcebergCatalogConfig::Rest { .. } => Err(iceberg::Error::new(
            iceberg::ErrorKind::FeatureUnsupported,
            "Only File catalog is supported currently",
        )),
        #[cfg(feature = "catalog-glue")]
        IcebergCatalogConfig::Glue { .. } => Err(iceberg::Error::new(
            iceberg::ErrorKind::FeatureUnsupported,
            "Only File catalog is supported currently",
        )),
    }
}

/// Reflect table updates to table metadata builder.
pub(crate) fn reflect_table_updates(
    mut builder: TableMetadataBuilder,
    table_updates: Vec<TableUpdate>,
) -> IcebergResult<TableMetadataBuilder> {
    for update in &table_updates {
        match update {
            TableUpdate::AddSnapshot { snapshot } => {
                builder = builder.add_snapshot(snapshot.clone())?;
            }
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                builder = builder.set_ref(ref_name, reference.clone())?;
            }
            TableUpdate::SetProperties { updates } => {
                builder = builder.set_properties(updates.clone())?;
            }
            TableUpdate::RemoveProperties { removals } => {
                builder = builder.remove_properties(removals)?;
            }
            TableUpdate::AddSchema { schema } => {
                builder = builder.add_schema(schema.clone())?;
            }
            TableUpdate::SetCurrentSchema { schema_id } => {
                builder = builder.set_current_schema(*schema_id)?;
            }
            _ => {
                unreachable!("Unimplemented table update: {:?}", update);
            }
        }
    }
    Ok(builder)
}

/// Test util function to create catalog with provided filesystem accessor.
#[cfg(test)]
pub fn create_catalog_with_filesystem_accessor(
    filesystem_accessor: std::sync::Arc<dyn BaseFileSystemAccess>,
    iceberg_schema: IcebergSchema,
) -> IcebergResult<Box<dyn MoonlinkCatalog>> {
    Ok(Box::new(FileCatalog::new_with_filesystem_accessor(
        filesystem_accessor,
        iceberg_schema,
    )?))
}

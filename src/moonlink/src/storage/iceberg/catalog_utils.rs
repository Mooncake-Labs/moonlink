#[cfg(test)]
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor_config::AccessorConfig;
use crate::storage::iceberg::file_catalog::FileCatalog;
use crate::storage::iceberg::moonlink_catalog::MoonlinkCatalog;

use iceberg::spec::Schema as IcebergSchema;
use iceberg::Result as IcebergResult;

/// Create a catelog based on the provided type.
///
/// It's worth noting catalog and warehouse uri are not 1-1 mapping; for example, rest catalog could handle warehouse.
/// Here we simply deduce catalog type from warehouse because both filesystem and object storage catalog are only able to handle certain scheme.
pub fn create_catalog(
    accessor_config: AccessorConfig,
    iceberg_schema: IcebergSchema,
) -> IcebergResult<Box<dyn MoonlinkCatalog>> {
    Ok(Box::new(FileCatalog::new(accessor_config, iceberg_schema)?))
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

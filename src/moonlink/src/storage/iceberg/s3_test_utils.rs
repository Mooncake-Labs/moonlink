use crate::storage::filesystem::s3::s3_test_utils::*;
/// This module provides a few test util functions.
use crate::storage::iceberg::file_catalog::FileCatalog;
use crate::storage::iceberg::file_catalog_test_utils::*;

/// Create a S3 catalog, which communicates with local minio server.
pub(crate) fn create_test_s3_catalog(warehouse_uri: &str) -> FileCatalog {
    let storage_config = create_s3_storage_config(warehouse_uri);
    FileCatalog::new(storage_config, get_test_schema()).unwrap()
}

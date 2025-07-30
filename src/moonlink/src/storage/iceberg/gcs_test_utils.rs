use crate::storage::filesystem::gcs::gcs_test_utils::*;
use crate::storage::iceberg::file_catalog::FileCatalog;
use crate::storage::iceberg::file_catalog_test_utils::*;

#[allow(dead_code)]
pub(crate) fn create_gcs_catalog(warehouse_uri: &str) -> FileCatalog {
    let storage_config = create_gcs_storage_config(warehouse_uri);
    FileCatalog::new(storage_config, get_test_schema()).unwrap()
}

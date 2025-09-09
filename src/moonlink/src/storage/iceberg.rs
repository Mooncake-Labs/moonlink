pub mod base_iceberg_snapshot_fetcher;
pub(super) mod catalog_utils;
mod data_file_manifest_manager;
pub(super) mod deletion_vector;
mod deletion_vector_manifest_manager;
pub(super) mod file_catalog;
mod file_index_manifest_manager;
mod iceberg_schema_manager;
pub mod iceberg_snapshot_fetcher;
pub(super) mod iceberg_table_config;
mod iceberg_table_loader;
pub(super) mod iceberg_table_manager;
mod iceberg_table_syncer;
pub(super) mod index;
pub(super) mod io_utils;
mod manifest_utils;
pub(super) mod moonlink_catalog;
pub(super) mod parquet_metadata_utils;
pub(super) mod parquet_stats_utils;
pub(super) mod parquet_utils;
pub(super) mod puffin_utils;
pub(super) mod puffin_writer_proxy;

#[cfg(feature = "catalog-rest")]
pub(super) mod rest_catalog;

mod schema_utils;
mod snapshot_utils;
mod table_commit_proxy;
pub(super) mod table_manager;
pub(super) mod table_property;
pub(super) mod utils;
pub(super) mod validation;

#[cfg(feature = "storage-s3")]
#[cfg(test)]
mod s3_test_utils;

#[cfg(feature = "storage-gcs")]
#[cfg(test)]
mod gcs_test_utils;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod state_tests;

#[cfg(test)]
mod compaction_tests;

#[cfg(test)]
pub(super) mod test_utils;

#[cfg(test)]
mod catalog_test_utils;

#[cfg(test)]
mod file_catalog_test_utils;

#[cfg(test)]
mod file_catalog_test;

#[cfg(feature = "catalog-rest")]
#[cfg(test)]
pub(super) mod rest_catalog_test_utils;

#[cfg(feature = "catalog-rest")]
#[cfg(test)]
pub(super) mod rest_catalog_test_guard;

#[cfg(feature = "catalog-rest")]
#[cfg(test)]
mod rest_catalog_test;

#[cfg(test)]
mod mock_filesystem_test;

#[cfg(test)]
mod snapshot_fetcher_test;

#[cfg(test)]
mod catalog_test_impl;

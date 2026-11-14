pub mod base_iceberg_snapshot_fetcher;
pub(crate) mod catalog_utils;
pub mod cloud_security_config;
pub(crate) mod data_file_manifest_manager;
pub(crate) mod deletion_vector;
mod deletion_vector_manifest_manager;
pub(crate) mod file_catalog;
mod file_index_manifest_manager;
pub(crate) mod gc;
mod iceberg_schema_manager;
pub mod iceberg_snapshot_fetcher;
pub(crate) mod iceberg_table_config;
mod iceberg_table_loader;
pub(crate) mod iceberg_table_manager;
mod iceberg_table_syncer;
pub(crate) mod index;
pub(crate) mod io_utils;
mod manifest_utils;
pub(crate) mod moonlink_catalog;
pub(crate) mod parquet_metadata_utils;
pub(crate) mod parquet_stats_utils;
pub(crate) mod parquet_utils;
pub(crate) mod puffin_utils;
pub(crate) mod puffin_writer_proxy;
mod table_update_proxy;

#[cfg(all(feature = "catalog-glue", feature = "storage-s3"))]
pub(crate) mod glue_catalog;

#[cfg(feature = "catalog-rest")]
pub(crate) mod rest_catalog;

mod schema_utils;
mod snapshot_utils;
mod table_commit_proxy;
pub(crate) mod table_property;
pub(crate) mod utils;
pub(crate) mod validation;

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
pub(crate) mod test_utils;

#[cfg(test)]
mod catalog_test_utils;

#[cfg(test)]
mod file_catalog_test_utils;

#[cfg(test)]
mod file_catalog_test;

#[cfg(feature = "catalog-rest")]
#[cfg(test)]
pub(crate) mod rest_catalog_test_utils;

#[cfg(feature = "catalog-rest")]
#[cfg(test)]
pub(crate) mod rest_catalog_test_guard;

#[cfg(feature = "catalog-rest")]
#[cfg(test)]
mod rest_catalog_test;

#[cfg(test)]
mod mock_filesystem_test;

#[cfg(test)]
mod snapshot_fetcher_test;

#[cfg(test)]
mod catalog_test_impl;

#[cfg(feature = "catalog-rest")]
#[cfg(test)]
mod iceberg_rest_catalog_test;

#[cfg(all(feature = "catalog-glue", feature = "storage-s3"))]
#[cfg(test)]
mod glue_catalog_test_utils;

#[cfg(all(feature = "catalog-glue", feature = "storage-s3"))]
#[cfg(test)]
mod glue_catalog_test;

#[cfg(all(feature = "catalog-glue", feature = "storage-s3"))]
#[cfg(test)]
mod iceberg_glue_catalog_test;

use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor_config::AccessorConfig;
use crate::storage::filesystem::storage_config::StorageConfig;
use crate::storage::iceberg::parquet_utils;

use std::path::Path;

use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::spec::DataFile;
use iceberg::spec::TableMetadata as IcebergTableMetadata;
use iceberg::table::Table as IcebergTable;
use iceberg::writer::file_writer::location_generator::{
    DefaultLocationGenerator, LocationGenerator,
};
use iceberg::{Error as IcebergError, Result as IcebergResult};

/// Write the given record batch in the given local file to the iceberg table (parquet file keeps unchanged).
pub(crate) async fn write_record_batch_to_iceberg(
    table: &IcebergTable,
    local_filepath: &String,
    table_metadata: &IcebergTableMetadata,
    filesystem_accessor: &dyn BaseFileSystemAccess,
) -> IcebergResult<DataFile> {
    let filename = Path::new(local_filepath)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
    let remote_filepath = location_generator.generate_location(&filename);

    // Import local parquet file to remote.
    filesystem_accessor
        .copy_from_local_to_remote(local_filepath, &remote_filepath)
        .await
        .map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to copy from {local_filepath} to {remote_filepath}: {e:?}"),
            )
            .with_retryable(true)
        })?;

    // Get data file from local parquet file.
    let data_file = parquet_utils::get_data_file_from_local_parquet_file(
        local_filepath,
        remote_filepath,
        table_metadata,
    )
    .await?;
    Ok(data_file)
}

/// Copy the given local index file to iceberg table, and return filepath within iceberg table.
pub(crate) async fn upload_index_file(
    table: &IcebergTable,
    local_index_filepath: &str,
    filesystem_accessor: &dyn BaseFileSystemAccess,
) -> IcebergResult<String> {
    let filename = Path::new(local_index_filepath)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let remote_filepath = location_generator.generate_location(&filename);
    filesystem_accessor
        .copy_from_local_to_remote(local_index_filepath, &remote_filepath)
        .await
        .map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to copy from {local_index_filepath} to {remote_filepath}: {e:?}"),
            )
            .with_retryable(true)
        })?;
    Ok(remote_filepath)
}

/// Create iceberg [`FileIO`].
pub(crate) fn create_file_io(accessor_config: &AccessorConfig) -> IcebergResult<FileIO> {
    match &accessor_config.storage_config {
        #[cfg(feature = "storage-fs")]
        StorageConfig::FileSystem { .. } => FileIOBuilder::new_fs_io().build(),
        #[cfg(feature = "storage-gcs")]
        StorageConfig::Gcs {
            project,
            region,
            endpoint,
            disable_auth,
            access_key_id,
            secret_access_key,
            ..
        } => {
            // Testing environment.
            if *disable_auth {
                let file_io_builder = FileIOBuilder::new("GCS")
                    .with_prop(iceberg::io::GCS_PROJECT_ID, project)
                    .with_prop(iceberg::io::GCS_SERVICE_PATH, endpoint.as_ref().unwrap())
                    .with_prop(iceberg::io::GCS_NO_AUTH, "true")
                    .with_prop(iceberg::io::GCS_ALLOW_ANONYMOUS, "true")
                    .with_prop(iceberg::io::GCS_DISABLE_CONFIG_LOAD, "true");
                return file_io_builder.build();
            }

            // Production environment.
            let file_io_builder = FileIOBuilder::new("S3")
                .with_prop(iceberg::io::S3_ENDPOINT, "https://storage.googleapis.com")
                .with_prop(iceberg::io::S3_REGION, region)
                .with_prop(iceberg::io::S3_ACCESS_KEY_ID, access_key_id)
                .with_prop(iceberg::io::S3_SECRET_ACCESS_KEY, secret_access_key);
            file_io_builder.build()
        }
        #[cfg(feature = "storage-s3")]
        StorageConfig::S3 {
            access_key_id,
            secret_access_key,
            region,
            endpoint,
            ..
        } => {
            let mut file_io_builder = FileIOBuilder::new("s3")
                .with_prop(iceberg::io::S3_REGION, region)
                .with_prop(iceberg::io::S3_ACCESS_KEY_ID, access_key_id)
                .with_prop(iceberg::io::S3_SECRET_ACCESS_KEY, secret_access_key);
            if let Some(endpoint) = endpoint {
                file_io_builder = file_io_builder.with_prop(iceberg::io::S3_ENDPOINT, endpoint);
            }
            file_io_builder.build()
        }
    }
}

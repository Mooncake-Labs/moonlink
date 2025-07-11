use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::filesystem_config::FileSystemConfig;
#[cfg(feature = "storage-gcs")]
use crate::storage::filesystem::gcs::cred_utils as gcs_cred_utils;
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
        })?;
    Ok(remote_filepath)
}

/// Create iceberg [`FileIO`].
pub(crate) fn create_file_io(config: &FileSystemConfig) -> IcebergResult<FileIO> {
    match config {
        #[cfg(feature = "storage-fs")]
        FileSystemConfig::FileSystem { .. } => FileIOBuilder::new_fs_io().build(),
        #[cfg(feature = "storage-gcs")]
        FileSystemConfig::Gcs {
            endpoint,
            disable_auth,
            cred_path,
            ..
        } => {
            if *disable_auth {
                assert!(cred_path.is_none());
            }

            let mut file_io_builder = FileIOBuilder::new("GCS")
                .with_prop(iceberg::io::GCS_PROJECT_ID, "coral-ring-465417-r0");
            if let Some(endpoint) = endpoint {
                file_io_builder =
                    file_io_builder.with_prop(iceberg::io::GCS_SERVICE_PATH, endpoint);
            }
            if *disable_auth {
                file_io_builder = file_io_builder
                    .with_prop(iceberg::io::GCS_NO_AUTH, "true")
                    .with_prop(iceberg::io::GCS_ALLOW_ANONYMOUS, "true")
                    .with_prop(iceberg::io::GCS_DISABLE_CONFIG_LOAD, "true");
            } else {
                let cred_json = gcs_cred_utils::load_gcs_credentials(cred_path).map_err(|e| {
                    IcebergError::new(
                        iceberg::ErrorKind::Unexpected,
                        format!("Failed to load GCS credential {cred_path:?}: {e:?}"),
                    )
                })?;
                file_io_builder =
                    file_io_builder.with_prop(iceberg::io::GCS_CREDENTIALS_JSON, cred_json);
            }
            file_io_builder.build()
        }
        #[cfg(feature = "storage-s3")]
        FileSystemConfig::S3 {
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

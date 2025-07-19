/// This module contains table creation tests utils.
use crate::row::IdentityProp as RowIdentity;
use crate::storage::compaction::compaction_config::DataCompactionConfig;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
#[cfg(feature = "storage-gcs")]
use crate::storage::filesystem::gcs::gcs_test_utils;
#[cfg(feature = "storage-s3")]
use crate::storage::filesystem::s3::s3_test_utils;
use crate::storage::iceberg::iceberg_table_config::IcebergTableConfig;
use crate::storage::iceberg::iceberg_table_manager::IcebergTableManager;
use crate::storage::index::index_merge_config::FileIndexMergeConfig;
use crate::storage::mooncake_table::test_utils_commons::*;
use crate::storage::mooncake_table::IcebergPersistenceConfig;
use crate::storage::mooncake_table::{MooncakeTableConfig, TableMetadata as MooncakeTableMetadata};
use crate::storage::MooncakeTable;
use crate::table_notify::TableEvent;
use crate::FileSystemAccessor;
use crate::FileSystemConfig;
use crate::ObjectStorageCache;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::datatypes::{DataType, Field};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;

/// Test util function to get iceberg table config for local filesystem.
pub(crate) fn get_iceberg_table_config(temp_dir: &TempDir) -> IcebergTableConfig {
    let root_directory = temp_dir.path().to_str().unwrap().to_string();
    IcebergTableConfig {
        namespace: vec![ICEBERG_TEST_NAMESPACE.to_string()],
        table_name: ICEBERG_TEST_TABLE.to_string(),
        filesystem_config: FileSystemConfig::FileSystem { root_directory },
    }
}

/// Test util function to create iceberg table config.
pub(crate) fn create_iceberg_table_config(warehouse_uri: String) -> IcebergTableConfig {
    let filesystem_config = if warehouse_uri.starts_with("s3://") {
        #[cfg(feature = "storage-s3")]
        {
            s3_test_utils::create_s3_filesystem_config(&warehouse_uri)
        }
        #[cfg(not(feature = "storage-s3"))]
        {
            panic!("S3 support not enabled. Enable `storage-s3` feature.");
        }
    } else if warehouse_uri.starts_with("gs://") {
        #[cfg(feature = "storage-gcs")]
        {
            gcs_test_utils::create_gcs_filesystem_config(&warehouse_uri)
        }
        #[cfg(not(feature = "storage-gcs"))]
        {
            panic!("GCS support not enabled. Enable `storage-gcs` feature.");
        }
    } else {
        FileSystemConfig::FileSystem {
            root_directory: warehouse_uri.clone(),
        }
    };

    IcebergTableConfig {
        filesystem_config,
        ..Default::default()
    }
}

/// Test util function to create arrow schema.
pub(crate) fn create_test_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "0".to_string(),
        )])),
        Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "1".to_string(),
        )])),
        Field::new("age", DataType::Int32, false).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "2".to_string(),
        )])),
    ]))
}

/// Test util function to create an arrow schema for schema evolution.
pub(crate) fn create_test_updated_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("a_new_id", DataType::Int32, false).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "3".to_string(),
        )])),
        Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "1".to_string(),
        )])),
    ]))
}

/// Test util function to create local filesystem accessor from iceberg table config.
pub(crate) fn create_test_filesystem_accessor(
    iceberg_table_config: &IcebergTableConfig,
) -> Arc<dyn BaseFileSystemAccess> {
    Arc::new(FileSystemAccessor::new(
        iceberg_table_config.filesystem_config.clone(),
    ))
}

/// Test util function to create mooncake table metadata.
pub(crate) fn create_test_table_metadata(
    local_table_directory: String,
) -> Arc<MooncakeTableMetadata> {
    let config = MooncakeTableConfig::new(local_table_directory.clone());
    create_test_table_metadata_with_config(local_table_directory, config)
}

/// Test util function to create mooncake table metadata with schema.
pub(crate) fn create_test_table_metadata_with_schema(
    local_table_directory: String,
    arrow_schema: Arc<ArrowSchema>,
) -> Arc<MooncakeTableMetadata> {
    let config = MooncakeTableConfig::new(local_table_directory.clone());
    Arc::new(MooncakeTableMetadata {
        name: ICEBERG_TEST_TABLE.to_string(),
        table_id: 0,
        schema: arrow_schema,
        config,
        path: std::path::PathBuf::from(local_table_directory),
        identity: RowIdentity::FullRow,
    })
}

/// Test util function to create mooncake table metadata with mooncake table config.
pub(crate) fn create_test_table_metadata_with_config(
    local_table_directory: String,
    mooncake_table_config: MooncakeTableConfig,
) -> Arc<MooncakeTableMetadata> {
    Arc::new(MooncakeTableMetadata {
        name: ICEBERG_TEST_TABLE.to_string(),
        table_id: 0,
        schema: create_test_arrow_schema(),
        config: mooncake_table_config,
        path: std::path::PathBuf::from(local_table_directory),
        identity: RowIdentity::FullRow,
    })
}

/// Test util function to create mooncake table metadata with index merge enable whenever there're two index blocks.
pub(crate) fn create_test_table_metadata_with_index_merge(
    local_table_directory: String,
) -> Arc<MooncakeTableMetadata> {
    let file_index_config = FileIndexMergeConfig {
        file_indices_to_merge: 2,
        index_block_final_size: u64::MAX,
    };
    let mut config = MooncakeTableConfig::new(local_table_directory.clone());
    config.file_index_config = file_index_config;
    create_test_table_metadata_with_config(local_table_directory, config)
}

/// Util function to create mooncake table and iceberg table manager; object storage cache will be created internally.
///
/// Iceberg snapshot will be created whenever `create_snapshot` is called.
pub(crate) async fn create_table_and_iceberg_manager(
    temp_dir: &TempDir,
) -> (MooncakeTable, IcebergTableManager, Receiver<TableEvent>) {
    let default_data_compaction_config = DataCompactionConfig::default();
    create_table_and_iceberg_manager_with_data_compaction_config(
        temp_dir,
        default_data_compaction_config,
    )
    .await
}

/// Similar to [`create_table_and_iceberg_manager`], but it takes data compaction config.
pub(crate) async fn create_table_and_iceberg_manager_with_data_compaction_config(
    temp_dir: &TempDir,
    data_compaction_config: DataCompactionConfig,
) -> (MooncakeTable, IcebergTableManager, Receiver<TableEvent>) {
    let path = temp_dir.path().to_path_buf();
    let object_storage_cache = ObjectStorageCache::default_for_test(temp_dir);
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let identity_property = mooncake_table_metadata.identity.clone();
    let iceberg_table_config = get_iceberg_table_config(temp_dir);
    let schema = create_test_arrow_schema();

    // Create iceberg snapshot whenever `create_snapshot` is called.
    let mooncake_table_config = MooncakeTableConfig {
        data_compaction_config,
        persistence_config: IcebergPersistenceConfig {
            new_data_file_count: 0,
            ..Default::default()
        },
        ..Default::default()
    };

    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        ICEBERG_TEST_TABLE.to_string(),
        /*table_id=*/ 1,
        path,
        identity_property,
        iceberg_table_config.clone(),
        mooncake_table_config,
        object_storage_cache.clone(),
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();

    let iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        create_test_filesystem_accessor(&iceberg_table_config),
        iceberg_table_config.clone(),
    )
    .unwrap();

    let (notify_tx, notify_rx) = mpsc::channel(100);
    table.register_table_notify(notify_tx).await;

    (table, iceberg_table_manager, notify_rx)
}

/// Test util function to create mooncake table and table notify for compaction test.
pub(crate) async fn create_mooncake_table_and_notify_for_compaction(
    temp_dir: &TempDir,
    object_storage_cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableEvent>) {
    let path = temp_dir.path().to_path_buf();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let identity_property = mooncake_table_metadata.identity.clone();
    let iceberg_table_config = get_iceberg_table_config(temp_dir);
    let schema = create_test_arrow_schema();

    // Create iceberg snapshot whenever `create_snapshot` is called.
    let mooncake_table_config = MooncakeTableConfig {
        persistence_config: IcebergPersistenceConfig {
            new_data_file_count: 0,
            ..Default::default()
        },
        // Trigger compaction as long as there're two data files.
        data_compaction_config: DataCompactionConfig {
            data_file_final_size: u64::MAX,
            data_file_to_compact: 2,
        },
        ..Default::default()
    };

    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        ICEBERG_TEST_TABLE.to_string(),
        /*version=*/ TEST_TABLE_ID.0,
        path,
        identity_property,
        iceberg_table_config.clone(),
        mooncake_table_config,
        object_storage_cache,
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();

    let (notify_tx, notify_rx) = mpsc::channel(100);
    table.register_table_notify(notify_tx).await;

    (table, notify_rx)
}

/// Test util function to create mooncake table and table notify.
pub(crate) async fn create_mooncake_table_and_notify(
    mooncake_table_metadata: Arc<MooncakeTableMetadata>,
    iceberg_table_config: IcebergTableConfig,
    object_storage_cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableEvent>) {
    let mut table = MooncakeTable::new(
        create_test_arrow_schema().as_ref().clone(),
        ICEBERG_TEST_TABLE.to_string(),
        /*version=*/ TEST_TABLE_ID.0,
        mooncake_table_metadata.path.clone(),
        mooncake_table_metadata.identity.clone(),
        iceberg_table_config.clone(),
        mooncake_table_metadata.config.clone(),
        object_storage_cache,
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();

    let (notify_tx, notify_rx) = mpsc::channel(100);
    table.register_table_notify(notify_tx).await;

    (table, notify_rx)
}

/// Test util function to create mooncake table and table notify for read test.
pub(crate) async fn create_mooncake_table_and_notify_for_read(
    temp_dir: &TempDir,
    object_storage_cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableEvent>) {
    let path = temp_dir.path().to_path_buf();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let identity_property = mooncake_table_metadata.identity.clone();

    let iceberg_table_config = get_iceberg_table_config(temp_dir);
    let schema = create_test_arrow_schema();

    // Create iceberg snapshot whenever `create_snapshot` is called.
    let mooncake_table_config = MooncakeTableConfig {
        persistence_config: IcebergPersistenceConfig {
            new_data_file_count: 0,
            ..Default::default()
        },
        ..Default::default()
    };

    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        ICEBERG_TEST_TABLE.to_string(),
        /*version=*/ TEST_TABLE_ID.0,
        path,
        identity_property,
        iceberg_table_config.clone(),
        mooncake_table_config,
        object_storage_cache,
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();

    let (notify_tx, notify_rx) = mpsc::channel(100);
    table.register_table_notify(notify_tx).await;

    (table, notify_rx)
}

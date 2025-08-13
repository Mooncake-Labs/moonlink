/// This module contains table creation tests utils.
use crate::row::IdentityProp as RowIdentity;
use crate::storage::compaction::compaction_config::DataCompactionConfig;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::factory::create_filesystem_accessor;
use crate::storage::filesystem::accessor_config::AccessorConfig;
#[cfg(feature = "storage-gcs")]
use crate::storage::filesystem::gcs::gcs_test_utils;
#[cfg(feature = "storage-s3")]
use crate::storage::filesystem::s3::s3_test_utils;
use crate::storage::iceberg::iceberg_table_config::IcebergTableConfig;
use crate::storage::iceberg::iceberg_table_manager::IcebergTableManager;
#[cfg(feature = "chaos-test")]
use crate::storage::index::index_merge_config::FileIndexMergeConfig;
use crate::storage::mooncake_table::test_utils_commons::*;
use crate::storage::mooncake_table::{MooncakeTableConfig, TableMetadata as MooncakeTableMetadata};
use crate::storage::mooncake_table_config::IcebergPersistenceConfig;
use crate::storage::wal::test_utils::WAL_TEST_TABLE_ID;
use crate::storage::wal::WalManager;
use crate::storage::MooncakeTable;
use crate::table_notify::TableEvent;
#[cfg(feature = "chaos-test")]
use crate::DiskSliceWriterConfig;
use crate::ObjectStorageCache;
use crate::StorageConfig;
use crate::WalConfig;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::datatypes::{DataType, Field};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;

impl MooncakeTable {
    #[cfg(test)]
    pub(crate) fn set_iceberg_snapshot_lsn(&mut self, lsn: u64) {
        self.last_iceberg_snapshot_lsn = Some(lsn);
    }
}

/// Test util function to get iceberg table config for local filesystem.
pub(crate) fn get_iceberg_table_config(temp_dir: &TempDir) -> IcebergTableConfig {
    let root_directory = temp_dir.path().to_str().unwrap().to_string();
    let storage_config = StorageConfig::FileSystem {
        root_directory,
        atomic_write_dir: None,
    };
    let accessor_config = AccessorConfig::new_with_storage_config(storage_config);
    IcebergTableConfig {
        namespace: vec![ICEBERG_TEST_NAMESPACE.to_string()],
        table_name: ICEBERG_TEST_TABLE.to_string(),
        accessor_config,
    }
}

/// Test util function to get iceberg table config from filesystem config.
#[cfg(feature = "chaos-test")]
pub(crate) fn get_iceberg_table_config_with_storage_config(
    storage_config: StorageConfig,
) -> IcebergTableConfig {
    let accessor_config = AccessorConfig::new_with_storage_config(storage_config);
    IcebergTableConfig {
        namespace: vec![ICEBERG_TEST_NAMESPACE.to_string()],
        table_name: ICEBERG_TEST_TABLE.to_string(),
        accessor_config,
    }
}

/// Test util function with error injection at filesystem layer.
#[cfg(feature = "chaos-test")]
pub(crate) fn get_iceberg_table_config_with_chaos_injection(
    storage_config: StorageConfig,
    random_seed: u64,
) -> IcebergTableConfig {
    use crate::storage::filesystem::accessor_config::{ChaosConfig, RetryConfig, TimeoutConfig};

    let chaos_config = ChaosConfig {
        random_seed: Some(random_seed),
        min_latency: std::time::Duration::from_secs(0),
        max_latency: std::time::Duration::from_secs(1),
        err_prob: 5, // 5% error probability, a few retry attempts should work
    };
    let accessor_config = AccessorConfig {
        storage_config,
        retry_config: RetryConfig::default(),
        timeout_config: TimeoutConfig::default(),
        chaos_config: Some(chaos_config),
    };
    IcebergTableConfig {
        namespace: vec![ICEBERG_TEST_NAMESPACE.to_string()],
        table_name: ICEBERG_TEST_TABLE.to_string(),
        accessor_config,
    }
}

/// Test util function to create iceberg table config.
pub(crate) fn create_iceberg_table_config(warehouse_uri: String) -> IcebergTableConfig {
    let accessor_config = if warehouse_uri.starts_with("s3://") {
        #[cfg(feature = "storage-s3")]
        {
            s3_test_utils::create_s3_storage_config(&warehouse_uri)
        }
        #[cfg(not(feature = "storage-s3"))]
        {
            panic!("S3 support not enabled. Enable `storage-s3` feature.");
        }
    } else if warehouse_uri.starts_with("gs://") {
        #[cfg(feature = "storage-gcs")]
        {
            gcs_test_utils::create_gcs_storage_config(&warehouse_uri)
        }
        #[cfg(not(feature = "storage-gcs"))]
        {
            panic!("GCS support not enabled. Enable `storage-gcs` feature.");
        }
    } else {
        let storage_config = StorageConfig::FileSystem {
            root_directory: warehouse_uri.clone(),
            atomic_write_dir: None,
        };
        AccessorConfig::new_with_storage_config(storage_config)
    };

    IcebergTableConfig {
        accessor_config,
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
pub(crate) fn create_test_updated_arrow_schema_remove_age() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "0".to_string(),
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
    create_filesystem_accessor(iceberg_table_config.accessor_config.clone())
}

/// Test util function to create mooncake table metadata.
pub(crate) fn create_test_table_metadata(
    local_table_directory: String,
) -> Arc<MooncakeTableMetadata> {
    let config = MooncakeTableConfig::new(local_table_directory.clone());
    create_test_table_metadata_with_config(local_table_directory, config)
}

/// Test util function to create mooncake table with the provided config and identity.
#[cfg(feature = "chaos-test")]
pub(crate) fn create_test_table_metadata_with_config_and_identity(
    local_table_directory: String,
    mooncake_table_config: MooncakeTableConfig,
    identity: RowIdentity,
) -> Arc<MooncakeTableMetadata> {
    Arc::new(MooncakeTableMetadata {
        name: ICEBERG_TEST_TABLE.to_string(),
        table_id: 0,
        schema: create_test_arrow_schema(),
        config: mooncake_table_config,
        path: std::path::PathBuf::from(local_table_directory),
        identity,
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

/// Test util function to get random row identity.
#[cfg(feature = "chaos-test")]
pub(crate) fn get_random_identity(random_seed: u64) -> RowIdentity {
    use rand::{seq::IndexedRandom, SeedableRng};
    let mut rng = rand::rngs::StdRng::seed_from_u64(random_seed);

    let identities = [
        RowIdentity::SinglePrimitiveKey(0),
        RowIdentity::Keys(vec![0]),
        RowIdentity::FullRow,
    ];
    identities.choose(&mut rng).unwrap().clone()
}

/// Test util function to create disk slice write option.
#[cfg(feature = "chaos-test")]
pub(crate) fn create_disk_slice_write_option(
    enable_disk_slice_write_chaos: bool,
    random_seed: u64,
) -> DiskSliceWriterConfig {
    use crate::storage::filesystem::accessor_config::ChaosConfig;

    let chaos_config = if enable_disk_slice_write_chaos {
        Some(ChaosConfig {
            random_seed: Some(random_seed),
            err_prob: 0,
            // Injected latency is comparable to mooncake snapshot interval.
            min_latency: std::time::Duration::from_millis(50),
            max_latency: std::time::Duration::from_millis(750),
        })
    } else {
        None
    };

    DiskSliceWriterConfig {
        parquet_file_size: DiskSliceWriterConfig::DEFAULT_DISK_SLICE_PARQUET_FILE_SIZE,
        chaos_config,
    }
}

/// Test util function to create mooncake table metadata, which disables flush at commit.
#[cfg(feature = "chaos-test")]
pub(crate) fn create_test_table_metadata_disable_flush(
    local_table_directory: String,
    disk_slice_write_config: DiskSliceWriterConfig,
    identity: RowIdentity,
) -> Arc<MooncakeTableMetadata> {
    let mut config = MooncakeTableConfig::new(local_table_directory.clone());
    config.mem_slice_size = usize::MAX; // Disable flush at commit if not force flush.
    config.disk_slice_writer_config = disk_slice_write_config;
    create_test_table_metadata_with_config_and_identity(local_table_directory, config, identity)
}

/// Test util function to create mooncake table metadata, with (1) index merge enabled whenever there're two index blocks; and (2) flush at commit is disabled.
#[cfg(feature = "chaos-test")]
pub(crate) fn create_test_table_metadata_with_index_merge_disable_flush(
    local_table_directory: String,
    disk_slice_write_config: DiskSliceWriterConfig,
    identity: RowIdentity,
) -> Arc<MooncakeTableMetadata> {
    let file_index_config = FileIndexMergeConfig {
        min_file_indices_to_merge: 2,
        max_file_indices_to_merge: u32::MAX,
        index_block_final_size: u64::MAX,
    };
    let mut config = MooncakeTableConfig::new(local_table_directory.clone());
    config.disk_slice_writer_config = disk_slice_write_config;
    config.file_index_config = file_index_config;
    config.mem_slice_size = usize::MAX; // Disable flush at commit if not force flush.
    create_test_table_metadata_with_config_and_identity(local_table_directory, config, identity)
}

/// Test util function to create mooncake table metadata, with (1) data compaction enabled whenever there're two index blocks; and (2) flush at commit is disabled.
#[cfg(feature = "chaos-test")]
pub(crate) fn create_test_table_metadata_with_data_compaction_disable_flush(
    local_table_directory: String,
    disk_slice_write_config: DiskSliceWriterConfig,
    identity: RowIdentity,
) -> Arc<MooncakeTableMetadata> {
    let data_compaction_config = DataCompactionConfig {
        min_data_file_to_compact: 2,
        max_data_file_to_compact: u32::MAX,
        data_file_final_size: u64::MAX,
        data_file_deletion_percentage: 0,
    };
    let mut config = MooncakeTableConfig::new(local_table_directory.clone());
    config.disk_slice_writer_config = disk_slice_write_config;
    config.data_compaction_config = data_compaction_config;
    config.mem_slice_size = usize::MAX; // Disable flush at commit if not force flush.
    create_test_table_metadata_with_config_and_identity(local_table_directory, config, identity)
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
    let wal_config = WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &path);
    let wal_manager = WalManager::new(&wal_config);

    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        ICEBERG_TEST_TABLE.to_string(),
        /*table_id=*/ 1,
        path,
        identity_property,
        iceberg_table_config.clone(),
        mooncake_table_config,
        wal_manager,
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
            min_data_file_to_compact: 2,
            max_data_file_to_compact: u32::MAX,
            data_file_deletion_percentage: 0,
        },
        ..Default::default()
    };
    let wal_config = WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &path);
    let wal_manager = WalManager::new(&wal_config);

    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        ICEBERG_TEST_TABLE.to_string(),
        /*version=*/ TEST_TABLE_ID.0,
        path,
        identity_property,
        iceberg_table_config.clone(),
        mooncake_table_config,
        wal_manager,
        object_storage_cache,
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();

    let (notify_tx, notify_rx) = mpsc::channel(100);
    table.register_table_notify(notify_tx).await;

    (table, notify_rx)
}

/// Test util function to create mooncake table.
pub(crate) async fn create_mooncake_table(
    mooncake_table_metadata: Arc<MooncakeTableMetadata>,
    iceberg_table_config: IcebergTableConfig,
    object_storage_cache: ObjectStorageCache,
) -> MooncakeTable {
    let wal_config =
        WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &mooncake_table_metadata.path);
    let wal_manager = WalManager::new(&wal_config);
    let table = MooncakeTable::new(
        create_test_arrow_schema().as_ref().clone(),
        ICEBERG_TEST_TABLE.to_string(),
        /*version=*/ TEST_TABLE_ID.0,
        mooncake_table_metadata.path.clone(),
        mooncake_table_metadata.identity.clone(),
        iceberg_table_config.clone(),
        mooncake_table_metadata.config.clone(),
        wal_manager,
        object_storage_cache,
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();

    table
}

/// Test util function to create mooncake table and table notify.
pub(crate) async fn create_mooncake_table_and_notify(
    mooncake_table_metadata: Arc<MooncakeTableMetadata>,
    iceberg_table_config: IcebergTableConfig,
    object_storage_cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableEvent>) {
    let mut table = create_mooncake_table(
        mooncake_table_metadata,
        iceberg_table_config,
        object_storage_cache,
    )
    .await;
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
    let wal_config = WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &path);
    let wal_manager = WalManager::new(&wal_config);

    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        ICEBERG_TEST_TABLE.to_string(),
        /*version=*/ TEST_TABLE_ID.0,
        path,
        identity_property,
        iceberg_table_config.clone(),
        mooncake_table_config,
        wal_manager,
        object_storage_cache,
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();

    let (notify_tx, notify_rx) = mpsc::channel(100);
    table.register_table_notify(notify_tx).await;

    (table, notify_rx)
}

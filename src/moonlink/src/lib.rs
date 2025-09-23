pub mod error;
pub mod event_sync;
pub mod mooncake_table_id;
pub mod row;
mod storage;
pub(crate) mod table_handler;
pub mod table_handler_timer;
pub(crate) mod table_notify;
mod union_read;

pub use error::*;
pub use event_sync::EventSyncSender;
pub use mooncake_table_id::MooncakeTableId;
pub use storage::mooncake_table::batch_id_counter::BatchIdCounter;
pub use storage::mooncake_table::data_batches::ColumnStoreBuffer;
pub use storage::parquet_utils::get_default_parquet_properties;
pub use storage::storage_utils::create_data_file;
#[cfg(feature = "catalog-glue")]
pub use storage::IcebergGlueCatalogConfig;
#[cfg(feature = "catalog-rest")]
pub use storage::IcebergRestCatalogConfig;
pub(crate) use storage::NonEvictableHandle;
pub use storage::{
    AccessorConfig, AwsSecurityConfig, BaseFileSystemAccess, BaseIcebergSnapshotFetcher,
    CacheTrait, CloudSecurityConfig, DataCompactionConfig, DiskSliceWriterConfig,
    EventSyncReceiver, FileIndexMergeConfig, FileSystemAccessor, FsChaosConfig, FsRetryConfig,
    FsThrottleConfig, FsTimeoutConfig, IcebergCatalogConfig, IcebergFileCatalogConfig,
    IcebergPersistenceConfig, IcebergSnapshotFetcher, IcebergTableConfig, IcebergTableManager,
    MooncakeTable, MooncakeTableConfig, MoonlinkSecretType, MoonlinkTableConfig,
    MoonlinkTableSecret, ObjectStorageCache, ObjectStorageCacheConfig, PersistentWalMetadata,
    SnapshotReadOutput, StorageConfig, TableEventManager, TableManager, TableSnapshotStatus,
    TableStatusReader, WalConfig, WalManager, WalTransactionState,
};
pub use table_handler::TableHandler;
pub use table_handler_timer::TableHandlerTimer;
pub use table_notify::TableEvent;
pub use union_read::{ReadState, ReadStateFilepathRemap, ReadStateManager};

#[cfg(any(test, feature = "test-utils"))]
pub use union_read::{decode_read_state_for_testing, decode_serialized_read_state_for_testing};

#[cfg(feature = "bench")]
pub use storage::GlobalIndex;
#[cfg(feature = "bench")]
pub use storage::GlobalIndexBuilder;

#[cfg(feature = "profile-test")]
pub use table_handler::profile_test::{
    test_append_only_table_profile_on_local_fs, test_no_iceberg_persistence_on_local_fs,
    test_normal_profile_on_local_fs,
};

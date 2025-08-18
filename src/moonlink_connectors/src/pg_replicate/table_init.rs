use crate::pg_replicate::replication_state::ReplicationState;
use crate::pg_replicate::table::SrcTableId;
use crate::{Error, Result};
use arrow_schema::Schema as ArrowSchema;
use moonlink::event_sync::create_table_event_syncer;
use moonlink::table_handler_timer::create_table_handler_timers;
use moonlink::PersistentWalMetadata;
use moonlink::ReadStateFilepathRemap;
use moonlink::{
    row::IdentityProp, AccessorConfig, BaseFileSystemAccess, EventSyncReceiver, EventSyncSender,
    FileSystemAccessor, IcebergTableConfig, MooncakeTable, MooncakeTableConfig, MoonlinkSecretType,
    MoonlinkTableConfig, MoonlinkTableSecret, ObjectStorageCache, ReadStateManager, StorageConfig,
    TableEvent, TableEventManager, TableHandler, TableStatusReader, WalConfig, WalManager,
};

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, mpsc::Sender, oneshot, watch};

/// Used to assign unique monotonically increasing id to mooncake table.
static NEXT_TABLE_ID: AtomicU32 = AtomicU32::new(0);

/// Get the next unique table id to use.
#[inline]
pub fn get_next_table_id() -> u32 {
    NEXT_TABLE_ID.fetch_add(1, Ordering::SeqCst)
}

/// Components required to create a mooncake table.
pub struct TableComponents {
    /// Functor used to remap filepaths at read state.
    pub read_state_filepath_remap: ReadStateFilepathRemap,
    /// Shared object storage cache for all mooncake tables.
    pub object_storage_cache: ObjectStorageCache,
    /// Mooncake table configuration.
    pub moonlink_table_config: MoonlinkTableConfig,
}

/// Resources that should be returned to the caller when a table is initialised.
pub struct TableResources {
    pub event_sender: Sender<TableEvent>,
    pub read_state_manager: ReadStateManager,
    pub table_event_manager: TableEventManager,
    pub table_status_reader: TableStatusReader,
    pub commit_lsn_tx: Option<watch::Sender<u64>>,
    pub flush_lsn_rx: Option<watch::Receiver<u64>>,
    pub wal_flush_lsn_rx: Option<watch::Receiver<u64>>,
    pub wal_file_accessor: Arc<dyn BaseFileSystemAccess>,
    pub wal_persistence_metadata: Option<PersistentWalMetadata>,
    pub last_iceberg_snapshot_lsn: Option<u64>,
}

/// Util function to delete and re-create the given directory.
async fn recreate_directory(dir: &PathBuf) -> Result<()> {
    // Clean up directory to place moonlink temporary files.
    match tokio::fs::remove_dir_all(dir).await {
        Ok(()) => {}
        Err(e) => {
            if e.kind() != ErrorKind::NotFound {
                return Err(e.into());
            }
        }
    }
    tokio::fs::create_dir_all(dir).await?;

    Ok(())
}

/// Build all components needed to replicate `table_schema`.
pub async fn build_table_components(
    mooncake_table_id: String,
    arrow_schema: ArrowSchema,
    mut identity: IdentityProp,
    src_table_name: String,
    src_table_id: SrcTableId,
    base_path: &str,
    replication_state: &ReplicationState,
    table_components: TableComponents,
    is_recovery: bool,
) -> Result<TableResources> {
    // Override identity to None if append_only is enabled
    if table_components
        .moonlink_table_config
        .mooncake_table_config
        .append_only
    {
        identity = IdentityProp::None;
    }
    // Recreate write-through cache directory.
    let write_cache_path = PathBuf::from(base_path).join(&mooncake_table_id);
    recreate_directory(&write_cache_path).await?;
    // Make sure temporary directory exists.
    tokio::fs::create_dir_all(
        &table_components
            .moonlink_table_config
            .mooncake_table_config
            .temp_files_directory,
    )
    .await?;

    let wal_config =
        WalConfig::default_wal_config_local(&mooncake_table_id, &PathBuf::from(base_path));
    let wal_file_accessor = Arc::new(FileSystemAccessor::new(
        wal_config.get_accessor_config().clone(),
    ));

    let wal_persistence_metadata = {
        if is_recovery {
            let recovered_wal_metadata = WalManager::recover_from_persistent_wal_metadata(
                wal_file_accessor.clone(),
                wal_config.clone(),
            )
            .await;
            recovered_wal_metadata
        } else {
            None
        }
    };

    let wal_manager = if let Some(wal_persistence_metadata) = wal_persistence_metadata.clone() {
        WalManager::from_persistent_wal_metadata(
            wal_file_accessor.clone(),
            wal_persistence_metadata,
            wal_config.clone(),
        )
    } else {
        WalManager::new(&wal_config)
    };

    let table = MooncakeTable::new(
        arrow_schema,
        mooncake_table_id,
        get_next_table_id(),
        write_cache_path,
        identity,
        table_components
            .moonlink_table_config
            .iceberg_table_config
            .clone(),
        table_components
            .moonlink_table_config
            .mooncake_table_config
            .clone(),
        wal_manager,
        Arc::new(table_components.object_storage_cache),
        Arc::new(FileSystemAccessor::new(
            table_components
                .moonlink_table_config
                .iceberg_table_config
                .accessor_config
                .clone(),
        )),
    )
    .await?;

    let last_iceberg_snapshot_lsn = table.get_iceberg_snapshot_lsn();

    let (commit_lsn_tx, commit_lsn_rx) = watch::channel(0u64);
    let read_state_manager = ReadStateManager::new(
        &table,
        replication_state.subscribe(),
        commit_lsn_rx,
        table_components.read_state_filepath_remap,
    );
    let table_status_reader = TableStatusReader::new(
        &table_components.moonlink_table_config.iceberg_table_config,
        &table,
    );
    let (event_sync_sender, event_sync_receiver) = create_table_event_syncer();
    let table_handler_timers = create_table_handler_timers();
    let table_handler = TableHandler::new(
        table,
        event_sync_sender,
        table_handler_timers,
        replication_state.subscribe(),
        /*event_replay_tx=*/ None,
        /*table_event_replay_tx=*/ None,
    )
    .await;
    let flush_lsn_rx = event_sync_receiver.flush_lsn_rx.clone();
    let wal_flush_lsn_rx = event_sync_receiver.wal_flush_lsn_rx.clone();
    let table_event_manager =
        TableEventManager::new(table_handler.get_event_sender(), event_sync_receiver);
    let event_sender = table_handler.get_event_sender();

    let table_resource = TableResources {
        event_sender,
        read_state_manager,
        table_status_reader,
        table_event_manager,
        commit_lsn_tx: Some(commit_lsn_tx),
        flush_lsn_rx: Some(flush_lsn_rx),
        wal_flush_lsn_rx: Some(wal_flush_lsn_rx),
        wal_file_accessor,
        wal_persistence_metadata,
        last_iceberg_snapshot_lsn,
    };
    Ok(table_resource)
}

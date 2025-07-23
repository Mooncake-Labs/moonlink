use crate::pg_replicate::replication_state::ReplicationState;
use crate::pg_replicate::table::TableSchema;
use crate::pg_replicate::util::postgres_schema_to_moonlink_schema;
use crate::{Error, Result};
use moonlink::event_sync::create_table_event_syncer;
use moonlink::{
    EventSyncReceiver, EventSyncSender, FileSystemAccessor, FileSystemConfig, IcebergTableConfig,
    MooncakeTable, MooncakeTableConfig, MoonlinkSecretType, MoonlinkTableConfig,
    MoonlinkTableSecret, ObjectStorageCache, ReadStateManager, TableEvent, TableEventManager,
    TableHandler, TableStateReader,
};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, mpsc::Sender, oneshot, watch};

/// Components required to replicate a single table.
/// Components that the [`Sink`] needs for processing CDC events.
pub struct TableComponents {
    pub event_sender: Sender<TableEvent>,
}

/// Resources that should be returned to the caller when a table is initialised.
pub struct TableResources {
    pub event_sender: Sender<TableEvent>,
    pub read_state_manager: ReadStateManager,
    pub table_event_manager: TableEventManager,
    pub table_state_reader: TableStateReader,
    pub commit_lsn_tx: watch::Sender<u64>,
    pub flush_lsn_rx: watch::Receiver<u64>,
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
    table_id: u32,
    table_schema: &TableSchema,
    base_path: &String,
    table_temp_files_directory: String,
    replication_state: &ReplicationState,
    object_storage_cache: ObjectStorageCache,
    iceberg_filesystem_config: Option<FileSystemConfig>,
) -> Result<(TableResources, MoonlinkTableConfig)> {
    let write_cache_path = PathBuf::from(base_path).join(&mooncake_table_id);
    recreate_directory(&write_cache_path).await?;
    let (arrow_schema, identity) = postgres_schema_to_moonlink_schema(table_schema);
    let iceberg_filesystem_config =
        iceberg_filesystem_config.unwrap_or(FileSystemConfig::FileSystem {
            root_directory: base_path.to_string(),
        });

    let iceberg_table_config = IcebergTableConfig {
        namespace: vec![table_schema.table_name.schema.clone()],
        table_name: mooncake_table_id,
        filesystem_config: iceberg_filesystem_config.clone(),
    };
    let mooncake_table_config = MooncakeTableConfig::new(table_temp_files_directory);
    let table = MooncakeTable::new(
        arrow_schema,
        table_schema.table_name.to_string(),
        table_id,
        write_cache_path,
        identity,
        iceberg_table_config.clone(),
        mooncake_table_config.clone(),
        object_storage_cache,
        Arc::new(FileSystemAccessor::new(iceberg_filesystem_config)),
    )
    .await?;

    let (commit_lsn_tx, commit_lsn_rx) = watch::channel(0u64);
    let read_state_manager =
        ReadStateManager::new(&table, replication_state.subscribe(), commit_lsn_rx);
    let table_state_reader = TableStateReader::new(table_id, &iceberg_table_config, &table);
    let (event_sync_sender, event_sync_receiver) = create_table_event_syncer();
    let table_handler = TableHandler::new(
        table,
        event_sync_sender,
        replication_state.subscribe(),
        /*event_replay_tx=*/ None,
    )
    .await;
    let flush_lsn_rx = event_sync_receiver.flush_lsn_rx.clone();
    let table_event_manager =
        TableEventManager::new(table_handler.get_event_sender(), event_sync_receiver);
    let event_sender = table_handler.get_event_sender();

    let table_resource = TableResources {
        event_sender,
        read_state_manager,
        table_state_reader,
        table_event_manager,
        commit_lsn_tx,
        flush_lsn_rx,
    };
    let moonlink_table_config = MoonlinkTableConfig {
        mooncake_table_config,
        iceberg_table_config,
    };

    Ok((table_resource, moonlink_table_config))
}

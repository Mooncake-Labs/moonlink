use crate::row::MoonlinkRow;
use crate::row::RowValue;
use crate::storage::cache::object_storage::cache_config::ObjectStorageCacheConfig;
use crate::storage::cache::object_storage::object_storage_cache::ObjectStorageCache;
use crate::storage::mooncake_table::replay::replay_events::MooncakeTableEvent;
use crate::storage::mooncake_table::snapshot::MooncakeSnapshotOutput;
use crate::storage::mooncake_table::DataCompactionResult;
use crate::storage::mooncake_table::DiskSliceWriter;
use crate::storage::mooncake_table::MooncakeTable;
use crate::storage::mooncake_table::TableMetadata;
use crate::storage::mooncake_table::{
    table_creation_test_utils::*, FileIndiceMergeResult, IcebergSnapshotResult,
};
use crate::storage::mooncake_table_config::DiskSliceWriterConfig;
use crate::table_handler::chaos_table_metadata::ReplayTableMetadata;
use crate::table_handler::test_utils::check_read_snapshot;
use crate::table_notify::{TableEvent, TableMaintenanceStatus};
use crate::IcebergTableConfig;
use crate::MooncakeTableConfig;
use crate::ReadStateManager;
use crate::{Result, StorageConfig};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tempfile::{tempdir, TempDir};
use tokio::io::AsyncBufReadExt;
use tokio::sync::watch;
use tokio::sync::Notify;
use tokio::sync::{mpsc, Mutex};

#[derive(Clone, Debug)]
struct CompletedFlush {
    // Transaction ID.
    xact_id: Option<u32>,
    /// Result for mem slice flush.
    flush_result: Option<Result<DiskSliceWriter>>,
}
#[derive(Clone, Debug)]
struct CompletedMooncakeSnapshot {
    /// Mooncake snapshot result.
    mooncake_snapshot_result: MooncakeSnapshotOutput,
}
#[derive(Clone, Debug)]
struct CompletedIcebergSnapshot {
    /// Result of iceberg snapshot.
    iceberg_snapshot_result: IcebergSnapshotResult,
}

#[derive(Clone, Debug)]
struct CompletedIndexMerge {
    /// Result for index merge.
    index_merge_result: FileIndiceMergeResult,
}

#[derive(Clone, Debug)]
struct CompletedDataCompaction {
    /// Result for data compaction.
    data_compaction_result: DataCompactionResult,
}

struct ReplayEnvironment {
    cache_temp_dir: TempDir,
    table_temp_dir: TempDir,
    iceberg_temp_dir: TempDir,
}

fn create_disk_writer_config() -> DiskSliceWriterConfig {
    DiskSliceWriterConfig::default()
}

/// Util function to get id from the given moonlink row.
fn get_id_from_row(row: &MoonlinkRow) -> i32 {
    let val = &row.values[0];
    match val {
        RowValue::Int32(id) => *id,
        _ => panic!("First element should be int32"),
    }
}

async fn create_mooncake_table_for_replay(
    replay_env: &ReplayEnvironment,
    lines: &mut tokio::io::Lines<tokio::io::BufReader<tokio::fs::File>>,
) -> (
    Arc<TableMetadata>,
    IcebergTableConfig,
    MooncakeTable,
    bool, /*is_upsert_table*/
) {
    let line = lines.next_line().await.unwrap().unwrap();
    let replay_table_metadata: ReplayTableMetadata = serde_json::from_str(&line).unwrap();
    let local_table_directory = replay_env
        .table_temp_dir
        .path()
        .to_str()
        .unwrap()
        .to_string();
    let mut mooncake_table_config = MooncakeTableConfig::new(local_table_directory.clone());
    mooncake_table_config.mem_slice_size = usize::MAX; // Disable flush at commit if not force flush.
    mooncake_table_config.append_only = replay_table_metadata.config.append_only;
    mooncake_table_config.disk_slice_writer_config = create_disk_writer_config();
    mooncake_table_config.file_index_config = replay_table_metadata.config.file_index_config;
    mooncake_table_config.data_compaction_config =
        replay_table_metadata.config.data_compaction_config;
    mooncake_table_config.row_identity = replay_table_metadata.config.row_identity;

    let table_metadata =
        create_test_table_metadata_with_config(local_table_directory, mooncake_table_config);
    let object_storage_cache = if replay_table_metadata.local_filesystem_optimization_enabled {
        let config = ObjectStorageCacheConfig::new(
            /*max_bytes=*/ 1 << 30, // 1GiB
            replay_env
                .cache_temp_dir
                .path()
                .to_str()
                .unwrap()
                .to_string(),
            /*optimize_local_filesystem=*/ true,
        );
        ObjectStorageCache::new(config)
    } else {
        ObjectStorageCache::default_for_test(&replay_env.cache_temp_dir)
    };
    // TODO(hjiang): Need to support remote storage and random bucket.
    let storage_config = StorageConfig::FileSystem {
        root_directory: replay_env
            .iceberg_temp_dir
            .path()
            .to_str()
            .unwrap()
            .to_string(),
        atomic_write_dir: None,
    };
    let iceberg_table_config = get_iceberg_table_config_with_storage_config(storage_config);
    let mooncake_table = create_mooncake_table(
        table_metadata.clone(),
        iceberg_table_config.clone(),
        Arc::new(object_storage_cache),
    )
    .await;

    (
        table_metadata,
        iceberg_table_config,
        mooncake_table,
        replay_table_metadata.is_upsert_table,
    )
}

/// Test util function to check whether iceberg snapshot contains expected content.
async fn validate_persisted_iceberg_table(
    mooncake_table_metadata: Arc<TableMetadata>,
    iceberg_table_config: IcebergTableConfig,
    snapshot_lsn: u64,
    expected_ids: Vec<i32>,
) {
    let (event_sender, _event_receiver) = mpsc::channel(100);
    let (replication_lsn_tx, replication_lsn_rx) = watch::channel(0u64);
    let (last_commit_lsn_tx, last_commit_lsn_rx) = watch::channel(0u64);
    replication_lsn_tx.send(snapshot_lsn).unwrap();
    last_commit_lsn_tx.send(snapshot_lsn).unwrap();

    // Use a fresh new cache for new iceberg table manager.
    let cache_temp_dir = tempdir().unwrap();
    let object_storage_cache = create_test_object_storage_cache(&cache_temp_dir);

    let mut table = create_mooncake_table(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        object_storage_cache,
    )
    .await;
    table.register_table_notify(event_sender).await;

    let read_state_filepath_remap = std::sync::Arc::new(|local_filepath: String| local_filepath);
    let read_state_manager = ReadStateManager::new(
        &table,
        replication_lsn_rx.clone(),
        last_commit_lsn_rx,
        read_state_filepath_remap,
    );
    check_read_snapshot(
        &read_state_manager,
        Some(snapshot_lsn),
        /*expected_ids=*/ &expected_ids,
    )
    .await;
}

pub(crate) async fn replay(replay_filepath: &str) {
    let cache_temp_dir = tempdir().unwrap();
    let table_temp_dir = tempdir().unwrap();
    let iceberg_temp_dir = tempdir().unwrap();
    let replay_env = ReplayEnvironment {
        cache_temp_dir,
        table_temp_dir,
        iceberg_temp_dir,
    };

    // Table event reader.
    let file = tokio::fs::File::open(replay_filepath).await.unwrap();
    let buf_reader = tokio::io::BufReader::new(file);
    let mut lines: tokio::io::Lines<tokio::io::BufReader<tokio::fs::File>> = buf_reader.lines();

    // Used to notify certain background table events have completed.
    let event_notification = Arc::new(Notify::new());
    let event_notification_clone = event_notification.clone();

    // Current table states.
    let mut ongoing_flush_event_id = HashSet::new();
    let completed_flush_events: Arc<Mutex<HashMap<uuid::Uuid, CompletedFlush>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let completed_flush_events_clone = completed_flush_events.clone();

    let mut ongoing_mooncake_snapshot_id = HashSet::new();
    let completed_mooncake_snapshots: Arc<Mutex<HashMap<uuid::Uuid, CompletedMooncakeSnapshot>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let completed_mooncake_snapshots_clone = completed_mooncake_snapshots.clone();

    let mut ongoing_iceberg_snapshot_id = HashSet::new();
    let completed_iceberg_snapshots: Arc<Mutex<HashMap<uuid::Uuid, CompletedIcebergSnapshot>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let completed_iceberg_snapshots_clone = completed_iceberg_snapshots.clone();

    let mut ongoing_index_merge_id = HashSet::new();
    let completed_index_merge: Arc<Mutex<HashMap<uuid::Uuid, CompletedIndexMerge>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let completed_index_merge_clone = completed_index_merge.clone();

    let mut ongoing_data_compaction_id = HashSet::new();
    let completed_data_compaction: Arc<Mutex<HashMap<uuid::Uuid, CompletedDataCompaction>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let completed_data_compaction_clone = completed_data_compaction.clone();

    // Pending background tasks to issue.
    // Maps from event id to payload.
    let pending_iceberg_snapshot_payloads = Arc::new(Mutex::new(HashMap::new()));
    let pending_index_merge_payloads = Arc::new(Mutex::new(HashMap::new()));
    let pending_data_compaction_payloads = Arc::new(Mutex::new(HashMap::new()));
    let pending_iceberg_snapshot_payloads_clone = pending_iceberg_snapshot_payloads.clone();
    let pending_index_merge_payloads_clone = pending_index_merge_payloads.clone();
    let pending_data_compaction_payloads_clone = pending_data_compaction_payloads.clone();

    // Maps from file id to data filepath.
    let data_files = Arc::new(Mutex::new(HashMap::new()));
    let data_files_clone = data_files.clone();

    let (table_metadata, iceberg_table_config, mut table, is_upsert_table) =
        create_mooncake_table_for_replay(&replay_env, &mut lines).await;
    let (table_event_sender, mut table_event_receiver) = mpsc::channel(100);
    let (event_replay_sender, _event_replay_receiver) = mpsc::unbounded_channel();
    table.register_table_notify(table_event_sender).await;
    table.register_event_replay_tx(Some(event_replay_sender));
    let (commit_lsn_tx, commit_lsn_rx) = watch::channel(0u64);
    let (replication_lsn_tx, replication_lsn_rx) = watch::channel(0u64);
    let read_state_filepath_remap = std::sync::Arc::new(|local_filepath: String| local_filepath);
    let read_state_manager = ReadStateManager::new(
        &table,
        replication_lsn_rx,
        commit_lsn_rx,
        read_state_filepath_remap,
    );

    // Start a background thread which continuously read from event receiver.
    tokio::spawn(async move {
        while let Some(table_event) = table_event_receiver.recv().await {
            #[allow(clippy::single_match)]
            match table_event {
                TableEvent::FlushResult {
                    event_id,
                    xact_id,
                    flush_result,
                } => {
                    {
                        let data_files = flush_result
                            .as_ref()
                            .unwrap()
                            .as_ref()
                            .unwrap()
                            .output_files();
                        let mut guard = data_files_clone.lock().await;
                        for (data_file_id, _) in data_files.iter() {
                            assert!(guard
                                .insert(data_file_id.file_id(), data_file_id.file_path().clone())
                                .is_none());
                        }
                    }
                    let completed_flush = CompletedFlush {
                        xact_id,
                        flush_result,
                    };
                    {
                        let mut guard = completed_flush_events_clone.lock().await;
                        assert!(guard.insert(event_id, completed_flush).is_none());
                        event_notification.notify_waiters();
                    }
                }
                TableEvent::MooncakeTableSnapshotResult {
                    mooncake_snapshot_result,
                } => {
                    let completed_mooncake_snapshot = CompletedMooncakeSnapshot {
                        mooncake_snapshot_result,
                    };

                    // Fill in background tasks payload to fill in.
                    if let Some(iceberg_snapshot_payload) = &completed_mooncake_snapshot
                        .mooncake_snapshot_result
                        .iceberg_snapshot_payload
                    {
                        let mut guard = pending_iceberg_snapshot_payloads.lock().await;
                        assert!(guard
                            .insert(
                                iceberg_snapshot_payload.uuid,
                                iceberg_snapshot_payload.clone()
                            )
                            .is_none());
                    }
                    match &completed_mooncake_snapshot
                        .mooncake_snapshot_result
                        .file_indices_merge_payload
                    {
                        TableMaintenanceStatus::Payload(payload) => {
                            let mut guard = pending_index_merge_payloads.lock().await;
                            assert!(guard.insert(payload.uuid, payload.clone()).is_none());
                        }
                        _ => {}
                    }
                    match &completed_mooncake_snapshot
                        .mooncake_snapshot_result
                        .data_compaction_payload
                    {
                        TableMaintenanceStatus::Payload(payload) => {
                            let mut guard = pending_data_compaction_payloads.lock().await;
                            assert!(guard.insert(payload.uuid, payload.clone()).is_none());
                        }
                        _ => {}
                    }

                    let mut guard = completed_mooncake_snapshots_clone.lock().await;
                    assert!(guard
                        .insert(
                            completed_mooncake_snapshot.mooncake_snapshot_result.uuid,
                            completed_mooncake_snapshot
                        )
                        .is_none());
                    event_notification.notify_waiters();
                }
                TableEvent::IcebergSnapshotResult {
                    iceberg_snapshot_result,
                } => {
                    let iceberg_snapshot_result = iceberg_snapshot_result.unwrap();
                    let mut guard = completed_iceberg_snapshots_clone.lock().await;
                    assert!(guard
                        .insert(
                            iceberg_snapshot_result.uuid,
                            CompletedIcebergSnapshot {
                                iceberg_snapshot_result
                            }
                        )
                        .is_none());
                    event_notification.notify_waiters();
                }
                TableEvent::IndexMergeResult { index_merge_result } => {
                    let index_merge_result = index_merge_result.unwrap();
                    let mut guard = completed_index_merge_clone.lock().await;
                    assert!(guard
                        .insert(
                            index_merge_result.uuid,
                            CompletedIndexMerge { index_merge_result }
                        )
                        .is_none());
                    event_notification.notify_waiters();
                }
                TableEvent::DataCompactionResult {
                    data_compaction_result,
                } => {
                    {
                        let data_files = &data_compaction_result.as_ref().unwrap().new_data_files;
                        let mut guard = data_files_clone.lock().await;
                        for (data_file_id, _) in data_files.iter() {
                            assert!(guard
                                .insert(data_file_id.file_id(), data_file_id.file_path().clone())
                                .is_none());
                        }
                    }
                    {
                        let data_compaction_result = data_compaction_result.unwrap();
                        let mut guard = completed_data_compaction_clone.lock().await;
                        assert!(guard
                            .insert(
                                data_compaction_result.uuid,
                                CompletedDataCompaction {
                                    data_compaction_result
                                }
                            )
                            .is_none());
                        event_notification.notify_waiters();
                    }
                }
                _ => {}
            }
        }
    });

    // Used to indicate valid rows for all versions.
    // Maps from commit LSN to valid ids.
    let mut versioned_committed_ids = HashMap::new();
    // Used to indicate valid rows.
    let mut committed_ids = HashSet::new();
    // Ids for the current ongoing transaction to append, which could be aborted.
    let mut uncommitted_appended_ids = HashSet::new();
    // Ids for the current ongoing transaction to delete, which could be aborted.
    let mut uncommitted_deleted_ids = HashSet::new();

    while let Some(serialized_event) = lines.next_line().await.unwrap() {
        let replay_table_event: MooncakeTableEvent =
            serde_json::from_str(&serialized_event).unwrap();
        match replay_table_event {
            // =====================
            // Foreground operations
            // =====================
            MooncakeTableEvent::Append(append_event) => {
                // Update in-memory state.
                let id = get_id_from_row(&append_event.row);
                assert!(uncommitted_appended_ids.insert(id));

                // Apply update to mooncake table.
                if let Some(xact_id) = append_event.xact_id {
                    table
                        .append_in_stream_batch(append_event.row, xact_id)
                        .unwrap();
                } else {
                    table.append(append_event.row).unwrap();
                }
            }
            MooncakeTableEvent::Delete(delete_event) => {
                // Update in-memory state.
                let id = get_id_from_row(&delete_event.row);
                if uncommitted_appended_ids.contains(&id) {
                    uncommitted_appended_ids.remove(&id);
                } else {
                    assert!(uncommitted_deleted_ids.insert(id));
                }

                // Apply update to mooncake table.
                if let Some(xact_id) = delete_event.xact_id {
                    table
                        .delete_in_stream_batch(delete_event.row, xact_id)
                        .await;
                } else if is_upsert_table {
                    table
                        .delete_if_exists(delete_event.row, delete_event.lsn.unwrap())
                        .await;
                } else {
                    table
                        .delete(delete_event.row, delete_event.lsn.unwrap())
                        .await;
                }
            }
            MooncakeTableEvent::Commit(commit_event) => {
                // Update in-memory state.
                let appended = std::mem::take(&mut uncommitted_appended_ids);
                let deleted = std::mem::take(&mut uncommitted_deleted_ids);
                {
                    // Consider update case,
                    // - It's possible to add an existing id.
                    // - We should apply deletion before addition.
                    for cur_delete in deleted.into_iter() {
                        committed_ids.remove(&cur_delete);
                    }
                    for cur_append in appended.into_iter() {
                        committed_ids.insert(cur_append);
                    }
                }
                assert!(versioned_committed_ids
                    .insert(commit_event.lsn, committed_ids.clone())
                    .is_none());

                // Update LSN.
                commit_lsn_tx.send(commit_event.lsn).unwrap();
                replication_lsn_tx.send(commit_event.lsn).unwrap();

                // Apply update to mooncake table.
                if let Some(xact_id) = commit_event.xact_id {
                    table
                        .commit_transaction_stream_impl(xact_id, commit_event.lsn)
                        .unwrap();
                } else {
                    table.commit(commit_event.lsn);
                }
            }
            MooncakeTableEvent::Abort(abort_event) => {
                // Update in-memory state.
                uncommitted_appended_ids.clear();
                uncommitted_deleted_ids.clear();

                // Apply update to mooncake table.
                table.abort_in_stream_batch(abort_event.xact_id);
            }
            // =====================
            // Flush operation
            // =====================
            MooncakeTableEvent::FlushInitiation(flush_initiation_event) => {
                assert!(ongoing_flush_event_id.insert(flush_initiation_event.uuid));
                let event_id = flush_initiation_event.uuid;
                if let Some(xact_id) = flush_initiation_event.xact_id {
                    table
                        .flush_stream(xact_id, flush_initiation_event.lsn, event_id)
                        .unwrap();
                } else {
                    table
                        .flush(flush_initiation_event.lsn.unwrap(), event_id)
                        .unwrap();
                }
            }
            MooncakeTableEvent::FlushCompletion(flush_completion_event) => {
                assert!(ongoing_flush_event_id.remove(&flush_completion_event.uuid));
                loop {
                    let completed_flush_event = {
                        let mut guard = completed_flush_events.lock().await;
                        guard.remove(&flush_completion_event.uuid)
                    };
                    if let Some(completed_flush_event) = completed_flush_event {
                        if let Some(disk_slice) = completed_flush_event.flush_result {
                            let disk_slice = disk_slice.unwrap();
                            if let Some(xact_id) = completed_flush_event.xact_id {
                                table.apply_stream_flush_result(
                                    xact_id,
                                    disk_slice,
                                    flush_completion_event.uuid,
                                );
                            } else {
                                table.apply_flush_result(disk_slice, flush_completion_event.uuid);
                            }
                        }
                        break;
                    }
                    // Otherwise block until the corresponding flush event completes.
                    event_notification_clone.notified().await;
                }
            }
            // =====================
            // Mooncake snapshot
            // =====================
            MooncakeTableEvent::MooncakeSnapshotInitiation(snapshot_initiation_event) => {
                assert!(ongoing_mooncake_snapshot_id.insert(snapshot_initiation_event.uuid));
                let mut snapshot_option = snapshot_initiation_event.option.clone();
                snapshot_option.dump_snapshot = true;
                // Event only recorded when snapshot gets created in source run.
                assert!(table.try_create_mooncake_snapshot(snapshot_option));
            }
            MooncakeTableEvent::MooncakeSnapshotCompletion(snapshot_completion_event) => {
                assert!(ongoing_mooncake_snapshot_id.remove(&snapshot_completion_event.uuid));
                loop {
                    let completed_mooncake_snapshot = {
                        let mut guard = completed_mooncake_snapshots.lock().await;
                        guard.remove(&snapshot_completion_event.uuid)
                    };
                    if let Some(completed_mooncake_snapshot) = completed_mooncake_snapshot {
                        table.mark_mooncake_snapshot_completed();
                        table.record_mooncake_snapshot_completion(
                            &completed_mooncake_snapshot.mooncake_snapshot_result,
                        );
                        table.notify_snapshot_reader(
                            completed_mooncake_snapshot
                                .mooncake_snapshot_result
                                .commit_lsn,
                        );
                        break;
                    }
                    // Otherwise block until the corresponding flush event completes.
                    event_notification_clone.notified().await;
                }

                // Validate mooncake snapshot.
                let commit_lsn = snapshot_completion_event.lsn;
                let mut expected_ids = versioned_committed_ids
                    .get(&commit_lsn)
                    .as_ref()
                    .unwrap()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>();
                expected_ids.sort();
                check_read_snapshot(
                    &read_state_manager,
                    /*target_lsn=*/ Some(commit_lsn),
                    &expected_ids,
                )
                .await;
            }
            // =====================
            // Iceberg snapshot
            // =====================
            MooncakeTableEvent::IcebergSnapshotInitiation(snapshot_initiation_event) => {
                assert!(ongoing_iceberg_snapshot_id.insert(snapshot_initiation_event.uuid));
                let payload = {
                    let mut guard = pending_iceberg_snapshot_payloads_clone.lock().await;

                    guard.remove(&snapshot_initiation_event.uuid).unwrap()
                };
                table.persist_iceberg_snapshot(payload);
            }
            MooncakeTableEvent::IcebergSnapshotCompletion(snapshot_completion_event) => {
                assert!(ongoing_iceberg_snapshot_id.remove(&snapshot_completion_event.uuid));
                loop {
                    let completed_iceberg_snapshot_event = {
                        let mut guard = completed_iceberg_snapshots.lock().await;
                        guard.remove(&snapshot_completion_event.uuid)
                    };
                    if let Some(completed_iceberg_snapshot) = completed_iceberg_snapshot_event {
                        table.set_iceberg_snapshot_res(
                            completed_iceberg_snapshot.iceberg_snapshot_result,
                        );
                        break;
                    }
                    // Otherwise block until the corresponding flush event completes.
                    event_notification_clone.notified().await;
                }

                // Validate iceberg snapshot.
                let commit_lsn = snapshot_completion_event.lsn;
                let mut expected_ids = versioned_committed_ids
                    .get(&commit_lsn)
                    .as_ref()
                    .unwrap()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>();
                expected_ids.sort();
                validate_persisted_iceberg_table(
                    table_metadata.clone(),
                    iceberg_table_config.clone(),
                    commit_lsn,
                    expected_ids,
                )
                .await;
            }
            // =====================
            // Index merge events
            // =====================
            MooncakeTableEvent::IndexMergeInitiation(index_merge_initiation_event) => {
                assert!(ongoing_index_merge_id.insert(index_merge_initiation_event.uuid));
                let payload = {
                    let mut guard = pending_index_merge_payloads_clone.lock().await;

                    guard.remove(&index_merge_initiation_event.uuid).unwrap()
                };
                table.perform_index_merge(payload);
            }
            MooncakeTableEvent::IndexMergeCompletion(index_merge_completion_event) => {
                assert!(ongoing_index_merge_id.remove(&index_merge_completion_event.uuid));
                loop {
                    let completed_index_merge_event = {
                        let mut guard = completed_index_merge.lock().await;
                        guard.remove(&index_merge_completion_event.uuid)
                    };
                    if let Some(completed_index_merge) = completed_index_merge_event {
                        table.set_file_indices_merge_res(completed_index_merge.index_merge_result);
                        break;
                    }
                    // Otherwise block until the corresponding flush event completes.
                    event_notification_clone.notified().await;
                }
            }
            // =====================
            // Data compaction events
            // =====================
            MooncakeTableEvent::DataCompactionInitiation(data_compaction_initiation_event) => {
                assert!(ongoing_data_compaction_id.insert(data_compaction_initiation_event.uuid));
                let payload = {
                    let mut guard = pending_data_compaction_payloads_clone.lock().await;

                    guard
                        .remove(&data_compaction_initiation_event.uuid)
                        .unwrap()
                };
                table.perform_data_compaction(payload);
            }
            MooncakeTableEvent::DataCompactionCompletion(data_compaction_completion_event) => {
                assert!(ongoing_data_compaction_id.remove(&data_compaction_completion_event.uuid));
                loop {
                    let completed_data_compaction_event = {
                        let mut guard = completed_data_compaction.lock().await;
                        guard.remove(&data_compaction_completion_event.uuid)
                    };
                    if let Some(completed_data_compaction) = completed_data_compaction_event {
                        let data_compaction_result =
                            completed_data_compaction.data_compaction_result;
                        table.set_data_compaction_res(data_compaction_result);
                        break;
                    }
                    // Otherwise block until the corresponding flush event completes.
                    event_notification_clone.notified().await;
                }
            }
        }
    }
}

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc::Receiver;

use crate::row::MoonlinkRow;
use crate::storage::io_utils;
use crate::storage::mooncake_table::disk_slice::DiskSliceWriter;
use crate::storage::mooncake_table::{
    AlterTableRequest, DataCompactionPayload, DataCompactionResult, FileIndiceMergePayload,
    FileIndiceMergeResult, IcebergSnapshotPayload, IcebergSnapshotResult,
    TableMetadata as MooncakeTableMetadata,
};
use crate::storage::snapshot_options::SnapshotOption;
use crate::storage::snapshot_options::{IcebergSnapshotOption, MaintenanceOption};
use crate::table_notify::{
    DataCompactionMaintenanceStatus, IndexMergeMaintenanceStatus, TableEvent,
};
use crate::{MooncakeTable, ReadStateFilepathRemap, SnapshotReadOutput};
use crate::{ReadState, Result};
use tracing::{debug, error};

/// ===============================
/// Flush
/// ===============================
///
/// Synchronize on ongoing flushes and return a map of <event id, disk slice>.
pub(crate) async fn get_flush_results(
    receiver: &mut Receiver<TableEvent>,
    expected_flushes: usize,
) -> HashMap<uuid::Uuid, DiskSliceWriter> {
    let mut flush_results = HashMap::new();
    for _ in 0..expected_flushes {
        let cur_flush_result = receiver.recv().await.unwrap();
        match cur_flush_result {
            TableEvent::FlushResult {
                event_id,
                xact_id: _,
                flush_result,
            } => match flush_result {
                Some(Ok(disk_slice)) => {
                    flush_results.insert(event_id, disk_slice);
                }
                Some(Err(e)) => {
                    error!(error = ?e, "failed to flush disk slice");
                }
                None => {
                    debug!("Flush result is none, disk slice was empty");
                }
            },
            _ => {
                panic!("Expected FlushResult as first event, but got others.");
            }
        }
    }
    flush_results
}

/// Flush mooncake, block wait its completion and reflect result to mooncake table.
#[cfg(test)]
pub(crate) async fn flush_table_and_sync(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
    lsn: u64,
) -> Result<()> {
    let event_id = uuid::Uuid::new_v4();
    table.flush(lsn, event_id).unwrap();
    let flush_result = receiver.recv().await.unwrap();
    match flush_result {
        TableEvent::FlushResult {
            event_id,
            xact_id: _,
            flush_result,
        } => match flush_result {
            Some(Ok(disk_slice)) => {
                table.apply_flush_result(disk_slice, event_id);
            }
            Some(Err(e)) => {
                error!(error = ?e, "failed to flush disk slice");
            }
            None => {
                debug!("Flush result is none, disk slice was empty");
            }
        },
        _ => {
            panic!("Expected FlushResult as first event, but got others.");
        }
    }
    Ok(())
}

/// Flush mooncake, block wait its completion but do NOT apply the result.
/// This leaves the LSN in ongoing_flush_lsns until apply_flush_result is called.
#[cfg(test)]
pub(crate) async fn flush_table_and_sync_no_apply(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
    lsn: u64,
) -> Option<DiskSliceWriter> {
    let event_id = uuid::Uuid::new_v4();
    table.flush(lsn, event_id).unwrap();
    let flush_result = receiver.recv().await.unwrap();
    match flush_result {
        TableEvent::FlushResult {
            event_id: _,
            xact_id: _,
            flush_result,
        } => match flush_result {
            Some(Ok(disk_slice)) => Some(disk_slice),
            Some(Err(e)) => {
                error!(error = ?e, "failed to flush disk slice");
                None
            }
            None => {
                debug!("Flush result is none, disk slice was empty");
                None
            }
        },
        _ => {
            panic!("Expected FlushResult as first event, but got others.");
        }
    }
}

/// Flush the given streaming transaction, block wait its completion.
#[cfg(test)]
pub(crate) async fn flush_stream_and_sync_no_apply(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
    xact_id: u32,
    lsn: Option<u64>,
) -> Option<DiskSliceWriter> {
    let event_id = uuid::Uuid::new_v4();
    table.flush_stream(xact_id, lsn, event_id).unwrap();
    let flush_result = receiver.recv().await.unwrap();
    match flush_result {
        TableEvent::FlushResult {
            event_id: _,
            xact_id: _,
            flush_result,
        } => match flush_result {
            Some(Ok(disk_slice)) => Some(disk_slice),
            Some(Err(e)) => {
                error!(error = ?e, "failed to flush disk slice");
                None
            }
            None => {
                debug!("Flush result is none, disk slice was empty");
                None
            }
        },
        _ => {
            panic!("Expected FlushResult as first event, but got others.");
        }
    }
}

/// Commit transaction stream, block wait its completion and reflect result to mooncake table.
pub(crate) async fn commit_transaction_stream_and_sync(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
    xact_id: u32,
    lsn: u64,
) {
    let event_id = uuid::Uuid::new_v4();
    table
        .commit_transaction_stream(xact_id, lsn, event_id)
        .unwrap();
    let flush_result = receiver.recv().await.unwrap();
    match flush_result {
        TableEvent::FlushResult {
            event_id,
            xact_id: Some(xact_id),
            flush_result,
        } => match flush_result {
            Some(Ok(disk_slice)) => {
                table.apply_stream_flush_result(xact_id, disk_slice, event_id);
            }
            Some(Err(e)) => {
                error!(error = ?e, "failed to flush disk slice");
            }
            None => {
                debug!("Flush result is none, disk slice was empty");
            }
        },
        _ => {
            panic!("Expected FlushResult as first event, but got others.");
        }
    }
}

/// ===============================
/// Delete evicted files
/// ===============================
///
/// Test util function to block wait delete request, and check whether matches expected data files.
pub(crate) async fn sync_delete_evicted_files(
    receiver: &mut Receiver<TableEvent>,
    mut expected_files_to_delete: Vec<String>,
) {
    let notification = receiver.recv().await.unwrap();
    if let TableEvent::EvictedFilesToDelete { evicted_files } = notification {
        let mut evicted_data_files = evicted_files.files;
        evicted_data_files.sort();
        expected_files_to_delete.sort();
        assert_eq!(evicted_data_files, expected_files_to_delete);
    } else {
        panic!("Receive other notifications other than delete evicted files")
    }
}

/// ===============================
/// Index merge
/// ===============================
///
/// Perform an index merge for the given table, and reflect the result to snapshot.
/// Return evicted files to delete.
pub(crate) async fn perform_index_merge_for_test(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
    index_merge_payload: FileIndiceMergePayload,
) -> Vec<String> {
    // Perform and block wait index merge.
    table.perform_index_merge(index_merge_payload);
    let index_merge_result = sync_index_merge(receiver).await;

    table.set_file_indices_merge_res(index_merge_result);
    assert!(table.try_create_mooncake_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::BestEffort(uuid::Uuid::new_v4()),
        index_merge_option: MaintenanceOption::BestEffort(uuid::Uuid::new_v4()),
        data_compaction_option: MaintenanceOption::Skip,
    }));
    let (_, _, _, _, evicted_files_to_delete) = sync_mooncake_snapshot(table, receiver).await;
    // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
    io_utils::delete_local_files(&evicted_files_to_delete)
        .await
        .unwrap();

    evicted_files_to_delete
}

/// ===============================
/// Data compaction
/// ===============================
///
/// Perform data compaction for the given table, and reflect the result to snapshot.
/// Return evicted files to delete.
pub(crate) async fn perform_data_compaction_for_test(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
    data_compaction_payload: DataCompactionPayload,
) -> Vec<String> {
    // Perform and block wait data compaction.
    table.perform_data_compaction(data_compaction_payload);
    let data_compaction_result = sync_data_compaction(receiver).await;

    table.set_data_compaction_res(data_compaction_result);
    assert!(table.try_create_mooncake_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::BestEffort(uuid::Uuid::new_v4()),
        index_merge_option: MaintenanceOption::Skip,
        data_compaction_option: MaintenanceOption::BestEffort(uuid::Uuid::new_v4()),
    }));
    let (_, _, _, _, evicted_files_to_delete) = sync_mooncake_snapshot(table, receiver).await;
    // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
    io_utils::delete_local_files(&evicted_files_to_delete)
        .await
        .unwrap();

    evicted_files_to_delete
}

/// ===================================
/// Operation synchronization function
/// ===================================
///
/// Test util function to block wait and get iceberg / file indices merge payload.
pub(crate) async fn sync_mooncake_snapshot(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
) -> (
    u64,
    Option<IcebergSnapshotPayload>,
    IndexMergeMaintenanceStatus,
    DataCompactionMaintenanceStatus,
    Vec<String>,
) {
    let notification = receiver.recv().await.unwrap();
    table.mark_mooncake_snapshot_completed();
    if let TableEvent::MooncakeTableSnapshotResult {
        mooncake_snapshot_result,
    } = notification
    {
        (
            mooncake_snapshot_result.commit_lsn,
            mooncake_snapshot_result.iceberg_snapshot_payload,
            mooncake_snapshot_result.file_indices_merge_payload,
            mooncake_snapshot_result.data_compaction_payload,
            mooncake_snapshot_result.evicted_data_files_to_delete,
        )
    } else {
        panic!("Expected mooncake snapshot completion notification, but get others.");
    }
}
pub(crate) async fn sync_iceberg_snapshot(
    receiver: &mut Receiver<TableEvent>,
) -> IcebergSnapshotResult {
    let notification = receiver.recv().await.unwrap();
    if let TableEvent::IcebergSnapshotResult {
        iceberg_snapshot_result,
    } = notification
    {
        iceberg_snapshot_result.unwrap()
    } else {
        panic!("Expected iceberg completion snapshot notification, but get mooncake one.");
    }
}
async fn sync_index_merge(receiver: &mut Receiver<TableEvent>) -> FileIndiceMergeResult {
    let notification = receiver.recv().await.unwrap();
    if let TableEvent::IndexMergeResult { index_merge_result } = notification {
        index_merge_result.unwrap()
    } else {
        panic!("Expected index merge completion notification, but get another one.");
    }
}
async fn sync_data_compaction(receiver: &mut Receiver<TableEvent>) -> DataCompactionResult {
    let notification = receiver.recv().await.unwrap();
    if let TableEvent::DataCompactionResult {
        data_compaction_result,
    } = notification
    {
        data_compaction_result.unwrap()
    } else {
        panic!("Expected data compaction completion notification, but get mooncake one.");
    }
}

/// ===================================
/// Composite util functions
/// ===================================
///
// Test util function, which creates mooncake snapshot for testing.
pub(crate) async fn create_mooncake_snapshot_for_test(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
) -> (
    u64,
    Option<IcebergSnapshotPayload>,
    IndexMergeMaintenanceStatus,
    DataCompactionMaintenanceStatus,
    Vec<String>,
) {
    let mooncake_snapshot_created = table.try_create_mooncake_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::BestEffort(uuid::Uuid::new_v4()),
        data_compaction_option: MaintenanceOption::BestEffort(uuid::Uuid::new_v4()),
        index_merge_option: MaintenanceOption::BestEffort(uuid::Uuid::new_v4()),
    });
    assert!(mooncake_snapshot_created);
    sync_mooncake_snapshot(table, receiver).await
}

// Test util function, which updates mooncake table snapshot and create iceberg snapshot in a serial fashion.
pub(crate) async fn create_mooncake_and_persist_for_test(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
) {
    // Create mooncake snapshot and block wait completion.
    let (_, iceberg_snapshot_payload, _, _, evicted_data_files_to_delete) =
        create_mooncake_snapshot_for_test(table, receiver).await;

    // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
    io_utils::delete_local_files(&evicted_data_files_to_delete)
        .await
        .unwrap();

    // Create iceberg snapshot if possible.
    if let Some(iceberg_snapshot_payload) = iceberg_snapshot_payload {
        table.persist_iceberg_snapshot(iceberg_snapshot_payload);
        let iceberg_snapshot_result = sync_iceberg_snapshot(receiver).await;
        table.set_iceberg_snapshot_res(iceberg_snapshot_result);
    }
}

// Test util to block wait current mooncake snapshot completion, get the iceberg persistence payload, and perform a new mooncake snapshot and wait completion.
pub(crate) async fn sync_mooncake_snapshot_and_create_new_by_iceberg_payload(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
) {
    let (_, iceberg_snapshot_payload, _, _, evicted_data_files_to_delete) =
        sync_mooncake_snapshot(table, receiver).await;
    // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
    io_utils::delete_local_files(&evicted_data_files_to_delete)
        .await
        .unwrap();

    let iceberg_snapshot_payload = iceberg_snapshot_payload.unwrap();
    table.persist_iceberg_snapshot(iceberg_snapshot_payload);
    let iceberg_snapshot_result = sync_iceberg_snapshot(receiver).await;
    table.set_iceberg_snapshot_res(iceberg_snapshot_result);

    // Create mooncake snapshot after buffering iceberg snapshot result, to make sure mooncake snapshot is at a consistent state.
    assert!(table.try_create_mooncake_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::Skip,
        index_merge_option: MaintenanceOption::Skip,
        data_compaction_option: MaintenanceOption::Skip,
    }));
    sync_mooncake_snapshot(table, receiver).await;
}

/// Test util function to perform an iceberg snapshot, block wait its completion and gets its result.
pub(crate) async fn create_iceberg_snapshot(
    table: &mut MooncakeTable,
    iceberg_snapshot_payload: Option<IcebergSnapshotPayload>,
    notify_rx: &mut Receiver<TableEvent>,
) -> Result<IcebergSnapshotResult> {
    table.persist_iceberg_snapshot(iceberg_snapshot_payload.unwrap());
    let notification = notify_rx.recv().await.unwrap();
    match notification {
        TableEvent::IcebergSnapshotResult {
            iceberg_snapshot_result,
        } => iceberg_snapshot_result,
        _ => {
            panic!(
                "Expects to receive iceberg snapshot completion notification, but receives others."
            )
        }
    }
}

// Test util function, which does the following things in serial fashion.
// (1) updates mooncake table snapshot, (2) create iceberg snapshot, (3) trigger data compaction, (4) perform data compaction, (5) another mooncake and iceberg snapshot.
//
// # Arguments
//
// * injected_committed_deletion_rows: rows to delete and commit in between data compaction initiation and snapshot creation
// * injected_uncommitted_deletion_rows: rows to delete but not commit in between data compaction initiation and snapshot creation
#[cfg(test)]
pub(crate) async fn create_mooncake_and_persist_for_data_compaction_for_test(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
    injected_committed_deletion_rows: Vec<(MoonlinkRow, u64 /*lsn*/)>,
    injected_uncommitted_deletion_rows: Vec<(MoonlinkRow, u64 /*lsn*/)>,
) {
    // Create mooncake snapshot.
    let force_snapshot_option = SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::BestEffort(uuid::Uuid::new_v4()),
        index_merge_option: MaintenanceOption::Skip,
        data_compaction_option: MaintenanceOption::BestEffort(uuid::Uuid::new_v4()),
    };
    assert!(table.try_create_mooncake_snapshot(force_snapshot_option.clone()));

    // Create iceberg snapshot.
    let (_, iceberg_snapshot_payload, _, _, evicted_data_files_to_delete) =
        sync_mooncake_snapshot(table, receiver).await;
    // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
    io_utils::delete_local_files(&evicted_data_files_to_delete)
        .await
        .unwrap();

    if let Some(iceberg_snapshot_payload) = iceberg_snapshot_payload {
        table.persist_iceberg_snapshot(iceberg_snapshot_payload);
        let iceberg_snapshot_result = sync_iceberg_snapshot(receiver).await;
        table.set_iceberg_snapshot_res(iceberg_snapshot_result);
    }

    // Get data compaction payload.
    assert!(table.try_create_mooncake_snapshot(force_snapshot_option.clone()));
    let (_, iceberg_snapshot_payload, _, data_compaction_payload, evicted_data_files_to_delete) =
        sync_mooncake_snapshot(table, receiver).await;
    // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
    io_utils::delete_local_files(&evicted_data_files_to_delete)
        .await
        .unwrap();

    assert!(iceberg_snapshot_payload.is_none());
    let data_compaction_payload = data_compaction_payload.take_payload().unwrap();

    // Perform and block wait data compaction.
    table.perform_data_compaction(data_compaction_payload);
    let data_compaction_result = sync_data_compaction(receiver).await;

    // Before create snapshot for compaction results, perform another deletion operations.
    for (cur_row, lsn) in injected_committed_deletion_rows {
        table.delete(cur_row, lsn - 1).await;
        table.commit(/*lsn=*/ lsn);
    }
    for (cur_row, lsn) in injected_uncommitted_deletion_rows {
        table.delete(cur_row, lsn - 1).await;
    }

    // Set data compaction result and trigger another iceberg snapshot.
    table.set_data_compaction_res(data_compaction_result);
    assert!(table.try_create_mooncake_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::BestEffort(uuid::Uuid::new_v4()),
        index_merge_option: MaintenanceOption::Skip,
        data_compaction_option: MaintenanceOption::BestEffort(uuid::Uuid::new_v4()),
    }));
    sync_mooncake_snapshot_and_create_new_by_iceberg_payload(table, receiver).await;
}

// Test util function, which does the following things in serial fashion.
// (1) updates mooncake table snapshot, (2) create iceberg snapshot, (3) trigger index merge, (4) perform index merge, (5) another mooncake and iceberg snapshot.
pub(crate) async fn create_mooncake_and_iceberg_snapshot_for_index_merge_for_test(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
) {
    // Create mooncake snapshot.
    let force_snapshot_option = SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::BestEffort(uuid::Uuid::new_v4()),
        index_merge_option: MaintenanceOption::BestEffort(uuid::Uuid::new_v4()),
        data_compaction_option: MaintenanceOption::BestEffort(uuid::Uuid::new_v4()),
    };
    assert!(table.try_create_mooncake_snapshot(force_snapshot_option.clone()));

    // Create iceberg snapshot.
    let (_, iceberg_snapshot_payload, _, _, evicted_data_files_to_delete) =
        sync_mooncake_snapshot(table, receiver).await;
    // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
    io_utils::delete_local_files(&evicted_data_files_to_delete)
        .await
        .unwrap();

    if let Some(iceberg_snapshot_payload) = iceberg_snapshot_payload {
        table.persist_iceberg_snapshot(iceberg_snapshot_payload);
        let iceberg_snapshot_result = sync_iceberg_snapshot(receiver).await;
        table.set_iceberg_snapshot_res(iceberg_snapshot_result);
    }

    // Perform index merge.
    assert!(table.try_create_mooncake_snapshot(force_snapshot_option.clone()));
    let (_, iceberg_snapshot_payload, file_indice_merge_payload, _, evicted_data_files_to_delete) =
        sync_mooncake_snapshot(table, receiver).await;
    // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
    io_utils::delete_local_files(&evicted_data_files_to_delete)
        .await
        .unwrap();

    assert!(iceberg_snapshot_payload.is_none());
    let file_indice_merge_payload = file_indice_merge_payload.take_payload().unwrap();

    table.perform_index_merge(file_indice_merge_payload);
    let index_merge_result = sync_index_merge(receiver).await;
    table.set_file_indices_merge_res(index_merge_result);
    assert!(table.try_create_mooncake_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::BestEffort(uuid::Uuid::new_v4()),
        index_merge_option: MaintenanceOption::BestEffort(uuid::Uuid::new_v4()),
        data_compaction_option: MaintenanceOption::BestEffort(uuid::Uuid::new_v4()),
    }));
    sync_mooncake_snapshot_and_create_new_by_iceberg_payload(table, receiver).await;
}

/// ===================================
/// Request read
/// ===================================
///
/// Test util function to drop read states, and apply the synchronized response to mooncake table.
pub(crate) async fn drop_read_states(read_states: Vec<Arc<ReadState>>) {
    for cur_read_state in read_states.into_iter() {
        drop(cur_read_state);
    }
}

/// Test util function to drop read states and create a mooncake snapshot to reflect.
/// Return evicted files to delete.
pub(crate) async fn drop_read_states_and_create_mooncake_snapshot(
    read_states: Vec<Arc<ReadState>>,
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
) {
    drop_read_states(read_states).await;
    let (_, _, _, _, _) = create_mooncake_snapshot_for_test(table, receiver).await;
}

/// Perform a read request for the given table.
pub(crate) async fn perform_read_request_for_test(table: &mut MooncakeTable) -> SnapshotReadOutput {
    let mut guard = table.snapshot.write().await;
    guard.request_read().await.unwrap()
}

pub(crate) async fn alter_table_and_persist_to_iceberg(
    table: &mut MooncakeTable,
    notify_rx: &mut Receiver<TableEvent>,
) -> Arc<MooncakeTableMetadata> {
    table.force_empty_iceberg_payload();
    // Create a mooncake and iceberg snapshot to reflect both data files and schema changes.
    let (_, iceberg_snapshot_payload, _, _, _) =
        create_mooncake_snapshot_for_test(table, notify_rx).await;
    if let Some(mut iceberg_snapshot_payload) = iceberg_snapshot_payload {
        let alter_table_request = AlterTableRequest {
            new_columns: vec![],
            dropped_columns: vec!["age".to_string()],
        };
        let new_table_metadata = table.alter_table(alter_table_request);
        iceberg_snapshot_payload.new_table_schema = Some(new_table_metadata.clone());
        let iceberg_snapshot_result =
            create_iceberg_snapshot(table, Some(iceberg_snapshot_payload), notify_rx).await;
        table.set_iceberg_snapshot_res(iceberg_snapshot_result.unwrap());
        new_table_metadata
    } else {
        panic!("Iceberg snapshot payload is not set");
    }
}

pub(crate) fn get_read_state_filepath_remap() -> ReadStateFilepathRemap {
    Arc::new(|local_filepath: String| local_filepath)
}

pub(crate) async fn alter_table(table: &mut MooncakeTable) {
    let alter_table_request = AlterTableRequest {
        new_columns: vec![],
        dropped_columns: vec!["age".to_string()],
    };
    table.alter_table(alter_table_request);
}

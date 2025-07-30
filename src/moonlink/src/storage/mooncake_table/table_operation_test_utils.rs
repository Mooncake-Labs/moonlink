use std::sync::Arc;

use tokio::sync::mpsc::Receiver;

#[cfg(test)]
use crate::row::MoonlinkRow;
use crate::storage::io_utils;
use crate::storage::mooncake_table::{
    AlterTableRequest, DataCompactionPayload, DataCompactionResult, FileIndiceMergePayload,
    FileIndiceMergeResult, IcebergSnapshotPayload, IcebergSnapshotResult, MaintenanceOption,
    SnapshotOption, TableMetadata as MooncakeTableMetadata,
};
use crate::table_notify::{
    DataCompactionMaintenanceStatus, IndexMergeMaintenanceStatus, TableEvent,
};
use crate::{MooncakeTable, SnapshotReadOutput};
use crate::{ReadState, Result};

/// ===============================
/// Flush
/// ===============================
///
/// Flush mooncake, block wait its completion and reflect result to mooncake table.
#[cfg(test)]
pub(crate) async fn flush_table_and_sync(
    table: &mut MooncakeTable,
    _receiver: &mut Receiver<TableEvent>,
    lsn: u64,
) -> Result<()> {
    // TODO(Nolan): Use receiver to block wait until table flush finishes.
    table.flush(lsn).await
}

/// ===============================
/// Delete evicted files
/// ===============================
///
/// Test util function to block wait delete request, and check whether matches expected data files.
#[cfg(test)]
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
    assert!(table.create_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        skip_iceberg_snapshot: false,
        index_merge_option: MaintenanceOption::BestEffort,
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
    assert!(table.create_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        skip_iceberg_snapshot: false,
        index_merge_option: MaintenanceOption::Skip,
        data_compaction_option: MaintenanceOption::BestEffort,
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
        lsn,
        uuid: _,
        iceberg_snapshot_payload,
        file_indice_merge_payload,
        data_compaction_payload,
        evicted_files_to_delete,
    } = notification
    {
        (
            lsn,
            iceberg_snapshot_payload,
            file_indice_merge_payload,
            data_compaction_payload,
            evicted_files_to_delete.files,
        )
    } else {
        panic!("Expected mooncake snapshot completion notification, but get others.");
    }
}
async fn sync_iceberg_snapshot(receiver: &mut Receiver<TableEvent>) -> IcebergSnapshotResult {
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
        index_merge_result
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
    let mooncake_snapshot_created = table.create_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        skip_iceberg_snapshot: false,
        data_compaction_option: MaintenanceOption::BestEffort,
        index_merge_option: MaintenanceOption::BestEffort,
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
async fn sync_mooncake_snapshot_and_create_new_by_iceberg_payload(
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
    assert!(table.create_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        skip_iceberg_snapshot: true,
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
        skip_iceberg_snapshot: false,
        index_merge_option: MaintenanceOption::Skip,
        data_compaction_option: MaintenanceOption::BestEffort,
    };
    assert!(table.create_snapshot(force_snapshot_option.clone()));

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
    assert!(table.create_snapshot(force_snapshot_option.clone()));
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
    assert!(table.create_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        skip_iceberg_snapshot: false,
        index_merge_option: MaintenanceOption::Skip,
        data_compaction_option: MaintenanceOption::BestEffort,
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
        skip_iceberg_snapshot: false,
        index_merge_option: MaintenanceOption::BestEffort,
        data_compaction_option: MaintenanceOption::BestEffort,
    };
    assert!(table.create_snapshot(force_snapshot_option.clone()));

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
    assert!(table.create_snapshot(force_snapshot_option.clone()));
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
    assert!(table.create_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        skip_iceberg_snapshot: false,
        index_merge_option: MaintenanceOption::BestEffort,
        data_compaction_option: MaintenanceOption::BestEffort,
    }));
    sync_mooncake_snapshot_and_create_new_by_iceberg_payload(table, receiver).await;
}

/// ===================================
/// Request read
/// ===================================
///
/// Test util function to drop read states, and apply the synchronized response to mooncake table.
pub(crate) async fn drop_read_states(
    read_states: Vec<Arc<ReadState>>,
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
) {
    for cur_read_state in read_states.into_iter() {
        drop(cur_read_state);
        sync_read_request_for_test(table, receiver).await;
    }
}

/// Test util function to drop read states and create a mooncake snapshot to reflect.
/// Return evicted files to delete.
pub(crate) async fn drop_read_states_and_create_mooncake_snapshot(
    read_states: Vec<Arc<ReadState>>,
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
) -> Vec<String> {
    drop_read_states(read_states, table, receiver).await;
    let (_, _, _, _, files_to_delete) = create_mooncake_snapshot_for_test(table, receiver).await;
    files_to_delete
}

/// Perform a read request for the given table.
pub(crate) async fn perform_read_request_for_test(table: &mut MooncakeTable) -> SnapshotReadOutput {
    let mut guard = table.snapshot.write().await;
    guard.request_read().await.unwrap()
}

/// Block wait read request to finish, and set the result to the snapshot buffer.
/// Precondition: there's ongoing read request.
pub(crate) async fn sync_read_request_for_test(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
) {
    let notification = receiver.recv().await.unwrap();
    if let TableEvent::ReadRequestCompletion {
        read_complete_handles,
    } = notification
    {
        table.set_read_request_res(read_complete_handles.handles);
    } else {
        panic!("Receive other notifications other than read request")
    }
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

pub(crate) async fn alter_table(table: &mut MooncakeTable) {
    let alter_table_request = AlterTableRequest {
        new_columns: vec![],
        dropped_columns: vec!["age".to_string()],
    };
    table.alter_table(alter_table_request);
}

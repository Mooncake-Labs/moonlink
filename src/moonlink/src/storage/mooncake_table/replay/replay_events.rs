use std::collections::{HashMap, HashSet};

/// This module defines data struct for mooncake table events.
use serde::{Deserialize, Serialize};

use crate::row::MoonlinkRow;
use crate::storage::compaction::table_compaction::SingleFileToCompact;
use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
use crate::storage::mooncake_table::table_snapshot::IcebergSnapshotDataCompactionPayload;
use crate::storage::mooncake_table::{
    DataCompactionPayload, FileIndiceMergePayload, IcebergSnapshotImportPayload,
    IcebergSnapshotIndexMergePayload, IcebergSnapshotPayload,
};
use crate::storage::snapshot_options::MaintenanceOption;
use crate::storage::snapshot_options::SnapshotOption;
use crate::storage::storage_utils::{FileId, RecordLocation, TableUniqueFileId};
use crate::NonEvictableHandle;

/// =====================
/// Foreground operations
/// =====================
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppendEvent {
    /// Moonlink row.
    pub row: MoonlinkRow,
    /// Transaction id, only assigned on streaming ones.
    pub xact_id: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeleteEvent {
    /// Moonlink row.
    pub row: MoonlinkRow,
    /// Deletion LSN.
    pub lsn: Option<u64>,
    /// Transaction id, only assigned on streaming ones.
    pub xact_id: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitEvent {
    /// Transaction id, only assigned on streaming ones.
    pub xact_id: Option<u32>,
    /// Commit LSN.
    pub lsn: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AbortEvent {
    /// Transaction id, only assigned on streaming ones.
    pub xact_id: u32,
}

/// =====================
/// Flush operation
/// =====================
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FlushEventInitiation {
    /// Event id.
    pub uuid: uuid::Uuid,
    /// Transaction id, only assigned on streaming ones.
    pub xact_id: Option<u32>,
    /// Flush LSN.
    pub lsn: Option<u64>,
    /// Commit check point.
    pub commit_check_point: RecordLocation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FlushEventCompletion {
    /// Event id.
    pub uuid: uuid::Uuid,
    /// flushed file ids.
    pub file_ids: Vec<FileId>,
}

/// =====================
/// Mooncake snapshot
/// =====================
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MooncakeSnapshotEventInitiation {
    /// Event id.
    pub uuid: uuid::Uuid,
    /// Mooncake snapshot options.
    pub option: SnapshotOption,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MooncakeSnapshotEventCompletion {
    /// Event id.
    pub uuid: uuid::Uuid,
    /// Commit LSN.
    pub lsn: u64,
}

/// =====================
/// Iceberg snapshot
/// =====================
///
/// For the ease of serde, replay event only stores necessary part of [`IcebergSnapshotImportPayload`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IcebergImportEvent {
    /// New data files to introduce to the iceberg table.
    pub data_files: Vec<FileId>,
    /// Maps from data filepath to its latest deletion vector (row index to delete).
    pub new_deletion_vector: HashMap<FileId, Vec<u64>>,
    /// New file indices to import.
    /// [`Vec<FileId>`] indicates the data files referenced by file indices.
    pub file_indices: Vec<Vec<FileId>>,
}

/// For the ease of serde, replay event only stores necessary part of [`IcebergSnapshotIndexMergePayload`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IcebergIndexMergeEvent {
    /// New file indices to import.
    /// [`Vec<FileId>`] indicates the data files referenced by file indices.
    pub new_file_indices: Vec<Vec<FileId>>,
    /// Old file indices to remove.
    /// [`Vec<FileId>`] indicates the data files referenced by file indices.
    pub old_file_indices: Vec<Vec<FileId>>,
}

/// For the ease of serde, replay event only stores necessary part of [`IcebergSnapshotDataCompactionPayload`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IcebergDataCompactionEvent {
    /// New data files to import.
    pub new_data_files_to_import: Vec<FileId>,
    /// Old data files to remove.
    pub old_data_files_to_remove: Vec<FileId>,
    /// New file indices to import.
    /// [`Vec<FileId>`] indicates the data files referenced by file indices.
    pub new_file_indices_to_import: Vec<Vec<FileId>>,
    /// Old file indices to remove.
    /// [`Vec<FileId>`] indicates the data files referenced by file indices.
    pub old_file_indices_to_remove: Vec<Vec<FileId>>,
}

/// For the ease of serde, replay event only stores necessary part of [`IcebergSnapshotPayload`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IcebergSnapshotEventInitiation {
    /// Event id.
    pub uuid: uuid::Uuid,
    /// Flush LSN.
    pub flush_lsn: u64,
    /// Committed deletion logs included in the current iceberg snapshot persistence operation, which is used to prune after persistence completion.
    pub committed_deletion_logs: HashSet<(FileId, usize /*row idx*/)>,
    /// Import payload.
    pub import_payload: IcebergImportEvent,
    /// Index merge payload.
    pub index_merge_payload: IcebergIndexMergeEvent,
    /// Data compaction payload.
    pub data_compaction_payload: IcebergDataCompactionEvent,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IcebergSnapshotEventCompletion {
    /// Event id.
    pub uuid: uuid::Uuid,
    /// Flush LSN.
    pub lsn: u64,
}

/// =====================
/// Index merge
/// =====================
///
/// For the ease of serde, replay event only stores necessary part of [`FileIndiceMergePayload`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexMergeEventInitiation {
    /// Event id.
    pub uuid: uuid::Uuid,
    /// Index merge payload.
    /// [`Vec<FileId>`] indicates the data files referenced by file indices.
    pub index_merge_payload: Vec<Vec<FileId>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexMergeEventCompletion {
    /// Event id.
    pub uuid: uuid::Uuid,
}

/// =====================
/// Data compaction
/// =====================
///
/// For the ease of serde, replay event only stores necessary part of [`NonEvictableHandle`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheHandleEvent {
    /// File handle id.
    pub file_id: TableUniqueFileId,
}

/// For the ease of serde, replay event only stores necessary part of [`SingleFileToCompact`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SingleCompactionPayloadEvent {
    /// File id.
    pub file_id: TableUniqueFileId,
    /// Number of rows deleted.
    pub num_rows_deleted: usize,
}

/// For the ease of serde, replay event only stores necessary part of [`DataCompactionPayload`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataCompactionEventInitiation {
    /// Event id.
    pub uuid: uuid::Uuid,
    /// Data files to compact.
    pub data_files: Vec<SingleCompactionPayloadEvent>,
    /// File indices to compact.
    /// [`Vec<FileId>`] indicates the data files referenced by file indices.
    pub file_indices: Vec<Vec<FileId>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataCompactionEventCompletion {
    /// Event id.
    pub uuid: uuid::Uuid,
    pub data_files: Vec<FileId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MooncakeTableEvent {
    /// =====================
    /// Foreground operations
    /// =====================
    ///
    /// Append a row.
    Append(AppendEvent),
    /// Delete a row.
    Delete(DeleteEvent),
    /// Commit operation.
    Commit(CommitEvent),
    /// Abort operation.
    Abort(AbortEvent),
    /// =====================
    /// Background operations
    /// =====================
    ///
    /// Flush operation initiation.
    FlushInitiation(FlushEventInitiation),
    FlushCompletion(FlushEventCompletion),
    /// Mooncake snapshot operation.
    MooncakeSnapshotInitiation(MooncakeSnapshotEventInitiation),
    MooncakeSnapshotCompletion(MooncakeSnapshotEventCompletion),
    /// Iceberg snapshot operation.
    IcebergSnapshotInitiation(Box<IcebergSnapshotEventInitiation>),
    IcebergSnapshotCompletion(IcebergSnapshotEventCompletion),
    /// Index merge operation.
    IndexMergeInitiation(IndexMergeEventInitiation),
    IndexMergeCompletion(IndexMergeEventCompletion),
    /// Data compaction operation.
    DataCompactionInitiation(DataCompactionEventInitiation),
    DataCompactionCompletion(DataCompactionEventCompletion),
}

/// Create append event.
pub fn create_append_event(row: MoonlinkRow, xact_id: Option<u32>) -> AppendEvent {
    AppendEvent { row, xact_id }
}
/// Create delete event.
pub fn create_delete_event(
    row: MoonlinkRow,
    lsn: Option<u64>,
    xact_id: Option<u32>,
) -> DeleteEvent {
    DeleteEvent { row, lsn, xact_id }
}
/// Create commit event.
pub fn create_commit_event(lsn: u64, xact_id: Option<u32>) -> CommitEvent {
    CommitEvent { lsn, xact_id }
}
/// Create abort event.
pub fn create_abort_event(xact_id: u32) -> AbortEvent {
    AbortEvent { xact_id }
}
/// Create flush events.
pub fn create_flush_event_initiation(
    uuid: uuid::Uuid,
    xact_id: Option<u32>,
    lsn: Option<u64>,
    commit_check_point: RecordLocation,
) -> FlushEventInitiation {
    FlushEventInitiation {
        uuid,
        xact_id,
        lsn,
        commit_check_point,
    }
}
pub fn create_flush_event_completion(
    uuid: uuid::Uuid,
    file_ids: Vec<FileId>,
) -> FlushEventCompletion {
    FlushEventCompletion { uuid, file_ids }
}
/// Create mooncake snapshot events.
pub fn create_mooncake_snapshot_event_initiation(
    uuid: uuid::Uuid,
    option: SnapshotOption,
) -> MooncakeSnapshotEventInitiation {
    MooncakeSnapshotEventInitiation { uuid, option }
}
pub fn create_mooncake_snapshot_event_completion(
    uuid: uuid::Uuid,
    lsn: u64,
) -> MooncakeSnapshotEventCompletion {
    MooncakeSnapshotEventCompletion { uuid, lsn }
}
/// Create iceberg snapshot events.
pub fn get_iceberg_snapshot_import_payload(
    payload: &IcebergSnapshotImportPayload,
) -> IcebergImportEvent {
    IcebergImportEvent {
        data_files: payload
            .data_files
            .iter()
            .map(|f| f.file_id())
            .collect::<Vec<_>>(),
        new_deletion_vector: payload
            .new_deletion_vector
            .iter()
            .map(|(f, dv)| (f.file_id(), dv.collect_deleted_rows()))
            .collect::<HashMap<_, _>>(),
        file_indices: payload
            .file_indices
            .iter()
            .map(|cur_index| {
                cur_index
                    .files
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>(),
    }
}
pub fn get_iceberg_index_merge_payload(
    payload: &IcebergSnapshotIndexMergePayload,
) -> IcebergIndexMergeEvent {
    IcebergIndexMergeEvent {
        new_file_indices: payload
            .new_file_indices_to_import
            .iter()
            .map(|cur_index| {
                cur_index
                    .files
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>(),
        old_file_indices: payload
            .old_file_indices_to_remove
            .iter()
            .map(|cur_index| {
                cur_index
                    .files
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>(),
    }
}
pub fn get_iceberg_data_compaction_payload(
    payload: &IcebergSnapshotDataCompactionPayload,
) -> IcebergDataCompactionEvent {
    IcebergDataCompactionEvent {
        new_data_files_to_import: payload
            .new_data_files_to_import
            .iter()
            .map(|f| f.file_id())
            .collect::<Vec<_>>(),
        old_data_files_to_remove: payload
            .old_data_files_to_remove
            .iter()
            .map(|f| f.file_id())
            .collect::<Vec<_>>(),
        new_file_indices_to_import: payload
            .new_file_indices_to_import
            .iter()
            .map(|cur_index| {
                cur_index
                    .files
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>(),
        old_file_indices_to_remove: payload
            .old_file_indices_to_remove
            .iter()
            .map(|cur_index| {
                cur_index
                    .files
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>(),
    }
}
pub fn create_iceberg_snapshot_event_initiation(
    uuid: uuid::Uuid,
    payload: &IcebergSnapshotPayload,
) -> IcebergSnapshotEventInitiation {
    IcebergSnapshotEventInitiation {
        uuid,
        flush_lsn: payload.flush_lsn,
        committed_deletion_logs: payload.committed_deletion_logs.clone(),
        import_payload: get_iceberg_snapshot_import_payload(&payload.import_payload),
        index_merge_payload: get_iceberg_index_merge_payload(&payload.index_merge_payload),
        data_compaction_payload: get_iceberg_data_compaction_payload(
            &payload.data_compaction_payload,
        ),
    }
}
pub fn create_iceberg_snapshot_event_completion(
    uuid: uuid::Uuid,
    lsn: u64,
) -> IcebergSnapshotEventCompletion {
    IcebergSnapshotEventCompletion { uuid, lsn }
}
/// Create index merge events.
pub fn create_index_merge_event_initiation(
    uuid: uuid::Uuid,
    payload: &FileIndiceMergePayload,
) -> IndexMergeEventInitiation {
    IndexMergeEventInitiation {
        uuid,
        index_merge_payload: payload
            .file_indices
            .iter()
            .map(|cur_index| {
                cur_index
                    .files
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>(),
    }
}
pub fn create_index_merge_event_completion(uuid: uuid::Uuid) -> IndexMergeEventCompletion {
    IndexMergeEventCompletion { uuid }
}
/// Create data compaction events.
pub fn get_file_compaction_payload(
    single_file_compaction: &SingleFileToCompact,
) -> SingleCompactionPayloadEvent {
    SingleCompactionPayloadEvent {
        file_id: single_file_compaction.file_id,
        num_rows_deleted: single_file_compaction
            .deletion_vector
            .as_ref()
            .map_or(0, |puffin_blob_ref| puffin_blob_ref.num_rows),
    }
}
pub fn create_data_compaction_event_initiation(
    uuid: uuid::Uuid,
    payload: &DataCompactionPayload,
) -> DataCompactionEventInitiation {
    DataCompactionEventInitiation {
        uuid,
        data_files: payload
            .disk_files
            .iter()
            .map(get_file_compaction_payload)
            .collect::<Vec<_>>(),
        file_indices: payload
            .file_indices
            .iter()
            .map(|cur_index| {
                cur_index
                    .files
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>(),
    }
}
pub fn create_data_compaction_event_completion(
    uuid: uuid::Uuid,
    data_files: Vec<FileId>,
) -> DataCompactionEventCompletion {
    DataCompactionEventCompletion { uuid, data_files }
}

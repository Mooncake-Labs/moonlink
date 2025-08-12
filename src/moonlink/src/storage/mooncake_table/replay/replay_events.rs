use std::collections::{HashMap, HashSet};

/// This module defines data struct for mooncake table events.
use serde::{Deserialize, Serialize};

use crate::row::MoonlinkRow;
use crate::storage::compaction::table_compaction::SingleFileToCompact;
use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
use crate::storage::mooncake_table::replay::event_id_assigner::get_next_event_id;
use crate::storage::mooncake_table::table_snapshot::IcebergSnapshotDataCompactionPayload;
use crate::storage::mooncake_table::{
    DataCompactionPayload, FileIndiceMergePayload, IcebergSnapshotImportPayload,
    IcebergSnapshotIndexMergePayload, IcebergSnapshotPayload, SnapshotOption,
};
use crate::storage::storage_utils::{FileId, TableUniqueFileId};
use crate::NonEvictableHandle;

/// Type alias for background event id.
pub(crate) type BackgroundEventId = u64;

/// =====================
/// Foreground operations
/// =====================
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct AppendEvent {
    /// Moonlink row.
    pub(crate) row: MoonlinkRow,
    /// Transaction id, only assigned on streaming ones.
    pub(crate) xact_id: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct DeleteEvent {
    /// Moonlink row.
    pub(crate) row: MoonlinkRow,
    /// Deletion LSN.
    pub(crate) lsn: u64,
    /// Transaction id, only assigned on streaming ones.
    pub(crate) xact_id: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CommitEvent {
    /// Transaction id, only assigned on streaming ones.
    pub(crate) xact_id: Option<u64>,
    /// Commit LSN.
    pub(crate) lsn: u64,
}

/// =====================
/// Flush operation
/// =====================
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct FlushEventInitiation {
    /// Unique event id, assigned globally.
    pub(crate) id: BackgroundEventId,
    /// Transaction id, only assigned on streaming ones.
    pub(crate) xact_id: Option<u64>,
    /// Flush LSN.
    pub(crate) lsn: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct FlushEventCompletion {
    /// Unique event id, assigned globally.
    pub(crate) id: BackgroundEventId,
}

/// =====================
/// Mooncake snapshot
/// =====================
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct MooncakeSnapshotEventInitiation {
    /// Unique event id, assigned globally.
    pub(crate) id: BackgroundEventId,
    /// Mooncake snapshot options.
    pub(crate) option: SnapshotOption,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct MooncakeSnapshotEventCompletion {
    /// Unique event id, assigned globally.
    pub(crate) id: BackgroundEventId,
}

/// =====================
/// Iceberg snapshot
/// =====================
///
/// For the ease of serde, replay event only stores necessary part of [`IcebergSnapshotImportPayload`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct IcebergImportEvent {
    /// New data files to introduce to the iceberg table.
    pub(crate) data_files: Vec<FileId>,
    /// Maps from data filepath to its latest deletion vector (row index to delete).
    pub(crate) new_deletion_vector: HashMap<FileId, Vec<u64>>,
    /// New file indices to import.
    /// [`Vec<FileId>`] indicates the data files referenced by file indices.
    pub(crate) file_indices: Vec<Vec<FileId>>,
}

/// For the ease of serde, replay event only stores necessary part of [`IcebergSnapshotIndexMergePayload`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct IcebergIndexMergeEvent {
    /// New file indices to import.
    /// [`Vec<FileId>`] indicates the data files referenced by file indices.
    pub(crate) new_file_indices: Vec<Vec<FileId>>,
    /// Old file indices to remove.
    /// [`Vec<FileId>`] indicates the data files referenced by file indices.
    pub(crate) old_file_indices: Vec<Vec<FileId>>,
}

/// For the ease of serde, replay event only stores necessary part of [`IcebergSnapshotDataCompactionPayload`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct IcebergDataCompactionEvent {
    /// New data files to import.
    pub(crate) new_data_files_to_import: Vec<FileId>,
    /// Old data files to remove.
    pub(crate) old_data_files_to_remove: Vec<FileId>,
    /// New file indices to import.
    /// [`Vec<FileId>`] indicates the data files referenced by file indices.
    pub(crate) new_file_indices_to_import: Vec<Vec<FileId>>,
    /// Old file indices to remove.
    /// [`Vec<FileId>`] indicates the data files referenced by file indices.
    pub(crate) old_file_indices_to_remove: Vec<Vec<FileId>>,
}

/// For the ease of serde, replay event only stores necessary part of [`IcebergSnapshotPayload`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct IcebergSnapshotEventInitiation {
    /// Unique event id, assigned globally.
    pub(crate) id: BackgroundEventId,
    /// Flush LSN.
    pub(crate) flush_lsn: u64,
    /// Committed deletion logs included in the current iceberg snapshot persistence operation, which is used to prune after persistence completion.
    pub(crate) committed_deletion_logs: HashSet<(FileId, usize /*row idx*/)>,
    /// Import payload.
    pub(crate) import_payload: IcebergImportEvent,
    /// Index merge payload.
    pub(crate) index_merge_payload: IcebergIndexMergeEvent,
    /// Data compaction payload.
    pub(crate) data_compaction_payload: IcebergDataCompactionEvent,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct IcebergSnapshotEventCompletion {
    /// Unique event id, assigned globally.
    pub(crate) id: BackgroundEventId,
}

/// =====================
/// Index merge
/// =====================
///
/// For the ease of serde, replay event only stores necessary part of [`FileIndiceMergePayload`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct IndexMergeEventInitiation {
    /// Unique event id, assigned globally.
    pub(crate) id: BackgroundEventId,
    /// Index merge payload.
    /// [`Vec<FileId>`] indicates the data files referenced by file indices.
    pub(crate) index_merge_payload: Vec<Vec<FileId>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct IndexMergeEventCompletion {
    /// Unique event id, assigned globally.
    pub(crate) id: BackgroundEventId,
}

/// =====================
/// Data compaction
/// =====================
///
/// For the ease of serde, replay event only stores necessary part of [`NonEvictableHandle`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CacheHandleEvent {
    /// File handle id.
    pub(crate) file_id: TableUniqueFileId,
}

/// For the ease of serde, replay event only stores necessary part of [`SingleFileToCompact`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SingleCompactionPayloadEvent {
    /// File id.
    pub(crate) file_id: TableUniqueFileId,
    /// Deletion vector.
    pub(crate) puffin_blob_ref: Option<CacheHandleEvent>,
}

/// For the ease of serde, replay event only stores necessary part of [`DataCompactionPayload`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct DataCompactionEventInitiation {
    /// Unique event id, assigned globally.
    pub(crate) id: BackgroundEventId,
    /// Data files to compact.
    pub(crate) data_files: Vec<SingleCompactionPayloadEvent>,
    /// File indices to compact.
    /// [`Vec<FileId>`] indicates the data files referenced by file indices.
    pub(crate) file_indices: Vec<Vec<FileId>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct DataCompactionEventCompletion {
    /// Unique event id, assigned globally.
    pub(crate) id: BackgroundEventId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum MooncakeTableEvent {
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
pub(crate) fn create_append_event(row: MoonlinkRow, xact_id: Option<u64>) -> AppendEvent {
    AppendEvent { row, xact_id }
}
/// Create delete event.
pub(crate) fn create_delete_event(row: MoonlinkRow, lsn: u64, xact_id: Option<u64>) -> DeleteEvent {
    DeleteEvent { row, lsn, xact_id }
}
/// Create commit event.
pub(crate) fn create_commit_event(lsn: u64, xact_id: Option<u64>) -> CommitEvent {
    CommitEvent { lsn, xact_id }
}
/// Create flush events.
pub(crate) fn create_flush_event_initiation(
    xact_id: Option<u64>,
    lsn: u64,
) -> FlushEventInitiation {
    FlushEventInitiation {
        id: get_next_event_id(),
        xact_id,
        lsn,
    }
}
pub(crate) fn create_flush_event_completion(id: BackgroundEventId) -> FlushEventCompletion {
    FlushEventCompletion { id }
}
/// Create mooncake snapshot events.
pub(crate) fn create_mooncake_snapshot_event_initiation(
    option: SnapshotOption,
) -> MooncakeSnapshotEventInitiation {
    MooncakeSnapshotEventInitiation {
        id: get_next_event_id(),
        option,
    }
}
pub(crate) fn create_mooncake_snapshot_event_completion(
    id: BackgroundEventId,
) -> MooncakeSnapshotEventCompletion {
    MooncakeSnapshotEventCompletion { id }
}
/// Create iceberg snapshot events.
pub(crate) fn get_iceberg_snapshot_import_payload(
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
pub(crate) fn get_iceberg_index_merge_payload(
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
pub(crate) fn get_iceberg_data_compaction_payload(
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
pub(crate) fn create_iceberg_snapshot_event_initiation(
    payload: &IcebergSnapshotPayload,
) -> IcebergSnapshotEventInitiation {
    IcebergSnapshotEventInitiation {
        id: get_next_event_id(),
        flush_lsn: payload.flush_lsn,
        committed_deletion_logs: payload.committed_deletion_logs.clone(),
        import_payload: get_iceberg_snapshot_import_payload(&payload.import_payload),
        index_merge_payload: get_iceberg_index_merge_payload(&payload.index_merge_payload),
        data_compaction_payload: get_iceberg_data_compaction_payload(
            &payload.data_compaction_payload,
        ),
    }
}
/// Create index merge events.
pub(crate) fn create_index_merge_event_initiation(
    payload: &FileIndiceMergePayload,
) -> IndexMergeEventInitiation {
    IndexMergeEventInitiation {
        id: get_next_event_id(),
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
/// Create data compaction events.
pub(crate) fn get_cache_handle_event(
    cache_handle: &Option<PuffinBlobRef>,
) -> Option<CacheHandleEvent> {
    if let Some(cache_handle) = cache_handle {
        return Some(CacheHandleEvent {
            file_id: cache_handle.puffin_file_cache_handle.file_id,
        });
    }
    None
}
pub(crate) fn get_file_compaction_payload(
    single_file_compaction: &SingleFileToCompact,
) -> SingleCompactionPayloadEvent {
    SingleCompactionPayloadEvent {
        file_id: single_file_compaction.file_id,
        puffin_blob_ref: get_cache_handle_event(&single_file_compaction.deletion_vector),
    }
}
pub(crate) fn create_data_compaction_event_initiation(
    payload: &DataCompactionPayload,
) -> DataCompactionEventInitiation {
    DataCompactionEventInitiation {
        id: get_next_event_id(),
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

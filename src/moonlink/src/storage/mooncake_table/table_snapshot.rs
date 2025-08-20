use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
use crate::storage::index::persisted_bucket_hash_map::GlobalIndex;
/// Items needed for iceberg snapshot.
use crate::storage::index::FileIndex as MooncakeFileIndex;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::replay::replay_events::BackgroundEventId;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::storage::storage_utils::FileId;
use crate::storage::storage_utils::MooncakeDataFileRef;
use crate::storage::TableManager;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

////////////////////////////
/// Iceberg snapshot payload
////////////////////////////
///
/// Iceberg snapshot payload by write operations.
#[derive(Clone, Default)]
pub struct IcebergSnapshotImportPayload {
    /// New data files to introduce to the iceberg table.
    pub(crate) data_files: Vec<MooncakeDataFileRef>,
    /// Maps from data filepath to its latest deletion vector.
    pub(crate) new_deletion_vector: HashMap<MooncakeDataFileRef, BatchDeletionVector>,
    /// New file indices to import.
    pub(crate) file_indices: Vec<MooncakeFileIndex>,
}

impl std::fmt::Debug for IcebergSnapshotImportPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergSnapshotImportPayload")
            .field("data files count", &self.data_files.len())
            .field("new deletion vector count", &self.new_deletion_vector.len())
            .field("file indices count", &self.file_indices.len())
            .finish()
    }
}

/// Iceberg snapshot payload by index merge operations.
#[derive(Clone, Default)]
pub struct IcebergSnapshotIndexMergePayload {
    /// New file indices to import to the iceberg table.
    pub(crate) new_file_indices_to_import: Vec<MooncakeFileIndex>,
    /// Merged file indices to remove from the iceberg table.
    pub(crate) old_file_indices_to_remove: Vec<MooncakeFileIndex>,
}

impl IcebergSnapshotIndexMergePayload {
    /// Return whether the payload is empty.
    pub fn is_empty(&self) -> bool {
        if self.new_file_indices_to_import.is_empty() {
            assert!(self.old_file_indices_to_remove.is_empty());
            return true;
        }

        assert!(!self.old_file_indices_to_remove.is_empty());
        false
    }
}

impl std::fmt::Debug for IcebergSnapshotIndexMergePayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergSnapshotIndexMergePayload")
            .field(
                "new file indices to import count",
                &self.new_file_indices_to_import.len(),
            )
            .field(
                "old file indices to remove count",
                &self.old_file_indices_to_remove.len(),
            )
            .finish()
    }
}

/// Iceberg snapshot payload by data file compaction operations.
#[derive(Clone, Default)]
pub struct IcebergSnapshotDataCompactionPayload {
    /// New data files to import to the iceberg table.
    pub(crate) new_data_files_to_import: Vec<MooncakeDataFileRef>,
    /// Old data files to remove from the iceberg table.
    pub(crate) old_data_files_to_remove: Vec<MooncakeDataFileRef>,
    /// New file indices to import to the iceberg table.
    pub(crate) new_file_indices_to_import: Vec<MooncakeFileIndex>,
    /// Old file indices to remove from the iceberg table.
    pub(crate) old_file_indices_to_remove: Vec<MooncakeFileIndex>,
}

impl std::fmt::Debug for IcebergSnapshotDataCompactionPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergSnapshotDataCompactionPayload")
            .field(
                "new data files to import count",
                &self.new_data_files_to_import.len(),
            )
            .field(
                "old data files to remove count",
                &self.old_data_files_to_remove.len(),
            )
            .field(
                "new file indices to import count",
                &self.new_file_indices_to_import.len(),
            )
            .field(
                "old file indices to remove count",
                &self.old_file_indices_to_remove.len(),
            )
            .finish()
    }
}

impl IcebergSnapshotDataCompactionPayload {
    /// Return whether data compaction payload is empty.
    pub fn is_empty(&self) -> bool {
        if self.old_data_files_to_remove.is_empty() {
            assert!(self.new_data_files_to_import.is_empty());
            assert!(self.new_file_indices_to_import.is_empty());
            assert!(self.old_file_indices_to_remove.is_empty());
            return true;
        }

        assert!(!self.old_file_indices_to_remove.is_empty());
        false
    }
}

#[derive(Clone)]
pub struct IcebergSnapshotPayload {
    /// Background event id.
    pub(crate) id: BackgroundEventId,
    /// UUID for the current persistence operation, used for observability purpose.
    pub(crate) uuid: uuid::Uuid,
    /// Flush LSN.
    pub(crate) flush_lsn: u64,
    /// Committed deletion logs included in the current iceberg snapshot persistence operation, which is used to prune after persistence completion.
    pub(crate) committed_deletion_logs: HashSet<(FileId, usize /*row idx*/)>,
    /// New mooncake table schema.
    pub(crate) new_table_schema: Option<Arc<MooncakeTableMetadata>>,
    /// Payload by import operations.
    pub(crate) import_payload: IcebergSnapshotImportPayload,
    /// Payload by index merge operations.
    pub(crate) index_merge_payload: IcebergSnapshotIndexMergePayload,
    /// Payload by data file compaction operations.
    pub(crate) data_compaction_payload: IcebergSnapshotDataCompactionPayload,
}

impl std::fmt::Debug for IcebergSnapshotPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergSnapshotPayload")
            .field("id", &self.id)
            .field("uuid", &self.uuid)
            .field("flush_lsn", &self.flush_lsn)
            .field(
                "committed deletion logs count",
                &self.committed_deletion_logs.len(),
            )
            .field("import payload", &self.import_payload)
            .field("index merge payload", &self.index_merge_payload)
            .field("data compaction payload", &self.data_compaction_payload)
            .finish()
    }
}

impl IcebergSnapshotPayload {
    /// Get the number of new files created in iceberg table.
    pub fn get_new_file_ids_num(&self) -> u32 {
        // Only deletion vector puffin blobs create files with new file ids.
        self.import_payload.new_deletion_vector.len() as u32
    }

    /// Return whether the payload contains table maintenance content.
    pub fn contains_table_maintenance_payload(&self) -> bool {
        if !self.index_merge_payload.is_empty() {
            return true;
        }
        if !self.data_compaction_payload.is_empty() {
            return true;
        }
        false
    }

    /// Get all new data files reference.
    #[cfg(any(test, debug_assertions))]
    pub fn get_new_data_files(&self) -> Vec<MooncakeDataFileRef> {
        let mut new_data_files = vec![];
        new_data_files.extend(self.import_payload.data_files.clone());
        new_data_files.extend(
            self.data_compaction_payload
                .new_data_files_to_import
                .clone(),
        );
        new_data_files
    }

    /// Get all old data files reference.
    #[cfg(any(test, debug_assertions))]
    pub fn get_old_data_files(&self) -> Vec<MooncakeDataFileRef> {
        let mut old_data_files = vec![];
        old_data_files.extend(
            self.data_compaction_payload
                .old_data_files_to_remove
                .clone(),
        );
        old_data_files
    }

    /// Get all new file indices reference.
    #[cfg(any(test, debug_assertions))]
    pub fn get_new_file_indices(&self) -> Vec<MooncakeFileIndex> {
        let mut new_file_indices = vec![];
        new_file_indices.extend(self.import_payload.file_indices.clone());
        new_file_indices.extend(self.index_merge_payload.new_file_indices_to_import.clone());
        new_file_indices.extend(
            self.data_compaction_payload
                .new_file_indices_to_import
                .clone(),
        );
        new_file_indices
    }

    /// Get all old file indices reference.
    #[cfg(any(test, debug_assertions))]
    pub fn get_old_file_indices(&self) -> Vec<MooncakeFileIndex> {
        let mut old_file_indices = vec![];
        old_file_indices.extend(self.index_merge_payload.old_file_indices_to_remove.clone());
        old_file_indices.extend(
            self.data_compaction_payload
                .old_file_indices_to_remove
                .clone(),
        );
        old_file_indices
    }
}

////////////////////////////
/// Iceberg snapshot result
////////////////////////////
///
/// Iceberg snapshot import result.
#[derive(Clone, Default)]
pub struct IcebergSnapshotImportResult {
    /// Persisted data files.
    pub(crate) new_data_files: Vec<MooncakeDataFileRef>,
    /// Persisted puffin blob reference.
    pub(crate) puffin_blob_ref: HashMap<FileId, PuffinBlobRef>,
    /// Imported file indices.
    pub(crate) new_file_indices: Vec<MooncakeFileIndex>,
}

impl IcebergSnapshotImportResult {
    /// Return whether import result is empty.
    pub fn is_empty(&self) -> bool {
        self.new_data_files.is_empty()
            && self.puffin_blob_ref.is_empty()
            && self.new_file_indices.is_empty()
    }
}

impl std::fmt::Debug for IcebergSnapshotImportResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergSnapshotImportResult")
            .field("new data file count", &self.new_data_files.len())
            .field("new file indices count", &self.new_file_indices.len())
            .field("puffin blob ref count", &self.puffin_blob_ref.len())
            .finish()
    }
}

/// Iceberg snapshot index merge result.
#[derive(Clone, Default)]
pub struct IcebergSnapshotIndexMergeResult {
    /// New file indices which are imported the iceberg table.
    pub(crate) new_file_indices_imported: Vec<MooncakeFileIndex>,
    /// Merged file indices which are removed from the iceberg table.
    pub(crate) old_file_indices_removed: Vec<MooncakeFileIndex>,
}

impl IcebergSnapshotIndexMergeResult {
    /// Return whether index merge result is empty.
    pub fn is_empty(&self) -> bool {
        if self.new_file_indices_imported.is_empty() {
            assert!(self.old_file_indices_removed.is_empty());
            return true;
        }

        assert!(!self.old_file_indices_removed.is_empty());
        false
    }
}

impl std::fmt::Debug for IcebergSnapshotIndexMergeResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergSnapshotIndexMergeResult")
            .field(
                "new file indices imported count",
                &self.new_file_indices_imported.len(),
            )
            .field(
                "old file indices removed count",
                &self.old_file_indices_removed.len(),
            )
            .finish()
    }
}

/// Iceberg snapshot data file compaction result.
#[derive(Clone, Default)]
pub struct IcebergSnapshotDataCompactionResult {
    /// New data files which are importedthe iceberg table.
    pub(crate) new_data_files_imported: Vec<MooncakeDataFileRef>,
    /// Old data files which are removed from the iceberg table.
    pub(crate) old_data_files_removed: Vec<MooncakeDataFileRef>,
    /// New file indices to import to the iceberg table.
    pub(crate) new_file_indices_imported: Vec<MooncakeFileIndex>,
    /// Old data files to remove from the iceberg table.
    pub(crate) old_file_indices_removed: Vec<MooncakeFileIndex>,
}

impl IcebergSnapshotDataCompactionResult {
    /// Return whether data compaction result is empty.
    pub fn is_empty(&self) -> bool {
        if self.old_data_files_removed.is_empty() {
            assert!(self.new_data_files_imported.is_empty());
            assert!(self.new_file_indices_imported.is_empty());
            assert!(self.old_file_indices_removed.is_empty());
            return true;
        }

        assert!(!self.old_data_files_removed.is_empty());
        false
    }
}

impl std::fmt::Debug for IcebergSnapshotDataCompactionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergSnapshotDataCompactionResult")
            .field(
                "new data files imported count",
                &self.new_data_files_imported.len(),
            )
            .field(
                "old data files removed count",
                &self.old_data_files_removed.len(),
            )
            .field(
                "new file indices imported count",
                &self.new_file_indices_imported.len(),
            )
            .field(
                "old file indices removed count",
                &self.old_file_indices_removed.len(),
            )
            .finish()
    }
}

pub struct IcebergSnapshotResult {
    /// Background event id.
    pub(crate) id: BackgroundEventId,
    /// UUID for the current persistence operation, used for observability purpose.
    pub(crate) uuid: uuid::Uuid,
    /// Table manager is (1) not `Sync` safe; (2) only used at iceberg snapshot creation, so we `move` it around every snapshot.
    pub(crate) table_manager: Option<Box<dyn TableManager>>,
    /// Iceberg flush LSN.
    pub(crate) flush_lsn: u64,
    /// Mooncake schema sync-ed to iceberg.
    pub(crate) new_table_schema: Option<Arc<MooncakeTableMetadata>>,
    /// Committed deletion logs included in the current iceberg snapshot persistence operation, which is used to prune after persistence completion.
    pub(crate) committed_deletion_logs: HashSet<(FileId, usize /*row idx*/)>,
    /// Iceberg import result.
    pub(crate) import_result: IcebergSnapshotImportResult,
    /// Iceberg index merge result.
    pub(crate) index_merge_result: IcebergSnapshotIndexMergeResult,
    /// Iceberg data file compaction result.
    pub(crate) data_compaction_result: IcebergSnapshotDataCompactionResult,
}

impl Clone for IcebergSnapshotResult {
    fn clone(&self) -> Self {
        IcebergSnapshotResult {
            id: self.id,
            uuid: self.uuid,
            table_manager: None,
            flush_lsn: self.flush_lsn,
            new_table_schema: self.new_table_schema.clone(),
            committed_deletion_logs: self.committed_deletion_logs.clone(),
            import_result: self.import_result.clone(),
            index_merge_result: self.index_merge_result.clone(),
            data_compaction_result: self.data_compaction_result.clone(),
        }
    }
}

impl IcebergSnapshotResult {
    /// Return whether iceberg snapshot result contains table maintenance persistence result.
    pub fn contains_maintenance_result(&self) -> bool {
        if !self.index_merge_result.is_empty() {
            return true;
        }
        if !self.data_compaction_result.is_empty() {
            return true;
        }
        false
    }
}

impl std::fmt::Debug for IcebergSnapshotResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergSnapshotResult")
            .field("id", &self.id)
            .field("uuid", &self.uuid)
            .field("id", &self.id)
            .field("flush_lsn", &self.flush_lsn)
            .field(
                "committed deletion log count",
                &self.committed_deletion_logs.len(),
            )
            .field("import_result", &self.import_result)
            .field("index_merge_result", &self.index_merge_result)
            .field("data_compaction_result", &self.data_compaction_result)
            .finish()
    }
}

////////////////////////////
/// Index merge
////////////////////////////
///
#[derive(Clone)]
pub struct FileIndiceMergePayload {
    /// Background event id.
    pub(crate) id: BackgroundEventId,
    /// UUID for current index merge operation, used for observability purpose.
    pub(crate) uuid: uuid::Uuid,
    /// File indices to merge.
    pub(crate) file_indices: HashSet<GlobalIndex>,
}

impl std::fmt::Debug for FileIndiceMergePayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileIndiceMergePayload")
            .field("id", &self.id)
            .field("uuid", &self.uuid)
            .field("file indices count", &self.file_indices.len())
            .finish()
    }
}

#[derive(Clone, Default)]
pub struct FileIndiceMergeResult {
    /// Background event id.
    pub(crate) id: BackgroundEventId,
    /// UUID for current index merge operation, used for observability purpose.
    pub(crate) uuid: uuid::Uuid,
    /// Old file indices being merged.
    pub(crate) old_file_indices: HashSet<GlobalIndex>,
    /// New file indice merged.
    pub(crate) new_file_indices: Vec<GlobalIndex>,
}

impl FileIndiceMergeResult {
    /// Return whether the merge result is not assigned and is empty.
    pub fn is_empty(&self) -> bool {
        if self.old_file_indices.is_empty() {
            assert!(self.new_file_indices.is_empty());
            return true;
        }
        false
    }
}

impl std::fmt::Debug for FileIndiceMergeResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileIndiceMergeResult")
            .field("id", &self.id)
            .field("uuid", &self.uuid)
            .field("old file indices count", &self.old_file_indices.len())
            .field("new file indices count", &self.new_file_indices.len())
            .finish()
    }
}

/// Util functions to take all data files to import.
pub fn take_data_files_to_import(
    snapshot_payload: &mut IcebergSnapshotPayload,
) -> Vec<MooncakeDataFileRef> {
    let mut new_data_files = std::mem::take(&mut snapshot_payload.import_payload.data_files);
    new_data_files.extend(std::mem::take(
        &mut snapshot_payload
            .data_compaction_payload
            .new_data_files_to_import,
    ));
    new_data_files
}

/// Util functions to take all data files to remove.
pub fn take_data_files_to_remove(
    snapshot_payload: &mut IcebergSnapshotPayload,
) -> Vec<MooncakeDataFileRef> {
    std::mem::take(
        &mut snapshot_payload
            .data_compaction_payload
            .old_data_files_to_remove,
    )
}

/// Util functions to take all file indices to import.
pub fn take_file_indices_to_import(
    snapshot_payload: &mut IcebergSnapshotPayload,
) -> Vec<MooncakeFileIndex> {
    let mut new_file_indices = std::mem::take(&mut snapshot_payload.import_payload.file_indices);
    new_file_indices.extend(std::mem::take(
        &mut snapshot_payload
            .index_merge_payload
            .new_file_indices_to_import,
    ));
    new_file_indices.extend(std::mem::take(
        &mut snapshot_payload
            .data_compaction_payload
            .new_file_indices_to_import,
    ));
    new_file_indices
}

/// Util function to take all file indices to remove.
pub fn take_file_indices_to_remove(
    snapshot_payload: &mut IcebergSnapshotPayload,
) -> Vec<MooncakeFileIndex> {
    let mut old_file_indices = std::mem::take(
        &mut snapshot_payload
            .index_merge_payload
            .old_file_indices_to_remove,
    );
    old_file_indices.extend(std::mem::take(
        &mut snapshot_payload
            .data_compaction_payload
            .old_file_indices_to_remove,
    ));
    old_file_indices
}

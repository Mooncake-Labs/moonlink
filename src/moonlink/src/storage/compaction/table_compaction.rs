use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
use crate::storage::index::FileIndex;
use crate::storage::storage_utils::MooncakeDataFileRef;
use crate::storage::storage_utils::RecordLocation;
use crate::storage::storage_utils::TableUniqueFileId;
use crate::ObjectStorageCache;

use std::collections::HashMap;
use std::collections::HashSet;

/// Single disk file and its deletion vector to apply.
#[derive(Clone, Debug)]
pub struct SingleFileToCompact {
    /// Unique file id to lookup in the object storage cache.
    pub(crate) file_id: TableUniqueFileId,
    /// Remote data file; only persisted data files will be compacted.
    pub(crate) filepath: String,
    /// Deletion vector.
    /// If assigned, the puffin file has been pinned so later accesses are valid.
    pub(crate) deletion_vector: Option<PuffinBlobRef>,
}

/// Payload to trigger a compaction operation.
#[derive(Clone, Debug)]
pub struct DataCompactionPayload {
    /// Object storage cache.
    pub(crate) object_storage_cache: ObjectStorageCache,
    /// Disk files to compact, including their deletion vector to apply.
    pub(crate) disk_files: Vec<SingleFileToCompact>,
    /// File indices to compact and rewrite.
    pub(crate) file_indices: Vec<FileIndex>,
}

/// Entry for compacted data files.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CompactedDataEntry {
    /// Number of rows for the compacted data file.
    pub(crate) num_rows: usize,
    /// Compacted file size.
    pub(crate) file_size: usize,
}

/// Remapped record location after compaction.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RemappedRecordLocation {
    pub(crate) record_location: RecordLocation,
    pub(crate) new_data_file: MooncakeDataFileRef,
}

/// Result for a compaction operation.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DataCompactionResult {
    /// Data files which get compacted, maps from old record location to new one.
    pub(crate) remapped_data_files: HashMap<RecordLocation, RemappedRecordLocation>,
    /// Old compacted data files, which maps to their corresponding compacted data file.
    pub(crate) old_data_files: HashSet<MooncakeDataFileRef>,
    /// New compacted data files.
    pub(crate) new_data_files: Vec<(MooncakeDataFileRef, CompactedDataEntry)>,
    /// Old compacted file indices.
    pub(crate) old_file_indices: HashSet<FileIndex>,
    /// New compacted file indices.
    pub(crate) new_file_indices: Vec<FileIndex>,
    /// Compaction interacts with object storage cache, this field records evicted files to delete.
    ///
    /// TODO(hjiang): No need to pass the files out, could directly delete in compaction.
    pub(crate) evicted_files_to_delete: Vec<String>,
}

impl DataCompactionResult {
    /// Return whether data compaction result is empty.
    pub fn is_empty(&self) -> bool {
        if self.old_data_files.is_empty() {
            assert!(self.remapped_data_files.is_empty());
            assert!(self.old_data_files.is_empty());
            assert!(self.old_file_indices.is_empty());
            assert!(self.new_file_indices.is_empty());
            assert!(self.evicted_files_to_delete.is_empty());
            return true;
        }

        // If all rows have been deleted after compaction, there'll be no new data files, file indices and remaps.
        assert!(!self.old_file_indices.is_empty());
        false
    }
}

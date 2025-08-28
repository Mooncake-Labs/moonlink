use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
use crate::storage::index::FileIndex;
use crate::storage::storage_utils::MooncakeDataFileRef;
use crate::storage::storage_utils::RecordLocation;
use crate::storage::storage_utils::TableUniqueFileId;
use crate::CacheTrait;

use std::borrow::Borrow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

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

impl Borrow<TableUniqueFileId> for SingleFileToCompact {
    fn borrow(&self) -> &TableUniqueFileId {
        &self.file_id
    }
}

impl Hash for SingleFileToCompact {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_id.hash(state);
    }
}
impl PartialEq for SingleFileToCompact {
    fn eq(&self, other: &Self) -> bool {
        self.file_id == other.file_id
    }
}
impl Eq for SingleFileToCompact {}

/// Payload to trigger a compaction operation.
#[derive(Clone)]
pub struct DataCompactionPayload {
    /// Background event id.
    pub(crate) uuid: uuid::Uuid,
    /// Object storage cache.
    pub(crate) object_storage_cache: Arc<dyn CacheTrait>,
    /// Filesystem accessor.
    pub(crate) filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
    /// Disk files to compact, including their deletion vector to apply.
    pub(crate) disk_files: Vec<SingleFileToCompact>,
    /// File indices to compact and rewrite.
    pub(crate) file_indices: Vec<FileIndex>,
}

impl std::fmt::Debug for DataCompactionPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataCompactionPayload")
            .field("uuid", &self.uuid)
            .field("object_storage_cache", &self.object_storage_cache)
            .field("filesystem_accessor", &self.filesystem_accessor)
            .field("disk file number", &self.disk_files.len())
            .field("file indices number", &self.file_indices.len())
            .finish()
    }
}

impl DataCompactionPayload {
    /// Get max possible number of new file ids number.
    pub fn get_new_compacted_data_file_ids_number(&self) -> u32 {
        // In worst case, we create two new files (one data file, one index block) per data file.
        self.disk_files.len() as u32 * 2
    }
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
#[derive(Clone, Default, PartialEq)]
pub struct DataCompactionResult {
    /// Background event id.
    pub(crate) uuid: uuid::Uuid,
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
        // If all rows have been deleted after compaction, there'll be no new data files, file indices and remaps.
        if self.old_data_files.is_empty() {
            assert!(self.remapped_data_files.is_empty());
            assert!(self.old_data_files.is_empty());
            assert!(self.old_file_indices.is_empty());
            assert!(self.new_file_indices.is_empty());
            assert!(self.evicted_files_to_delete.is_empty());
            return true;
        }

        false
    }
}

impl std::fmt::Debug for DataCompactionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataCompactionResult")
            .field("uuid", &self.uuid)
            .field("remapped data files count", &self.remapped_data_files.len())
            .field("old data files count", &self.old_data_files.len())
            .field("old file indices count", &self.old_file_indices.len())
            .field("new data files count", &self.new_data_files.len())
            .field("new file indices count", &self.new_file_indices.len())
            .finish()
    }
}

use crate::row::MoonlinkRow;
use more_asserts as ma;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Arc;

#[derive(Debug)]
pub struct MooncakeDataFile {
    pub(crate) file_id: FileId,
    pub(crate) file_path: String,
}

impl MooncakeDataFile {
    pub fn file_id(&self) -> FileId {
        self.file_id
    }

    pub fn file_path(&self) -> &String {
        &self.file_path
    }
}

impl PartialEq for MooncakeDataFile {
    fn eq(&self, other: &Self) -> bool {
        self.file_id == other.file_id
    }
}
impl Eq for MooncakeDataFile {}
impl Hash for MooncakeDataFile {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_id.0.hash(state);
    }
}

pub type MooncakeDataFileRef = Arc<MooncakeDataFile>;

const LOCAL_FILE_ID_BASE: u64 = 10000000000000000;
pub const NUM_FILES_PER_FLUSH: u64 = 100;

pub fn get_unique_file_id_for_flush(table_auto_incr_id: u64, file_idx: u64) -> u64 {
    ma::assert_lt!(file_idx, NUM_FILES_PER_FLUSH);
    LOCAL_FILE_ID_BASE + table_auto_incr_id * NUM_FILES_PER_FLUSH + file_idx
}

pub fn get_random_file_name_in_dir(dir_path: &Path) -> String {
    dir_path
        .join(format!("data-{}.parquet", uuid::Uuid::now_v7()))
        .to_string_lossy()
        .to_string()
}

pub fn create_data_file(file_id: u64, file_path: String) -> MooncakeDataFileRef {
    Arc::new(MooncakeDataFile {
        file_id: FileId(file_id),
        file_path,
    })
}

// UNDONE(UPDATE_DELETE): a better way to handle file ids
#[derive(Debug, Clone, PartialEq, Eq, Copy, Hash, Deserialize, Serialize)]
pub struct FileId(pub(crate) u64);

/// Unique table id.
#[derive(Debug, Clone, PartialEq, Eq, Copy, Hash, Deserialize, Serialize)]
pub struct TableId(pub(crate) u32);

/// A globally unique id for a file.
#[derive(Debug, Clone, PartialEq, Eq, Copy, Deserialize, Serialize)]
pub struct TableUniqueFileId {
    pub(crate) table_id: TableId,
    pub(crate) file_id: FileId,
}
impl Hash for TableUniqueFileId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_id.hash(state);
        self.file_id.hash(state);
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RecordLocation {
    /// Record is in a memory batch
    /// (batch_id, row_offset)
    MemoryBatch(u64, usize),

    /// Record is in a disk file
    /// (file_id, row_offset)
    DiskFile(FileId, usize),
}

impl RecordLocation {
    /// Get file id from the current record.
    pub fn get_file_id(&self) -> Option<FileId> {
        match self {
            RecordLocation::DiskFile(file_id, _) => Some(*file_id),
            RecordLocation::MemoryBatch(_, _) => None,
        }
    }

    /// Get row index from the current record.
    pub fn get_row_idx(&self) -> usize {
        match self {
            RecordLocation::DiskFile(_, row_idx) => *row_idx,
            RecordLocation::MemoryBatch(_, row_idx) => *row_idx,
        }
    }
}

impl Hash for RecordLocation {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            RecordLocation::MemoryBatch(batch_id, offset) => {
                batch_id.hash(state);
                offset.hash(state);
            }
            RecordLocation::DiskFile(file_id, offset) => {
                file_id.hash(state);
                offset.hash(state);
            }
        }
    }
}

#[derive(Debug)]
pub struct RawDeletionRecord {
    pub(crate) lookup_key: u64,
    pub(crate) row_identity: Option<MoonlinkRow>,
    pub(crate) pos: Option<(u64, usize)>,
    pub(crate) lsn: u64,
}

#[derive(Clone, Debug)]
pub struct ProcessedDeletionRecord {
    pub(crate) pos: RecordLocation,
    pub(crate) lsn: u64,
}

impl ProcessedDeletionRecord {
    /// Return deletion record's file id, if it represents disk file.
    pub fn get_file_id(&self) -> Option<FileId> {
        match self.pos {
            RecordLocation::DiskFile(file_id, _) => Some(file_id),
            _ => None,
        }
    }
}

impl From<RecordLocation> for (u64, usize) {
    fn from(val: RecordLocation) -> Self {
        match val {
            RecordLocation::MemoryBatch(batch_id, row_offset) => (batch_id, row_offset),
            _ => panic!("Cannot convert RecordLocation to (u64, usize)"),
        }
    }
}

impl From<(u64, usize)> for RecordLocation {
    fn from(value: (u64, usize)) -> Self {
        RecordLocation::MemoryBatch(value.0, value.1)
    }
}

impl From<(MooncakeDataFile, usize)> for RecordLocation {
    fn from(value: (MooncakeDataFile, usize)) -> Self {
        RecordLocation::DiskFile(value.0.file_id, value.1)
    }
}
impl Borrow<FileId> for MooncakeDataFileRef {
    fn borrow(&self) -> &FileId {
        &self.file_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    #[test]
    fn test_data_file_id() {
        let mut set: HashSet<Arc<MooncakeDataFile>> = HashSet::new();

        let df = Arc::new(MooncakeDataFile {
            file_id: FileId(42),
            file_path: "hello.txt".into(),
        });
        set.insert(df.clone());

        let lookup_id = df.file_id;
        assert!(set.contains(&lookup_id));
    }
}

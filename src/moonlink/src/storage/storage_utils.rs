use std::path::PathBuf;
use std::sync::Arc;

// UNDONE(UPDATE_DELETE): a better way to handle file ids
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileId(pub(crate) Arc<PathBuf>);

#[derive(Debug, Clone, PartialEq)]
pub enum RecordLocation {
    /// Record is in a memory batch
    /// (batch_id, row_offset)
    MemoryBatch(u64, usize),

    /// Record is in a disk file
    /// (file_id, row_offset)
    DiskFile(FileId, usize),
}

impl RecordLocation {
    pub fn new_disk_file(path: &PathBuf, row_offset: usize) -> Self {
        RecordLocation::DiskFile(FileId(Arc::new(path.to_path_buf())), row_offset)
    }
}

// UNDONE(REPLICATION IDENTITY):
#[derive(Debug, Clone, PartialEq)]
pub struct RecordIdentity {}

#[derive(Clone, Debug)]
pub struct RawDeletionRecord {
    pub(crate) lookup_key: i64,
    pub(crate) _row_identity: Option<RecordIdentity>,
    pub(crate) pos: Option<(u64, usize)>,
    pub(crate) lsn: u64,
}

#[derive(Clone, Debug)]
pub struct ProcessedDeletionRecord {
    pub(crate) _lookup_key: i64,
    pub(crate) pos: RecordLocation,
    pub(crate) lsn: u64,
}

impl Into<(u64, usize)> for RecordLocation {
    fn into(self) -> (u64, usize) {
        match self {
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

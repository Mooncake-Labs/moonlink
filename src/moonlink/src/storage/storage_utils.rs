use std::path::PathBuf;
use std::sync::Arc;

use crate::row::MoonlinkRow;

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

#[derive(Debug)]
pub struct RawDeletionRecord {
    pub(crate) lookup_key: u64,
    pub(crate) row_identity: Option<MoonlinkRow>,
    pub(crate) pos: Option<(u64, usize)>,
    pub(crate) lsn: u64,
    pub(crate) xact_id: Option<u32>,
}

#[derive(Clone, Debug)]
pub struct ProcessedDeletionRecord {
    pub(crate) _lookup_key: u64,
    pub(crate) pos: RecordLocation,
    pub(crate) lsn: u64,
    pub(crate) xact_id: Option<u32>,
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

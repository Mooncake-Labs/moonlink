pub mod test_in_memory_index;

use crate::row::{MoonlinkRow, RowValue};
use crate::storage::storage_utils::{RawDeletionRecord, RecordLocation};
use multimap::MultiMap;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub trait Index: Send + Sync {
    fn find_record(&self, raw_record: &RawDeletionRecord) -> Option<Vec<&RecordLocation>>;
}

pub struct MooncakeIndex {
    in_memory_index: HashSet<IndexPtr>,
    file_indices: Vec<ParquetFileIndex>,
}

pub fn get_lookup_key(row: &MoonlinkRow) -> i64 {
    // UNDONE(REPLICATION IDENTITY):
    // For now in testing, we assume the primary key is the first column!

    match row.values[0] {
        RowValue::Int32(value) => value as i64,

        RowValue::Int64(value) => value,

        _ => todo!("Handle other types of primary keys"),
    }
}

/// Type for primary keys
pub type PrimaryKey = i64;

/// Index containing records in memory
pub type MemIndex = MultiMap<PrimaryKey, RecordLocation>; // key -> (batch_id, row_offset)
/// Index containing records in files
pub type FileIndex = MultiMap<PrimaryKey, RecordLocation>; // key -> (batch_id, row_offset)

pub struct ParquetFileIndex {
    pub(crate) index: FileIndex,
}

// Wrapper that uses Arc pointer identity
#[derive(Clone)]
struct IndexPtr(Arc<MemIndex>);

impl PartialEq for IndexPtr {
    fn eq(&self, other: &Self) -> bool {
        Arc::as_ptr(&self.0) == Arc::as_ptr(&other.0)
    }
}

impl Eq for IndexPtr {}

impl Hash for IndexPtr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}

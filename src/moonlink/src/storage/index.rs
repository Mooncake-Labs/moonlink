pub mod hash_index;
pub mod persisted_bucket_hash_map;

use crate::storage::storage_utils::{RawDeletionRecord, RecordLocation};
use multimap::MultiMap;
use persisted_bucket_hash_map::GlobalIndex;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub trait Index<'a>: Send + Sync {
    type ReturnType;
    fn find_record(&'a self, raw_record: &RawDeletionRecord) -> Option<Vec<Self::ReturnType>>;
}

pub struct MooncakeIndex {
    in_memory_index: HashSet<IndexPtr>,
    file_indices: Vec<FileIndex>,
}
/// Type for primary keys
pub type PrimaryKey = u64;

/// Index containing records in memory
pub type MemIndex = MultiMap<PrimaryKey, RecordLocation>; // key -> (batch_id, row_offset)
/// Index containing records in files
pub type FileIndex = GlobalIndex; // key -> (file, row_offset)

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

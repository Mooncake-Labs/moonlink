pub mod hash_index;
pub mod mem_index;
pub mod persisted_bucket_hash_map;

use crate::row::MoonlinkRow;
use crate::storage::storage_utils::{RawDeletionRecord, RecordLocation};
use multimap::MultiMap;
use persisted_bucket_hash_map::GlobalIndex;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
pub trait Index: Send + Sync {
    async fn find_record(&self, raw_record: &RawDeletionRecord) -> Vec<RecordLocation>;
}

pub struct MooncakeIndex {
    pub(crate) in_memory_index: HashSet<IndexPtr>,
    pub(crate) file_indices: Vec<FileIndex>,
}
/// Type for primary keys
pub type PrimaryKey = u64;

pub struct SinglePrimitiveKey {
    hash: PrimaryKey,
    location: RecordLocation,
}
pub struct KeyWithIdentity {
    hash: PrimaryKey,
    identity: MoonlinkRow,
    location: RecordLocation,
}
/// Index containing records in memory
pub enum MemIndex {
    SinglePrimitive(hashbrown::HashTable<SinglePrimitiveKey>),
    Key(hashbrown::HashTable<KeyWithIdentity>),
    FullRow(MultiMap<PrimaryKey, RecordLocation>),
}

/// Index containing records in files
pub type FileIndex = GlobalIndex; // key -> (file, row_offset)

// Wrapper that uses Arc pointer identity
#[derive(Clone)]
pub(crate) struct IndexPtr(Arc<MemIndex>);

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

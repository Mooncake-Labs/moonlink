use crate::storage::index::persisted_bucket_hash_map::splitmix64;
use crate::storage::index::*;
use crate::storage::storage_utils::{RawDeletionRecord, RecordLocation};
use std::collections::HashSet;
use std::sync::Arc;

impl MooncakeIndex {
    /// Create a new, empty in-memory index
    pub fn new() -> Self {
        Self {
            in_memory_index: HashSet::new(),
            file_indices: Vec::new(),
        }
    }

    /// Insert a memory index (batch of in-memory records)
    ///
    pub fn insert_memory_index(&mut self, mem_index: Arc<MemIndex>) {
        self.in_memory_index.insert(IndexPtr(mem_index));
    }

    pub fn delete_memory_index(&mut self, mem_index: &Arc<MemIndex>) {
        self.in_memory_index.remove(&IndexPtr(mem_index.clone()));
    }

    /// Insert a file index (batch of on-disk records)
    ///
    /// This adds a new file index to the collection of file indices
    pub fn insert_file_index(&mut self, file_index: FileIndex) {
        self.file_indices.push(file_index);
    }
}

impl MooncakeIndex {
    pub async fn find_record(&self, raw_record: &RawDeletionRecord) -> Vec<RecordLocation> {
        let mut res: Vec<RecordLocation> = Vec::new();

        // Check in-memory indices
        for index in self.in_memory_index.iter() {
            res.extend(index.0.find_record(raw_record));
        }

        let value_and_hashes = vec![(raw_record.lookup_key, splitmix64(raw_record.lookup_key))];

        // Check file indices
        for file_index_meta in &self.file_indices {
            let locations = file_index_meta.search_values(&value_and_hashes).await;
            res.extend(locations.into_iter().map(|(_, location)| location));
        }
        res
    }

    pub async fn find_records(
        &self,
        raw_records: &[RawDeletionRecord],
    ) -> Vec<(u64, RecordLocation)> {
        let mut res: Vec<(u64, RecordLocation)> = Vec::new();

        // Check in-memory indices
        for index in self.in_memory_index.iter() {
            for record in raw_records {
                res.extend(
                    index
                        .0
                        .find_record(record)
                        .into_iter()
                        .map(|location| (record.lookup_key, location)),
                );
            }
        }
        if self.file_indices.is_empty() {
            return res;
        }
        let value_and_hashes = GlobalIndex::prepare_hashes_for_lookup(
            raw_records.iter().map(|record| record.lookup_key),
        );
        // Check file indices
        for file_index_meta in &self.file_indices {
            let locations = file_index_meta.search_values(&value_and_hashes).await;
            res.extend(locations);
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::IdentityProp;
    #[tokio::test]
    async fn test_in_memory_index_basic() {
        let mut index = MooncakeIndex::new();

        let identity = IdentityProp::SinglePrimitiveKey(0);
        // Insert memory records as a batch
        let mut mem_index = MemIndex::new(identity);
        mem_index.insert(1, None, RecordLocation::MemoryBatch(0, 5));
        mem_index.insert(2, None, RecordLocation::MemoryBatch(0, 10));
        mem_index.insert(3, None, RecordLocation::MemoryBatch(1, 3));
        index.insert_memory_index(Arc::new(mem_index));

        let record = RawDeletionRecord {
            lookup_key: 1,
            row_identity: None,
            pos: None,
            lsn: 1,
        };

        // Test the Index trait implementation
        let trait_locations = index.find_record(&record).await;
        assert_eq!(trait_locations.len(), 1);
    }
}

use crate::row::IdentityProp;
use crate::storage::index::*;

impl MemIndex {
    pub fn find_record(&self, raw_record: &RawDeletionRecord) -> Vec<RecordLocation> {
        match self {
            MemIndex::SinglePrimitive(map) => {
                if let Some(entry) = map.find(raw_record.lookup_key, |key| {
                    key.hash == raw_record.lookup_key
                }) {
                    vec![entry.location.clone()]
                } else {
                    vec![]
                }
            }
            MemIndex::Key(map) => {
                if let Some(entry) = map.find(raw_record.lookup_key, |k| {
                    k.identity.values == raw_record.row_identity.as_ref().unwrap().values
                }) {
                    vec![entry.location.clone()]
                } else {
                    vec![]
                }
            }
            MemIndex::FullRow(map) => {
                if let Some(locations) = map.get_vec(&raw_record.lookup_key) {
                    locations.clone()
                } else {
                    vec![]
                }
            }
            MemIndex::None => panic!("AppendOnly index does not support record lookups"),
        }
    }
}

impl MemIndex {
    pub fn new(identity: IdentityProp) -> Self {
        match identity {
            IdentityProp::SinglePrimitiveKey(_) => {
                MemIndex::SinglePrimitive(hashbrown::HashTable::new())
            }
            IdentityProp::Keys(_) => MemIndex::Key(hashbrown::HashTable::new()),
            IdentityProp::FullRow => MemIndex::FullRow(MultiMap::new()),
            IdentityProp::None => MemIndex::None,
        }
    }

    pub fn new_like(other: &MemIndex) -> Self {
        match other {
            MemIndex::SinglePrimitive(_) => MemIndex::SinglePrimitive(hashbrown::HashTable::new()),
            MemIndex::Key(_) => MemIndex::Key(hashbrown::HashTable::new()),
            MemIndex::FullRow(_) => MemIndex::FullRow(MultiMap::new()),
            MemIndex::None => MemIndex::None,
        }
    }

    pub fn allow_duplicate(&self) -> bool {
        match self {
            MemIndex::SinglePrimitive(_) => false,
            MemIndex::Key(_) => false,
            MemIndex::FullRow(_) => true,
            MemIndex::None => panic!("AppendOnly index does not support duplicate checking"),
        }
    }

    pub fn fast_delete(&mut self, raw_record: &RawDeletionRecord) -> Option<RecordLocation> {
        match self {
            MemIndex::SinglePrimitive(map) => {
                let entry = map.find_entry(raw_record.lookup_key, |key| {
                    key.hash == raw_record.lookup_key
                });
                if let Ok(entry) = entry {
                    Some(entry.remove().0.location)
                } else {
                    None
                }
            }
            MemIndex::Key(map) => {
                let entry = map.find_entry(raw_record.lookup_key, |k| {
                    k.hash == raw_record.lookup_key
                        && k.identity.values == raw_record.row_identity.as_ref().unwrap().values
                });
                if let Ok(entry) = entry {
                    Some(entry.remove().0.location)
                } else {
                    None
                }
            }
            MemIndex::FullRow(_) => {
                panic!("FullRow index does not support fast delete")
            }
            MemIndex::None => {
                panic!("AppendOnly index does not support delete operations")
            }
        }
    }

    pub fn insert(
        &mut self,
        key: u64,
        identity_for_key: Option<MoonlinkRow>,
        location: RecordLocation,
    ) {
        match self {
            MemIndex::SinglePrimitive(map) => {
                assert!(identity_for_key.is_none());
                map.insert_unique(
                    key,
                    SinglePrimitiveKey {
                        hash: key,
                        location,
                    },
                    |k| k.hash,
                );
            }
            MemIndex::Key(map) => {
                let key_with_id = KeyWithIdentity {
                    hash: key,
                    identity: identity_for_key.unwrap(),
                    location,
                };
                map.insert_unique(key, key_with_id, |k| k.hash);
            }
            MemIndex::FullRow(map) => {
                assert!(identity_for_key.is_none());
                map.insert(key, location);
            }
            MemIndex::None => {
                panic!("AppendOnly index does not support insert operations")
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            MemIndex::SinglePrimitive(map) => map.is_empty(),
            MemIndex::Key(map) => map.is_empty(),
            MemIndex::FullRow(map) => map.is_empty(),
            MemIndex::None => true, // Append-only tables are always considered "empty" for index purposes
        }
    }

    pub fn remap_into_vec(
        &self,
        batch_id_to_idx: &std::collections::HashMap<u64, usize>,
        row_offset_mapping: &[Vec<Option<(usize, usize)>>],
    ) -> Vec<(u64, usize, usize)> {
        let remap = |key: u64, location: &RecordLocation| match location {
            RecordLocation::MemoryBatch(batch_id, row_idx) => {
                let old_location = (batch_id_to_idx[batch_id], row_idx);
                let new_location = row_offset_mapping[old_location.0][*old_location.1];
                new_location.map(|new_location| (key, new_location.0, new_location.1))
            }
            RecordLocation::DiskFile(_, _) => panic!("No disk file in mem index"),
        };

        match self {
            MemIndex::SinglePrimitive(map) => map
                .into_iter()
                .filter_map(|v| remap(v.hash, &v.location))
                .collect(),
            MemIndex::Key(map) => map
                .into_iter()
                .filter_map(|v| remap(v.hash, &v.location))
                .collect(),
            MemIndex::FullRow(map) => map.flat_iter().filter_map(|(k, v)| remap(*k, v)).collect(),
            MemIndex::None => panic!("AppendOnly index does not support remapping operations"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::row::RowValue;

    use super::*;

    #[test]
    fn test_fast_delete_with_single_primitive_key() {
        let mut mem_index = MemIndex::new(IdentityProp::SinglePrimitiveKey(0));
        mem_index.insert(
            /*key=*/ 10,
            /*identity_for_key=*/ None,
            RecordLocation::MemoryBatch(0, 0),
        );

        // Delete for a non-existent entry.
        let record_loc = mem_index.fast_delete(&RawDeletionRecord {
            lookup_key: 0,
            row_identity: None,
            pos: None,
            lsn: 0,
        });
        assert!(record_loc.is_none());

        // Delete for an existent entry.
        let deletion_record = RawDeletionRecord {
            lookup_key: 10,
            row_identity: None,
            pos: None,
            lsn: 0,
        };
        let record_loc = mem_index.fast_delete(&deletion_record);
        assert!(matches!(
            record_loc.unwrap(),
            RecordLocation::MemoryBatch(_, _)
        ));

        // No entry left after a successful deletion.
        let record_loc = mem_index.fast_delete(&deletion_record);
        assert!(record_loc.is_none());
    }

    #[test]
    fn test_fast_delete_with_keys() {
        let existent_row = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::Float32(2.0),
            RowValue::ByteArray(b"abc".to_vec()),
        ]);

        let mut mem_index = MemIndex::new(IdentityProp::Keys(vec![0, 1]));
        mem_index.insert(
            /*key=*/ 10,
            /*identity_for_key=*/ Some(existent_row.clone()),
            RecordLocation::MemoryBatch(0, 0),
        );

        // Delete for a non-existent entry, with different lookup key.
        let non_existent_row = MoonlinkRow::new(vec![
            RowValue::Int32(2),
            RowValue::Float32(3.0),
            RowValue::ByteArray(b"bcd".to_vec()),
        ]);
        let record_loc = mem_index.fast_delete(&RawDeletionRecord {
            lookup_key: 0,
            row_identity: Some(non_existent_row.clone()),
            pos: None,
            lsn: 0,
        });
        assert!(record_loc.is_none());

        // Delete for a non-existent entry, with the same key, but different row identity.
        let record_loc = mem_index.fast_delete(&RawDeletionRecord {
            lookup_key: 10,
            row_identity: Some(non_existent_row.clone()),
            pos: None,
            lsn: 0,
        });
        assert!(record_loc.is_none());

        // Delete for an existent entry.
        let deletion_record = RawDeletionRecord {
            lookup_key: 10,
            row_identity: Some(existent_row.clone()),
            pos: None,
            lsn: 0,
        };
        let record_loc = mem_index.fast_delete(&deletion_record);
        assert!(matches!(
            record_loc.unwrap(),
            RecordLocation::MemoryBatch(_, _)
        ));

        // No entry left after a successful deletion.
        let record_loc = mem_index.fast_delete(&deletion_record);
        assert!(record_loc.is_none());
    }

    #[tokio::test]
    async fn test_find_record_with_single_primitive_key() {
        let mut mem_index = MemIndex::new(IdentityProp::SinglePrimitiveKey(0));
        mem_index.insert(
            /*key=*/ 10,
            /*identity_for_key=*/ None,
            RecordLocation::MemoryBatch(0, 0),
        );

        // Search for a non-existent entry.
        let record_locs = mem_index.find_record(&RawDeletionRecord {
            lookup_key: 0,
            row_identity: None,
            pos: None,
            lsn: 0,
        });
        assert!(record_locs.is_empty());

        // Search for an existent entry.
        let deletion_record = RawDeletionRecord {
            lookup_key: 10,
            row_identity: None,
            pos: None,
            lsn: 0,
        };
        let record_loc = mem_index.find_record(&deletion_record);
        assert_eq!(record_loc.len(), 1);
        assert!(matches!(record_loc[0], RecordLocation::MemoryBatch(_, _)));
    }

    #[tokio::test]
    async fn test_find_record_with_keys() {
        let existent_row = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::Float32(2.0),
            RowValue::ByteArray(b"abc".to_vec()),
        ]);

        let mut mem_index = MemIndex::new(IdentityProp::Keys(vec![0, 1]));
        mem_index.insert(
            /*key=*/ 10,
            /*identity_for_key=*/ Some(existent_row.clone()),
            RecordLocation::MemoryBatch(0, 0),
        );

        // Search for a non-existent entry, with different lookup key.
        let non_existent_row = MoonlinkRow::new(vec![
            RowValue::Int32(2),
            RowValue::Float32(3.0),
            RowValue::ByteArray(b"bcd".to_vec()),
        ]);
        let record_loc = mem_index.find_record(&RawDeletionRecord {
            lookup_key: 0,
            row_identity: Some(non_existent_row.clone()),
            pos: None,
            lsn: 0,
        });
        assert!(record_loc.is_empty());

        // Search for a non-existent entry, with the same key, but different row identity.
        let record_loc = mem_index.find_record(&RawDeletionRecord {
            lookup_key: 10,
            row_identity: Some(non_existent_row.clone()),
            pos: None,
            lsn: 0,
        });
        assert!(record_loc.is_empty());

        // Search for an existent entry.
        let deletion_record = RawDeletionRecord {
            lookup_key: 10,
            row_identity: Some(existent_row.clone()),
            pos: None,
            lsn: 0,
        };
        let record_loc = mem_index.find_record(&deletion_record);
        assert_eq!(record_loc.len(), 1);
        assert!(matches!(record_loc[0], RecordLocation::MemoryBatch(_, _)));
    }

    #[tokio::test]
    async fn test_find_record_with_full_rows() {
        let existent_row = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::Float32(2.0),
            RowValue::ByteArray(b"abc".to_vec()),
        ]);

        let mut mem_index = MemIndex::new(IdentityProp::FullRow);
        mem_index.insert(
            /*key=*/ 10,
            /*identity_for_key=*/ None,
            RecordLocation::MemoryBatch(0, 0),
        );

        // Search for a non-existent entry, with different lookup key.
        let non_existent_row = MoonlinkRow::new(vec![
            RowValue::Int32(2),
            RowValue::Float32(3.0),
            RowValue::ByteArray(b"bcd".to_vec()),
        ]);
        let record_loc = mem_index.find_record(&RawDeletionRecord {
            lookup_key: 0,
            row_identity: Some(non_existent_row.clone()),
            pos: None,
            lsn: 0,
        });
        assert!(record_loc.is_empty());

        // Search for an existent entry.
        let deletion_record = RawDeletionRecord {
            lookup_key: 10,
            row_identity: Some(existent_row.clone()),
            pos: None,
            lsn: 0,
        };
        let record_loc = mem_index.find_record(&deletion_record);
        assert_eq!(record_loc.len(), 1);
        assert!(matches!(record_loc[0], RecordLocation::MemoryBatch(_, _)));
    }

    #[test]
    fn test_append_only_mem_index() {
        let mem_index = MemIndex::new(IdentityProp::None);

        // Test that new_like creates another append-only index
        let new_index = MemIndex::new_like(&mem_index);
        assert!(matches!(new_index, MemIndex::None));

        // These operations should panic for AppendOnly index since they shouldn't be called
        // for append-only tables that don't use index-based operations
    }
}

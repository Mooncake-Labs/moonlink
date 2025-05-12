use crate::row::IdentityProp;
use crate::storage::index::*;

impl Index for MemIndex {
    fn find_record(&self, raw_record: &RawDeletionRecord) -> Vec<RecordLocation> {
        match self {
            MemIndex::SinglePrimitive(map) => {
                if let Some(entry) = map.find(raw_record.lookup_key, |_| true) {
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
        }
    }

    pub fn new_like(other: &MemIndex) -> Self {
        match other {
            MemIndex::SinglePrimitive(_) => MemIndex::SinglePrimitive(hashbrown::HashTable::new()),
            MemIndex::Key(_) => MemIndex::Key(hashbrown::HashTable::new()),
            MemIndex::FullRow(_) => MemIndex::FullRow(MultiMap::new()),
        }
    }

    pub fn allow_duplicate(&self) -> bool {
        match self {
            MemIndex::SinglePrimitive(_) => false,
            MemIndex::Key(_) => false,
            MemIndex::FullRow(_) => true,
        }
    }

    pub fn fast_delete(&mut self, raw_record: &RawDeletionRecord) -> Option<RecordLocation> {
        match self {
            MemIndex::SinglePrimitive(map) => {
                let entry = map.find_entry(raw_record.lookup_key, |_| true);
                if let Ok(entry) = entry {
                    Some(entry.remove().0.location)
                } else {
                    None
                }
            }
            MemIndex::Key(map) => {
                let entry = map.find_entry(raw_record.lookup_key, |k| {
                    k.identity.values == raw_record.row_identity.as_ref().unwrap().values
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
            MemIndex::FullRow(map) => map.iter().filter_map(|(k, v)| remap(*k, v)).collect(),
        }
    }
}

use crate::row::MoonlinkRow;
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Arc;

#[derive(Debug)]
pub struct DataFile {
    file_id: FileId,
    file_name: String,
}

impl DataFile {
    pub fn file_id(&self) -> FileId {
        self.file_id
    }

    pub fn file_name(&self) -> &String {
        &self.file_name
    }
}

impl PartialEq for DataFile {
    fn eq(&self, other: &Self) -> bool {
        self.file_id == other.file_id
    }
}
impl Eq for DataFile {}
impl Hash for DataFile {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_id.0.hash(state);
    }
}

pub type DataFileRef = Arc<DataFile>;

pub fn get_random_file_name_in_dir(dir_path: &Path) -> String {
    dir_path
        .join(format!("data-{}.parquet", uuid::Uuid::new_v4()))
        .to_string_lossy()
        .to_string()
}

pub fn create_data_file(file_id: u64, file_path: String) -> DataFileRef {
    Arc::new(DataFile {
        file_id: FileId(file_id),
        file_name: file_path,
    })
}

// UNDONE(UPDATE_DELETE): a better way to handle file ids
#[derive(Debug, Clone, PartialEq, Eq, Copy, Hash)]
pub struct FileId(pub(crate) u64);

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
}

#[derive(Clone, Debug)]
pub struct ProcessedDeletionRecord {
    pub(crate) pos: RecordLocation,
    pub(crate) lsn: u64,
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

impl From<(DataFile, usize)> for RecordLocation {
    fn from(value: (DataFile, usize)) -> Self {
        RecordLocation::DiskFile(value.0.file_id, value.1)
    }
}
impl Borrow<FileId> for DataFileRef {
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
        let mut set: HashSet<Arc<DataFile>> = HashSet::new();

        let df = Arc::new(DataFile {
            file_id: FileId(42),
            file_name: "hello.txt".into(),
        });
        set.insert(df.clone());

        let lookup_id = FileId(42);

        // No dummy object needed 🎯
        if let Some(found) = set.get(&lookup_id) {
            println!("Found: {:?}", found.file_name);
        }
    }
}

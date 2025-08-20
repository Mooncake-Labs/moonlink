use tempfile::TempDir;

use crate::storage::{
    mooncake_table::replay::replay_events::BackgroundEventId,
    storage_utils::{FileId, MooncakeDataFileRef, TableId, TableUniqueFileId},
};

/// Test constant to mimic an infinitely large object storage cache.
pub(crate) const INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE: u64 = u64::MAX;
/// File index blocks are always pinned at cache, whose size is much less than data file.
/// Test constant to allow only one data file and multiple index block files in object storage cache.
const INDEX_BLOCK_FILES_SIZE_UPPER_BOUND: u64 = 100;
pub(crate) const ONE_FILE_CACHE_SIZE: u64 = FAKE_FILE_SIZE + INDEX_BLOCK_FILES_SIZE_UPPER_BOUND;
/// Iceberg test namespace and table name.
pub(crate) const ICEBERG_TEST_NAMESPACE: &str = "namespace";
pub(crate) const ICEBERG_TEST_TABLE: &str = "test_table";
/// Test constant for table id.
pub(crate) const TEST_TABLE_ID: TableId = TableId(0);
/// File attributes for a fake file.
///
/// File id for the fake file.
pub(crate) const FAKE_FILE_ID: TableUniqueFileId = TableUniqueFileId {
    table_id: TEST_TABLE_ID,
    file_id: FileId(100),
};
/// Fake file size.
pub(crate) const FAKE_FILE_SIZE: u64 = 1 << 30; // 1GiB
/// Fake filename.
pub(crate) const FAKE_FILE_NAME: &str = "fake-file-name";
/// Background events are attached with an event id for replay, not needed for unit tests.
pub(crate) const DUMMY_EVENT_ID: BackgroundEventId = 0;

/// Test util function to get unique table file id.
pub(crate) fn get_unique_table_file_id(file_id: FileId) -> TableUniqueFileId {
    TableUniqueFileId {
        table_id: TEST_TABLE_ID,
        file_id,
    }
}

/// Test util function to decide whether a given file is remote file.
pub(crate) fn is_remote_file(file: &MooncakeDataFileRef, temp_dir: &TempDir) -> bool {
    // Local filesystem directory for iceberg warehouse.
    let mut temp_pathbuf = temp_dir.path().to_path_buf();
    temp_pathbuf.push(ICEBERG_TEST_NAMESPACE); // iceberg namespace
    temp_pathbuf.push(ICEBERG_TEST_TABLE); // iceberg table name

    file.file_path().starts_with(temp_pathbuf.to_str().unwrap())
}

/// Test util function to decide whether a given file is local file.
pub(crate) fn is_local_file(file: &MooncakeDataFileRef, temp_dir: &TempDir) -> bool {
    !is_remote_file(file, temp_dir)
}

/// Test util function to get fake file path.
pub(crate) fn get_fake_file_path(temp_dir: &TempDir) -> String {
    temp_dir
        .path()
        .join(FAKE_FILE_NAME)
        .to_str()
        .unwrap()
        .to_string()
}

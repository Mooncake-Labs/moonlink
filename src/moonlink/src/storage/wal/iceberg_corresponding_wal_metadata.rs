/// Metadata on WAL persistence information.
use serde::{Deserialize, Serialize};

/// WAL persistence metadata, which will be persisted into iceberg snapshot property.
/// WARNING: it's supposed to be small.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IcebergCorrespondingWalMetadata {
    /// Earliest WAL file number. Points to the oldest WAL file that should be read from.
    /// Note that it is possible that the file may not exist yet, which means the related Iceberg snapshot is fully caught up.
    /// In that case, the next WAL file will be flushed to that wal file number.
    pub(crate) earliest_wal_file_num: u64,
}

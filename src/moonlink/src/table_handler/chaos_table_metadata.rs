use crate::{MooncakeTableConfig, StorageConfig};

use serde::{Deserialize, Serialize};

/// Part of table metadata persisted locally, used to recover the same mooncake table at replay.

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ReplayTableMetadata {
    pub(crate) config: MooncakeTableConfig,
    pub(crate) local_filesystem_optimization_enabled: bool,
    pub(crate) storage_config: StorageConfig,
    /// Whether it's a upsert table, which means all delete operations follows "delete if exists" semantics.
    #[serde(default)]
    pub(crate) is_upsert_table: bool,
}

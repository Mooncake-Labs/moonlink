use serde::{Deserialize, Serialize};

use crate::storage::mooncake_table::replay::replay_events::BackgroundEventId;

/// Option for a maintenance option.
///
/// For all types of maintenance tasks, we have two basic dimensions:
/// - Selection criteria: for full-mode maintenance task, all files will take part in, however big it is; for non-full-mode, only those meet certain threshold will be selected.
///   For example, for non-full-mode, only small files will be compacted.
/// - Trigger criteria: to avoid overly frequent background maintenance task, it's only triggered when selected files reaches certain threshold.
///   While for force maintenance request, as long as there're at least two files, task will be triggered.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum MaintenanceOption {
    /// Regular maintenance task, which perform a best effort attempt.
    /// This is the default option, which is used for background task.
    BestEffort,
    /// Force a regular maintenance attempt.
    ForceRegular,
    /// Force a full maintenance attempt.
    ForceFull,
    /// Skip maintenance attempt.
    Skip,
}

/// Options to create mooncake snapshot.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SnapshotOption {
    /// Table event id, only assigned when a mooncake snapshot creation operation gets created.
    pub(crate) id: Option<BackgroundEventId>,
    /// UUID for the current mooncake snapshot operation.
    pub(crate) uuid: uuid::Uuid,
    /// Whether to return mooncake snapshot status in the snapshot result.
    pub(crate) dump_snapshot: bool,
    /// Whether to force create snapshot.
    /// When specified, mooncake snapshot will be created with snapshot threshold ignored.
    pub(crate) force_create: bool,
    /// Whether to skip iceberg snapshot creation.
    pub(crate) skip_iceberg_snapshot: bool,
    /// Index merge operation option.
    pub(crate) index_merge_option: MaintenanceOption,
    /// Data compaction operation option.
    pub(crate) data_compaction_option: MaintenanceOption,
}

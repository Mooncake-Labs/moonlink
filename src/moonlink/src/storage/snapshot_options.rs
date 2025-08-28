use serde::{Deserialize, Serialize};

/// Option for a maintenance option.
///
/// Event id, which is a UUID, will be used for background events to ensure deterministic reproduction.
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
    BestEffort(uuid::Uuid),
    /// Force a regular maintenance attempt.
    ForceRegular(uuid::Uuid),
    /// Force a full maintenance attempt.
    ForceFull(uuid::Uuid),
    /// Skip maintenance attempt.
    Skip,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum IcebergSnapshotOption {
    /// Skip iceberg snapshot attempt.
    Skip,
    /// Perform a best effort attempt, this is the default option for background tasks.
    BestEffort(uuid::Uuid),
}

impl IcebergSnapshotOption {
    /// Get event id.
    pub fn get_event_id(&self) -> Option<uuid::Uuid> {
        match &self {
            IcebergSnapshotOption::Skip => None,
            IcebergSnapshotOption::BestEffort(event_id) => Some(*event_id),
        }
    }
}

/// Options to create mooncake snapshot.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SnapshotOption {
    /// UUID for the current mooncake snapshot operation.
    pub(crate) uuid: uuid::Uuid,
    /// Whether to return mooncake snapshot status in the snapshot result.
    pub(crate) dump_snapshot: bool,
    /// Whether to force create snapshot.
    /// When specified, mooncake snapshot will be created with snapshot threshold ignored.
    pub(crate) force_create: bool,
    /// Iceberg snapshot option.
    pub(crate) iceberg_snapshot_option: IcebergSnapshotOption,
    /// Index merge operation option.
    pub(crate) index_merge_option: MaintenanceOption,
    /// Data compaction operation option.
    pub(crate) data_compaction_option: MaintenanceOption,
}

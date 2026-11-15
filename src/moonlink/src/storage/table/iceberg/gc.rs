//! Garbage collection of iceberg table files that neither the current iceberg version, its
//! predecessor, nor anything a reader can still see references.
//!
//! A file one commit stops referencing stays visible until the mooncake snapshot applies that
//! commit (the current mooncake snapshot hands its files to readers, in place when the cache
//! points at the iceberg files), and after that to every read state and in-flight compaction /
//! index merge built from an earlier snapshot. So a file becomes deletable only once
//! 1. a mooncake snapshot has applied the commit that removed it (sequence `applied_seq`), and
//! 2. no in-flight maintenance and no live reader uses a snapshot older than `applied_seq`
//!    (without reader information: once a grace period has passed).
use crate::storage::table::iceberg::file_catalog::VERSION_HINT_FILENAME;
use crate::storage::table::iceberg::iceberg_table_manager::IcebergTableManager;
use crate::Result;

use iceberg::spec::TableMetadata;
use iceberg::NamespaceIdent;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Returns the minimum mooncake snapshot sequence over all live read states, or None if there
/// are none. A read state's sequence is the value of the table's `snapshot_seq` read while
/// holding the snapshot read lock the read state was built under.
pub(crate) type LiveReadSeqProvider = Arc<dyn Fn() -> Option<u64> + Send + Sync>;

/// Without reader liveness information, wait this long before deleting a data or puffin file that
/// left the table, so in-flight scans can finish.
const GC_GRACE_WITHOUT_READER_INFO: Duration = Duration::from_secs(3600);

/// When garbage collection runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GcTrigger {
    /// At table load: no read state exists yet, so unreferenced files go immediately (this also
    /// removes leftovers of crashes and failed commits).
    Load,
    /// After a successful commit.
    Commit,
    /// After a commit that failed without changing the table: files that commit wrote were never
    /// visible to anyone and go immediately, so a full disk gets its space back.
    FailedCommit,
}

/// Environment variable to switch garbage collection `off`, to `dry-run` (only log what would be
/// deleted) or `on` (default).
pub(crate) const GC_MODE_ENV: &str = "MOONLINK_ICEBERG_GC";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GcMode {
    On,
    DryRun,
    Off,
}

fn gc_mode() -> GcMode {
    static MODE: OnceLock<GcMode> = OnceLock::new();
    *MODE.get_or_init(|| {
        let mode = match std::env::var(GC_MODE_ENV).as_deref() {
            Ok("off") => GcMode::Off,
            Ok("dry-run") => GcMode::DryRun,
            Ok("on") | Err(_) => GcMode::On,
            Ok(other) => {
                tracing::warn!(value = other, "unknown {GC_MODE_ENV} value, using on");
                GcMode::On
            }
        };
        if mode != GcMode::On {
            tracing::info!(
                ?mode,
                "iceberg garbage collection mode set by {GC_MODE_ENV}"
            );
        }
        mode
    })
}

/// Shared between a mooncake table, its snapshot state and its iceberg table manager.
pub(crate) struct IcebergGcContext {
    /// Mooncake snapshot sequence, bumped by every mooncake snapshot.
    pub(crate) snapshot_seq: Arc<AtomicU64>,
    /// Installed by the read path; without it a time-based grace period is used.
    pub(crate) live_read_seq_provider: Mutex<Option<LiveReadSeqProvider>>,
    /// Successful iceberg commits of this table manager so far.
    pub(crate) committed: AtomicU64,
    /// Latest commit the mooncake snapshot has applied, and the snapshot sequence that applied it.
    applied: Mutex<(u64, u64)>,
    /// Snapshot sequences in-flight compactions and index merges read their inputs from.
    maintenance_read_seqs: Mutex<Vec<u64>>,
    grace_without_reader_info: Mutex<Duration>,
}

impl Default for IcebergGcContext {
    fn default() -> Self {
        Self {
            snapshot_seq: Arc::new(AtomicU64::new(0)),
            live_read_seq_provider: Mutex::new(None),
            committed: AtomicU64::new(0),
            applied: Mutex::new((0, 0)),
            maintenance_read_seqs: Mutex::new(Vec::new()),
            grace_without_reader_info: Mutex::new(GC_GRACE_WITHOUT_READER_INFO),
        }
    }
}

impl std::fmt::Debug for IcebergGcContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergGcContext")
            .field("snapshot_seq", &self.snapshot_seq.load(Ordering::SeqCst))
            .field("committed", &self.committed.load(Ordering::SeqCst))
            .field("applied", &*self.applied.lock().unwrap())
            .finish()
    }
}

/// Registered for the lifetime of a compaction or index merge task, which reads files of the
/// snapshot it was planned from.
pub(crate) struct MaintenanceReadGuard {
    gc_context: Arc<IcebergGcContext>,
    seq: u64,
}

impl Drop for MaintenanceReadGuard {
    fn drop(&mut self) {
        let mut seqs = self.gc_context.maintenance_read_seqs.lock().unwrap();
        if let Some(pos) = seqs.iter().position(|s| *s == self.seq) {
            seqs.swap_remove(pos);
        }
    }
}

impl IcebergGcContext {
    /// Record that the mooncake snapshot with sequence `seq` applied iceberg commit `commit`.
    pub(crate) fn mark_applied(&self, commit: u64, seq: u64) {
        let mut applied = self.applied.lock().unwrap();
        if commit > applied.0 {
            *applied = (commit, seq);
        }
    }

    /// Keep the files of the current snapshot until the returned guard is dropped.
    pub(crate) fn register_maintenance_reader(self: &Arc<Self>) -> MaintenanceReadGuard {
        let seq = self.snapshot_seq.load(Ordering::SeqCst);
        self.maintenance_read_seqs.lock().unwrap().push(seq);
        MaintenanceReadGuard {
            gc_context: self.clone(),
            seq,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_grace_without_reader_info(&self, grace: Duration) {
        *self.grace_without_reader_info.lock().unwrap() = grace;
    }

    /// Whether a file unreferenced since commit `since_commit` (first noticed at `since`) can no
    /// longer be read by anyone.
    fn deletable(&self, since_commit: u64, since: Instant, now: Instant) -> bool {
        let (applied_commit, applied_seq) = *self.applied.lock().unwrap();
        if applied_commit < since_commit {
            return false;
        }
        if self
            .maintenance_read_seqs
            .lock()
            .unwrap()
            .iter()
            .any(|seq| *seq < applied_seq)
        {
            return false;
        }
        match self.live_read_seq_provider.lock().unwrap().as_ref() {
            Some(provider) => provider().is_none_or(|min_seq| min_seq >= applied_seq),
            None => now.duration_since(since) >= *self.grace_without_reader_info.lock().unwrap(),
        }
    }
}

fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn metadata_version(name: &str) -> Option<u32> {
    name.strip_prefix('v')?
        .strip_suffix(".metadata.json")?
        .parse()
        .ok()
}

impl IcebergTableManager {
    /// File names (unique per table) the current snapshot of `metadata` references.
    async fn referenced_files(
        metadata: &TableMetadata,
        file_io: &iceberg::io::FileIO,
    ) -> Result<HashSet<String>> {
        let mut referenced = HashSet::new();
        let Some(snapshot) = metadata.current_snapshot() else {
            return Ok(referenced);
        };
        referenced.insert(basename(snapshot.manifest_list()).to_string());
        let manifest_list = snapshot.load_manifest_list(file_io, metadata).await?;
        for manifest_file in manifest_list.entries() {
            referenced.insert(basename(&manifest_file.manifest_path).to_string());
            let manifest = manifest_file.load_manifest(file_io).await?;
            for entry in manifest.entries() {
                referenced.insert(basename(entry.data_file().file_path()).to_string());
            }
        }
        Ok(referenced)
    }

    /// Files the predecessor of the current metadata version references, read from its metadata
    /// file; empty if it cannot be read (then it is no usable fallback anyway).
    async fn referenced_by_metadata_file(
        &self,
        path: &str,
        file_io: &iceberg::io::FileIO,
    ) -> HashSet<String> {
        let Ok(bytes) = self.filesystem_accessor.read_object(path).await else {
            return HashSet::new();
        };
        let Ok(metadata) = serde_json::from_slice::<TableMetadata>(&bytes) else {
            return HashSet::new();
        };
        Self::referenced_files(&metadata, file_io)
            .await
            .unwrap_or_default()
    }

    /// Delete files of this table that are no longer referenced (see the module comment).
    pub(crate) async fn collect_garbage(&mut self, trigger: GcTrigger) -> Result<()> {
        let at_load = trigger == GcTrigger::Load;
        let mode = gc_mode();
        if mode == GcMode::Off {
            return Ok(());
        }
        let Some(gc_context) = self.gc_context.clone() else {
            return Ok(());
        };
        let Some(table) = self.iceberg_table.as_ref() else {
            return Ok(());
        };
        let metadata = table.metadata();
        if metadata.current_snapshot().is_none() {
            return Ok(());
        }

        let table_dir = format!(
            "{}/{}",
            NamespaceIdent::from_strs(&self.config.namespace)?.to_url_string(),
            self.config.table_name
        );
        let metadata_dir = format!("{table_dir}/metadata");
        if at_load && !self.loaded_hinted_version(table, &metadata_dir).await {
            tracing::warn!(
                table = self.config.table_name,
                "loaded an iceberg metadata file other than the one the version hint names; skipping garbage collection until the next commit"
            );
            return Ok(());
        }
        // The version hint is the commit point; without a readable one, keep everything.
        let Some(current_version) = self.hinted_version(&metadata_dir).await else {
            return Ok(());
        };
        let file_io = table.file_io().clone();

        // Everything the current version references, by file name (names are unique).
        let current_referenced = Self::referenced_files(metadata, &file_io).await?;
        // The predecessor version stays loadable as a fallback, so its files are kept too. After a
        // commit they are what the current version referenced at the previous run; at load, read
        // them from its metadata file.
        let metadata_files = self
            .filesystem_accessor
            .list_direct_files(&metadata_dir)
            .await?;
        let previous_version = metadata_files
            .iter()
            .filter_map(|n| metadata_version(n))
            .filter(|v| *v < current_version)
            .max();
        // Files the current version referenced at the last run (None if nothing ran since load).
        let last_referenced = self.gc_previous_referenced.take();
        let previous_referenced = match (last_referenced.clone(), previous_version) {
            // After a commit, its predecessor is the version the last run saw as current.
            (Some(previous), _) if trigger == GcTrigger::Commit => previous,
            (_, Some(version)) => {
                let path = format!("{metadata_dir}/v{version}.metadata.json");
                self.referenced_by_metadata_file(&path, &file_io).await
            }
            (_, None) => HashSet::new(),
        };
        let mut current_referenced = current_referenced;
        // Index block files are referenced from inside the index puffin blobs.
        for block_paths in self.persisted_index_block_files.values() {
            for block_path in block_paths {
                current_referenced.insert(basename(block_path).to_string());
            }
        }
        let referenced =
            |name: &str| current_referenced.contains(name) || previous_referenced.contains(name);

        let mut to_delete = Vec::new();

        // Metadata files are only read at load: keep the current version and its predecessor, and
        // delete versions above the current one (left by a commit interrupted before its hint).
        for name in metadata_files.iter() {
            let garbage = match metadata_version(name) {
                Some(v) => v != current_version && Some(v) != previous_version,
                None => name.ends_with(".avro") && !referenced(name.as_str()),
            };
            if garbage {
                to_delete.push(format!("{metadata_dir}/{name}"));
            }
        }

        // Data, deletion-vector and index files may still be read.
        let data_dir = format!("{table_dir}/data");
        let data_files = self
            .filesystem_accessor
            .list_direct_files(&data_dir)
            .await?;
        let now = Instant::now();
        let current_commit = gc_context.committed.load(Ordering::SeqCst);
        // Tracked from the commit that removed a file from the current version on.
        let removed: HashSet<&String> = data_files
            .iter()
            .filter(|n| !current_referenced.contains(n.as_str()))
            .collect();
        self.gc_unreferenced_since
            .retain(|name, _| removed.contains(name));
        for name in removed {
            let newly_removed = !self.gc_unreferenced_since.contains_key(name.as_str());
            let (since, since_commit) = *self
                .gc_unreferenced_since
                .entry(name.clone())
                .or_insert((now, current_commit));
            if previous_referenced.contains(name.as_str()) {
                continue;
            }
            // Neither referenced at the last run nor tracked since: written by the failed commit.
            let never_committed = trigger == GcTrigger::FailedCommit
                && newly_removed
                && last_referenced
                    .as_ref()
                    .is_some_and(|last| !last.contains(name.as_str()));
            if at_load || never_committed || gc_context.deletable(since_commit, since, now) {
                to_delete.push(format!("{data_dir}/{name}"));
                self.gc_unreferenced_since.remove(name);
            }
        }
        self.gc_previous_referenced = Some(current_referenced);

        if to_delete.is_empty() {
            return Ok(());
        }
        let mut bytes = 0u64;
        for path in to_delete.iter() {
            if let Ok(stats) = self.filesystem_accessor.stats_object(path).await {
                bytes += stats.content_length();
            }
        }
        if mode == GcMode::DryRun {
            tracing::info!(
                table = self.config.table_name,
                files = to_delete.len(),
                bytes,
                paths = ?to_delete,
                "dry run: would delete unreferenced iceberg files"
            );
            return Ok(());
        }
        tracing::info!(
            table = self.config.table_name,
            files = to_delete.len(),
            bytes,
            at_load,
            "deleting unreferenced iceberg files"
        );
        for path in to_delete {
            self.filesystem_accessor.delete_object(&path).await?;
        }
        Ok(())
    }

    /// Whether the loaded metadata file is the one `version-hint.text` names (false after a
    /// fallback load).
    async fn loaded_hinted_version(
        &self,
        table: &iceberg::table::Table,
        metadata_dir: &str,
    ) -> bool {
        let Some(loaded) = table
            .metadata_location()
            .map(basename)
            .and_then(metadata_version)
        else {
            return false;
        };
        self.hinted_version(metadata_dir).await == Some(loaded)
    }

    async fn hinted_version(&self, metadata_dir: &str) -> Option<u32> {
        let hint_path = format!("{metadata_dir}/{VERSION_HINT_FILENAME}");
        let bytes = self
            .filesystem_accessor
            .read_object(&hint_path)
            .await
            .ok()?;
        std::str::from_utf8(&bytes).ok()?.trim().parse().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deletable_needs_applied_commit_and_no_older_reader() {
        let gc = Arc::new(IcebergGcContext::default());
        gc.set_grace_without_reader_info(Duration::ZERO);
        let now = Instant::now();
        // Removed by commit 2, which no mooncake snapshot has applied yet.
        gc.mark_applied(1, 5);
        assert!(!gc.deletable(2, now, now));
        gc.mark_applied(2, 7);
        assert!(gc.deletable(2, now, now));

        // A maintenance task planned from snapshot 6 may still read the file.
        gc.snapshot_seq.store(6, Ordering::SeqCst);
        let maintenance = gc.register_maintenance_reader();
        assert!(!gc.deletable(2, now, now));
        drop(maintenance);
        assert!(gc.deletable(2, now, now));

        // Live readers: only ones from snapshot 7 on are safe.
        let min_live = Arc::new(Mutex::new(Some(6u64)));
        let provider_min = min_live.clone();
        *gc.live_read_seq_provider.lock().unwrap() =
            Some(Arc::new(move || *provider_min.lock().unwrap()));
        assert!(!gc.deletable(2, now, now));
        *min_live.lock().unwrap() = Some(7);
        assert!(gc.deletable(2, now, now));
        *min_live.lock().unwrap() = None;
        assert!(gc.deletable(2, now, now));
    }

    #[test]
    fn test_grace_without_reader_info() {
        let gc = IcebergGcContext::default();
        gc.mark_applied(1, 1);
        let since = Instant::now();
        assert!(!gc.deletable(1, since, since + Duration::from_secs(60)));
        assert!(gc.deletable(1, since, since + GC_GRACE_WITHOUT_READER_INFO));
    }
}

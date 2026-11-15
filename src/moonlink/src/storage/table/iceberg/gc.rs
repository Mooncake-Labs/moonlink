//! Garbage collection of iceberg table files that the current snapshot no longer references:
//! metadata files right after each commit, and data/puffin/index files once no reader can still
//! use them.
use crate::storage::table::iceberg::file_catalog::VERSION_HINT_FILENAME;
use crate::storage::table::iceberg::iceberg_table_manager::IcebergTableManager;
use crate::Result;

use iceberg::NamespaceIdent;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Returns the minimum mooncake snapshot sequence over all live read states, or None if there
/// are none.
pub(crate) type LiveReadSeqProvider = Arc<dyn Fn() -> Option<u64> + Send + Sync>;

/// Shared between a mooncake table and its iceberg table manager.
#[derive(Default)]
pub(crate) struct IcebergGcContext {
    /// Mooncake snapshot sequence, bumped by every mooncake snapshot.
    pub(crate) snapshot_seq: Arc<AtomicU64>,
    /// Installed by the read path; without it a time-based grace period is used.
    pub(crate) live_read_seq_provider: Mutex<Option<LiveReadSeqProvider>>,
}

impl std::fmt::Debug for IcebergGcContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergGcContext")
            .field("snapshot_seq", &self.snapshot_seq.load(Ordering::SeqCst))
            .finish()
    }
}

/// Without reader liveness information, wait this long before deleting a data or puffin file that
/// left the table, so in-flight scans can finish.
#[cfg(not(test))]
const GC_GRACE_WITHOUT_READER_INFO: Duration = Duration::from_secs(3600);
#[cfg(test)]
const GC_GRACE_WITHOUT_READER_INFO: Duration = Duration::ZERO;

/// Metadata files kept besides the current one, as a fallback if the newest cannot be read.
const PREVIOUS_METADATA_FILES_TO_KEEP: usize = 1;

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
    /// Delete files of this table that the current iceberg snapshot no longer references.
    /// `at_load`: no read state exists yet, so unreferenced data files go immediately (this also
    /// removes leftovers of crashes and failed commits).
    pub(crate) async fn collect_garbage(&mut self, at_load: bool) -> Result<()> {
        let Some(gc_context) = self.gc_context.clone() else {
            return Ok(());
        };
        let Some(table) = self.iceberg_table.as_ref() else {
            return Ok(());
        };
        let metadata = table.metadata();
        let Some(snapshot) = metadata.current_snapshot() else {
            return Ok(());
        };

        // Everything the current snapshot references, by file name (names are unique).
        let file_io = table.file_io();
        let mut referenced: HashSet<String> = HashSet::new();
        referenced.insert(basename(snapshot.manifest_list()).to_string());
        let manifest_list = snapshot.load_manifest_list(file_io, metadata).await?;
        for manifest_file in manifest_list.entries() {
            referenced.insert(basename(&manifest_file.manifest_path).to_string());
            let manifest = manifest_file.load_manifest(file_io).await?;
            for entry in manifest.entries() {
                referenced.insert(basename(entry.data_file().file_path()).to_string());
            }
        }
        // Index block files are referenced from inside the index puffin blobs.
        for block_paths in self.persisted_index_block_files.values() {
            for block_path in block_paths {
                referenced.insert(basename(block_path).to_string());
            }
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
        let mut to_delete = Vec::new();

        // Metadata files are only read at load: delete unreferenced ones right away.
        let metadata_files = self
            .filesystem_accessor
            .list_direct_files(&metadata_dir)
            .await?;
        let mut versions: Vec<u32> = metadata_files
            .iter()
            .filter_map(|n| metadata_version(n))
            .collect();
        versions.sort_unstable_by(|a, b| b.cmp(a));
        let kept_versions: HashSet<u32> = versions
            .iter()
            .take(1 + PREVIOUS_METADATA_FILES_TO_KEEP)
            .copied()
            .collect();
        for name in metadata_files.iter() {
            let garbage = match metadata_version(name) {
                Some(v) => !kept_versions.contains(&v),
                None => name.ends_with(".avro") && !referenced.contains(name.as_str()),
            };
            if garbage {
                to_delete.push(format!("{metadata_dir}/{name}"));
            }
        }

        // Data, deletion-vector and index files may still be read by in-flight scans.
        let data_dir = format!("{table_dir}/data");
        let data_files = self
            .filesystem_accessor
            .list_direct_files(&data_dir)
            .await?;
        let now = Instant::now();
        let current_seq = gc_context.snapshot_seq.load(Ordering::SeqCst);
        let min_live_read_seq = gc_context
            .live_read_seq_provider
            .lock()
            .unwrap()
            .as_ref()
            .map(|provider| provider());
        let unreferenced: HashSet<&String> = data_files
            .iter()
            .filter(|n| !referenced.contains(n.as_str()))
            .collect();
        self.gc_unreferenced_since
            .retain(|name, _| unreferenced.contains(name));
        for name in unreferenced {
            let (since, since_seq) = *self
                .gc_unreferenced_since
                .entry(name.clone())
                .or_insert((now, current_seq));
            let deletable = at_load
                || match min_live_read_seq {
                    // No live reader, or every live reader is newer than the file's removal.
                    Some(None) => true,
                    Some(Some(min_seq)) => min_seq >= since_seq,
                    None => now.duration_since(since) >= GC_GRACE_WITHOUT_READER_INFO,
                };
            if deletable {
                to_delete.push(format!("{data_dir}/{name}"));
                self.gc_unreferenced_since.remove(name);
            }
        }

        if !to_delete.is_empty() {
            tracing::debug!(
                table = self.config.table_name,
                files = to_delete.len(),
                "deleting unreferenced iceberg files"
            );
        }
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
        let hint_path = format!("{metadata_dir}/{VERSION_HINT_FILENAME}");
        match self.filesystem_accessor.read_object(&hint_path).await {
            Ok(bytes) => {
                std::str::from_utf8(&bytes)
                    .ok()
                    .and_then(|text| text.trim().parse::<u32>().ok())
                    == Some(loaded)
            }
            Err(_) => false,
        }
    }
}

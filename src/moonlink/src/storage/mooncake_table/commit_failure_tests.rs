//! Iceberg commits that fail right before or right after their commit point (the version hint
//! write), with the real iceberg table manager.
use super::test_utils::*;
use super::*;
use crate::row::IdentityProp;
use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::base_unbuffered_stream_writer::BaseUnbufferedStreamWriter;
use crate::storage::filesystem::accessor::metadata::ObjectMetadata;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::table_operation_test_utils::*;

use async_trait::async_trait;
use futures::Stream;
use std::pin::Pin;
use std::sync::Mutex;
use tokio::sync::mpsc;

#[derive(Clone, Copy, Debug, PartialEq)]
enum FailWrite {
    /// The write does not happen.
    Before,
    /// The write happens, but the caller sees an error (e.g. a lost acknowledgement).
    After,
}

/// Delegates to a real accessor; fails the next writes of a path ending in the armed suffix.
#[derive(Debug)]
struct FailingAccessor {
    inner: Arc<dyn BaseFileSystemAccess>,
    rule: Mutex<Option<(String, FailWrite, u32)>>,
    stats_calls: std::sync::atomic::AtomicUsize,
    /// Run garbage collection of a table opened on this accessor in dry-run mode.
    dry_run_gc: bool,
}

impl FailingAccessor {
    fn arm(&self, path_suffix: &str, when: FailWrite) {
        self.arm_times(path_suffix, when, 1);
    }

    fn arm_times(&self, path_suffix: &str, when: FailWrite, times: u32) {
        *self.rule.lock().unwrap() = Some((path_suffix.to_string(), when, times));
    }

    fn take_rule(&self, path: &str) -> Option<FailWrite> {
        let mut rule = self.rule.lock().unwrap();
        match rule.as_mut() {
            Some((suffix, when, times)) if path.ends_with(suffix.as_str()) => {
                let when = *when;
                *times -= 1;
                if *times == 0 {
                    *rule = None;
                }
                Some(when)
            }
            _ => None,
        }
    }
}

fn injected_failure() -> crate::Error {
    std::io::Error::other("injected write failure").into()
}

#[async_trait]
impl BaseFileSystemAccess for FailingAccessor {
    async fn list_direct_subdirectories(&self, folder: &str) -> Result<Vec<String>> {
        self.inner.list_direct_subdirectories(folder).await
    }
    async fn list_direct_files(&self, folder: &str) -> Result<Vec<String>> {
        self.inner.list_direct_files(folder).await
    }
    async fn remove_directory(&self, directory: &str) -> Result<()> {
        self.inner.remove_directory(directory).await
    }
    async fn object_exists(&self, object: &str) -> Result<bool> {
        self.inner.object_exists(object).await
    }
    async fn stats_object(&self, object: &str) -> Result<opendal::Metadata> {
        self.stats_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.stats_object(object).await
    }
    async fn read_object(&self, object: &str) -> Result<Vec<u8>> {
        self.inner.read_object(object).await
    }
    async fn read_object_as_string(&self, object: &str) -> Result<String> {
        self.inner.read_object_as_string(object).await
    }
    async fn stream_read(
        &self,
        object: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<u8>>> + Send>>> {
        self.inner.stream_read(object).await
    }
    async fn write_object(
        &self,
        object_filepath: &str,
        content: Vec<u8>,
    ) -> Result<opendal::Metadata> {
        match self.take_rule(object_filepath) {
            Some(FailWrite::Before) => Err(injected_failure()),
            Some(FailWrite::After) => {
                self.inner.write_object(object_filepath, content).await?;
                Err(injected_failure())
            }
            None => self.inner.write_object(object_filepath, content).await,
        }
    }
    async fn conditional_write_object(
        &self,
        object_filepath: &str,
        content: Vec<u8>,
        etag: Option<String>,
    ) -> Result<opendal::Metadata> {
        match self.take_rule(object_filepath) {
            Some(FailWrite::Before) => Err(injected_failure()),
            Some(FailWrite::After) => {
                self.inner
                    .conditional_write_object(object_filepath, content, etag)
                    .await?;
                Err(injected_failure())
            }
            None => {
                self.inner
                    .conditional_write_object(object_filepath, content, etag)
                    .await
            }
        }
    }
    async fn create_unbuffered_stream_writer(
        &self,
        object_filepath: &str,
    ) -> Result<Box<dyn BaseUnbufferedStreamWriter>> {
        self.inner
            .create_unbuffered_stream_writer(object_filepath)
            .await
    }
    async fn delete_object(&self, object_filepath: &str) -> Result<()> {
        self.inner.delete_object(object_filepath).await
    }
    async fn copy_from_local_to_remote(&self, src: &str, dst: &str) -> Result<ObjectMetadata> {
        self.inner.copy_from_local_to_remote(src, dst).await
    }
    async fn copy_from_remote_to_local(&self, src: &str, dst: &str) -> Result<ObjectMetadata> {
        self.inner.copy_from_remote_to_local(src, dst).await
    }
}

/// Open the table; with `failing` the iceberg catalog writes go through it.
async fn open(
    context: &TestContext,
    failing: Option<Arc<FailingAccessor>>,
) -> Result<(MooncakeTable, mpsc::Receiver<TableEvent>)> {
    open_with_cache(
        context,
        failing,
        create_test_object_storage_cache(&context.temp_dir),
    )
    .await
}

async fn open_with_cache(
    context: &TestContext,
    failing: Option<Arc<FailingAccessor>>,
    cache: Arc<dyn CacheTrait>,
) -> Result<(MooncakeTable, mpsc::Receiver<TableEvent>)> {
    let iceberg_table_config = test_iceberg_table_config(context, "commit");
    let mut table_config = test_mooncake_table_config(context);
    table_config.row_identity = IdentityProp::FullRow;
    let mut table = match failing {
        Some(failing) => {
            let metadata = Arc::new(TableMetadata {
                mooncake_table_id: "commit".to_string(),
                table_id: 1,
                schema: create_test_arrow_schema(),
                config: table_config,
                path: context.path(),
            });
            let mut manager = IcebergTableManager::new_with_filesystem_accessor(
                metadata.clone(),
                cache.clone(),
                failing.clone(),
                iceberg_table_config,
            )?;
            let gc_context = crate::storage::table::iceberg::gc::IcebergGcContext::default();
            if failing.dry_run_gc {
                gc_context.set_dry_run_for_test();
            }
            manager.gc_context = Some(Arc::new(gc_context));
            MooncakeTable::new_with_table_manager(metadata, Box::new(manager), cache, failing)
                .await?
        }
        None => {
            MooncakeTable::new(
                (*create_test_arrow_schema()).clone(),
                "commit".to_string(),
                1,
                context.path(),
                iceberg_table_config.clone(),
                table_config,
                cache,
                create_test_filesystem_accessor(&iceberg_table_config),
            )
            .await?
        }
    };
    let (tx, rx) = mpsc::channel(100);
    table.register_table_notify(tx).await;
    Ok((table, rx))
}

fn failing_accessor(context: &TestContext) -> Arc<FailingAccessor> {
    let iceberg_table_config = test_iceberg_table_config(context, "commit");
    Arc::new(FailingAccessor {
        inner: create_test_filesystem_accessor(&iceberg_table_config),
        rule: Mutex::new(None),
        stats_calls: std::sync::atomic::AtomicUsize::new(0),
        dry_run_gc: false,
    })
}

async fn persisted_rows(context: &TestContext) -> (Option<u64>, usize) {
    let (table, _rx) = open(context, None).await.unwrap();
    let guard = table.snapshot.read().await;
    let rows = guard
        .current_snapshot
        .disk_files
        .values()
        .map(|e| e.num_rows)
        .sum();
    (table.get_persistence_snapshot_lsn(), rows)
}

/// Row 1 committed to iceberg, row 2 flushed and ready to persist; returns the payload.
async fn table_with_pending_commit(
    table: &mut MooncakeTable,
    rx: &mut mpsc::Receiver<TableEvent>,
) -> Option<PersistenceSnapshotPayload> {
    table.append(test_row(1, "r", 1)).unwrap();
    table.commit(10);
    flush_table_and_sync(table, rx, 10).await.unwrap();
    create_mooncake_and_persist_for_test(table, rx).await;
    table.append(test_row(2, "r", 2)).unwrap();
    table.commit(20);
    flush_table_and_sync(table, rx, 20).await.unwrap();
    let (_, payload, _, _, _) = create_mooncake_snapshot_for_test(table, rx).await;
    assert!(payload.is_some());
    payload
}

/// The commit reached its commit point but reported a failure: retrying would commit the rows a
/// second time, so the outcome is reported as unknown (the table handler errors the table).
#[tokio::test]
async fn test_commit_failing_after_its_hint_reports_unknown_outcome() {
    let context = TestContext::new("commit");
    let failing = failing_accessor(&context);
    let (mut table, mut rx) = open(&context, Some(failing.clone())).await.unwrap();
    let payload = table_with_pending_commit(&mut table, &mut rx).await;

    failing.arm("version-hint.text", FailWrite::After);
    let res = create_iceberg_snapshot(&mut table, payload, &mut rx).await;
    assert!(
        matches!(res, Err(crate::Error::IcebergCommitOutcomeUnknown(_))),
        "{:?}",
        res.err()
    );
    drop(table);
    // The commit did happen, exactly once.
    assert_eq!(persisted_rows(&context).await, (Some(20), 2));
}

/// The commit failed before its commit point: nothing changed, so it is retried (by the catalog
/// or by the next persistence) and the rows are committed exactly once.
#[tokio::test]
async fn test_commit_failing_before_its_hint_is_retried() {
    let context = TestContext::new("commit");
    let failing = failing_accessor(&context);
    let (mut table, mut rx) = open(&context, Some(failing.clone())).await.unwrap();
    let payload = table_with_pending_commit(&mut table, &mut rx).await;

    failing.arm("version-hint.text", FailWrite::Before);
    let result = match create_iceberg_snapshot(&mut table, payload, &mut rx).await {
        Ok(result) => result,
        Err(err) => {
            assert!(
                !matches!(err, crate::Error::IcebergCommitOutcomeUnknown(_)),
                "{err:?}"
            );
            assert!(table.on_persistence_failed());
            let (_, payload, _, _, _) =
                create_mooncake_snapshot_for_test(&mut table, &mut rx).await;
            create_iceberg_snapshot(&mut table, payload, &mut rx)
                .await
                .unwrap()
        }
    };
    assert!(
        failing.rule.lock().unwrap().is_none(),
        "failure not injected"
    );
    table.set_persistence_snapshot_res(result);
    drop(table);
    assert_eq!(persisted_rows(&context).await, (Some(20), 2));
}

/// A table whose creation was interrupted before its version hint was written loads as a new,
/// empty table (the first metadata file is written before the hint, so a hint never names a
/// missing file).
#[tokio::test]
async fn test_interrupted_table_creation_leaves_a_loadable_table() {
    let context = TestContext::new("commit");
    let failing = failing_accessor(&context);
    failing.arm("v0.metadata.json", FailWrite::Before);
    let opened = open(&context, Some(failing.clone())).await;
    if let Ok((mut table, mut rx)) = opened {
        // Creation is lazy: the first commit creates the table.
        table.append(test_row(1, "r", 1)).unwrap();
        table.commit(10);
        flush_table_and_sync(&mut table, &mut rx, 10).await.unwrap();
        let (_, payload, _, _, _) = create_mooncake_snapshot_for_test(&mut table, &mut rx).await;
        assert!(create_iceberg_snapshot(&mut table, payload, &mut rx)
            .await
            .is_err());
    }
    let (table, _rx) = open(&context, None)
        .await
        .expect("table must load after an interrupted creation");
    assert_eq!(table.get_persistence_snapshot_lsn(), None);
}

/// A commit that failed without changing the table frees the files it already wrote (e.g. data
/// files copied into the iceberg data directory), so a full disk gets its space back.
#[tokio::test]
async fn test_failed_commit_removes_the_files_it_wrote() {
    let context = TestContext::new("commit");
    let failing = failing_accessor(&context);
    let (mut table, mut rx) = open(&context, Some(failing.clone())).await.unwrap();
    let payload = table_with_pending_commit(&mut table, &mut rx).await;
    let data_dir = context.path().join("default").join("commit").join("data");
    let committed: Vec<_> = std::fs::read_dir(&data_dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name())
        .collect();

    // Every metadata write fails: the commit fails before its commit point, after copying row 2's
    // data file.
    failing.arm_times(".metadata.json", FailWrite::Before, 100);
    let err = create_iceberg_snapshot(&mut table, payload, &mut rx)
        .await
        .expect_err("injected failure");
    assert!(
        !matches!(err, crate::Error::IcebergCommitOutcomeUnknown(_)),
        "{err:?}"
    );
    *failing.rule.lock().unwrap() = None;
    let mut left: Vec<_> = std::fs::read_dir(&data_dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name())
        .collect();
    left.sort();
    let mut committed = committed;
    committed.sort();
    assert_eq!(left, committed, "files of the failed commit left behind");
}

/// A failed commit that carried deletes releases the cache entries it pinned for the new
/// deletion vectors (they would otherwise stay pinned, never evictable, until restart).
#[tokio::test]
async fn test_failed_commit_releases_deletion_vector_cache_pins() {
    let context = TestContext::new("commit");
    let failing = failing_accessor(&context);
    let cache = crate::ObjectStorageCache::default_for_test(&context.temp_dir);
    let (mut table, mut rx) =
        open_with_cache(&context, Some(failing.clone()), Arc::new(cache.clone()))
            .await
            .unwrap();
    table.append(test_row(1, "r", 1)).unwrap();
    table.append(test_row(2, "r", 2)).unwrap();
    table.commit(10);
    flush_table_and_sync(&mut table, &mut rx, 10).await.unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut rx).await;
    create_mooncake_and_persist_for_test(&mut table, &mut rx).await;
    let pinned_before: std::collections::HashSet<_> = cache
        .get_non_evictable_filenames()
        .await
        .into_iter()
        .collect();

    table.delete(test_row(1, "r", 1), 15).await;
    table.commit(20);
    flush_table_and_sync(&mut table, &mut rx, 20).await.unwrap();
    let (_, payload, _, _, _) = create_mooncake_snapshot_for_test(&mut table, &mut rx).await;
    failing.arm_times(".metadata.json", FailWrite::Before, 100);
    assert!(create_iceberg_snapshot(&mut table, payload, &mut rx)
        .await
        .is_err());
    *failing.rule.lock().unwrap() = None;

    let pinned_after: std::collections::HashSet<_> = cache
        .get_non_evictable_filenames()
        .await
        .into_iter()
        .collect();
    assert_eq!(pinned_after, pinned_before);
}

/// A dry run of garbage collection deletes nothing and stats every candidate once, rather than
/// every candidate found so far on every commit.
#[tokio::test]
async fn test_gc_dry_run_stats_each_candidate_once() {
    let context = TestContext::new("commit");
    let iceberg_table_config = test_iceberg_table_config(&context, "commit");
    let failing = Arc::new(FailingAccessor {
        inner: create_test_filesystem_accessor(&iceberg_table_config),
        rule: Mutex::new(None),
        stats_calls: std::sync::atomic::AtomicUsize::new(0),
        dry_run_gc: true,
    });
    let (mut table, mut rx) = open(&context, Some(failing.clone())).await.unwrap();
    let commits = 30;
    for i in 0..commits {
        let lsn = (i as u64 + 1) * 10;
        table.append(test_row(i, "r", i)).unwrap();
        table.commit(lsn);
        flush_table_and_sync(&mut table, &mut rx, lsn)
            .await
            .unwrap();
        create_mooncake_and_persist_for_test(&mut table, &mut rx).await;
    }
    let table_dir = context.path().join("default").join("commit");
    let count = |dir: &str| {
        std::fs::read_dir(table_dir.join(dir))
            .map(|d| d.flatten().count())
            .unwrap_or(0)
    };
    let versions = std::fs::read_dir(table_dir.join("metadata"))
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".metadata.json"))
        .count();
    assert!(
        versions >= commits as usize,
        "the dry run deleted metadata files"
    );
    let files = count("metadata") + count("data");
    let stats = failing
        .stats_calls
        .load(std::sync::atomic::Ordering::SeqCst);
    assert!(stats <= files, "{stats} stat calls for {files} files");
}

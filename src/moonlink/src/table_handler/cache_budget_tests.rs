//! A table must keep working when the object storage cache budget is smaller than the files it
//! has to keep pinned (index blocks, deletion vectors, newly flushed files). The budget is sized
//! from free disk space at startup, so this happens on nearly full disks.
use super::test_utils::*;
use crate::row::IdentityProp;
use crate::storage::mooncake_table::table_creation_test_utils::create_test_arrow_schema;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::storage::IcebergTableConfig;
use crate::{
    FileSystemAccessor, IcebergTableManager, MooncakeTable, MooncakeTableConfig,
    ObjectStorageCache, ObjectStorageCacheConfig, TableEventManager, TableManager,
};

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tempfile::{tempdir, TempDir};

const TABLE_NAME: &str = "table_name";
/// Smaller than any index block or deletion vector the tests produce.
const TINY_CACHE_BYTES: u64 = 64;

/// Cache as created by the backend for local filesystem storage.
fn local_fs_cache(dir: &Path, max_bytes: u64) -> ObjectStorageCache {
    ObjectStorageCache::new(ObjectStorageCacheConfig::new(
        max_bytes,
        dir.to_str().unwrap().to_string(),
        /*optimize_local_filesystem=*/ true,
    ))
}

struct Dirs {
    write_through: PathBuf,
    warehouse: PathBuf,
    cache: PathBuf,
}

fn dirs(root: &Path) -> Dirs {
    let d = Dirs {
        write_through: root.join("write_through"),
        warehouse: root.join("warehouse"),
        cache: root.join("cache"),
    };
    for p in [&d.write_through, &d.warehouse, &d.cache] {
        std::fs::create_dir_all(p).unwrap();
    }
    d
}

fn table_config(temp_dir: &TempDir) -> MooncakeTableConfig {
    let mut config =
        MooncakeTableConfig::new(temp_dir.path().join("temp").to_str().unwrap().to_string());
    config.row_identity = IdentityProp::FullRow;
    config
}

async fn create_env(
    temp_dir: TempDir,
    cache_max_bytes: u64,
) -> (
    TestEnvironment,
    IcebergTableConfig,
    Dirs,
    MooncakeTableConfig,
) {
    let d = dirs(temp_dir.path());
    let config = table_config(&temp_dir);
    let iceberg_table_config = get_iceberg_manager_config(
        TABLE_NAME.to_string(),
        d.warehouse.to_str().unwrap().to_string(),
    );
    let table = MooncakeTable::new(
        (*create_test_arrow_schema()).clone(),
        TABLE_NAME.to_string(),
        /*table_id=*/ 1,
        d.write_through.clone(),
        iceberg_table_config.clone(),
        config.clone(),
        Arc::new(local_fs_cache(&d.cache, cache_max_bytes)),
        Arc::new(FileSystemAccessor::new(
            iceberg_table_config.data_accessor_config.clone(),
        )),
    )
    .await
    .unwrap();
    let env = TestEnvironment::new_with_mooncake_table(temp_dir, table).await;
    (env, iceberg_table_config, d, config)
}

async fn force_snapshot(env: &mut TestEnvironment, lsn: u64) {
    let rx = env.table_event_manager.initiate_snapshot(lsn).await;
    tokio::time::timeout(
        Duration::from_secs(30),
        TableEventManager::synchronize_force_snapshot_request(rx, lsn),
    )
    .await
    .expect("snapshot did not complete")
    .unwrap();
}

/// Insert 0..200 at LSN 1, then delete 0..50 at LSN 3 so the persisted table has a file index
/// and a deletion vector.
async fn write_rows_and_deletes(env: &mut TestEnvironment) {
    for i in 0..200 {
        env.append_row(i, "name", i, /*lsn=*/ 1, None).await;
    }
    env.commit(1).await;
    force_snapshot(env, 1).await;
    for i in 0..50 {
        env.delete_row(i, "name", i, /*lsn=*/ 2, None).await;
    }
    env.commit(3).await;
    force_snapshot(env, 3).await;
}

/// Flushing, persisting and reading work with a cache budget smaller than the pinned files.
#[tokio::test]
async fn test_ingest_and_persist_with_cache_smaller_than_pinned_files() {
    let temp_dir = tempdir().unwrap();
    let (mut env, _, _, _) = create_env(temp_dir, TINY_CACHE_BYTES).await;
    write_rows_and_deletes(&mut env).await;
    env.set_readable_lsn(3);
    env.verify_snapshot(3, &(50..200).collect::<Vec<_>>()).await;
    env.shutdown().await;
}

/// Loading a persisted table (as at restart) with a cache budget smaller than its index blocks
/// and deletion vectors succeeds and sees the deletions.
#[tokio::test]
async fn test_recovery_with_cache_smaller_than_pinned_files() {
    let temp_dir = tempdir().unwrap();
    let (mut env, iceberg_table_config, d, config) = create_env(temp_dir, u64::MAX).await;
    write_rows_and_deletes(&mut env).await;

    let metadata = Arc::new(MooncakeTableMetadata {
        mooncake_table_id: TABLE_NAME.to_string(),
        table_id: 0,
        schema: create_test_arrow_schema(),
        config,
        path: d.write_through.clone(),
    });
    let restart_cache_dir = tempdir().unwrap();
    let mut table_manager = IcebergTableManager::new(
        metadata,
        Arc::new(local_fs_cache(restart_cache_dir.path(), TINY_CACHE_BYTES)),
        Arc::new(FileSystemAccessor::new(
            iceberg_table_config.data_accessor_config.clone(),
        )),
        iceberg_table_config,
    )
    .await
    .unwrap();
    let (_, snapshot) = table_manager.load_snapshot_from_table().await.unwrap();
    assert_eq!(snapshot.flush_lsn, Some(3));
    assert!(!snapshot.indices.file_indices.is_empty());
    let deleted_rows: usize = snapshot
        .disk_files
        .values()
        .map(|entry| {
            entry
                .puffin_deletion_blob
                .as_ref()
                .map(|blob| blob.num_rows)
                .unwrap_or(0)
        })
        .sum();
    assert_eq!(deleted_rows, 50);
    env.shutdown().await;
}

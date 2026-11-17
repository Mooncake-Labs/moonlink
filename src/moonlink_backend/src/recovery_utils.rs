use crate::error::Result;
use moonlink::MooncakeTableId;
use moonlink::ReadStateFilepathRemap;
use moonlink_connectors::errored_tables::ErroredTable;
use moonlink_connectors::ReplicationManager;
use moonlink_metadata_store::base_metadata_store::{MetadataStoreTrait, TableMetadataEntry};

use futures::FutureExt;
use std::collections::HashSet;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Weak;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// Backend related attributes used for recovery.
#[derive(Clone, Debug)]
pub(crate) struct BackendAttributes {
    // Temporary files directory.
    pub(crate) temp_files_dir: String,
    // Base directory, where moonlink 0.2 kept its WAL.
    pub(crate) base_path: String,
}

/// Directory under the base path where moonlink 0.2 kept each mirror's WAL.
const LEGACY_WAL_DIRECTORY: &str = "_wal";

/// Mirrors among `mooncake_table_ids` that still have files in moonlink 0.2's WAL directory.
/// That version acknowledged changes once they were in its WAL, so changes only there were never
/// replayed after the upgrade unless the mirror was snapshotted before it.
async fn mirrors_with_legacy_wal(
    base_path: &str,
    mooncake_table_ids: &[MooncakeTableId],
) -> Vec<String> {
    let mut mirrors = Vec::new();
    for mooncake_table_id in mooncake_table_ids {
        let directory = std::path::Path::new(base_path)
            .join(LEGACY_WAL_DIRECTORY)
            .join(mooncake_table_id.to_string());
        let Ok(mut entries) = tokio::fs::read_dir(&directory).await else {
            continue;
        };
        if matches!(entries.next_entry().await, Ok(Some(_))) {
            mirrors.push(mooncake_table_id.to_string());
        }
    }
    mirrors
}

/// Delay before recovering each table after the first, so tests can widen the window in which
/// some tables are registered and others are not.
#[cfg(feature = "test-utils")]
static RECOVERY_DELAY_BETWEEN_TABLES_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(feature = "test-utils")]
pub fn set_recovery_delay_between_tables_for_test(delay: std::time::Duration) {
    RECOVERY_DELAY_BETWEEN_TABLES_MS.store(
        delay.as_millis() as u64,
        std::sync::atomic::Ordering::SeqCst,
    );
}

/// Recover the given table.
async fn recover_table(
    mut metadata_entry: TableMetadataEntry,
    replication_manager: &mut ReplicationManager,
    read_state_filepath_remap: ReadStateFilepathRemap,
) -> Result<()> {
    let mooncake_table_id = MooncakeTableId {
        database: metadata_entry.database,
        table: metadata_entry.table,
    };
    replication_manager
        .add_table(
            &metadata_entry.src_table_uri,
            mooncake_table_id,
            &metadata_entry.src_table_name,
            &mut metadata_entry.moonlink_table_config,
            read_state_filepath_remap,
            /*is_recovery=*/ true,
        )
        .await?;
    Ok(())
}

/// Run one table's recovery, turning a panic into an error so that table is errored instead of
/// aborting startup for every mirror.
async fn recover_isolated(recovery: impl Future<Output = Result<()>>) -> Result<()> {
    match AssertUnwindSafe(recovery).catch_unwind().await {
        Ok(result) => result,
        Err(panic) => {
            let reason = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("unknown panic");
            Err(crate::Error::recovery_panicked(reason))
        }
    }
}

/// Load persisted metadata, and return recovered metadata storage clients.
///
/// TODO(hjiang): Parallelize all IO operations.
pub(super) async fn recover_all_tables(
    backend_attributes: BackendAttributes,
    metadata_store_accessor: &dyn MetadataStoreTrait,
    read_state_filepath_remap: ReadStateFilepathRemap,
    replication_manager: &mut ReplicationManager,
) -> Result<Vec<PendingRecovery>> {
    let mut unique_uris = HashSet::<String>::new();

    // Skep-1: check metadata store table existence, skip if not.
    if !metadata_store_accessor.metadata_table_exists().await? {
        return Ok(Vec::new());
    }

    // Step-2: load persisted metadata from storage, perform recovery for each managed tables.
    //
    // Get all mooncake tables to recovery.
    let table_metadata_entries = metadata_store_accessor
        .get_all_table_metadata_entries()
        .await?;

    // Perform recovery on all managed tables. A table that cannot be recovered never stops the
    // others (or the service): it is registered as errored, so it reports why and can be dropped,
    // or, for a transient reason, as pending and retried (see [`retry_pending_recoveries`]).
    // No slot advances until every table is restored or pending.
    replication_manager.set_hold_all_slots(true);
    let mut pending = Vec::new();
    let mut recovered_table_ids = Vec::new();
    for (index, mut cur_metadata_entry) in table_metadata_entries.into_iter().enumerate() {
        #[cfg(feature = "test-utils")]
        if index > 0 {
            let delay = RECOVERY_DELAY_BETWEEN_TABLES_MS.load(std::sync::atomic::Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }
        #[cfg(not(feature = "test-utils"))]
        let _ = index;
        // Update certain attributes, which are not persisted before crash.
        cur_metadata_entry
            .moonlink_table_config
            .mooncake_table_config
            .temp_files_directory = backend_attributes.temp_files_dir.clone();
        let mooncake_table_id = MooncakeTableId {
            database: cur_metadata_entry.database.clone(),
            table: cur_metadata_entry.table.clone(),
        };
        recovered_table_ids.push(mooncake_table_id.clone());
        let errored_table = |message: String| ErroredTable {
            message,
            iceberg_table_config: cur_metadata_entry
                .moonlink_table_config
                .iceberg_table_config
                .clone(),
            src_uri: cur_metadata_entry.src_table_uri.clone(),
            src_table_name: cur_metadata_entry.src_table_name.clone(),
        };
        if let Some(message) = replication_manager
            .persisted_errored_message(&mooncake_table_id)
            .await
        {
            warn!(table = %mooncake_table_id, "mirror is errored; not resuming replication into it");
            replication_manager
                .add_errored_table(
                    mooncake_table_id,
                    errored_table(message),
                    /*persist=*/ false,
                )
                .await;
            continue;
        }
        unique_uris.insert(cur_metadata_entry.src_table_uri.clone());
        let failed = errored_table(String::new());
        if let Err(e) = recover_isolated(recover_table(
            cur_metadata_entry.clone(),
            replication_manager,
            read_state_filepath_remap.clone(),
        ))
        .await
        {
            register_failed_recovery(replication_manager, mooncake_table_id.clone(), failed, &e)
                .await;
            if is_transient(&e) {
                pending.push(PendingRecovery {
                    mooncake_table_id,
                    metadata_entry: cur_metadata_entry,
                    read_state_filepath_remap: read_state_filepath_remap.clone(),
                });
            }
        }
    }
    replication_manager.set_hold_all_slots(false);

    let legacy_wal_mirrors =
        mirrors_with_legacy_wal(&backend_attributes.base_path, &recovered_table_ids).await;
    if !legacy_wal_mirrors.is_empty() {
        warn!(
            mirrors = ?legacy_wal_mirrors,
            directory = %std::path::Path::new(&backend_attributes.base_path).join(LEGACY_WAL_DIRECTORY).display(),
            "mirrors have files in the WAL directory of moonlink 0.2; changes that were only in it are missing unless create_snapshot was run on each mirror before upgrading. Otherwise drop and re-create these mirrors; then delete the directory"
        );
    }

    // Stream only now that every recovered mirror is registered: until then the slot must not be
    // acknowledged past changes a mirror still has to replay.
    for uri in unique_uris.into_iter() {
        if let Err(e) = replication_manager.start_replication(&uri).await {
            warn!(error = ?e, "no replication started for a source with no recovered mirror");
        }
    }

    Ok(pending)
}

/// A mirror whose recovery failed for a transient reason, to retry.
pub(crate) struct PendingRecovery {
    mooncake_table_id: MooncakeTableId,
    metadata_entry: TableMetadataEntry,
    read_state_filepath_remap: ReadStateFilepathRemap,
}

/// How long a mirror may stay pending. Its database's slot is held meanwhile (the source keeps
/// WAL), so a failure that never clears (a removed role, a host that no longer resolves) turns
/// into an errored mirror instead of holding the slot forever.
const PENDING_RECOVERY_LIMIT: Duration = Duration::from_secs(3600);

/// Whether a recovery failure is about reaching the source rather than about the table: such
/// a mirror is retried (its slot held meanwhile), never persisted as errored.
fn is_transient(e: &crate::Error) -> bool {
    use moonlink_connectors::error::{error_struct_has_cause, is_transient_cause};
    match e {
        crate::Error::RecoveryPanicked(_) => false,
        e => error_struct_has_cause(e.error_struct(), &is_transient_cause),
    }
}

/// Register a mirror that failed recovery: pending (retried) for a transient failure, persisted
/// as errored otherwise.
async fn register_failed_recovery(
    replication_manager: &mut ReplicationManager,
    mooncake_table_id: MooncakeTableId,
    failed: ErroredTable,
    e: &crate::Error,
) {
    let table = mooncake_table_id.to_string();
    if is_transient(e) {
        warn!(error = ?e, %table, "mirror cannot be restored yet; retrying");
        let message = format!(
            "mirror {table} is not available yet: it could not be restored ({e}); moonlink keeps retrying"
        );
        replication_manager
            .add_pending_table(mooncake_table_id, ErroredTable { message, ..failed });
    } else {
        error!(error = ?e, %table, "failed to recover mirror");
        let message = moonlink::Error::errored_table_message(
            &table,
            &format!("it could not be restored at startup ({e})"),
        );
        replication_manager
            .add_errored_table(
                mooncake_table_id,
                ErroredTable { message, ..failed },
                /*persist=*/ true,
            )
            .await;
    }
}

/// How long to wait for a source to answer before a retry is attempted (outside the lock).
const PENDING_RECOVERY_PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Retry the pending recoveries with backoff (2 s doubling up to 60 s) until each is restored,
/// fails for good, or is dropped, or the backend is gone.
pub(crate) fn retry_pending_recoveries(
    replication_manager: Weak<RwLock<ReplicationManager>>,
    mut pending: Vec<PendingRecovery>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let started = tokio::time::Instant::now();
        let mut backoff = Duration::from_secs(2);
        while !pending.is_empty() {
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(60));
            let mut still_pending = Vec::new();
            for retry in pending {
                let Some(manager) = replication_manager.upgrade() else {
                    return;
                };
                // Cheap reachability check, so an unreachable source never holds the lock.
                let reachable = tokio::time::timeout(
                    PENDING_RECOVERY_PROBE_TIMEOUT,
                    moonlink_connectors::pg_replicate::SourceIdentity::identify(
                        &retry.metadata_entry.src_table_uri,
                    ),
                )
                .await
                .is_ok_and(|identified| identified.is_ok());
                let given_up = started.elapsed() >= PENDING_RECOVERY_LIMIT;
                if !reachable && !given_up {
                    still_pending.push(retry);
                    continue;
                }
                let mut manager = manager.write().await;
                if !reachable {
                    let Some(failed) = manager.begin_retry(&retry.mooncake_table_id) else {
                        continue;
                    };
                    let table = retry.mooncake_table_id.to_string();
                    error!(%table, "mirror could not be restored for an hour; giving up, it is errored now and its slot no longer held");
                    let message = moonlink::Error::errored_table_message(
                        &table,
                        "its source could not be reached for an hour after a restart",
                    );
                    manager
                        .end_retry(
                            retry.mooncake_table_id,
                            Some((ErroredTable { message, ..failed }, false)),
                        )
                        .await;
                    continue;
                }
                // Dropped by the user meanwhile.
                let Some(failed) = manager.begin_retry(&retry.mooncake_table_id) else {
                    continue;
                };
                let result = recover_isolated(recover_table(
                    retry.metadata_entry.clone(),
                    &mut manager,
                    retry.read_state_filepath_remap.clone(),
                ))
                .await;
                let table = retry.mooncake_table_id.to_string();
                match result {
                    Ok(()) => {
                        info!(%table, "mirror restored");
                        manager.end_retry(retry.mooncake_table_id, None).await;
                        // At startup no mirror of this source may have been restored, so nothing
                        // started streaming yet.
                        if let Err(e) = manager
                            .start_replication(&retry.metadata_entry.src_table_uri)
                            .await
                        {
                            warn!(error = ?e, %table, "failed to start replication");
                        }
                    }
                    Err(e) if is_transient(&e) && started.elapsed() < PENDING_RECOVERY_LIMIT => {
                        warn!(error = ?e, %table, "mirror cannot be restored yet; retrying");
                        let message = format!(
                            "mirror {table} is not available yet: it could not be restored ({e}); moonlink keeps retrying"
                        );
                        let errored = ErroredTable { message, ..failed };
                        manager
                            .end_retry(retry.mooncake_table_id.clone(), Some((errored, true)))
                            .await;
                        still_pending.push(retry);
                    }
                    Err(e) => {
                        error!(error = ?e, %table, "failed to recover mirror");
                        let message = moonlink::Error::errored_table_message(
                            &table,
                            &format!("it could not be restored at startup ({e})"),
                        );
                        let errored = ErroredTable { message, ..failed };
                        manager
                            .end_retry(retry.mooncake_table_id, Some((errored, false)))
                            .await;
                    }
                }
            }
            pending = still_pending;
        }
    })
}

/// Names of the errored and of the pending mirrors among `placeholders`, as the placeholder
/// watcher reports them.
pub(crate) fn placeholder_names(
    placeholders: &[(MooncakeTableId, String, bool)],
) -> (Vec<String>, Vec<String>) {
    let (pending, errored): (Vec<_>, Vec<_>) =
        placeholders.iter().partition(|(_, _, pending)| *pending);
    let names = |mirrors: Vec<&(MooncakeTableId, String, bool)>| {
        mirrors.iter().map(|(id, _, _)| id.to_string()).collect()
    };
    (names(errored), names(pending))
}

/// Every minute (first shortly after startup): warn about mirrors that are not served after a
/// restart, and advance the slot of each database whose remaining mirrors are all errored
/// placeholders, which a replication loop would otherwise do (by design, errored mirrors do not
/// hold WAL). Nothing is advanced while any mirror is pending: it may need any slot.
pub(crate) fn watch_placeholders(
    replication_manager: Weak<RwLock<ReplicationManager>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        loop {
            let Some(manager) = replication_manager.upgrade() else {
                return;
            };
            let placeholders = manager.read().await.placeholders();
            let (errored_names, pending_names) = placeholder_names(&placeholders);
            if !errored_names.is_empty() {
                warn!(mirrors = ?errored_names, "mirrors in the errored state receive no changes; drop and re-create them");
            }
            if !pending_names.is_empty() {
                warn!(mirrors = ?pending_names, "mirrors not available yet are retried; their databases' slots are held, so the source retains WAL meanwhile");
            } else {
                let uris: HashSet<String> = placeholders
                    .iter()
                    .filter(|(_, _, pending)| !*pending)
                    .map(|(_, uri, _)| uri.clone())
                    .collect();
                for uri in uris {
                    let Ok(identity) =
                        moonlink_connectors::pg_replicate::SourceIdentity::identify(&uri).await
                    else {
                        continue;
                    };
                    if manager.read().await.has_connection(&identity.key()) {
                        continue;
                    }
                    if let Err(e) =
                        moonlink_connectors::pg_replicate::advance_unused_slot(&uri).await
                    {
                        warn!(error = ?e, "could not advance the slot of a database whose mirrors are all errored");
                    }
                }
            }
            drop(manager);
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A recovery that times out waiting for an earlier connection's shutdown is retried; the
    /// mirror is not persisted as errored.
    #[test]
    fn waiting_for_a_shutdown_is_transient() {
        let e = crate::Error::from(moonlink_connectors::Error::source_shutting_down("db"));
        assert!(is_transient(&e), "{e:?}");
    }

    #[tokio::test]
    async fn panicking_recovery_is_an_error() {
        let err = recover_isolated(async { panic!("index rebuild failed") })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("index rebuild failed"), "{err}");
        assert!(recover_isolated(async { Ok(()) }).await.is_ok());
    }

    #[tokio::test]
    async fn mirrors_with_files_in_the_legacy_wal_are_found() {
        let base = tempfile::tempdir().unwrap();
        let base_path = base.path().to_str().unwrap();
        let id = |table: &str| MooncakeTableId {
            database: "db".to_string(),
            table: table.to_string(),
        };
        let wal = base.path().join(LEGACY_WAL_DIRECTORY);
        std::fs::create_dir_all(wal.join("db.with_wal")).unwrap();
        std::fs::write(wal.join("db.with_wal").join("wal_0.json"), "{}").unwrap();
        std::fs::create_dir_all(wal.join("db.empty_wal")).unwrap();
        let ids = [id("with_wal"), id("empty_wal"), id("no_wal")];
        assert_eq!(
            mirrors_with_legacy_wal(base_path, &ids).await,
            vec!["db.with_wal".to_string()]
        );
    }
}

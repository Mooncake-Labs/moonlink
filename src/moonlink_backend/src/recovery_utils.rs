use crate::error::Result;
use moonlink::MooncakeTableId;
use moonlink::ReadStateFilepathRemap;
use moonlink_connectors::ReplicationManager;
use moonlink_metadata_store::base_metadata_store::{MetadataStoreTrait, TableMetadataEntry};

use futures::FutureExt;
use std::collections::HashSet;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use tracing::{error, warn};

/// Backend related attributes used for recovery.
#[derive(Clone, Debug)]
pub(crate) struct BackendAttributes {
    // Temporary files directory.
    pub(crate) temp_files_dir: String,
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
) -> Result<()> {
    let mut unique_uris = HashSet::<String>::new();

    // Skep-1: check metadata store table existence, skip if not.
    if !metadata_store_accessor.metadata_table_exists().await? {
        return Ok(());
    }

    // Step-2: load persisted metadata from storage, perform recovery for each managed tables.
    //
    // Get all mooncake tables to recovery.
    let table_metadata_entries = metadata_store_accessor
        .get_all_table_metadata_entries()
        .await?;

    // Perform recovery on all managed tables. A table that cannot be recovered never stops the
    // others (or the service): it is registered as errored, so it reports why and can be dropped.
    for mut cur_metadata_entry in table_metadata_entries.into_iter() {
        // Update certain attributes, which are not persisted before crash.
        cur_metadata_entry
            .moonlink_table_config
            .mooncake_table_config
            .temp_files_directory = backend_attributes.temp_files_dir.clone();
        let mooncake_table_id = MooncakeTableId {
            database: cur_metadata_entry.database.clone(),
            table: cur_metadata_entry.table.clone(),
        };
        let iceberg_table_config = cur_metadata_entry
            .moonlink_table_config
            .iceberg_table_config
            .clone();
        if let Some(message) = replication_manager
            .persisted_errored_message(&mooncake_table_id)
            .await
        {
            warn!(table = %mooncake_table_id, "mirror is errored; not resuming replication into it");
            replication_manager.add_errored_table(mooncake_table_id, message, iceberg_table_config);
            continue;
        }
        unique_uris.insert(cur_metadata_entry.src_table_uri.clone());
        if let Err(e) = recover_isolated(recover_table(
            cur_metadata_entry,
            replication_manager,
            read_state_filepath_remap.clone(),
        ))
        .await
        {
            error!(error = ?e, table = %mooncake_table_id, "failed to recover mirror");
            let message = moonlink::Error::errored_table_message(
                &mooncake_table_id.to_string(),
                &format!("it could not be restored at startup ({e}); restarting retries"),
            );
            replication_manager.add_errored_table(mooncake_table_id, message, iceberg_table_config);
        }
    }

    for uri in unique_uris.into_iter() {
        if let Err(e) = replication_manager.start_replication(&uri).await {
            warn!(error = ?e, "no replication started for a source with no recovered mirror");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn panicking_recovery_is_an_error() {
        let err = recover_isolated(async { panic!("index rebuild failed") })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("index rebuild failed"), "{err}");
        assert!(recover_isolated(async { Ok(()) }).await.is_ok());
    }
}

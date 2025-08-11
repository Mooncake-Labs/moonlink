use crate::error::Result;
use crate::mooncake_table_id::MooncakeTableId;
use moonlink::ReadStateFilepathRemap;
use moonlink_connectors::ReplicationManager;
use moonlink_metadata_store::base_metadata_store::{MetadataStoreTrait, TableMetadataEntry};

use std::collections::HashSet;
use std::hash::Hash;

/// Backend related attributes used for recovery.
pub(crate) struct BackendAttributes {
    // Temporary files directory.
    pub(crate) temp_files_dir: String,
}

/// Recovery the given table.
async fn recover_table<D, T>(
    metadata_entry: TableMetadataEntry,
    replication_manager: &mut ReplicationManager<MooncakeTableId<D, T>>,
    read_state_filepath_remap: ReadStateFilepathRemap,
) -> Result<()>
where
    D: std::convert::From<u32> + Eq + Hash + Clone + std::fmt::Display,
    T: std::convert::From<u32> + Eq + Hash + Clone + std::fmt::Display,
{
    let mooncake_table_id = MooncakeTableId {
        database_id: D::from(metadata_entry.database_id),
        table_id: T::from(metadata_entry.table_id),
    };
    replication_manager
        .add_table(
            &metadata_entry.src_table_uri,
            mooncake_table_id,
            metadata_entry.table_id,
            &metadata_entry.src_table_name,
            metadata_entry.moonlink_table_config,
            read_state_filepath_remap,
            /*is_recovery=*/ true,
        )
        .await?;
    Ok(())
}

/// Load persisted metadata, and return recovered metadata storage clients.
///
/// TODO(hjiang): Parallelize all IO operations.
pub(super) async fn recover_all_tables<D, T>(
    backend_attributes: BackendAttributes,
    metadata_store_accessor: &dyn MetadataStoreTrait,
    read_state_filepath_remap: ReadStateFilepathRemap,
    replication_manager: &mut ReplicationManager<MooncakeTableId<D, T>>,
) -> Result<()>
where
    D: std::convert::From<u32> + Eq + Hash + Clone + std::fmt::Display,
    T: std::convert::From<u32> + Eq + Hash + Clone + std::fmt::Display,
{
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

    // Perform recovery on all managed tables.
    for mut cur_metadata_entry in table_metadata_entries.into_iter() {
        // Update certain attributes, which are not persisted before crash.
        cur_metadata_entry
            .moonlink_table_config
            .mooncake_table_config
            .temp_files_directory = backend_attributes.temp_files_dir.clone();
        // Recover current table.
        unique_uris.insert(cur_metadata_entry.src_table_uri.clone());
        recover_table(
            cur_metadata_entry,
            replication_manager,
            read_state_filepath_remap.clone(),
        )
        .await?;
    }

    for uri in unique_uris.into_iter() {
        replication_manager.start_replication(&uri).await?;
    }

    Ok(())
}

use crate::error::Result;
use moonlink::MooncakeTableId;
use moonlink::ReadStateFilepathRemap;
use moonlink::{BaseIcebergSnapshotFetcher, IcebergSnapshotFetcher};
use moonlink_connectors::{ReplicationManager, REST_API_URI};
use moonlink_metadata_store::base_metadata_store::{MetadataStoreTrait, TableMetadataEntry};

use std::collections::HashSet;

/// Backend related attributes used for recovery.
#[derive(Clone, Debug)]
pub(crate) struct BackendAttributes {
    // Temporary files directory.
    pub(crate) temp_files_dir: String,
    // Base path.
    pub(crate) base_path: String,
}

/// Recover REST ingestion table.
async fn recover_rest_table(
    backend_attributes: BackendAttributes,
    metadata_entry: TableMetadataEntry,
    replication_manager: &mut ReplicationManager,
    read_state_filepath_remap: ReadStateFilepathRemap,
) -> Result<()> {
    assert_eq!(metadata_entry.src_table_uri, REST_API_URI);

    let iceberg_table_config = metadata_entry
        .moonlink_table_config
        .iceberg_table_config
        .clone();
    let iceberg_snapshot_fetcher = IcebergSnapshotFetcher::new(iceberg_table_config)?;
    let arrow_schema = iceberg_snapshot_fetcher.fetch_table_schema().await?;
    let flush_lsn = iceberg_snapshot_fetcher.get_flush_lsn().await?;

    // Only perform recovery when there's valid iceberg snapshot.
    if arrow_schema.is_none() {
        return Ok(());
    }
    if flush_lsn.is_none() {
        return Ok(());
    }

    // Perform recovery based on the latest iceberg snapshot.
    let mooncake_table_id = MooncakeTableId {
        database: metadata_entry.database,
        table: metadata_entry.table,
    };
    replication_manager
        .initialize_event_api_for_once(&backend_attributes.base_path)
        .await?;
    replication_manager
        .add_rest_table(
            &metadata_entry.src_table_uri,
            mooncake_table_id,
            &metadata_entry.src_table_name,
            arrow_schema.unwrap(),
            metadata_entry.moonlink_table_config,
            read_state_filepath_remap,
            flush_lsn,
        )
        .await?;
    Ok(())
}

/// Recovery non-REST ingestion table.
async fn recover_non_rest_table(
    mut metadata_entry: TableMetadataEntry,
    replication_manager: &mut ReplicationManager,
    read_state_filepath_remap: ReadStateFilepathRemap,
) -> Result<()> {
    assert_ne!(metadata_entry.src_table_uri, REST_API_URI);
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

/// Recovery the given table.
async fn recover_table(
    backend_attributes: BackendAttributes,
    metadata_entry: TableMetadataEntry,
    replication_manager: &mut ReplicationManager,
    read_state_filepath_remap: ReadStateFilepathRemap,
) -> Result<()> {
    // Table created by REST API doesn't support recovery.
    if metadata_entry.src_table_uri == REST_API_URI {
        return recover_rest_table(
            backend_attributes,
            metadata_entry,
            replication_manager,
            read_state_filepath_remap,
        )
        .await;
    }
    recover_non_rest_table(
        metadata_entry,
        replication_manager,
        read_state_filepath_remap,
    )
    .await
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

    // Perform recovery on all managed tables.
    for mut cur_metadata_entry in table_metadata_entries.into_iter() {
        // Update certain attributes, which are not persisted before crash.
        cur_metadata_entry
            .moonlink_table_config
            .mooncake_table_config
            .temp_files_directory = backend_attributes.temp_files_dir.clone();
        // Recover current table; rest table doesn't require replication.
        if cur_metadata_entry.src_table_uri != REST_API_URI {
            unique_uris.insert(cur_metadata_entry.src_table_uri.clone());
        }
        recover_table(
            backend_attributes.clone(),
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

/// Table state reader is a class, which fetches current table status.
use std::sync::Arc;

use crate::storage::mooncake_table::table_status::TableSnapshotStatus;
use crate::storage::IcebergTableConfig;
use crate::storage::MooncakeTable;
use crate::storage::SnapshotTableState;
use crate::Result;

use arrow_schema::Schema;
use tokio::sync::RwLock;

pub struct TableStatusReader {
    /// Iceberg warehouse location.
    iceberg_warehouse_location: String,
    /// Table snapshot.
    table_snapshot: Arc<RwLock<SnapshotTableState>>,
}

impl TableStatusReader {
    pub fn new(iceberg_table_config: &IcebergTableConfig, table: &MooncakeTable) -> Self {
        let (table_snapshot, _) = table.get_state_for_reader();
        Self {
            iceberg_warehouse_location: iceberg_table_config.accessor_config.get_root_path(),
            table_snapshot,
        }
    }

    /// Get current table state.
    pub async fn get_current_table_state(&self) -> Result<TableSnapshotStatus> {
        let table_snapshot_state = {
            let snapshot_guard = self.table_snapshot.read().await;
            snapshot_guard.get_table_snapshot_states()?
        };
        Ok(TableSnapshotStatus {
            commit_lsn: table_snapshot_state.commit_lsn,
            flush_lsn: table_snapshot_state.flush_lsn,
            cardinality: table_snapshot_state.cardinality,
            iceberg_warehouse_location: self.iceberg_warehouse_location.clone(),
        })
    }

    /// Get current table schema.
    pub async fn get_current_table_schema(&self) -> Result<Arc<Schema>> {
        let table_schema = {
            let snapshot_guard = self.table_snapshot.read().await;
            snapshot_guard.get_table_schema()?
        };
        Ok(table_schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::row::MoonlinkRow;
    use crate::row::RowValue;
    use crate::storage::mooncake_table::table_creation_test_utils::*;
    use crate::storage::mooncake_table::table_operation_test_utils::*;

    /// Test util function to get moonlink row to append.
    fn get_test_row() -> MoonlinkRow {
        MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::ByteArray("John".as_bytes().to_vec()),
            RowValue::Int32(10),
        ])
    }

    /// =========================
    /// Read table states
    /// =========================
    ///
    /// Testing scenario: no write operation to the table.
    #[tokio::test]
    async fn test_initial_table_state() {
        let temp_dir = tempfile::tempdir().unwrap();
        let iceberg_table_config = get_iceberg_table_config(&temp_dir);

        let (table, _, _) = create_table_and_iceberg_manager(&temp_dir).await;
        let table_state_reader = TableStatusReader::new(&iceberg_table_config, &table);

        // Get table state and check.
        let actual_table_state = table_state_reader.get_current_table_state().await.unwrap();
        let expected_table_state = TableSnapshotStatus {
            iceberg_warehouse_location: iceberg_table_config.accessor_config.get_root_path(),
            commit_lsn: 0,
            cardinality: 0,
            flush_lsn: None,
        };
        assert_eq!(actual_table_state, expected_table_state);
    }

    /// Testing scenario: there's write operation to the table, but not committed or persisted.
    #[tokio::test]
    async fn test_table_state_with_append() {
        let temp_dir = tempfile::tempdir().unwrap();
        let iceberg_table_config = get_iceberg_table_config(&temp_dir);

        let (mut table, _, _) = create_table_and_iceberg_manager(&temp_dir).await;
        let table_state_reader = TableStatusReader::new(&iceberg_table_config, &table);

        // Write to the mooncake table.
        table.append(get_test_row()).unwrap();

        // Get table state and check.
        let actual_table_state = table_state_reader.get_current_table_state().await.unwrap();
        let expected_table_state = TableSnapshotStatus {
            iceberg_warehouse_location: iceberg_table_config.accessor_config.get_root_path(),
            commit_lsn: 0,
            cardinality: 0,
            flush_lsn: None,
        };
        assert_eq!(actual_table_state, expected_table_state);
    }

    /// Testing scenario: there's write operation to the table, committed but not persisted.
    #[tokio::test]
    async fn test_table_state_with_committed_write() {
        let temp_dir = tempfile::tempdir().unwrap();
        let iceberg_table_config = get_iceberg_table_config(&temp_dir);

        let (mut table, _, mut notifier) = create_table_and_iceberg_manager(&temp_dir).await;
        let table_state_reader = TableStatusReader::new(&iceberg_table_config, &table);

        // Write to the mooncake table.
        table.append(get_test_row()).unwrap();
        table.commit(/*lsn=*/ 10);

        // Force a mooncake snapshot, otherwise commit result could still reside in snapshot buffer.
        create_mooncake_snapshot_for_test(&mut table, &mut notifier).await;

        // Get table state and check.
        let actual_table_state = table_state_reader.get_current_table_state().await.unwrap();
        let expected_table_state = TableSnapshotStatus {
            iceberg_warehouse_location: iceberg_table_config.accessor_config.get_root_path(),
            commit_lsn: 10,
            cardinality: 1,
            flush_lsn: None,
        };
        assert_eq!(actual_table_state, expected_table_state);
    }

    /// Testing scenario: there's write operation to the table, committed and persisted.
    #[tokio::test]
    async fn test_table_state_with_persisted_write() {
        let temp_dir = tempfile::tempdir().unwrap();
        let iceberg_table_config = get_iceberg_table_config(&temp_dir);

        let (mut table, _, mut notifier) = create_table_and_iceberg_manager(&temp_dir).await;
        let table_state_reader = TableStatusReader::new(&iceberg_table_config, &table);

        // Write to the mooncake table.
        table.append(get_test_row()).unwrap();
        table.commit(/*lsn=*/ 10);
        flush_table_and_sync(&mut table, &mut notifier, /*lsn=*/ 10)
            .await
            .unwrap();
        create_mooncake_and_persist_for_test(&mut table, &mut notifier).await;

        // Get table state and check.
        let actual_table_state = table_state_reader.get_current_table_state().await.unwrap();
        let expected_table_state = TableSnapshotStatus {
            iceberg_warehouse_location: iceberg_table_config.accessor_config.get_root_path(),
            commit_lsn: 10,
            flush_lsn: Some(10),
            cardinality: 1,
        };
        assert_eq!(actual_table_state, expected_table_state);
    }

    /// =========================
    /// Read table schema
    /// =========================
    ///
    /// Testing scenario: no schema change to the table.
    #[tokio::test]
    async fn test_initial_table_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let iceberg_table_config = get_iceberg_table_config(&temp_dir);

        let (table, _, _) = create_table_and_iceberg_manager(&temp_dir).await;
        let table_state_reader = TableStatusReader::new(&iceberg_table_config, &table);

        // Get table state and check.
        let actual_table_schema = table_state_reader.get_current_table_schema().await.unwrap();
        let expected_table_schema = create_test_arrow_schema();
        assert_eq!(actual_table_schema, expected_table_schema);
    }

    /// Testing scenario: get table schema after schema evolution.
    #[tokio::test]
    async fn test_table_schema_after_update() {
        let temp_dir = tempfile::tempdir().unwrap();
        let iceberg_table_config = get_iceberg_table_config(&temp_dir);

        let (mut table, _, mut notifier) = create_table_and_iceberg_manager(&temp_dir).await;
        let table_state_reader = TableStatusReader::new(&iceberg_table_config, &table);

        // Perform an schema update.
        let _ = alter_table_and_persist_to_iceberg(&mut table, &mut notifier).await;

        // Get table state and check.
        let actual_table_schema = table_state_reader.get_current_table_schema().await.unwrap();
        let expected_table_schema = create_test_updated_arrow_schema_remove_age();
        assert_eq!(actual_table_schema, expected_table_schema);
    }
}

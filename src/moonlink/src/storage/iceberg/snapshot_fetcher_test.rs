use tempfile::tempdir;

use crate::row::MoonlinkRow;
use crate::row::RowValue;
use crate::storage::iceberg::base_iceberg_snapshot_fetcher::BaseIcebergSnapshotFetcher;
use crate::storage::iceberg::iceberg_snapshot_fetcher::IcebergSnapshotFetcher;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::table_operation_test_utils::*;

fn get_test_row() -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(10),
    ])
}

#[tokio::test]
async fn test_snapshot_for_empty_table() {
    let iceberg_temp_dir = tempdir().unwrap();
    let config = get_iceberg_table_config(&iceberg_temp_dir);
    let snapshot_fetcher = IcebergSnapshotFetcher::new(config).unwrap();
    let arrow_schema = snapshot_fetcher.fetch_table_schema().await.unwrap();
    assert!(arrow_schema.is_none());
    let flush_lsn = snapshot_fetcher.get_flush_lsn().await.unwrap();
    assert!(flush_lsn.is_none());
}

#[tokio::test]
async fn test_snapshot_fetch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, _, mut notify_rx) = create_table_and_iceberg_manager(&temp_dir).await;
    table.append(get_test_row()).unwrap();
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 10)
        .await
        .unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    let config = get_iceberg_table_config(&temp_dir);
    let snapshot_fetcher = IcebergSnapshotFetcher::new(config).unwrap();
    let arrow_schema = snapshot_fetcher.fetch_table_schema().await.unwrap();
    assert_eq!(arrow_schema.unwrap(), *create_test_arrow_schema());
    let flush_lsn = snapshot_fetcher.get_flush_lsn().await.unwrap();
    assert_eq!(flush_lsn.unwrap(), 10);
}

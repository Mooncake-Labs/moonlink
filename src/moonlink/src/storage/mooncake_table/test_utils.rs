use super::*;
use crate::row::{Identity, RowValue};
use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashSet;
use std::fs::{create_dir_all, File};
use tempfile::{tempdir, TempDir};

pub struct TestContext {
    _temp_dir: TempDir,
    test_dir: PathBuf,
}

impl TestContext {
    pub fn new(subdir_name: &str) -> Self {
        let temp_dir = tempdir().unwrap();
        let test_dir = temp_dir.path().join(subdir_name);
        create_dir_all(&test_dir).unwrap();

        Self {
            _temp_dir: temp_dir,
            test_dir,
        }
    }

    fn path(&self) -> PathBuf {
        self.test_dir.clone()
    }
}

pub fn test_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int32, false),
    ])
}

pub fn test_row(id: i32, name: &str, age: i32) -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(id),
        RowValue::ByteArray(name.as_bytes().to_vec()),
        RowValue::Int32(age),
    ])
}

pub fn test_table(context: &TestContext, table_name: &str, identity: Identity) -> MooncakeTable {
    MooncakeTable::new(
        test_schema(),
        table_name.to_string(),
        1,
        context.path(),
        identity,
    )
}

pub fn read_ids_from_parquet(file_path: &String) -> Vec<Option<i32>> {
    let file = File::open(file_path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.into_iter().next().unwrap().unwrap();
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    (0..col.len()).map(|i| Some(col.value(i))).collect()
}

pub fn verify_file_contents(
    file_path: &String,
    expected_ids: &[i32],
    expected_row_count: Option<usize>,
) {
    let actual = read_ids_from_parquet(file_path)
        .into_iter()
        .flatten()
        .collect::<HashSet<_>>();
    let expected: HashSet<_> = expected_ids.iter().copied().collect();

    if let Some(count) = expected_row_count {
        assert_eq!(actual.len(), count, "Unexpected number of rows in file");
    }

    assert_eq!(actual, expected, "File contents don't match expected IDs");
    println!("File contains {} rows with IDs: {:?}", actual.len(), actual);
}

pub fn append_rows(table: &mut MooncakeTable, rows: Vec<MoonlinkRow>) -> Result<()> {
    for row in rows {
        table.append(row)?;
    }
    Ok(())
}

pub fn batch_rows(start_id: i32, count: i32) -> Vec<MoonlinkRow> {
    (start_id..start_id + count)
        .map(|id| test_row(id, &format!("Row {}", id), 30 + id))
        .collect()
}

pub async fn snapshot(table: &mut MooncakeTable) {
    let snapshot_handle = table.create_snapshot().unwrap();
    assert!(snapshot_handle.await.is_ok(), "Snapshot creation failed");
}

pub async fn append_commit_flush_snapshot(
    table: &mut MooncakeTable,
    rows: Vec<MoonlinkRow>,
    lsn: u64,
) -> Result<()> {
    append_rows(table, rows)?;
    table.commit(lsn);
    table.flush(lsn).await?;
    snapshot(table).await;
    Ok(())
}

pub fn verify_files_and_deletions(
    files: &[String],
    deletions: &[(u32, u32)],
    expected_ids: &[i32],
) {
    let mut res = vec![];
    for (i, path) in files.iter().enumerate() {
        let mut ids = read_ids_from_parquet(path);
        for deletion in deletions {
            if deletion.0 == i as u32 {
                ids[deletion.1 as usize] = None;
            }
        }
        res.extend(ids.into_iter().flatten());
    }
    res.sort();
    assert_eq!(res, expected_ids);
}

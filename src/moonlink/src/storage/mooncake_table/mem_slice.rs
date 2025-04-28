use super::data_batches::{BatchEntry, ColumnStoreBuffer};
use crate::error::Result;
use crate::row::MoonlinkRow;
use crate::storage::index::{Index, MemIndex};
use crate::storage::storage_utils::{RawDeletionRecord, RecordLocation};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use std::mem::swap;
use std::sync::Arc;

/// MemSlice is a table slice that is stored in memory.
/// It contains a column store buffer for storing data
///
/// MemSlice is Copy-On-Read
/// Reader will create a snapshot of the current state of the table,
/// by applying all deletions to column store buffer
///
pub(super) struct MemSlice {
    /// Column store buffer for storing data
    ///
    column_store: ColumnStoreBuffer,

    /// Mem index for the table
    ///
    mem_index: MemIndex,
}

impl MemSlice {
    pub(super) fn new(schema: Arc<Schema>, max_rows_per_buffer: usize) -> Self {
        Self {
            column_store: ColumnStoreBuffer::new(schema, max_rows_per_buffer),
            mem_index: MemIndex::new(),
        }
    }

    pub(super) fn delete(&mut self, record: &RawDeletionRecord) -> Option<(u64, usize)> {
        let locations = self.mem_index.find_record(record)?;

        for location in locations {
            // Clone the reference to create an owned copy
            let location = location.clone();
            let location_tuple: (u64, usize) = location.into();
            if self.column_store.delete_if_exists(location_tuple) {
                return Some(location_tuple);
            }
        }
        None
    }

    /// Find the first non-deleted position for a given lookup key
    pub fn find_non_deleted_position(&self, record: &RawDeletionRecord) -> Option<(u64, usize)> {
        let locations = self.mem_index.find_record(record)?;

        for location in locations {
            let location_tuple: (u64, usize) = location.clone().into();
            let (batch_id, row_offset) = location_tuple;

            if !self.column_store.is_deleted(location_tuple) {
                return Some((batch_id, row_offset));
            }
        }

        None
    }

    pub(super) fn append(
        &mut self,
        lookup_key: u64,
        row: &MoonlinkRow,
    ) -> Result<Option<(u64, Arc<RecordBatch>)>> {
        let (seg_idx, row_idx, new_batch) = self.column_store.append_row(row)?;
        self.mem_index.insert(lookup_key, (seg_idx, row_idx).into());
        Ok(new_batch)
    }

    pub(super) fn get_num_rows(&self) -> usize {
        self.column_store.get_num_rows()
    }

    #[allow(clippy::type_complexity)]
    pub(super) fn drain(
        &mut self,
    ) -> Result<(Option<(u64, Arc<RecordBatch>)>, Vec<BatchEntry>, MemIndex)> {
        let batch = self.column_store.finalize_current_batch()?;
        let entries = self.column_store.drain();
        let mut index = MemIndex::new();
        swap(&mut index, &mut self.mem_index);
        Ok((batch, entries, index))
    }

    pub(super) fn get_commit_check_point(&self) -> RecordLocation {
        self.column_store.get_commit_check_point()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::row::RowValue;
    use arrow::datatypes::{DataType, Field};
    use arrow_schema::Schema;

    #[test]
    fn test_mem_slice() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);
        let mut mem_table = MemSlice::new(Arc::new(schema), 4);

        // Create arrays properly
        mem_table
            .append(
                1,
                &MoonlinkRow::new(vec![
                    RowValue::Int32(1),
                    RowValue::ByteArray("John".as_bytes().to_vec()),
                    RowValue::Int32(30),
                ]),
            )
            .unwrap();

        mem_table
            .append(
                2,
                &MoonlinkRow::new(vec![
                    RowValue::Int32(2),
                    RowValue::ByteArray("Jane".as_bytes().to_vec()),
                    RowValue::Int32(25),
                ]),
            )
            .unwrap();

        mem_table
            .append(
                3,
                &MoonlinkRow::new(vec![
                    RowValue::Int32(3),
                    RowValue::ByteArray("Bob".as_bytes().to_vec()),
                    RowValue::Int32(40),
                ]),
            )
            .unwrap();
        assert_eq!(
            mem_table.delete(&RawDeletionRecord {
                lookup_key: 2,
                lsn: 0,
                pos: None,
                _row_identity: None,
                xact_id: None,
            }),
            Some((0, 1))
        );
        assert_eq!(
            mem_table.delete(&RawDeletionRecord {
                lookup_key: 3,
                lsn: 0,
                pos: None,
                _row_identity: None,
                xact_id: None
            }),
            Some((0, 2))
        );
        assert_eq!(
            mem_table.delete(&RawDeletionRecord {
                lookup_key: 1,
                lsn: 0,
                pos: None,
                _row_identity: None,
                xact_id: None,
            }),
            Some((0, 0))
        );
    }
}

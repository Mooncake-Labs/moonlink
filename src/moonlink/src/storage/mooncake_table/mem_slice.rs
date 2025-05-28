use super::data_batches::{BatchEntry, ColumnStoreBuffer};
use crate::error::Result;
use crate::row::{IdentityProp, MoonlinkRow};
use crate::storage::index::{Index, MemIndex};
use crate::storage::mooncake_table::shared_array::SharedRowBufferSnapshot;
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
    pub(super) fn new(
        schema: Arc<Schema>,
        max_rows_per_buffer: usize,
        identity: IdentityProp,
    ) -> Self {
        Self {
            column_store: ColumnStoreBuffer::new(schema, max_rows_per_buffer),
            mem_index: MemIndex::new(identity),
        }
    }

    /// Return whether slice is empty.
    pub fn is_empty(&self) -> bool {
        self.column_store.get_num_rows() == 0
    }

    /// Delete the given record from mem slice, and return its location if exists.
    pub(super) async fn delete(
        &mut self,
        record: &RawDeletionRecord,
        identity: &IdentityProp,
    ) -> Option<(u64 /*batch_id*/, usize /*row_offset*/)> {
        if !self.mem_index.allow_duplicate() {
            let location = self.mem_index.fast_delete(record);
            if let Some(location) = location {
                self.column_store
                    .delete_row_by_record(record, &location, identity)
            } else {
                None
            }
        } else {
            let locations = self.mem_index.find_record(record).await;
            for location in locations {
                let ret = self
                    .column_store
                    .delete_row_by_record(record, &location, identity);
                if ret.is_some() {
                    return ret;
                }
            }
            None
        }
    }

    /// Find the first non-deleted position for a given lookup key
    pub async fn find_non_deleted_position(
        &self,
        record: &RawDeletionRecord,
        identity: &IdentityProp,
    ) -> Option<(u64, usize)> {
        let locations = self.mem_index.find_record(record).await;

        for location in locations {
            let ret = self
                .column_store
                .find_valid_row_by_record(record, &location, identity);
            if ret.is_some() {
                return ret;
            }
        }
        None
    }

    pub fn try_delete_at_pos(&mut self, pos: (u64, usize)) -> bool {
        self.column_store.try_delete_at_pos(pos)
    }

    /// Append the given row into column store buffer and mem index.
    /// Return the finalized record batch if the current one's full.
    pub(super) fn append(
        &mut self,
        lookup_key: u64,
        row: MoonlinkRow,
        identity_for_key: Option<MoonlinkRow>,
    ) -> Result<Option<(u64, Arc<RecordBatch>)>> {
        let (seg_idx, row_idx, new_batch) = self.column_store.append_row(row)?;
        self.mem_index
            .insert(lookup_key, identity_for_key, (seg_idx, row_idx).into());
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
        let mut index = MemIndex::new_like(&self.mem_index);
        swap(&mut index, &mut self.mem_index);
        Ok((batch, entries, index))
    }

    pub(super) fn get_commit_check_point(&self) -> RecordLocation {
        self.column_store.get_commit_check_point()
    }

    pub(super) fn get_latest_rows(&self) -> SharedRowBufferSnapshot {
        self.column_store.get_latest_rows()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::row::RowValue;
    use arrow::datatypes::{DataType, Field};
    use arrow_schema::Schema;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_mem_slice() {
        let identity = IdentityProp::SinglePrimitiveKey(0);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "1".to_string(),
            )])),
            Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "2".to_string(),
            )])),
            Field::new("age", DataType::Int32, false).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "3".to_string(),
            )])),
        ]);
        let mut mem_table = MemSlice::new(Arc::new(schema), 4, identity);

        // Create arrays properly
        mem_table
            .append(
                1,
                MoonlinkRow::new(vec![
                    RowValue::Int32(1),
                    RowValue::ByteArray("John".as_bytes().to_vec()),
                    RowValue::Int32(30),
                ]),
                None,
            )
            .unwrap();

        mem_table
            .append(
                2,
                MoonlinkRow::new(vec![
                    RowValue::Int32(2),
                    RowValue::ByteArray("Jane".as_bytes().to_vec()),
                    RowValue::Int32(25),
                ]),
                None,
            )
            .unwrap();

        mem_table
            .append(
                3,
                MoonlinkRow::new(vec![
                    RowValue::Int32(3),
                    RowValue::ByteArray("Bob".as_bytes().to_vec()),
                    RowValue::Int32(40),
                ]),
                None,
            )
            .unwrap();
        assert_eq!(
            mem_table
                .delete(
                    &RawDeletionRecord {
                        lookup_key: 2,
                        lsn: 0,
                        pos: None,
                        row_identity: None,
                    },
                    &IdentityProp::SinglePrimitiveKey(0)
                )
                .await,
            Some((0, 1))
        );
        assert_eq!(
            mem_table
                .delete(
                    &RawDeletionRecord {
                        lookup_key: 3,
                        lsn: 0,
                        pos: None,
                        row_identity: None,
                    },
                    &IdentityProp::SinglePrimitiveKey(0)
                )
                .await,
            Some((0, 2))
        );
        assert_eq!(
            mem_table
                .delete(
                    &RawDeletionRecord {
                        lookup_key: 1,
                        lsn: 0,
                        pos: None,
                        row_identity: None,
                    },
                    &IdentityProp::SinglePrimitiveKey(0)
                )
                .await,
            Some((0, 0))
        );
    }
}

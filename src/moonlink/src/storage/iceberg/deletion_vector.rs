use crate::storage::iceberg::puffin_utils;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;

use std::collections::HashMap;

use iceberg::io::FileIO;
use iceberg::puffin::{Blob, DELETION_VECTOR_V1};
use iceberg::spec::DataFile;
use iceberg::{Error as IcebergError, Result as IcebergResult};
use roaring::RoaringTreemap;

// Magic bytes for deletion vector for puffin file.
const DELETION_VECTOR_MAGIC_BYTES: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];

// Min length for serialized blob for deletion vector.
const MIN_SERIALIZED_DELETION_VECTOR_BLOB: usize = 12;

// Deletion vector puffin blob properties which must be contained.
pub(crate) const DELETION_VECTOR_CADINALITY: &str = "cardinality";
pub(crate) const DELETION_VECTOR_REFERENCED_DATA_FILE: &str = "referenced-data-file";

pub(crate) struct DeletionVector {
    /// Roaring bitmap representing deleted rows.
    pub(crate) bitmap: RoaringTreemap,
}

impl DeletionVector {
    /// Creates a new empty deletion vector.
    pub fn new() -> Self {
        Self {
            bitmap: RoaringTreemap::new(),
        }
    }

    /// Marks a row as deleted.
    /// Pre-requisite: row indices must be in ascending order.
    pub fn mark_rows_deleted(&mut self, rows: Vec<u64>) {
        let row_count = rows.len();
        let appended_num = self.bitmap.append(rows).unwrap();
        assert_eq!(appended_num as usize, row_count);
    }

    /// Deserializes a byte vector into a DeletionVector.
    fn deserialize_roaring_map(data: &[u8]) -> IcebergResult<Self> {
        RoaringTreemap::deserialize_from(data)
            .map(|bitmap| Self { bitmap })
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::DataInvalid,
                    format!("Failed to deserialize DeletionVector: {}", e),
                )
            })
    }

    /// Sanity check required blob properties have been properly set.
    fn check_properties(properties: &HashMap<String, String>) {
        assert!(
            properties.contains_key(DELETION_VECTOR_CADINALITY),
            "Deletion vector blob properties should contain {}",
            DELETION_VECTOR_CADINALITY
        );
        assert!(
            properties.contains_key(DELETION_VECTOR_REFERENCED_DATA_FILE),
            "Deletion vector blob properties should contain {}",
            DELETION_VECTOR_REFERENCED_DATA_FILE
        );
    }

    /// Serialize the deletion vector into `Blob` to write to puffin files.
    ///
    /// Serialization storage format:
    /// | len for magic and vector | magic | vector | crc32c |
    /// - len field records the combined length of the vector and magic bytes stored as 4 bytes in big-endian.
    /// - vector is the serialized bitmap in u64 format: https://github.com/RoaringBitmap/RoaringFormatSpec?tab=readme-ov-file#extension-for-64-bit-implementations
    /// - crc32c field is checksum of the magic bytes and serialized vector as 4 bytes in big-endian.
    pub fn serialize(&self, properties: HashMap<String, String>) -> Blob {
        DeletionVector::check_properties(&properties);

        // Calculate combined length (magic bytes + bitmap).
        let serialized_bitmap_size = self.bitmap.serialized_size();
        let combined_length = (DELETION_VECTOR_MAGIC_BYTES.len() + serialized_bitmap_size) as u32;

        // Create a buffer to hold all the data.
        let blob_total_size = std::mem::size_of_val(&combined_length) + // length
        DELETION_VECTOR_MAGIC_BYTES.len() + // magic sequence
        serialized_bitmap_size + // serialized roaring bitmap
        4; // crc
        let mut data = Vec::with_capacity(blob_total_size);

        // Set blob length and get the mutable pointer to fill in data ourselves.
        #[allow(clippy::uninit_vec)]
        unsafe {
            data.set_len(blob_total_size);
        }
        let ptr: *mut u8 = data.as_mut_ptr();
        let mut offset = 0;

        // Write combined length.
        let combined_length_bytes = combined_length.to_be_bytes();
        unsafe {
            std::ptr::copy_nonoverlapping(combined_length_bytes.as_ptr(), ptr.add(offset), 4);
        }
        offset += 4;

        // Write magic bytes.
        unsafe {
            std::ptr::copy_nonoverlapping(
                DELETION_VECTOR_MAGIC_BYTES.as_ptr(),
                ptr.add(offset),
                DELETION_VECTOR_MAGIC_BYTES.len(),
            );
        }
        offset += DELETION_VECTOR_MAGIC_BYTES.len();

        // Serialized and write bitmap, which is the standard roaring on-disk format.
        // Spec: https://github.com/RoaringBitmap/RoaringFormatSpec
        let bitmap_slice =
            unsafe { std::slice::from_raw_parts_mut(ptr.add(offset), serialized_bitmap_size) };
        let mut bitmap_writer = std::io::Cursor::new(bitmap_slice);
        self.bitmap.serialize_into(&mut bitmap_writer).unwrap();
        offset += serialized_bitmap_size;

        // Calculate CRC (magic bytes + serialized bitmap).
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&data[4..offset]);
        let crc = hasher.finalize();

        // Write CRC.
        let crc_bytes = crc.to_be_bytes();
        unsafe {
            std::ptr::copy_nonoverlapping(crc_bytes.as_ptr(), ptr.add(offset), crc_bytes.len());
        }

        // Snapshot ID and sequence number are not known at the time the Puffin file is created,
        // so they're set to -1 in blob metadata for puffin v1.
        // Reference: https://iceberg.apache.org/puffin-spec/?h=puffin#blob-types
        Blob::builder()
            .r#type(DELETION_VECTOR_V1.to_string())
            .fields(vec![])
            .snapshot_id(-1)
            .sequence_number(-1)
            .data(data)
            .properties(properties)
            .build()
    }

    /// Deserialize from `Blob` to deletion vector.
    pub fn deserialize(blob: Blob) -> IcebergResult<Self> {
        let data = blob.data();

        // Minimum length for serialized blob is 12 bytes (4 length + 4 magic + 4 crc).
        if data.len() < MIN_SERIALIZED_DELETION_VECTOR_BLOB {
            return Err(IcebergError::new(
                iceberg::ErrorKind::DataInvalid,
                "Serialized deletion vector blob should be at least 12 bytes.".to_string(),
            ));
        }

        // Check magic bytes.
        let magic_in_data = &data[4..8];
        if magic_in_data != DELETION_VECTOR_MAGIC_BYTES {
            return Err(IcebergError::new(
                iceberg::ErrorKind::DataInvalid,
                "Data corruption detected for serialized deletion vector blob.".to_string(),
            ));
        }

        // Check combined length.
        let combined_length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        if std::mem::size_of_val(&combined_length) + (combined_length as usize) + 4 != data.len() {
            return Err(IcebergError::new(
                iceberg::ErrorKind::DataInvalid,
                format!(
                    "Serialized deletion vector blob length mismatch: expected {}, actual {}",
                    std::mem::size_of_val(&combined_length) + (combined_length as usize) + 4, /*crc32c*/
                    data.len()
                ),
            ));
        }

        // The rest between magic bytes and CRC is the serialized bitmap.
        let bitmap_data_start = 8;
        let bitmap_data_end = data.len() - 4;
        let bitmap_data = &data[bitmap_data_start..bitmap_data_end];

        // Check CRC.
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&DELETION_VECTOR_MAGIC_BYTES);
        hasher.update(bitmap_data);
        let expected_crc = hasher.finalize();

        let stored_crc = u32::from_be_bytes([
            data[data.len() - 4],
            data[data.len() - 3],
            data[data.len() - 2],
            data[data.len() - 1],
        ]);

        if expected_crc != stored_crc {
            return Err(IcebergError::new(
                iceberg::ErrorKind::DataInvalid,
                format!("Within serialized deletion vector blob persisted crc32c is {expected_crc}, actual crc32c is {stored_crc}."),
            ));
        }

        // Deserialize the bitmap.
        DeletionVector::deserialize_roaring_map(bitmap_data)
    }

    /// Load deletion vector from puffin file blob.
    ///
    /// TODO(hjiang): Add unit test for load blob from local filesystem.
    pub async fn load_from_dv_blob(file_io: FileIO, puffin_file: &DataFile) -> IcebergResult<Self> {
        let blob =
            puffin_utils::load_blob_from_puffin_file(file_io, puffin_file.file_path()).await?;
        DeletionVector::deserialize(blob)
    }

    /// Convert self to `BatchDeletionVector`, after which self ownership is terminated.
    pub fn take_as_batch_delete_vector(self, batch_size: usize) -> BatchDeletionVector {
        let mut batch_delete_vector = BatchDeletionVector::new(batch_size);
        for row_idx in self.bitmap.iter() {
            batch_delete_vector.delete_row(row_idx as usize);
        }
        batch_delete_vector
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_blob_properties(deleted_rows: usize) -> HashMap<String, String> {
        let mut properties = HashMap::new();
        properties.insert(
            DELETION_VECTOR_CADINALITY.to_string(),
            deleted_rows.to_string(),
        );
        properties.insert(
            DELETION_VECTOR_REFERENCED_DATA_FILE.to_string(),
            "/tmp/iceberg/data/filename".to_string(),
        );
        properties
    }

    #[test]
    fn test_empty_deletion_vector() {
        let dv = DeletionVector::new();
        let blob = dv.serialize(create_test_blob_properties(/*deleted_rows=*/ 0));
        let deserialized_dv = DeletionVector::deserialize(blob).unwrap();
        assert!(dv.bitmap.is_empty());
        assert!(deserialized_dv.bitmap.is_empty());
    }

    #[test]
    fn test_mark_and_serialize_deserialize_deletion_vector() {
        let mut dv = DeletionVector::new();
        let deleted_rows: Vec<u64> = vec![1, 3, 5, 7, 1000];
        dv.mark_rows_deleted(deleted_rows.clone());
        let blob = dv.serialize(create_test_blob_properties(
            /*deleted_rows=*/ deleted_rows.len(),
        ));
        let deserialized_dv = DeletionVector::deserialize(blob).unwrap();
        for row in deleted_rows.iter() {
            assert!(deserialized_dv.bitmap.contains(*row));
        }
        assert_eq!(dv.bitmap, deserialized_dv.bitmap);

        // Check conversion into BatchDeletionVector.
        let batch_deletion_vector = dv.take_as_batch_delete_vector(1024);
        assert_eq!(batch_deletion_vector.collect_deleted_rows(), deleted_rows);
    }
}

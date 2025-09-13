/// This module contains util structs and functions for puffin access.
use std::collections::HashMap;

use iceberg::io::FileIO;
use iceberg::puffin::PuffinWriter;
use iceberg::puffin::{Blob, PuffinReader};
use iceberg::{Error as IcebergError, Result as IcebergResult};

use crate::storage::iceberg::deletion_vector::DeletionVector;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::NonEvictableHandle;

/// Reference to puffin blob, which is always cached on-disk.
#[derive(Clone, Debug)]
pub struct PuffinBlobRef {
    /// Invariant: puffin blob file represents the same deleted rows as batch deletion vector.
    ///
    /// Puffin file cache handle.
    pub(crate) puffin_file_cache_handle: NonEvictableHandle,
    /// Start offset for the blob.
    pub(crate) start_offset: u32,
    /// Blob size.
    pub(crate) blob_size: u32,
    /// Number of rows deleted in the puffin blob.
    pub(crate) num_rows: usize,
}

/// Get puffin writer with the given file io.
pub(crate) async fn create_puffin_writer(
    file_io: &FileIO,
    puffin_filepath: &str,
) -> IcebergResult<PuffinWriter> {
    let out_file = file_io.new_output(puffin_filepath)?;
    let puffin_writer = PuffinWriter::new(
        &out_file,
        /*properties=*/ HashMap::new(),
        /*compress_footer=*/ false,
    )
    .await?;
    Ok(puffin_writer)
}

/// Load blob from the given puffin filepath.
/// Note: this function assumes there's only one blob in the puffin file.
pub(crate) async fn load_blob_from_puffin_file(
    file_io: FileIO,
    file_path: &str,
) -> IcebergResult<Blob> {
    let input_file = file_io.new_input(file_path)?;
    let puffin_reader = PuffinReader::new(input_file);
    let puffin_file_metadata = puffin_reader.file_metadata().await?;

    // Moonlink places one deletion vector in each puffin file.
    if puffin_file_metadata.blobs().len() != 1 {
        return Err(IcebergError::new(
            iceberg::ErrorKind::DataInvalid,
            format!(
                "Puffin file expects to have one blob, but has {} blobs",
                puffin_file_metadata.blobs().len()
            ),
        ));
    }

    let blob_metadata = &puffin_file_metadata.blobs()[0];
    puffin_reader.blob(blob_metadata).await
}

/// Util function to load batch deletion vector from puffin blob.
/// Precondition: there's only one deletion vector blob in the puffin file.
pub(crate) async fn load_deletion_vector_from_blob(
    puffin_blob_ref: &PuffinBlobRef,
) -> IcebergResult<BatchDeletionVector> {
    let cache_filepath = puffin_blob_ref
        .puffin_file_cache_handle
        .get_cache_filepath();
    let file_io = FileIO::from_path(cache_filepath)?.build()?;
    let puffin_blob = load_blob_from_puffin_file(file_io, cache_filepath).await?;
    let deletion_vector = DeletionVector::deserialize(puffin_blob)?;
    Ok(deletion_vector.take_as_batch_delete_vector())
}

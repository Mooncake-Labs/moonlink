use iceberg::spec::{DataContentType, DataFileFormat, ManifestEntry};
/// This module validates iceberg specification.
use iceberg::Result as IcebergResult;

/// Validate invariants for the given puffin manifest, if precondition detected broken return error.
pub(crate) fn validate_puffin_manifest_entry(entry: &ManifestEntry) -> IcebergResult<()> {
    assert_eq!(entry.file_format(), DataFileFormat::Puffin);

    // Check data file content type.
    let data_file = entry.data_file();
    if data_file.content_type() != DataContentType::PositionDeletes {
        return Err(iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            format!(
                "Puffin manifest should have content type `data`, but has {:?} for data file {:?}",
                data_file.content_type(),
                data_file
            ),
        ));
    }

    // Check data file path.
    if data_file.file_path().is_empty() {
        return Err(iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            format!("Puffin manifest doesn't have file path assigned for data file {data_file:?}"),
        ));
    }

    // Check data file format.
    if data_file.file_format() != DataFileFormat::Puffin {
        return Err(iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            format!(
                "Puffin manifest should have data file format `puffin`, but has {:?} for data file {:?}",
                data_file.file_format(),
                data_file
            )
        ));
    }

    // Check referenced data file.
    if data_file.referenced_data_file().is_none() {
        return Err(iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            format!("Puffin deletion vector should reference to data file {data_file:?}"),
        ));
    }

    // Check content offset and blob size.
    if data_file.content_offset().is_none() || data_file.content_size_in_bytes().is_none() {
        return Err(iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            format!(
                "Puffin deletion vector should have content offset and blob size assigned for data file {data_file:?}"
            )
        ));
    }

    Ok(())
}

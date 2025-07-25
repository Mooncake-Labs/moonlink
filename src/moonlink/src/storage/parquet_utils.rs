/// This module contains parquet related constants and utils.
use parquet::{basic::Compression, file::properties::WriterProperties};

/// Default compression.
const DEFAULT_COMPRESSION: Compression = parquet::basic::Compression::SNAPPY;

pub(crate) fn get_default_parquet_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(DEFAULT_COMPRESSION)
        .set_dictionary_enabled(true)
        .build()
}

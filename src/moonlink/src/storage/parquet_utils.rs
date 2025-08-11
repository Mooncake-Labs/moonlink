/// This module contains parquet related constants and utils.
use parquet::{basic::Compression, file::properties::WriterProperties};

/// Default compression.
const DEFAULT_COMPRESSION: Compression = parquet::basic::Compression::SNAPPY;

// Default row group size from duckdb.
const DEFAULT_ROW_GROUP_SIZE: usize = 122880;

pub(crate) fn get_default_parquet_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(DEFAULT_COMPRESSION)
        .set_dictionary_enabled(true)
        .set_dictionary_page_size_limit(DEFAULT_ROW_GROUP_SIZE / 100)
        .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_1_0)
        .build()
}

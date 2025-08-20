/// This module contains parquet related constants and utils.
use parquet::basic::Compression;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};

/// Default compression.
const DEFAULT_COMPRESSION: Compression = parquet::basic::Compression::SNAPPY;

/// Default row group size from duckdb.
const DEFAULT_ROW_GROUP_SIZE: usize = 122880;

/// Default false positive probability (fpp).
const DEFAULT_FPP: f64 = 0.01;

fn get_default_parquet_properties_builder() -> WriterPropertiesBuilder {
    WriterProperties::builder()
        .set_compression(DEFAULT_COMPRESSION)
        .set_dictionary_enabled(true)
        .set_dictionary_page_size_limit(DEFAULT_ROW_GROUP_SIZE / 100)
        .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_1_0)
}

pub(crate) fn get_default_parquet_properties() -> WriterProperties {
    let builder = get_default_parquet_properties_builder();
    builder.build()
}

/// Parquet option for already compacted files.
pub(crate) fn get_parquet_properties_for_compaction() -> WriterProperties {
    let builder = get_default_parquet_properties_builder();
    builder
        .set_bloom_filter_enabled(true)
        .set_bloom_filter_fpp(DEFAULT_FPP)
        .build()
}

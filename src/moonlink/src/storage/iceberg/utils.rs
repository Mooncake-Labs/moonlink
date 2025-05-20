use crate::storage::iceberg::file_catalog::{CatalogConfig, FileCatalog};
use crate::storage::iceberg::moonlink_catalog::MoonlinkCatalog;
#[cfg(feature = "storage-s3")]
use crate::storage::iceberg::s3_test_utils;
use crate::storage::iceberg::table_property;

use futures::TryStreamExt;
use std::collections::HashMap;
use std::path::Path;
use url::Url;
use uuid::Uuid;

use arrow_schema::Schema as ArrowSchema;
use iceberg::arrow as IcebergArrow;
use iceberg::io::FileIOBuilder;
use iceberg::spec::DataFile;
use iceberg::spec::{DataContentType, DataFileFormat, ManifestEntry};
use iceberg::table::Table as IcebergTable;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, LocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::IcebergWriter;
use iceberg::writer::IcebergWriterBuilder;
use iceberg::{
    Error as IcebergError, NamespaceIdent, Result as IcebergResult, TableCreation, TableIdent,
};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::file::properties::WriterProperties;

/// Return whether the given manifest entry represents data files.
pub fn is_data_file_entry(entry: &ManifestEntry) -> bool {
    let f = entry.data_file();
    let is_data_file =
        f.content_type() == DataContentType::Data && f.file_format() == DataFileFormat::Parquet;
    if !is_data_file {
        return false;
    }
    assert!(f.referenced_data_file().is_none());
    assert!(f.content_offset().is_none());
    assert!(f.content_size_in_bytes().is_none());
    true
}

/// Return whether the given manifest entry represents deletion vector.
pub fn is_deletion_vector_entry(entry: &ManifestEntry) -> bool {
    let f = entry.data_file();
    let is_deletion_vector = f.content_type() == DataContentType::PositionDeletes;
    if !is_deletion_vector {
        return false;
    }
    assert_eq!(f.file_format(), DataFileFormat::Puffin);
    assert!(f.referenced_data_file().is_some());
    assert!(f.content_offset().is_some());
    assert!(f.content_size_in_bytes().is_some());
    true
}

/// Return whether the given manifest entry represents file index.
pub fn is_file_index(entry: &ManifestEntry) -> bool {
    let f = entry.data_file();
    let is_file_index =
        f.content_type() == DataContentType::Data && f.file_format() == DataFileFormat::Puffin;
    if !is_file_index {
        return false;
    }
    assert!(f.referenced_data_file().is_none());
    assert!(f.content_offset().is_none());
    assert!(f.content_size_in_bytes().is_none());
    true
}

/// Create a catelog based on the provided type.
///
/// It's worth noting catalog and warehouse uri are not 1-1 mapping; for example, rest catalog could handle warehouse.
/// Here we simply deduce catalog type from warehouse because both filesystem and object storage catalog are only able to handle certain scheme.
pub fn create_catalog(warehouse_uri: &str) -> IcebergResult<Box<dyn MoonlinkCatalog>> {
    // Special handle testing situation.
    #[cfg(feature = "storage-s3")]
    {
        if warehouse_uri.starts_with(s3_test_utils::MINIO_TEST_WAREHOUSE_URI_PREFIX) {
            let test_bucket = s3_test_utils::get_test_minio_bucket(warehouse_uri);
            return Ok(Box::new(s3_test_utils::create_minio_s3_catalog(
                &test_bucket,
                warehouse_uri,
            )));
        }
    }

    let url = Url::parse(warehouse_uri)
        .or_else(|_| Url::from_file_path(warehouse_uri))
        .map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Invalid warehouse URI {}: {:?}", warehouse_uri, e),
            )
        })?;

    if url.scheme() == "file" {
        let absolute_path = url.path();
        return Ok(Box::new(FileCatalog::new(
            absolute_path.to_string(),
            CatalogConfig::FileSystem {},
        )));
    }

    // TODO(hjiang): Fallback to object storage for all warehouse uris.
    todo!("Need to take secrets from client side and create object storage catalog.")
}

// Create iceberg table properties from table config.
fn create_iceberg_table_properties() -> HashMap<String, String> {
    let mut props = HashMap::with_capacity(3);
    props.insert(
        table_property::PARQUET_COMPRESSION.to_string(),
        table_property::PARQUET_COMPRESSION_DEFAULT.to_string(),
    );
    props.insert(
        table_property::METADATA_COMPRESSION.to_string(),
        table_property::METADATA_COMPRESSION_DEFAULT.to_string(),
    );
    props
}

/// Create an iceberg table in the given catalog from the given namespace and table name.
/// Precondition: table doesn't exist in the given catalog.
async fn create_iceberg_table<C: MoonlinkCatalog + ?Sized>(
    catalog: &C,
    warehouse_uri: &str,
    table_name: &str,
    namespace_ident: NamespaceIdent,
    arrow_schema: &ArrowSchema,
) -> IcebergResult<IcebergTable> {
    let namespace_already_exists = catalog.namespace_exists(&namespace_ident).await?;
    if !namespace_already_exists {
        catalog
            .create_namespace(&namespace_ident, /*properties=*/ HashMap::new())
            .await?;
    }

    let iceberg_schema = IcebergArrow::arrow_schema_to_schema(arrow_schema)?;
    let tbl_creation = TableCreation::builder()
        .name(table_name.to_string())
        .location(format!(
            "{}/{}/{}",
            warehouse_uri,
            namespace_ident.to_url_string(),
            table_name
        ))
        .schema(iceberg_schema)
        .properties(create_iceberg_table_properties())
        .build();
    let table = catalog.create_table(&namespace_ident, tbl_creation).await?;
    Ok(table)
}

/// Get or create an iceberg table in the given catalog from the given namespace and table name.
///
/// There're several options:
/// - If the table doesn't exist, create a new one
/// - If the table already exists, and [drop_if_exists] true (overwrite use case), delete the table and re-create
/// - If already exists and not requested to drop (recovery use case), do nothing and return the table directly
pub(crate) async fn get_iceberg_table<C: MoonlinkCatalog + ?Sized>(
    catalog: &C,
    warehouse_uri: &str,
    namespace: &Vec<String>,
    table_name: &str,
    arrow_schema: &ArrowSchema,
    drop_if_exists: bool,
) -> IcebergResult<IcebergTable> {
    let namespace_ident = NamespaceIdent::from_strs(namespace).unwrap();
    let table_ident = TableIdent::new(namespace_ident.clone(), table_name.to_string());
    let should_create = match catalog.load_table(&table_ident).await {
        Ok(existing_table) => {
            if drop_if_exists {
                catalog.drop_table(&table_ident).await?;
                true
            } else {
                return Ok(existing_table);
            }
        }
        Err(_) => true,
    };

    if should_create {
        create_iceberg_table(
            catalog,
            warehouse_uri,
            table_name,
            namespace_ident,
            arrow_schema,
        )
        .await
    } else {
        unreachable!()
    }
}

/// Write the given record batch in the given local file to the iceberg table (parquet file keeps unchanged).
//
// TODO(hjiang):
// 1. Uploading local file to remote is inefficient, it reads local arrow batches and write them one by one.
// The reason we keep the dummy style, instead of copying the file directly to target is we need the `DataFile` struct,
// which is used when upload to iceberg table.
// One way to resolve is to use DataFileWrite on local write, and remember the `DataFile` returned.
pub(crate) async fn write_record_batch_to_iceberg(
    table: &IcebergTable,
    parquet_filepath: &String,
) -> IcebergResult<DataFile> {
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
    let file_name_generator = DefaultFileNameGenerator::new(
        // Use UUID as prefix to avoid hotspotting on storage access.
        /*prefix=*/
        Uuid::new_v4().to_string(),
        /*suffix=*/ None,
        /*format=*/ DataFileFormat::Parquet,
    );

    let parquet_writer_builder = ParquetWriterBuilder::new(
        /*props=*/ WriterProperties::default(),
        /*schame=*/ table.metadata().current_schema().clone(),
        /*file_io=*/ table.file_io().clone(),
        /*location_generator=*/ location_generator,
        /*file_name_generator=*/ file_name_generator,
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(
        parquet_writer_builder,
        /*partition_value=*/ None,
        /*partition_spec_id=*/ 0,
    );
    let mut data_file_writer = data_file_writer_builder.build().await?;

    let local_file = tokio::fs::File::open(parquet_filepath).await?;
    let mut read_stream = ParquetRecordBatchStreamBuilder::new(local_file)
        .await?
        .build()?;
    while let Some(record_batch) = read_stream.try_next().await? {
        data_file_writer.write(record_batch).await?;
    }

    let data_files = data_file_writer.close().await?;
    assert_eq!(
        data_files.len(),
        1,
        "Should only have one parquet file written"
    );

    Ok(data_files[0].clone())
}

/// Get URL scheme for the given path.
fn get_url_scheme(url: &str) -> String {
    let url = Url::parse(url)
        .or_else(|_| Url::from_file_path(url))
        .unwrap_or_else(|_| panic!("Cannot get URL scheme from {:?}", url));
    url.scheme().to_string()
}

/// Copy the given local index file to iceberg table, and return filepath within iceberg table.
pub(crate) async fn upload_index_file(
    table: &IcebergTable,
    local_index_filepath: &str,
) -> IcebergResult<String> {
    let filename = Path::new(local_index_filepath)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let remote_filepath = location_generator.generate_location(&filename);
    let src = FileIOBuilder::new_fs_io()
        .build()?
        .new_input(local_index_filepath)?;
    let dst = FileIOBuilder::new(get_url_scheme(&remote_filepath))
        .build()?
        .new_output(remote_filepath.clone())?;

    // TODO(hjiang): Switch to parallel chunk-based reading.
    let bytes = src.read().await?;
    dst.write(bytes).await?;

    Ok(remote_filepath)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_url_scheme() {
        assert_eq!(get_url_scheme("/tmp/iceberg_table"), "file");
        assert_eq!(get_url_scheme("file:///tmp/iceberg_table"), "file");
        assert_eq!(get_url_scheme("s3://bucket/iceberg_table"), "s3");
    }
}

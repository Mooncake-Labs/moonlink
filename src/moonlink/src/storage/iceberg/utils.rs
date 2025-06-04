use crate::storage::iceberg::file_catalog::{CatalogConfig, FileCatalog};
use crate::storage::iceberg::moonlink_catalog::MoonlinkCatalog;
use crate::storage::iceberg::parquet_utils;
#[cfg(feature = "storage-s3")]
use crate::storage::iceberg::s3_test_utils;
use crate::storage::iceberg::table_property;

use std::collections::HashMap;
use std::path::Path;
use url::Url;

use arrow_schema::Schema as ArrowSchema;
use iceberg::arrow as IcebergArrow;
use iceberg::io::FileIOBuilder;
use iceberg::spec::DataFile;
use iceberg::spec::TableMetadata as IcebergTableMetadata;
use iceberg::spec::{DataContentType, DataFileFormat, ManifestEntry};
use iceberg::table::Table as IcebergTable;
use iceberg::writer::file_writer::location_generator::{
    DefaultLocationGenerator, LocationGenerator,
};
use iceberg::{
    Error as IcebergError, NamespaceIdent, Result as IcebergResult, TableCreation, TableIdent,
};

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
        )?));
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
pub(crate) async fn get_or_create_iceberg_table<C: MoonlinkCatalog + ?Sized>(
    catalog: &C,
    warehouse_uri: &str,
    namespace: &Vec<String>,
    table_name: &str,
    arrow_schema: &ArrowSchema,
) -> IcebergResult<IcebergTable> {
    let namespace_ident = NamespaceIdent::from_strs(namespace).unwrap();
    let table_ident = TableIdent::new(namespace_ident.clone(), table_name.to_string());
    let should_create = match catalog.load_table(&table_ident).await {
        Ok(existing_table) => {
            return Ok(existing_table);
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

/// Get iceberg table if exists.
pub(crate) async fn get_table_if_exists<C: MoonlinkCatalog + ?Sized>(
    catalog: &C,
    namespace: &Vec<String>,
    table_name: &str,
) -> IcebergResult<Option<IcebergTable>> {
    let namespace_ident = NamespaceIdent::from_strs(namespace).unwrap();
    let table_ident = TableIdent::new(namespace_ident.clone(), table_name.to_string());

    let table_exists = catalog.table_exists(&table_ident).await?;
    if !table_exists {
        return Ok(None);
    }

    let table = catalog.load_table(&table_ident).await?;
    Ok(Some(table))
}

/// Copy source filepath to destination filepath.
async fn copy_from_local_to_remote(src: &str, dst: &str) -> IcebergResult<()> {
    let src = FileIOBuilder::new_fs_io().build()?.new_input(src)?;
    let dst = FileIOBuilder::new(get_url_scheme(dst))
        .build()?
        .new_output(dst)?;

    // TODO(hjiang): Switch to parallel chunk-based reading if source file large.
    let bytes = src.read().await?;
    dst.write(bytes).await?;

    Ok(())
}

/// Write the given record batch in the given local file to the iceberg table (parquet file keeps unchanged).
pub(crate) async fn write_record_batch_to_iceberg(
    table: &IcebergTable,
    local_filepath: &String,
    table_metadata: &IcebergTableMetadata,
) -> IcebergResult<DataFile> {
    let filename = Path::new(local_filepath)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
    let remote_filepath = location_generator.generate_location(&filename);

    // Import local parquet file to remote.
    copy_from_local_to_remote(local_filepath, &remote_filepath).await?;

    // Get data file from local parquet file.
    let data_file = parquet_utils::get_data_file_from_local_parquet_file(
        local_filepath,
        remote_filepath,
        table_metadata,
    )
    .await?;
    Ok(data_file)
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
    copy_from_local_to_remote(local_index_filepath, &remote_filepath).await?;
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

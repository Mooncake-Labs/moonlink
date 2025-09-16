use crate::storage::iceberg::moonlink_catalog::MoonlinkCatalog;
use crate::storage::iceberg::table_property;

use std::collections::HashMap;

use arrow_schema::Schema as ArrowSchema;
use iceberg::arrow as IcebergArrow;
use iceberg::spec::{DataContentType, DataFileFormat, ManifestEntry};
use iceberg::table::Table as IcebergTable;
use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
use iceberg::writer::file_writer::location_generator::LocationGenerator;
use iceberg::{NamespaceIdent, Result as IcebergResult, TableCreation, TableIdent};

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
        .properties(table_property::create_iceberg_table_properties())
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
    let table_ident = TableIdent::new(namespace_ident, table_name.to_string());

    let table_exists = catalog.table_exists(&table_ident).await?;
    if !table_exists {
        return Ok(None);
    }

    let table = catalog.load_table(&table_ident).await?;
    Ok(Some(table))
}

/// Get a unique remote index filepath.
pub(crate) fn get_unique_hash_index_v1_filepath(iceberg_table: &IcebergTable) -> String {
    let location_generator =
        DefaultLocationGenerator::new(iceberg_table.metadata().clone()).unwrap();
    location_generator.generate_location(
        /*partition_key=*/ None,
        &format!("{}-hash-index-v1-puffin.bin", uuid::Uuid::now_v7()),
    )
}

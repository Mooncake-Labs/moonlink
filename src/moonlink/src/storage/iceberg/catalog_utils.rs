#[cfg(test)]
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::iceberg::file_catalog::FileCatalog;
#[cfg(feature = "catalog-glue")]
use crate::storage::iceberg::glue_catalog::GlueCatalog;
use crate::storage::iceberg::iceberg_table_config::IcebergCatalogConfig;
use crate::storage::iceberg::iceberg_table_config::IcebergTableConfig;
use crate::storage::iceberg::moonlink_catalog::MoonlinkCatalog;
use crate::storage::iceberg::moonlink_catalog::PuffinBlobType;
use crate::storage::iceberg::puffin_writer_proxy::append_puffin_metadata_and_rewrite;
use crate::storage::iceberg::puffin_writer_proxy::get_puffin_metadata_and_close;
#[cfg(feature = "catalog-rest")]
use crate::storage::iceberg::rest_catalog::RestCatalog;
use crate::storage::iceberg::table_commit_proxy::TableCommitProxy;
use crate::storage::iceberg::table_update_proxy::TableUpdateProxy;

use iceberg::io::FileIO;
use iceberg::puffin::PuffinWriter;
use iceberg::spec::Schema as IcebergSchema;
use iceberg::spec::TableMetadata;
use iceberg::table::Table;
use iceberg::Catalog;
use iceberg::NamespaceIdent;
use iceberg::TableCommit;
use iceberg::TableCreation;
use iceberg::{spec::TableMetadataBuilder, Result as IcebergResult, TableRequirement, TableUpdate};

/// Create a catelog based on the provided type.
///
/// It's worth noting catalog and warehouse uri are not 1-1 mapping; for example, rest catalog could handle warehouse.
/// Here we simply deduce catalog type from warehouse because both filesystem and object storage catalog are only able to handle certain scheme.
pub async fn create_catalog(
    config: IcebergTableConfig,
    iceberg_schema: IcebergSchema,
) -> IcebergResult<Box<dyn MoonlinkCatalog>> {
    match config.metadata_accessor_config {
        IcebergCatalogConfig::File { accessor_config } => {
            Ok(Box::new(FileCatalog::new(accessor_config, iceberg_schema)?))
        }
        #[cfg(feature = "catalog-rest")]
        IcebergCatalogConfig::Rest {
            rest_catalog_config,
        } => Ok(Box::new(
            RestCatalog::new(
                rest_catalog_config,
                config.data_accessor_config,
                iceberg_schema,
            )
            .await?,
        )),
        #[cfg(feature = "catalog-glue")]
        IcebergCatalogConfig::Glue {
            glue_catalog_config,
        } => Ok(Box::new(
            GlueCatalog::new(
                glue_catalog_config,
                config.data_accessor_config,
                iceberg_schema,
            )
            .await?,
        )),
    }
}

/// Create a catalog with no schema provided.
pub async fn create_catalog_without_schema(
    config: IcebergTableConfig,
) -> IcebergResult<Box<dyn MoonlinkCatalog>> {
    match config.metadata_accessor_config {
        IcebergCatalogConfig::File { accessor_config } => {
            Ok(Box::new(FileCatalog::new_without_schema(accessor_config)?))
        }
        #[cfg(feature = "catalog-rest")]
        IcebergCatalogConfig::Rest {
            rest_catalog_config,
        } => Ok(Box::new(
            RestCatalog::new_without_schema(rest_catalog_config, config.data_accessor_config)
                .await?,
        )),
        #[cfg(feature = "catalog-glue")]
        IcebergCatalogConfig::Glue {
            glue_catalog_config,
        } => Ok(Box::new(
            GlueCatalog::new_without_schema(glue_catalog_config, config.data_accessor_config)
                .await?,
        )),
    }
}

/// Reflect table updates to table metadata builder.
pub(crate) fn reflect_table_updates(
    mut builder: TableMetadataBuilder,
    table_updates: Vec<TableUpdate>,
) -> IcebergResult<TableMetadataBuilder> {
    for update in &table_updates {
        match update {
            TableUpdate::AddSnapshot { snapshot } => {
                builder = builder.add_snapshot(snapshot.clone())?;
            }
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                builder = builder.set_ref(ref_name, reference.clone())?;
            }
            TableUpdate::SetProperties { updates } => {
                builder = builder.set_properties(updates.clone())?;
            }
            TableUpdate::RemoveProperties { removals } => {
                builder = builder.remove_properties(removals)?;
            }
            TableUpdate::AddSchema { schema } => {
                builder = builder.add_schema(schema.clone())?;
            }
            TableUpdate::SetCurrentSchema { schema_id } => {
                builder = builder.set_current_schema(*schema_id)?;
            }
            _ => {
                unreachable!("Unimplemented table update: {:?}", update);
            }
        }
    }
    Ok(builder)
}

/// Validate table commit requirements.
pub(crate) fn validate_table_requirements(
    table_requirements: Vec<TableRequirement>,
    table_metadata: &TableMetadata,
) -> IcebergResult<()> {
    for cur_requirement in table_requirements.into_iter() {
        cur_requirement.check(Some(table_metadata))?;
    }
    Ok(())
}

/// Test util function to create catalog with provided filesystem accessor.
#[cfg(test)]
pub fn create_catalog_with_filesystem_accessor(
    filesystem_accessor: std::sync::Arc<dyn BaseFileSystemAccess>,
    iceberg_schema: IcebergSchema,
) -> IcebergResult<Box<dyn MoonlinkCatalog>> {
    Ok(Box::new(FileCatalog::new_with_filesystem_accessor(
        filesystem_accessor,
        iceberg_schema,
    )?))
}

/// Close puffin writer and record metadata in [`table_update_proxy`].
pub(crate) async fn close_puffin_writer_and_record_metadata(
    puffin_filepath: String,
    puffin_writer: PuffinWriter,
    puffin_blob_type: PuffinBlobType,
    table_update_proxy: &mut TableUpdateProxy,
) -> IcebergResult<()> {
    let puffin_metadata = get_puffin_metadata_and_close(puffin_writer).await?;
    table_update_proxy.record_puffin_metadata(puffin_filepath, puffin_metadata, puffin_blob_type);
    Ok(())
}

/// Create table implementation, with schema de-normalized.
#[allow(unused)]
pub(crate) async fn create_table_impl(
    internal_catalog: &dyn Catalog,
    namespace_ident: &NamespaceIdent,
    creation: TableCreation,
    iceberg_schema: IcebergSchema,
) -> IcebergResult<Table> {
    let old_table = internal_catalog
        .create_table(namespace_ident, creation)
        .await?;
    let old_metadata = old_table.metadata();

    // Craft a new schema with a new schema id, which has to be different from the existing one.
    let old_schema = iceberg_schema;
    let new_schema_id = old_schema.schema_id() + 1;
    let mut new_schema_builder = old_schema.into_builder();
    new_schema_builder = new_schema_builder.with_schema_id(new_schema_id);
    let new_schema = new_schema_builder.build()?;

    // On table creation, iceberg-rust normalize field id, which breaks the mapping between mooncake table arrow schema and iceberg schema.
    // Intentionally perform a schema evolution to reflect desired schema.
    let mut builder = TableMetadataBuilder::new_from_metadata(
        old_metadata.clone(),
        /*current_file_location=*/ None,
    );
    builder = builder.add_current_schema(new_schema)?;
    let build_result = builder.build()?;
    let normalized_updates = build_result.changes;
    let new_commit = TableCommitProxy {
        ident: old_table.identifier().clone(),
        requirements: Vec::new(),
        updates: normalized_updates,
    }
    .take_as_table_commit();

    let updated_table = internal_catalog.update_table(new_commit).await?;
    Ok(updated_table)
}

/// Update table implementation.
///
/// Transaction commit operations include:
/// - iceberg-rust write metadata file and manifest file
/// - catalog check requirements
/// - catalog craft request body
/// - catalog commit
#[allow(unused)]
pub(crate) async fn update_table_impl(
    internal_catalog: &dyn Catalog,
    file_io: &FileIO,
    table_update_proxy: &TableUpdateProxy,
    mut commit: TableCommit,
) -> IcebergResult<Table> {
    let table = internal_catalog.load_table(commit.identifier()).await?;
    let metadata = table.metadata();
    let requirements = commit.take_requirements();

    let builder = TableMetadataBuilder::new_from_metadata(metadata.clone(), None);
    let updates = commit.take_updates();
    let builder = reflect_table_updates(builder, updates)?;
    let build_result = builder.build()?;

    // Rewrite manifest list/entries to append puffin metadata and handle removals before commit
    append_puffin_metadata_and_rewrite(
        &build_result.metadata,
        file_io,
        &table_update_proxy.deletion_vector_blobs_to_add,
        &table_update_proxy.file_index_blobs_to_add,
        &table_update_proxy.data_files_to_remove,
        &table_update_proxy.puffin_blobs_to_remove,
    )
    .await?;

    let normalized_updates = build_result.changes;

    // Repackage normalized updates and original requirements into a [`TableCommit`] and delegate to the internal REST catalog implementation.
    let new_commit = TableCommitProxy {
        ident: commit.identifier().clone(),
        requirements,
        updates: normalized_updates,
    }
    .take_as_table_commit();

    let table = internal_catalog.update_table(new_commit).await?;
    Ok(table)
}

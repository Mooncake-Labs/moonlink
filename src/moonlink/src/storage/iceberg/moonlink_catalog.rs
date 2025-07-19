use async_trait::async_trait;
/// A trait which defines deletion vector write related interfaces.
use iceberg::puffin::PuffinWriter;
use iceberg::spec::{Schema as IcebergSchema, SchemaId};
use iceberg::{Catalog, Result as IcebergResult, TableIdent};

use std::collections::HashSet;

/// TODO(hjiang): iceberg-rust currently doesn't support puffin write, to workaround and reduce code change,
/// we record puffin metadata ourselves and rewrite manifest file before transaction commits.
#[async_trait]
pub trait PuffinWrite {
    /// Add puffin metadata from the writer, and close it.
    async fn record_puffin_metadata_and_close(
        &mut self,
        puffin_filepath: String,
        puffin_writer: PuffinWriter,
    ) -> IcebergResult<()>;

    /// Set data files to remove, their corresponding deletion vectors will be removed alongside.
    fn set_data_files_to_remove(&mut self, data_files: HashSet<String>);

    /// Set puffin file to remove.
    fn set_puffin_files_to_remove(&mut self, puffin_filepaths: HashSet<String>);

    /// After transaction commits, puffin metadata should be cleared for next puffin write.
    fn clear_puffin_metadata(&mut self);
}

/// TODO(hjiang): iceberg-rust currently doesn't support schema evolution, to workaround and reduce code change,
/// we do schema evolution by directly setting table commits.
#[async_trait]
pub trait SchemaUpdate {
    /// Update table schema, and return the updated iceberg table.
    async fn update_table_schema(
        &mut self,
        schema: IcebergSchema,
        old_schema_id: SchemaId,
        table_ident: TableIdent,
    ) -> IcebergResult<()>;
}

pub trait MoonlinkCatalog: PuffinWrite + SchemaUpdate + Catalog {}
impl<T: PuffinWrite + SchemaUpdate + Catalog> MoonlinkCatalog for T {}

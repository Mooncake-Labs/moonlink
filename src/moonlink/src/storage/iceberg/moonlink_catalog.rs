use async_trait::async_trait;
/// A trait which defines deletion vector write related interfaces.
use iceberg::puffin::PuffinWriter;
use iceberg::{Catalog, Result as IcebergResult};

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

    /// Set puffin file to remove.
    fn set_puffin_file_to_remove(&mut self, puffin_filepaths: HashSet<String>);

    /// After transaction commits, puffin metadata should be cleared for next puffin write.
    fn clear_puffin_metadata(&mut self);
}

pub trait MoonlinkCatalog: PuffinWrite + Catalog {}
impl<T: PuffinWrite + Catalog> MoonlinkCatalog for T {}

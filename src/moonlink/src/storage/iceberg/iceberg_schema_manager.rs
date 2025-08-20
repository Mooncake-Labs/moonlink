use crate::storage::iceberg::iceberg_table_manager::*;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::Result;

use iceberg::arrow as IcebergArrow;

use std::sync::Arc;

impl IcebergTableManager {
    pub(super) async fn alter_table_schema_impl(
        &mut self,
        updated_table_metadata: Arc<MooncakeTableMetadata>,
    ) -> Result<()> {
        // Initialize iceberg table on access.
        self.initialize_iceberg_table_for_once().await?;

        let table_ident = self.get_table_ident();
        let new_schema = IcebergArrow::arrow_schema_to_schema(&updated_table_metadata.schema)?;
        let updated_table = self
            .catalog
            .update_table_schema(new_schema, table_ident)
            .await?;
        self.iceberg_table = Some(updated_table);

        Ok(())
    }
}

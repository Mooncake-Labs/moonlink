use crate::storage::iceberg::iceberg_table_manager::*;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;

use iceberg::arrow as IcebergArrow;
use iceberg::Result as IcebergResult;

use std::sync::Arc;

impl IcebergTableManager {
    pub(super) async fn alter_table_schema_impl(
        &mut self,
        updated_table_metadata: Arc<MooncakeTableMetadata>,
    ) -> IcebergResult<()> {
        // Initialize iceberg table on access.
        self.initialize_iceberg_table_for_once().await?;

        let table_ident = self.get_table_ident();
        let table_metadata = self.iceberg_table.as_ref().unwrap().metadata();
        let current_schema_id = table_metadata.current_schema_id();

        let new_schema = IcebergArrow::arrow_schema_to_schema(&updated_table_metadata.schema)?;
        self.catalog
            .update_table_schema(new_schema, current_schema_id, table_ident)
            .await?;

        Ok(())
    }
}

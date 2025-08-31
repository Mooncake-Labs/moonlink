use iceberg::spec::TableMetadataBuilder;
use iceberg::{Result as IcebergResult, TableUpdate};

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

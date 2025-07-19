use iceberg::spec::{Schema as IcebergSchema, SchemaId};
use iceberg::{TableCommit, TableIdent, TableRequirement, TableUpdate};

/// Struct which mimics [`TableCommit`], to workaround the limitation that [`TableCommit`] is not exposed to public.
#[repr(C)]
pub(crate) struct TableCommitProxy {
    pub(crate) ident: TableIdent,
    pub(crate) requirements: Vec<TableRequirement>,
    pub(crate) updates: Vec<TableUpdate>,
}

impl TableCommitProxy {
    pub(crate) fn new(ident: TableIdent) -> Self {
        Self {
            ident,
            requirements: vec![],
            updates: vec![],
        }
    }

    /// Update table schema to the given one.
    pub(crate) fn update_schema(&mut self, schema: IcebergSchema, old_schema_id: SchemaId) {
        let schema_id = schema.schema_id();

        // Make schema update operation.
        let schema_update = TableUpdate::AddSchema { schema };
        self.updates.push(schema_update);

        // Make set current schema operation.
        let set_current_schema = TableUpdate::SetCurrentSchema { schema_id };
        self.updates.push(set_current_schema);

        // Remove old schema operation.
        let remove_old_schema = TableUpdate::RemoveSchemas {
            schema_ids: vec![old_schema_id],
        };
        self.updates.push(remove_old_schema);
    }

    /// Take as [`TableCommit`].
    pub(crate) fn take_as_table_commit(self) -> TableCommit {
        unsafe { std::mem::transmute::<TableCommitProxy, TableCommit>(self) }
    }
}

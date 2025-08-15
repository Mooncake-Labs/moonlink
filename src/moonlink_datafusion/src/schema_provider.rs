use crate::table_provider::MooncakeTableProvider;
use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct MooncakeSchemaProvider {
    uri: String,
    schema: String,
}

impl MooncakeSchemaProvider {
    pub(crate) fn new(uri: String, schema: String) -> Self {
        Self { uri, schema }
    }
}

#[async_trait]
impl SchemaProvider for MooncakeSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        unimplemented!()
    }

    async fn table(&self, table: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let res =
            MooncakeTableProvider::try_new(&self.uri, self.schema.clone(), table.to_string(), 0)
                .await;
        let Ok(table) = res else {
            return Ok(None);
        };
        Ok(Some(Arc::new(table)))
    }

    fn table_exist(&self, _name: &str) -> bool {
        unimplemented!()
    }
}

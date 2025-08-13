use crate::table_provider::MooncakeTableProvider;
use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct MooncakeSchemaProvider {
    uri: String,
    database_id: u32,
}

impl MooncakeSchemaProvider {
    pub(crate) fn new(uri: String, database_id: u32) -> Self {
        Self { uri, database_id }
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

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let Ok(table_id) = name.parse() else {
            return Ok(None);
        };
        let res = MooncakeTableProvider::try_new(&self.uri, self.database_id, table_id, 0).await;
        let Ok(table) = res else {
            return Ok(None);
        };
        Ok(Some(Arc::new(table)))
    }

    fn table_exist(&self, _name: &str) -> bool {
        unimplemented!()
    }
}

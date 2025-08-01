use crate::table_provider::MooncakeTableProvider;
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use std::{any::Any, sync::Arc};
use tokio::net::UnixStream;
use tokio::sync::Mutex;

#[derive(Debug)]
pub(crate) struct MooncakeSchemaProvider {
    stream: Arc<Mutex<UnixStream>>,
    database_id: u32,
    tables: DashMap<u32, Arc<dyn TableProvider>>,
}

impl MooncakeSchemaProvider {
    pub(crate) fn new(stream: Arc<Mutex<UnixStream>>, database_id: u32) -> Self {
        Self {
            stream,
            database_id,
            tables: DashMap::new(),
        }
    }
}

#[async_trait]
impl SchemaProvider for MooncakeSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        todo!()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let Ok(table_id) = name.parse() else {
            return Ok(None);
        };
        if let Some(table) = self.tables.get(&table_id) {
            return Ok(Some(table.clone()));
        }

        let table: Arc<dyn TableProvider> = Arc::new(
            MooncakeTableProvider::try_new(Arc::clone(&self.stream), self.database_id, table_id, 0)
                .await
                .unwrap(),
        );
        self.tables.insert(table_id, Arc::clone(&table));
        Ok(Some(table))
    }

    fn table_exist(&self, _name: &str) -> bool {
        todo!()
    }
}

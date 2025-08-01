use crate::error::Result;
use crate::schema_provider::MooncakeSchemaProvider;
use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use std::{any::Any, sync::Arc};
use tokio::net::UnixStream;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct MooncakeCatalogProvider {
    stream: Arc<Mutex<UnixStream>>,
    schemas: DashMap<u32, Arc<dyn SchemaProvider>>,
}

impl MooncakeCatalogProvider {
    pub async fn try_new(uri: &str) -> Result<Self> {
        let stream = UnixStream::connect(uri).await?;
        Ok(Self {
            stream: Arc::new(Mutex::new(stream)),
            schemas: DashMap::new(),
        })
    }
}

impl CatalogProvider for MooncakeCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        todo!()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let database_id: u32 = name.parse().ok()?;
        let schema = self.schemas.entry(database_id).or_insert_with(|| {
            Arc::new(MooncakeSchemaProvider::new(
                Arc::clone(&self.stream),
                database_id,
            ))
        });
        Some(schema.clone())
    }
}

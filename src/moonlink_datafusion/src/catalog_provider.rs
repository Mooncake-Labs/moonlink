use crate::connection_pool::Pool;
use crate::error::Result;
use crate::schema_provider::MooncakeSchemaProvider;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct MooncakeCatalogProvider {
    uri: String,
}

impl MooncakeCatalogProvider {
    pub async fn try_new(uri: String) -> Result<Self> {
        let _ = Pool::get_stream(&uri).await?;

        Ok(Self { uri })
    }
}

impl CatalogProvider for MooncakeCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        unimplemented!()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let database_id = name.parse().ok()?;
        Some(Arc::new(MooncakeSchemaProvider::new(
            self.uri.clone(),
            database_id,
        )))
    }
}

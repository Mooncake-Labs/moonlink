use crate::error::{Error, Result};
use crate::schema_provider::MooncakeSchemaProvider;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use std::any::Any;
use std::sync::Arc;
use tokio::net::UnixStream;

#[derive(Debug)]
pub struct MooncakeCatalogProvider {
    uri: String,
}

impl MooncakeCatalogProvider {
    pub async fn try_new(uri: String) -> Result<Self> {
        match UnixStream::connect(&uri).await {
            Ok(_) => Ok(Self { uri }),
            Err(e) => Err(Error::Io(e)),
        }
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

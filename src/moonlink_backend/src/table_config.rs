use moonlink::FileSystemConfig;
/// Configuration on table creation.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ObjectStorageConfig {
    /// Either "s3" or "gcs".
    pub backend_type: String,
    /// Secret key id.
    pub key_id: String,
    /// Secret.
    pub secret: String,
    /// Bucket name.
    pub bucket: String,
    /// Project where the bucket belongs to.
    pub project: Option<String>,
    /// Endpoint for the object storage.
    pub endpoint: Option<String>,
    /// Region for the object storage.
    pub region: Option<String>,
}

impl ObjectStorageConfig {
    /// Get filesystem config.
    pub(crate) fn get_filesystem_config(&self) -> FileSystemConfig {
        match &self.backend_type.to_lowercase() {
            "s3" => {
                FileSystemConfig::S3 { 
                    access_key_id: self.key_id.clone(), 
                    secret_access_key: self.secret.clone(),
                    region: self.region.as_ref().unwrap().to_string(),
                    bucket: self.bucket.clone(),
                    endpoint: self.endpoint.clone(),
                }
            }
            "gcs" => {
                FileSystemConfig::Gcs { 
                    project: (), 
                    region: (), 
                    bucket: (), 
                    access_key_id: (), 
                    secret_access_key: (), 
                    endpoint: (), 
                    disable_auth: (),
                }
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FsConfig {
    /// Local filesystem directory.
    pub directory: String,
}

/// Storage backend configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum IcebergStorageConfig {
    ObjectStorageConfig(ObjectStorageConfig),
    FsStorageConfig(String),
}

impl IcebergStorageConfig {
    /// Get filesystem config.
    pub(crate) fn get_filesystem_config(&self) -> FileSystemConfig {
        match &self {
            IcebergStorageConfig::FsStorageConfig(directory) => {
                FileSystemConfig::FileSystem {
                    root_directory: directory.clone(),
                }
            }
            IcebergStorageConfig::ObjectStorageConfig(obj_store_config) => {

            }
        }
    }
}

/// Mooncake table config.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TableConfig {

}

/// Mooncake table configuration specified at creation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TableCreationConfig {
    /// Object storage configuration, use local filesystem if unassigned.
    pub storage_config: IcebergStorageConfig,
    /// Mooncake table configuration.
    pub table_config: TableConfig,
}

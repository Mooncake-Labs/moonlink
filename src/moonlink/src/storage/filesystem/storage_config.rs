#[cfg(any(feature = "storage-gcs", feature = "storage-s3"))]
use crate::MoonlinkSecretType;
use crate::MoonlinkTableSecret;
use serde::{Deserialize, Serialize};

/// StorageConfig contains configuration for multiple storage backends.
#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub enum StorageConfig {
    #[cfg(feature = "storage-fs")]
    FileSystem { root_directory: String },
    #[cfg(feature = "storage-s3")]
    S3 {
        access_key_id: String,
        secret_access_key: String,
        region: String,
        bucket: String,
        endpoint: Option<String>,
    },
    #[cfg(feature = "storage-gcs")]
    Gcs {
        /// GCS project.
        project: String,
        /// GCS bucket region.
        region: String,
        /// GCS bucket.
        bucket: String,
        /// HMAC key and secret.
        access_key_id: String,
        secret_access_key: String,
        /// Used for fake GCS server.
        endpoint: Option<String>,
        /// Used for fake GCS server.
        disable_auth: bool,
    },
}

impl std::fmt::Debug for StorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "storage-fs")]
            StorageConfig::FileSystem { root_directory } => f
                .debug_struct("FileSystem")
                .field("root_directory", root_directory)
                .finish(),

            #[cfg(feature = "storage-s3")]
            StorageConfig::S3 {
                region,
                bucket,
                endpoint,
                access_key_id: _,
                secret_access_key: _,
            } => f
                .debug_struct("S3")
                .field("region", region)
                .field("bucket", bucket)
                .field("endpoint", endpoint)
                .field("access key id", &"xxxxx")
                .field("secret access key", &"xxxxx")
                .finish(),

            #[cfg(feature = "storage-gcs")]
            StorageConfig::Gcs {
                project,
                region,
                bucket,
                endpoint,
                disable_auth,
                access_key_id: _,
                secret_access_key: _,
            } => f
                .debug_struct("Gcs")
                .field("project", project)
                .field("region", region)
                .field("bucket", bucket)
                .field("endpoint", endpoint)
                .field("disable_auth", disable_auth)
                .field("access key id", &"xxxxx")
                .field("secret access key", &"xxxxx")
                .finish(),
        }
    }
}

impl StorageConfig {
    /// Get root path for the given filesystem config.
    pub fn get_root_path(&self) -> String {
        match &self {
            #[cfg(feature = "storage-fs")]
            StorageConfig::FileSystem { root_directory } => root_directory.to_string(),
            #[cfg(feature = "storage-gcs")]
            StorageConfig::Gcs { bucket, .. } => format!("gs://{bucket}"),
            #[cfg(feature = "storage-s3")]
            StorageConfig::S3 { bucket, .. } => format!("s3://{bucket}"),
        }
    }

    /// Extract security metadata entry from current filesystem config.
    pub fn extract_security_metadata_entry(&self) -> Option<MoonlinkTableSecret> {
        match &self {
            #[cfg(feature = "storage-fs")]
            StorageConfig::FileSystem { .. } => None,
            #[cfg(feature = "storage-gcs")]
            StorageConfig::Gcs {
                project,
                region,
                access_key_id,
                secret_access_key,
                endpoint,
                ..
            } => Some(MoonlinkTableSecret {
                secret_type: MoonlinkSecretType::Gcs,
                key_id: access_key_id.to_string(),
                secret: secret_access_key.to_string(),
                project: Some(project.to_string()),
                endpoint: endpoint.clone(),
                region: Some(region.to_string()),
            }),
            #[cfg(feature = "storage-s3")]
            StorageConfig::S3 {
                access_key_id,
                secret_access_key,
                region,
                endpoint,
                ..
            } => Some(MoonlinkTableSecret {
                secret_type: MoonlinkSecretType::S3,
                key_id: access_key_id.to_string(),
                secret: secret_access_key.to_string(),
                project: None,
                endpoint: endpoint.clone(),
                region: Some(region.clone()),
            }),
        }
    }
}

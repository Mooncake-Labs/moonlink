#[cfg(feature = "storage-s3")]
use crate::MoonlinkSecretType;
use crate::MoonlinkTableSecret;
use serde::{Deserialize, Serialize};

/// StorageConfig contains configuration for multiple storage backends.
#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub enum StorageConfig {
    #[cfg(feature = "storage-fs")]
    #[serde(rename = "fs")]
    FileSystem {
        root_directory: String,
        // Used for atomic write operation: write files to a temporary directory and rename.
        //
        // Caveat:
        // - Not every filesystem provides atomic [`rename`] semantics;
        // - Rename doesn't work across different devices.
        atomic_write_dir: Option<String>,
    },
    #[cfg(feature = "storage-s3")]
    #[serde(rename = "s3")]
    S3 {
        access_key_id: String,
        secret_access_key: String,
        region: String,
        bucket: String,
        #[serde(default)]
        endpoint: Option<String>,
    },
}

impl std::fmt::Debug for StorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "storage-fs")]
            StorageConfig::FileSystem {
                root_directory,
                atomic_write_dir,
            } => f
                .debug_struct("FileSystem")
                .field("root_directory", root_directory)
                .field("atomic_write_dir", atomic_write_dir)
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
        }
    }
}

impl StorageConfig {
    /// Get root path for the given filesystem config.
    pub fn get_root_path(&self) -> String {
        match &self {
            #[cfg(feature = "storage-fs")]
            StorageConfig::FileSystem { root_directory, .. } => root_directory.to_string(),
            #[cfg(feature = "storage-s3")]
            StorageConfig::S3 { bucket, .. } => format!("s3://{bucket}"),
        }
    }

    /// Get region for object storage config.
    pub fn get_region(&self) -> Option<String> {
        match &self {
            #[cfg(feature = "storage-fs")]
            StorageConfig::FileSystem { .. } => None,
            #[cfg(feature = "storage-s3")]
            StorageConfig::S3 { region, .. } => Some(region.clone()),
        }
    }

    /// Get access key id.
    pub fn get_access_key_id(&self) -> Option<String> {
        match &self {
            #[cfg(feature = "storage-fs")]
            StorageConfig::FileSystem { .. } => None,
            #[cfg(feature = "storage-s3")]
            StorageConfig::S3 { access_key_id, .. } => Some(access_key_id.clone()),
        }
    }

    /// Get secret access key.
    pub fn get_secret_access_key(&self) -> Option<String> {
        match &self {
            #[cfg(feature = "storage-fs")]
            StorageConfig::FileSystem { .. } => None,
            #[cfg(feature = "storage-s3")]
            StorageConfig::S3 {
                secret_access_key, ..
            } => Some(secret_access_key.clone()),
        }
    }

    /// Extract security metadata entry from current filesystem config.
    pub fn extract_security_metadata_entry(&self) -> Option<MoonlinkTableSecret> {
        match &self {
            #[cfg(feature = "storage-fs")]
            StorageConfig::FileSystem { .. } => None,
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

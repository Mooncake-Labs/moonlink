use crate::FileSystemConfig;

/// Get root path for the given filesystem config.
pub fn get_root_path(filesystem_config: &FileSystemConfig) -> String {
    match &filesystem_config {
        #[cfg(feature = "storage-fs")]
        FileSystemConfig::FileSystem { root_directory } => root_directory.to_string(),
        #[cfg(feature = "storage-gcs")]
        FileSystemConfig::Gcs { bucket, .. } => format!("gs://{bucket}"),
        #[cfg(feature = "storage-s3")]
        FileSystemConfig::S3 { bucket, .. } => format!("s3://{bucket}"),
    }
}

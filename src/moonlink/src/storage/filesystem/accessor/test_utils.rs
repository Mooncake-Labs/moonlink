use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::filesystem::accessor_config::AccessorConfig;

use rand::Rng;
use tokio::io::AsyncWriteExt;

/// Test util function to generate random string with the requested size.
pub(crate) fn create_random_string(size: usize) -> String {
    const ALLOWED_CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    let random_string: String = (0..size)
        .map(|_| {
            let idx = rng.random_range(0..ALLOWED_CHARS.len());
            ALLOWED_CHARS[idx] as char
        })
        .collect();
    random_string
}

/// Test util function to create local file with random content of given [`file_size`] to the destunation file (indicated by absolute path).
pub(crate) async fn create_local_file(filepath: &str, file_size: usize) -> String {
    let content = create_random_string(file_size);
    let mut file = tokio::fs::File::create(filepath).await.unwrap();

    let mut written = 0;
    let bytes = content.as_bytes();
    while written < bytes.len() {
        let n = file.write(&bytes[written..]).await.unwrap();
        written += n;
    }

    file.flush().await.unwrap();
    content
}

/// Test util function to create a remote file with random content of given [`file_size`], and write it to the destination file (indicated by absolute path).
pub(crate) async fn create_remote_file(
    filepath: &str,
    accessor_config: AccessorConfig,
    file_size: usize,
) -> String {
    let content = create_random_string(file_size);
    let accessor = FileSystemAccessor::new(accessor_config);
    accessor
        .write_object(filepath, content.as_bytes().to_vec())
        .await
        .unwrap();
    content
}

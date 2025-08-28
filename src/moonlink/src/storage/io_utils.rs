use crate::Result;

/// Util function to delete local file in parallel.
/// If the file doesn't exists on local filesystem, ignore and proceed.
pub(crate) async fn delete_local_files(local_files: &[String]) -> Result<()> {
    let delete_futures = local_files.iter().map(|file_path| async move {
        if tokio::fs::try_exists(file_path).await? {
            tokio::fs::remove_file(file_path).await?;
        }
        Ok::<(), std::io::Error>(())
    });
    let delete_results = futures::future::join_all(delete_futures).await;
    for cur_res in delete_results.into_iter() {
        cur_res?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_delete_local_files() {
        // Create two temp files.
        let temp_dir = tempfile::tempdir().unwrap();
        let file1 = temp_dir.path().join("test_tokio_file_1.tmp");
        let file2 = temp_dir.path().join("test_tokio_file_2.tmp");
        tokio::fs::File::create(&file1).await.unwrap();
        tokio::fs::File::create(&file2).await.unwrap();

        // Confirm files exist.
        assert!(tokio::fs::try_exists(&file1).await.unwrap());
        assert!(tokio::fs::try_exists(&file2).await.unwrap());

        // Delete the files.
        let paths = vec![
            file1.to_string_lossy().to_string(),
            file2.to_string_lossy().to_string(),
        ];
        delete_local_files(&paths).await.unwrap();

        // Confirm files are deleted.
        assert!(!tokio::fs::try_exists(&file1).await.unwrap());
        assert!(!tokio::fs::try_exists(&file2).await.unwrap());
    }

    /// Testing scenario: deleting remote files won't have any effect.
    #[tokio::test]
    async fn test_delete_remote_files() {
        let paths = vec!["s3://bucket/object".to_string()];
        delete_local_files(&paths).await.unwrap();
    }
}

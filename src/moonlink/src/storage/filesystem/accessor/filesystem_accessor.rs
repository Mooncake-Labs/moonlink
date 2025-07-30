use async_stream::try_stream;
use async_trait::async_trait;
#[cfg(feature = "storage-gcs")]
use futures::TryStreamExt;
use futures::{Stream, StreamExt};
use opendal::options::WriteOptions;
use opendal::Metadata;
use opendal::Operator;
#[cfg(test)]
use tempfile::TempDir;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
/// FileSystemAccessor built upon opendal.
use tokio::sync::OnceCell;

use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::base_unbuffered_stream_writer::BaseUnbufferedStreamWriter;
use crate::storage::filesystem::accessor::metadata::ObjectMetadata;
use crate::storage::filesystem::accessor::operator_utils;
use crate::storage::filesystem::accessor::unbuffered_stream_writer::UnbufferedStreamWriter;
use crate::storage::filesystem::accessor_config::AccessorConfig;
use crate::Result;

use std::pin::Pin;

/// IO block size for parallel read and write.
const IO_BLOCK_SIZE: usize = 2 * 1024 * 1024;
/// Max number of ongoing parallel sub IO operations for one single upload and download operation.
const MAX_SUB_IO_OPERATION: usize = 8;

// TODO(hjiang): Add stats cache for exists, get file size, etc invocation.
pub struct FileSystemAccessor {
    /// Root path.
    root_path: String,
    /// Operator to manager all IO operations.
    operator: OnceCell<Operator>,
    /// Accessor configuration.
    config: AccessorConfig,
}

impl std::fmt::Debug for FileSystemAccessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileSystemAccessor")
            .field("root_path", &self.root_path)
            .field("config", &self.config)
            .finish()
    }
}

impl FileSystemAccessor {
    pub fn new(config: AccessorConfig) -> Self {
        Self {
            root_path: config.get_root_path(),
            operator: OnceCell::new(),
            config,
        }
    }

    #[cfg(test)]
    pub fn default_for_test(temp_dir: &TempDir) -> std::sync::Arc<dyn BaseFileSystemAccess> {
        use crate::storage::filesystem::accessor::factory::create_filesystem_accessor;

        let storage_config = crate::StorageConfig::FileSystem {
            root_directory: temp_dir.path().to_str().unwrap().to_string(),
        };
        let accessor_config = AccessorConfig::new_with_storage_config(storage_config);
        create_filesystem_accessor(accessor_config)
    }

    /// Sanitize given path.
    /// Opendal works on relative path, so attempt to sanitize absolute path to relative one if applicable.
    fn sanitize_path<'a>(&self, path: &'a str) -> &'a str {
        path.strip_prefix(&self.root_path).unwrap_or(path)
    }

    /// Get IO operator from the catalog.
    async fn get_operator(&self) -> Result<&Operator> {
        self.operator
            .get_or_try_init(|| async { operator_utils::create_opendal_operator(&self.config) })
            .await
    }

    /// Util function to get local file size.
    async fn get_local_file_size(path: &str) -> Result<u64> {
        let file = tokio::fs::File::open(path).await?;
        let metadata = file.metadata().await?;
        let file_size = metadata.len();
        Ok(file_size)
    }

    /// Upload a small file from [`src`] to [`dst`].
    /// Precondition: both paths have been sanitized.
    async fn upload_small_file(
        &self,
        src: &str,
        dst: &str,
        file_size: u64,
    ) -> Result<ObjectMetadata> {
        let content = tokio::fs::read(src).await?;
        self.write_object(dst, content).await?;
        Ok(ObjectMetadata { size: file_size })
    }

    /// Download a small file from [`src`] to [`dst`].
    /// Precondition: both paths have been sanitized.
    async fn download_small_file(
        &self,
        src: &str,
        dst: &str,
        file_size: u64,
    ) -> Result<ObjectMetadata> {
        let content = self.read_object(src).await?;
        let mut dst_file = tokio::fs::File::create(dst).await?;
        dst_file.write_all(&content).await?;
        dst_file.flush().await?;
        Ok(ObjectMetadata { size: file_size })
    }
}

#[async_trait]
impl BaseFileSystemAccess for FileSystemAccessor {
    /// ===============================
    /// Directory operations
    /// ===============================
    ///
    async fn list_direct_subdirectories(&self, folder: &str) -> Result<Vec<String>> {
        let sanitized_folder = self.sanitize_path(folder);
        let prefix = format!("{sanitized_folder}/");
        let mut dirs = Vec::new();
        let lister = self.get_operator().await?.list(&prefix).await?;

        let entries = lister;
        for cur_entry in entries.iter() {
            // Both directories and objects will be returned, here we only care about sub-directories.
            if !cur_entry.path().ends_with('/') {
                continue;
            }
            let dir_name = cur_entry
                .path()
                .trim_start_matches(&prefix)
                .trim_end_matches('/')
                .to_string();
            if !dir_name.is_empty() {
                dirs.push(dir_name);
            }
        }

        Ok(dirs)
    }

    /// TODO(hjiang): Check whether we could unify the implementation with [`remove_directory`].
    #[cfg(feature = "storage-gcs")]
    async fn remove_directory(&self, directory: &str) -> Result<()> {
        let path = if directory.ends_with('/') {
            directory.to_string()
        } else {
            format!("{directory}/")
        };
        let sanitized_path = self.sanitize_path(&path);

        let operator = self.get_operator().await?;
        let mut lister = operator.lister(sanitized_path).await?;
        let mut entries = Vec::new();

        while let Some(entry) = lister.try_next().await? {
            // List operation returns target path.
            if entry.path() != sanitized_path {
                entries.push(entry.path().to_string());
            }
        }
        for entry_path in entries {
            if entry_path.ends_with('/') {
                Box::pin(self.remove_directory(&entry_path)).await?;
            } else {
                operator.delete(&entry_path).await?;
            }
        }

        if !sanitized_path.is_empty() && sanitized_path != "/" {
            operator.remove_all(sanitized_path).await?;
        }

        Ok(())
    }

    #[cfg(not(feature = "storage-gcs"))]
    async fn remove_directory(&self, directory: &str) -> Result<()> {
        let sanitized_directory = self.sanitize_path(directory);
        let op = self.get_operator().await?.clone();
        op.remove_all(sanitized_directory).await?;
        Ok(())
    }

    /// ===============================
    /// Object operations
    /// ===============================
    ///
    async fn object_exists(&self, object: &str) -> Result<bool> {
        let sanitized_object = self.sanitize_path(object);
        match self.get_operator().await?.stat(sanitized_object).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn stats_object(&self, object: &str) -> Result<opendal::Metadata> {
        let sanitized_object = self.sanitize_path(object);
        match self.get_operator().await?.stat(sanitized_object).await {
            Ok(metadata) => Ok(metadata),
            Err(e) => Err(e.into()),
        }
    }

    async fn read_object(&self, object: &str) -> Result<Vec<u8>> {
        let sanitized_object = self.sanitize_path(object);
        let content = self.get_operator().await?.read(sanitized_object).await?;
        Ok(content.to_vec())
    }
    async fn read_object_as_string(&self, object: &str) -> Result<String> {
        let bytes = self.read_object(object).await?;
        Ok(String::from_utf8(bytes)?)
    }

    async fn stream_read(
        &self,
        object_filepath: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<u8>>> + Send>>> {
        let sanitized_object = self.sanitize_path(object_filepath).to_string();
        let operator = self.get_operator().await?.clone();
        let stream = try_stream! {
            let meta = operator.stat(&sanitized_object).await?;
            let file_size = meta.content_length();
            let mut ranges = Vec::new();
            let mut start = 0;
            while start < file_size {
                let end = (start + IO_BLOCK_SIZE as u64).min(file_size);
                ranges.push((start, end));
                start = end;
            }

            let tasks = futures::stream::iter(ranges.into_iter().map(move |(start, end)| {
                let op = operator.clone();
                let path = sanitized_object.clone();
                async move {
                    let reader = op.reader(&path).await?;
                    let buf = reader.read(start..end).await?;
                    Ok::<_, opendal::Error>(buf)
                }
            }))
            .buffered(MAX_SUB_IO_OPERATION);

            // You may collect and sort if you want ordered output, or yield immediately:
            futures::pin_mut!(tasks);
            while let Some(res) = tasks.next().await {
                let buffer = res?;
                for chunk in buffer.into_iter() {
                    yield chunk.to_vec();
                }
            }
        };

        Ok(Box::pin(stream))
    }

    async fn write_object(&self, object: &str, content: Vec<u8>) -> Result<Metadata> {
        let sanitized_object = self.sanitize_path(object);
        let operator = self.get_operator().await?;
        let expected_len = content.len();
        let metadata = operator.write(sanitized_object, content).await?;
        assert_eq!(metadata.content_length(), expected_len as u64);
        Ok(metadata)
    }

    async fn conditional_write_object(
        &self,
        object: &str,
        content: Vec<u8>,
        etag: Option<String>,
    ) -> Result<opendal::Metadata> {
        let sanitized_object = self.sanitize_path(object);
        let operator = self.get_operator().await?;
        let expected_len = content.len();

        // Conditional write is not supported for all storage backends, if not supported, fallback to [`write_object`].
        if !operator.info().full_capability().write_with_if_match {
            return self.write_object(object, content).await;
        }

        let mut write_option = WriteOptions::default();
        if let Some(etag) = etag {
            write_option.if_match = Some(etag);
        } else {
            write_option.if_not_exists = true;
        }
        let metadata = operator
            .write_options(sanitized_object, content, write_option)
            .await?;
        assert_eq!(metadata.content_length(), expected_len as u64);
        Ok(metadata)
    }

    async fn create_unbuffered_stream_writer(
        &self,
        object_filepath: &str,
    ) -> Result<Box<dyn BaseUnbufferedStreamWriter>> {
        let sanitized_object = self.sanitize_path(object_filepath);
        let operator = self.get_operator().await?;
        Ok(Box::new(UnbufferedStreamWriter::new(
            operator.clone(),
            sanitized_object.to_string(),
        )?))
    }

    async fn delete_object(&self, object: &str) -> Result<()> {
        let sanitized_object = self.sanitize_path(object);
        let operator = self.get_operator().await?;
        operator.delete(sanitized_object).await?;
        Ok(())
    }

    async fn copy_from_local_to_remote(&self, src: &str, dst: &str) -> Result<ObjectMetadata> {
        // For small files, no need to parallelize IO operations.
        let sanitized_dst = self.sanitize_path(dst);
        let file_size = Self::get_local_file_size(src).await?;
        if file_size <= IO_BLOCK_SIZE as u64 {
            let metadata = self
                .upload_small_file(src, sanitized_dst, file_size)
                .await?;
            return Ok(metadata);
        }

        // Handle large objects.
        let operator = self.get_operator().await?;
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(MAX_SUB_IO_OPERATION);

        // Spawn reader task in blocks and place into queue.
        let src_path = src.to_string();
        let reader_task_handle = tokio::spawn(async move {
            let mut file = tokio::fs::File::open(&src_path).await?;
            #[allow(clippy::uninit_vec)]
            let mut buffer: Vec<u8> = {
                let mut buf = Vec::with_capacity(IO_BLOCK_SIZE);
                unsafe {
                    buf.set_len(IO_BLOCK_SIZE);
                }
                buf
            };
            loop {
                let n = file.read(&mut buffer).await?;
                if n == 0 {
                    break;
                }
                tx.send(buffer[..n].to_vec()).await.unwrap();
            }
            Ok::<(), std::io::Error>(())
        });

        // Write main task.
        let mut writer = operator.writer(sanitized_dst).await?;
        let mut total_size = 0u64;
        while let Some(cur_chunk) = rx.recv().await {
            let cur_byte_len = cur_chunk.len();
            writer.write(cur_chunk).await?;
            total_size += cur_byte_len as u64;
        }
        writer.close().await?;

        // Wait for reader task to finish.
        reader_task_handle.await??;

        Ok(ObjectMetadata { size: total_size })
    }

    async fn copy_from_remote_to_local(&self, src: &str, dst: &str) -> Result<ObjectMetadata> {
        let sanitized_src = self.sanitize_path(src).to_string();
        let file_size = self.stats_object(&sanitized_src).await?.content_length();

        // For small objects, no need to parallelize IO operation and pre-allocate buffer.
        if file_size <= IO_BLOCK_SIZE as u64 {
            let metadata = self
                .download_small_file(&sanitized_src, dst, file_size)
                .await?;
            return Ok(metadata);
        }

        // Handle large objects.
        let operator = self.get_operator().await?.clone();
        let (tx, mut rx) = mpsc::channel(MAX_SUB_IO_OPERATION);

        // Spawn the reader task.
        let reader_handle = tokio::task::spawn(async move {
            let reader = operator
                .reader(&sanitized_src)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let mut start_offset = 0;
            while start_offset < file_size {
                let end = (start_offset + IO_BLOCK_SIZE as u64).min(file_size);
                let range = start_offset..end;
                let buf = reader
                    .read(range)
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                tx.send(buf).await.unwrap();
                start_offset = end;
            }

            Ok::<(), std::io::Error>(())
        });

        // Create and write to the destination file
        let mut file = tokio::fs::File::create(dst).await?;
        let mut total_size = 0u64;

        while let Some(cur_chunk) = rx.recv().await {
            let cur_chunk_len = cur_chunk.len();
            for cur_bytes in cur_chunk.into_iter() {
                file.write_all(&cur_bytes).await?;
            }
            total_size += cur_chunk_len as u64;
        }
        file.flush().await?;

        // Wait for the reader to finish.
        reader_handle.await??;

        Ok(ObjectMetadata { size: total_size })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::filesystem::accessor::factory::create_filesystem_accessor;
    use crate::storage::filesystem::accessor::test_utils::*;
    use crate::storage::filesystem::storage_config::StorageConfig;
    use crate::storage::filesystem::test_utils::writer_test_utils::test_unbuffered_stream_writer_impl;
    use rstest::rstest;

    #[tokio::test]
    async fn test_stats_object() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root_directory = temp_dir.path().to_str().unwrap().to_string();
        let storage_config = StorageConfig::FileSystem {
            root_directory: root_directory.clone(),
        };
        let filesystem_accessor = create_filesystem_accessor(
            AccessorConfig::new_with_storage_config(storage_config.clone()),
        );

        const DST_FILENAME: &str = "target";
        const TARGET_FILESIZE: usize = 10;

        // Write object.
        let random_content = create_random_string(TARGET_FILESIZE);
        filesystem_accessor
            .write_object(DST_FILENAME, random_content.as_bytes().to_vec())
            .await
            .unwrap();

        // Stats object.
        let metadata = filesystem_accessor
            .stats_object(DST_FILENAME)
            .await
            .unwrap();
        assert_eq!(metadata.content_length(), TARGET_FILESIZE as u64);
    }

    // Local filesystem doesn't support conditional write, which should behave the same as [`write_object`].
    #[tokio::test]
    async fn test_conditional_write() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root_directory = temp_dir.path().to_str().unwrap().to_string();
        let storage_config = StorageConfig::FileSystem {
            root_directory: root_directory.clone(),
        };
        let filesystem_accessor = create_filesystem_accessor(
            AccessorConfig::new_with_storage_config(storage_config.clone()),
        );

        const DST_FILENAME: &str = "target";
        const TARGET_FILESIZE: usize = 10;
        const ETAG: &str = "etag";

        // Write object conditionally, with destination file doesn't exist.
        let random_content = create_random_string(TARGET_FILESIZE);
        filesystem_accessor
            .conditional_write_object(
                DST_FILENAME,
                random_content.as_bytes().to_vec(),
                /*etag=*/ None,
            )
            .await
            .unwrap();
        // Read object and check.
        let actual_content = filesystem_accessor.read_object(DST_FILENAME).await.unwrap();
        assert_eq!(actual_content, random_content.as_bytes().to_vec());

        // Write object conditionally, with destination file already exists; for local filesystem the semantics is overwrite without check.
        let random_content = create_random_string(TARGET_FILESIZE);
        filesystem_accessor
            .conditional_write_object(
                DST_FILENAME,
                random_content.as_bytes().to_vec(),
                /*etag=*/ Some(ETAG.to_string()),
            )
            .await
            .unwrap();
        // Read object and check.
        let actual_content = filesystem_accessor.read_object(DST_FILENAME).await.unwrap();
        assert_eq!(actual_content, random_content.as_bytes().to_vec());
    }

    #[tokio::test]
    #[rstest]
    #[case(10)]
    #[case(18 * 1024 * 1024)]
    async fn test_stream_read(#[case] file_size: usize) {
        let temp_dir = tempfile::tempdir().unwrap();
        let root_directory = temp_dir.path().to_str().unwrap().to_string();
        let storage_config = StorageConfig::FileSystem {
            root_directory: root_directory.clone(),
        };
        let accessor_config = AccessorConfig::new_with_storage_config(storage_config.clone());
        let filesystem_accessor = create_filesystem_accessor(accessor_config.clone());

        // Prepare src file.
        let remote_filepath = format!("{}/remote", &root_directory);
        let expected_content =
            create_remote_file(&remote_filepath, accessor_config, file_size).await;

        // Stream read from destination path.
        let mut actual_content = vec![];
        let mut read_stream = filesystem_accessor
            .stream_read(&remote_filepath)
            .await
            .unwrap();
        while let Some(chunk) = read_stream.next().await {
            let data = chunk.unwrap();
            actual_content.extend_from_slice(&data);
        }

        // Validate destination file content.
        let actual_content = String::from_utf8(actual_content).unwrap();
        assert_eq!(actual_content.len(), expected_content.len());
        assert_eq!(actual_content, expected_content);
    }

    #[tokio::test]
    #[rstest]
    #[case(10)]
    #[case(18 * 1024 * 1024)]
    async fn test_copy_from_local_to_remote(#[case] file_size: usize) {
        let temp_dir = tempfile::tempdir().unwrap();
        let root_directory = temp_dir.path().to_str().unwrap().to_string();
        let storage_config = StorageConfig::FileSystem {
            root_directory: root_directory.clone(),
        };
        let accessor_config = AccessorConfig::new_with_storage_config(storage_config.clone());
        let filesystem_accessor = create_filesystem_accessor(accessor_config.clone());

        // Prepare src file.
        let src_filepath = format!("{}/src", &root_directory);
        let expected_content = create_local_file(&src_filepath, file_size).await;

        // Copy from src to dst.
        let dst_filepath = format!("{}/dst", &root_directory);
        filesystem_accessor
            .copy_from_local_to_remote(&src_filepath, &dst_filepath)
            .await
            .unwrap();

        // Validate destination file content.
        let actual_content = filesystem_accessor
            .read_object_as_string(&dst_filepath)
            .await
            .unwrap();
        assert_eq!(actual_content.len(), expected_content.len());
        assert_eq!(actual_content, expected_content);
    }

    #[tokio::test]
    #[rstest]
    #[case(10)]
    #[case(18 * 1024 * 1024)]
    async fn test_copy_from_remote_to_local(#[case] file_size: usize) {
        let temp_dir = tempfile::tempdir().unwrap();
        let root_directory = temp_dir.path().to_str().unwrap().to_string();
        let storage_config = StorageConfig::FileSystem {
            root_directory: root_directory.clone(),
        };
        let accessor_config = AccessorConfig::new_with_storage_config(storage_config.clone());
        let filesystem_accessor = create_filesystem_accessor(accessor_config.clone());

        // Prepare src file.
        let src_filepath = format!("{}/src", &root_directory);
        let expected_content = create_remote_file(&src_filepath, accessor_config, file_size).await;

        // Copy from src to dst.
        let dst_filepath = format!("{}/dst", &root_directory);
        filesystem_accessor
            .copy_from_remote_to_local(&src_filepath, &dst_filepath)
            .await
            .unwrap();

        // Validate destination file content.
        let actual_content = filesystem_accessor
            .read_object_as_string(&dst_filepath)
            .await
            .unwrap();
        assert_eq!(actual_content, expected_content);
    }

    #[tokio::test]
    async fn test_unbuffered_stream_write() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root_directory = temp_dir.path().to_str().unwrap().to_string();
        let storage_config = StorageConfig::FileSystem {
            root_directory: root_directory.clone(),
        };
        let accessor_config = AccessorConfig::new_with_storage_config(storage_config.clone());
        let filesystem_accessor = create_filesystem_accessor(accessor_config.clone());

        let dst_filename = "dst".to_string();
        let dst_filepath = format!("{}/{}", &root_directory, dst_filename);
        let writer = filesystem_accessor
            .create_unbuffered_stream_writer(&dst_filepath)
            .await
            .unwrap();
        test_unbuffered_stream_writer_impl(writer, dst_filename, accessor_config).await;
    }
}

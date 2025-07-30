/// A customized opendal layer, which provides chaos features like injected delay, intended errors, etc.
use opendal::raw::{
    Access, Layer, LayeredAccess, OpList, OpRead, OpWrite, RpDelete, RpList, RpRead, RpWrite,
};
use opendal::Metadata;
use opendal::Result;

use crate::storage::filesystem::accessor::chaos_generator::ChaosGenerator;
use crate::storage::filesystem::accessor_config::ChaosConfig;

/// A wrapper that delegates all operations to an inner [`FileSystemAccessor`].
#[derive(Debug)]
pub struct ChaosLayer {
    /// Chaos generator.
    chaos_generator: ChaosGenerator,
}

impl ChaosLayer {
    pub fn new(config: ChaosConfig) -> Self {
        Self {
            chaos_generator: ChaosGenerator::new(config),
        }
    }
}

impl<A: Access> Layer<A> for ChaosLayer {
    type LayeredAccess = ChaosAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        ChaosAccessor {
            chaos_generator: self.chaos_generator.clone(),
            inner,
        }
    }
}

#[derive(Debug)]
pub struct ChaosAccessor<A> {
    /// Chaos generator.
    chaos_generator: ChaosGenerator,
    /// Inner accessor.
    inner: A,
}

impl<A: Access> LayeredAccess for ChaosAccessor<A> {
    type Inner = A;
    type Reader = ChaosReader<A::Reader>;
    type Writer = ChaosWriter<A::Writer>;
    type Lister = A::Lister;
    type Deleter = A::Deleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.chaos_generator.perform_wrapper_function().await?;
        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| (rp, ChaosReader::new(r, self.chaos_generator.clone())))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.chaos_generator.perform_wrapper_function().await?;
        self.inner
            .write(path, args)
            .await
            .map(|(rp, w)| (rp, ChaosWriter::new(w, self.chaos_generator.clone())))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.chaos_generator.perform_wrapper_function().await?;
        self.inner.list(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.chaos_generator.perform_wrapper_function().await?;
        self.inner.delete().await
    }
}

/// ==========================
/// Chaos reader
/// ==========================
///
pub struct ChaosReader<R> {
    /// Chaos generator.
    chaos_generator: ChaosGenerator,
    /// Inner reader.
    inner: R,
}

impl<R> ChaosReader<R> {
    fn new(inner: R, chaos_generator: ChaosGenerator) -> Self {
        Self {
            chaos_generator,
            inner,
        }
    }
}

impl<R: opendal::raw::oio::Read> opendal::raw::oio::Read for ChaosReader<R> {
    async fn read(&mut self) -> Result<opendal::Buffer> {
        self.chaos_generator.perform_wrapper_function().await?;
        self.inner.read().await
    }
}

/// ==========================
/// Chaos writer
/// ==========================
///
pub struct ChaosWriter<W> {
    /// Chaos generator.
    chaos_generator: ChaosGenerator,
    /// Inner writer.
    inner: W,
}

impl<W> ChaosWriter<W> {
    fn new(inner: W, chaos_generator: ChaosGenerator) -> Self {
        Self {
            chaos_generator,
            inner,
        }
    }
}

impl<W: opendal::raw::oio::Write> opendal::raw::oio::Write for ChaosWriter<W> {
    async fn write(&mut self, bs: opendal::Buffer) -> Result<()> {
        self.chaos_generator.perform_wrapper_function().await?;
        self.inner.write(bs).await
    }

    async fn abort(&mut self) -> Result<()> {
        self.chaos_generator.perform_wrapper_function().await?;
        self.inner.abort().await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.chaos_generator.perform_wrapper_function().await?;
        self.inner.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
    use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
    use crate::storage::filesystem::accessor_config::AccessorConfig;
    use crate::storage::filesystem::accessor_config::RetryConfig;
    use crate::storage::filesystem::accessor_config::TimeoutConfig;
    use crate::storage::filesystem::storage_config::StorageConfig;
    use tempfile::{tempdir, TempDir};

    /// Test util function to create a filesystem accessor, based on the given chaos option.
    fn create_filesystem_accessor(
        temp_dir: &TempDir,
        chaos_config: ChaosConfig,
        timeout_config: TimeoutConfig,
    ) -> FileSystemAccessor {
        let storage_config = StorageConfig::FileSystem {
            root_directory: temp_dir.path().to_str().unwrap().to_string(),
        };
        let accessor_config = AccessorConfig {
            storage_config,
            chaos_config: Some(chaos_config),
            retry_config: RetryConfig::default(),
            timeout_config,
        };
        FileSystemAccessor::new(accessor_config)
    }

    /// Test util function to write and read an object, which should succeed whether delay injected.
    async fn perform_read_write_op(filesystem_accessor: &FileSystemAccessor) {
        // Write object.
        let filename = "test_object.txt".to_string();
        let content = b"helloworld".to_vec();
        filesystem_accessor
            .write_object(&filename, content.clone())
            .await
            .unwrap();

        // Read object.
        let read_content = filesystem_accessor.read_object(&filename).await.unwrap();
        assert_eq!(read_content, content);
    }

    #[tokio::test]
    async fn test_no_delay_no_error() {
        let temp_dir = tempdir().unwrap();
        let chaos_config = ChaosConfig {
            min_latency: std::time::Duration::ZERO,
            max_latency: std::time::Duration::ZERO,
            err_prob: 0,
        };
        let filesystem_accessor =
            create_filesystem_accessor(&temp_dir, chaos_config, TimeoutConfig::default());
        perform_read_write_op(&filesystem_accessor).await;
    }

    #[tokio::test]
    async fn test_delay_injected() {
        let temp_dir = tempdir().unwrap();
        let chaos_config = ChaosConfig {
            min_latency: std::time::Duration::from_millis(5000),
            max_latency: std::time::Duration::from_millis(5000),
            err_prob: 0,
        };
        let timeout_config = TimeoutConfig {
            timeout: std::time::Duration::from_millis(500),
        };
        // Timeout is less than injected delay.
        let filesystem_accessor =
            create_filesystem_accessor(&temp_dir, chaos_config, timeout_config);
        let res = filesystem_accessor.read_object("FAKE_FILEPATH").await;
        assert!(res.is_err());
    }
}

#[cfg(test)]
use tempfile::TempDir;

/// Configuration for object storage cache.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectStorageCacheConfig {
    /// Max number of bytes for cache entries at local filesystem.
    pub max_bytes: u64,
    /// Directory to store local cache files.
    pub cache_directory: String,
    // Option to optimize cases where persistent table also sits at local filesystem cases, so only one copy will be stored.
    pub optimize_local_filesystem: bool,
}

impl ObjectStorageCacheConfig {
    pub fn new(max_bytes: u64, cache_directory: String, optimize_local_filesystem: bool) -> Self {
        Self {
            max_bytes,
            cache_directory,
            optimize_local_filesystem,
        }
    }

    /// Provide a default option for ease of testing.
    /// It requires to take a testcase-unique temporary directory.
    #[cfg(test)]
    pub fn default_for_test(temp_dir: &TempDir) -> Self {
        const DEFAULT_MAX_BYTES_FOR_TEST: u64 = 1 << 30; // 1GiB
        Self {
            max_bytes: DEFAULT_MAX_BYTES_FOR_TEST,
            cache_directory: temp_dir.path().to_str().unwrap().to_string(),
            // By default disable local filesystem optimization, to mimic production use case where there's remote storage.
            optimize_local_filesystem: false,
        }
    }

    /// Provide a default option for ease of benchmark.
    #[cfg(feature = "bench")]
    pub fn default_for_bench() -> Self {
        const DEFAULT_MAX_BYTES_FOR_TEST: u64 = 1 << 30; // 1GiB
        const DEFAULT_CACHE_DIRECTORY: &str = "/tmp/moonlink_test_bench";

        // Re-create default cache directory for testing.
        match std::fs::remove_dir_all(DEFAULT_CACHE_DIRECTORY) {
            Ok(()) => {}
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    panic!("Failed to remove directory: {e:?}");
                }
            }
        }
        std::fs::create_dir_all(DEFAULT_CACHE_DIRECTORY).unwrap();

        Self {
            max_bytes: DEFAULT_MAX_BYTES_FOR_TEST,
            cache_directory: DEFAULT_CACHE_DIRECTORY.to_string(),
            optimize_local_filesystem: true,
        }
    }
}

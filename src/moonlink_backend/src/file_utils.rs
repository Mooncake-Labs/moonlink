use crate::error::Result;
use moonlink::{ObjectStorageCache, ObjectStorageCacheConfig};

use std::io::ErrorKind;

/// Default local filesystem directory under the above base directory (which defaults to `PGDATA/pg_mooncake`) where all temporary files (used for union read) will be stored under.
/// The whole directory is cleaned up at moonlink backend start, to prevent file leak.
pub const DEFAULT_MOONLINK_TEMP_FILE_PATH: &str = "temp/";
/// Default object storage read-through cache directory under the above mooncake directory (which defaults to `PGDATA/pg_mooncake`).
/// The whole directory is cleaned up at moonlink backend start, to prevent file leak.
pub const DEFAULT_MOONLINK_OBJECT_STORAGE_CACHE_PATH: &str = "read_through_cache/";
/// Min left disk space for on-disk cache of the filesystem which cache directory is mounted on.
const MIN_DISK_SPACE_FOR_CACHE: u64 = 1 << 30; // 1GiB

/// Get directory path under the given base directory.
fn get_directory_under_base(base: &str, subdir: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(base).join(subdir)
}
/// Get temporary directory under base path.
/// [`base_path`] is expected to be the canonicalized path.
pub(super) fn get_temp_file_directory_under_base(base_path: &str) -> std::path::PathBuf {
    get_directory_under_base(base_path, DEFAULT_MOONLINK_TEMP_FILE_PATH)
}
/// Get cache directory under base path.
/// [`base_path`] is expected to be the canonicalized path.
pub(super) fn get_cache_directory_under_base(base_path: &str) -> std::path::PathBuf {
    get_directory_under_base(base_path, DEFAULT_MOONLINK_OBJECT_STORAGE_CACHE_PATH)
}

/// Smallest read-cache budget used when the filesystem is (nearly) full.
const MIN_CACHE_BYTES: u64 = 64 << 20; // 64MiB

/// Bytes available to unprivileged users on the filesystem holding `path`, or None if unknown.
// statvfs field widths differ across platforms, so the casts are not always no-ops.
#[allow(clippy::unnecessary_cast)]
fn get_available_bytes(path: &str) -> Option<u64> {
    match nix::sys::statvfs::statvfs(path) {
        Ok(vfs_stat) => Some(
            (vfs_stat.fragment_size() as u64).saturating_mul(vfs_stat.blocks_available() as u64),
        ),
        Err(e) => {
            tracing::warn!(path, error = %e, "statvfs failed for read cache directory");
            None
        }
    }
}

/// Read-cache budget for the given available bytes. Never fails: a full disk only shrinks the
/// cache, it must not stop the service from starting.
pub(super) fn cache_bytes_for_available(available_bytes: Option<u64>) -> u64 {
    match available_bytes {
        Some(available) if available >= MIN_DISK_SPACE_FOR_CACHE + MIN_CACHE_BYTES => {
            available - MIN_DISK_SPACE_FOR_CACHE
        }
        Some(available) => {
            let budget = std::cmp::max(available / 2, MIN_CACHE_BYTES);
            tracing::warn!(
                available,
                budget,
                "low disk space for the read cache, using a small cache"
            );
            budget
        }
        None => MIN_CACHE_BYTES,
    }
}

/// Create default object storage cache.
/// Precondition: cache directory has been created beforehand.
pub(super) fn create_default_object_storage_cache(
    cache_directory_pathbuf: std::path::PathBuf,
) -> Result<ObjectStorageCache> {
    let cache_directory = cache_directory_pathbuf.to_str().unwrap().to_string();
    let max_bytes = cache_bytes_for_available(get_available_bytes(&cache_directory));
    let cache_config = ObjectStorageCacheConfig {
        max_bytes,
        cache_directory,
        optimize_local_filesystem: true,
    };
    Ok(ObjectStorageCache::new(cache_config))
}

/// Util function to delete and re-create the given directory.
pub fn recreate_directory(dir: &str) -> Result<()> {
    // Clean up directory to place moonlink temporary files.
    match std::fs::remove_dir_all(dir) {
        Ok(()) => {}
        Err(e) => {
            if e.kind() != ErrorKind::NotFound {
                return Err(e.into());
            }
        }
    }
    std::fs::create_dir_all(dir)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::TempDir;

    #[test]
    fn test_recreate_directory() {
        let tmp = TempDir::new().unwrap();
        let file = tmp.path().join("tmp.txt");
        std::fs::write(&file, b"x").unwrap();
        assert!(file.exists());

        // idempotent "wipe" of an existing dir
        recreate_directory(tmp.path().to_str().unwrap()).unwrap();
        assert!(!file.exists());

        // creation of a brand-new path
        let inner = tmp.path().join("sub");
        recreate_directory(inner.to_str().unwrap()).unwrap();
        assert!(inner.exists());
    }

    #[test]
    fn test_get_directory_under_base() {
        const SUBDIR: &str = "subdir";

        // Root directory as base path.
        let base = "/";
        let newdir = get_directory_under_base(base, SUBDIR);
        assert_eq!(newdir.to_str().unwrap(), format!("/{SUBDIR}"));

        // Non-root directory as base path.
        let base = "/tmp";
        let newdir = get_directory_under_base(base, SUBDIR);
        assert_eq!(newdir.to_str().unwrap(), format!("/tmp/{SUBDIR}"));
    }
}

#[cfg(test)]
mod cache_size_tests {
    use super::*;

    #[test]
    fn test_cache_bytes_ignore_inode_counts_and_never_fail() {
        // Plenty of space: everything but the reserve.
        assert_eq!(
            cache_bytes_for_available(Some(10 << 30)),
            (10 << 30) - MIN_DISK_SPACE_FOR_CACHE
        );
        // Low space (or a filesystem reporting nothing useful): small cache, no error.
        assert_eq!(cache_bytes_for_available(Some(0)), MIN_CACHE_BYTES);
        assert_eq!(cache_bytes_for_available(Some(512 << 20)), 256 << 20);
        assert_eq!(cache_bytes_for_available(None), MIN_CACHE_BYTES);
    }

    #[test]
    fn test_available_bytes_uses_blocks_not_inodes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let vfs = nix::sys::statvfs::statvfs(path).unwrap();
        #[allow(clippy::unnecessary_cast)]
        let expected = vfs.fragment_size() as u64 * vfs.blocks_available() as u64;
        let got = get_available_bytes(path).unwrap();
        // Free space can move a little between the two calls.
        let diff = got.abs_diff(expected);
        assert!(diff < (64 << 20), "got {got}, expected about {expected}");
    }
}

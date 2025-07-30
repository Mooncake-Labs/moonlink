use crate::storage::filesystem::accessor::base_unbuffered_stream_writer::BaseUnbufferedStreamWriter;
/// This module defines the interface for filesystem accessor.
use crate::storage::filesystem::accessor::metadata::ObjectMetadata;
use crate::Result;

use async_trait::async_trait;
use futures::Stream;

use std::pin::Pin;

#[cfg(test)]
use mockall::*;

/// All interfaces accept both relative path (relative to root directory on local filesystem, or bucket for object storage) and absolute path.
#[async_trait]
#[cfg_attr(test, automock)]
pub trait BaseFileSystemAccess: std::fmt::Debug + Send + Sync {
    /// ===============================
    /// Directory operations
    /// ===============================
    ///
    /// List all direct sub-directory under the given directory.
    ///
    /// For example, we have directory "a", "a/b", "a/b/c", listing direct subdirectories for "a" will return "a/b".
    async fn list_direct_subdirectories(&self, folder: &str) -> Result<Vec<String>>;

    /// Remove the whole directory recursively.
    async fn remove_directory(&self, directory: &str) -> Result<()>;

    /// ===============================
    /// Object operations
    /// ===============================
    ///
    /// Return whether the given object exists.
    async fn object_exists(&self, object: &str) -> Result<bool>;

    /// Return the object metadata.
    async fn stats_object(&self, object: &str) -> Result<opendal::Metadata>;

    /// Read the whole content for the given object.
    /// Notice, it's not suitable to read large files; as of now it's made for metadata files.
    async fn read_object(&self, object: &str) -> Result<Vec<u8>>;
    /// Similar to [`read_object`], but return content in string format.
    async fn read_object_as_string(&self, object: &str) -> Result<String>;

    /// Stream read the content for the given object.
    /// It's suitable for large objects.
    async fn stream_read(
        &self,
        object: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<u8>>> + Send>>>;

    /// Write the whole content to the given object.
    async fn write_object(
        &self,
        object_filepath: &str,
        content: Vec<u8>,
    ) -> Result<opendal::Metadata>;
    /// Write the whole content with conditional write and put-if-absent semantics support.
    /// If if-match feature is not supported for the current storage backend, fallback to [`write_object`].
    ///
    /// # Arguments
    ///
    /// * etag: if unspecified, attempt put-if-absent logic.
    async fn conditional_write_object(
        &self,
        object_filepath: &str,
        content: Vec<u8>,
        etag: Option<String>,
    ) -> Result<opendal::Metadata>;

    /// Return a writer, which used for stream writer.
    /// Notice: no IO operation is performed under the hood.
    ///
    /// TODO(hjiang): Consider to take a [`config`]
    async fn create_unbuffered_stream_writer(
        &self,
        object_filepath: &str,
    ) -> Result<Box<dyn BaseUnbufferedStreamWriter>>;

    /// Delete the given object.
    async fn delete_object(&self, object_filepath: &str) -> Result<()>;

    /// Copy from local file [`src`] to remote file [`dst`].
    async fn copy_from_local_to_remote(&self, src: &str, dst: &str) -> Result<ObjectMetadata>;

    /// Copy from remote file [`src`] to local file [`dst`].
    async fn copy_from_remote_to_local(&self, src: &str, dst: &str) -> Result<ObjectMetadata>;
}

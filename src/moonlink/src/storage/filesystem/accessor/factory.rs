use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::filesystem::accessor_config::AccessorConfig;

use std::sync::Arc;

/// A factory function to create a filesystem accessor based on the given [`config`].
pub(crate) fn create_filesystem_accessor(
    accessor_config: AccessorConfig,
) -> Arc<dyn BaseFileSystemAccess> {
    Arc::new(FileSystemAccessor::new(accessor_config))
}

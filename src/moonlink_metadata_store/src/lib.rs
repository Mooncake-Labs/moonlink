pub mod base_metadata_store;
mod config_utils;
pub mod error;
pub mod metadata_store_utils;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

#[cfg(feature = "metadata-postgres")]
mod postgres;
mod sqlite;

#[cfg(feature = "metadata-postgres")]
pub use {postgres::pg_metadata_store::PgMetadataStore, postgres::utils as PgUtils};

pub use sqlite::sqlite_metadata_store::SqliteMetadataStore;
pub use sqlite::utils as SqliteUtils;

pub(crate) mod table_manager;

pub(crate) const MOONCAKE_TABLE_FLUSH_LSN: &str = "moonlink.table-flush-lsn";

/// Snapshot property recording which lookup-key hash the persisted file indices use.
pub(crate) const MOONCAKE_LOOKUP_KEY_VERSION: &str = "moonlink.lookup-key-version";

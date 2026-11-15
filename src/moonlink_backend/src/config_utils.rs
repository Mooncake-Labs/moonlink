use crate::error::Result;
use crate::table_config::TableConfig;
use moonlink::row::IdentityProp;
use moonlink::{MooncakeTableId, MoonlinkTableConfig};

/// Parse replication table config, and fill in default value if unassigned.
pub(crate) fn parse_replication_table_config(
    moonlink_table_config: &str,
    mooncake_table_id: &MooncakeTableId,
    table_base_path: &str,
    temp_files_dir: &str,
) -> Result<MoonlinkTableConfig> {
    let mut table_config =
        TableConfig::from_json_or_default(moonlink_table_config, table_base_path)?;
    table_config.mooncake_config.row_identity = Some(IdentityProp::FullRow);
    table_config.mooncake_config.append_only = Some(false);
    table_config.take_as_moonlink_config(temp_files_dir.to_string(), mooncake_table_id)
}

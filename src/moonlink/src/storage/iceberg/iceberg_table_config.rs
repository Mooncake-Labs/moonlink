use crate::{storage::filesystem::accessor_config::AccessorConfig, StorageConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RestCatalogConfig {
    uri: String,
    warehouse: Option<String>,
    props: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GlueCatalogConfig {
    name: Option<String>,
    uri: Option<String>,
    catalog_id: Option<String>,
    warehouse: String,
    props: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum IcebergCatalogConfig {
    File {
        accessor_config: AccessorConfig,
    },
    Rest {
        rest_catalog_config: RestCatalogConfig,
    },
    Glue {
        glue_catalog_config: GlueCatalogConfig,
    },
}

impl IcebergCatalogConfig {
    pub fn get_warehouse_uri(&self) -> String {
        match self {
            IcebergCatalogConfig::File { accessor_config } => accessor_config.get_root_path(),
            IcebergCatalogConfig::Rest {
                rest_catalog_config,
            } => rest_catalog_config.warehouse.clone().unwrap_or_default(),
            IcebergCatalogConfig::Glue {
                glue_catalog_config,
            } => glue_catalog_config.warehouse.clone(),
        }
    }

    pub fn get_file_catalog_accessor_config(&self) -> Option<AccessorConfig> {
        match self {
            IcebergCatalogConfig::File { accessor_config } => Some(accessor_config.clone()),
            _ => None,
        }
    }
    pub fn get_rest_catalog_config(&self) -> Option<RestCatalogConfig> {
        match self {
            IcebergCatalogConfig::Rest {
                rest_catalog_config,
            } => Some(rest_catalog_config.clone()),
            _ => None,
        }
    }
    pub fn get_glue_catalog_config(&self) -> Option<GlueCatalogConfig> {
        match self {
            IcebergCatalogConfig::Glue {
                glue_catalog_config,
            } => Some(glue_catalog_config.clone()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IcebergTableConfig {
    /// Namespace for the iceberg table.
    pub namespace: Vec<String>,
    /// Iceberg table name.
    pub table_name: String,
    /// Accessor config for accessing data files.
    pub data_accessor_config: AccessorConfig,
    /// Catalog configuration (defaults to File).
    pub metadata_accessor_config: IcebergCatalogConfig,
}

impl IcebergTableConfig {
    const DEFAULT_WAREHOUSE_URI: &str = "/tmp/moonlink_iceberg";
    const DEFAULT_NAMESPACE: &str = "namespace";
    const DEFAULT_TABLE: &str = "table";
}

impl Default for IcebergTableConfig {
    fn default() -> Self {
        let storage_config = StorageConfig::FileSystem {
            root_directory: Self::DEFAULT_WAREHOUSE_URI.to_string(),
            // There's only one iceberg writer per-table, no need for atomic write feature.
            atomic_write_dir: None,
        };
        Self {
            namespace: vec![Self::DEFAULT_NAMESPACE.to_string()],
            table_name: Self::DEFAULT_TABLE.to_string(),
            data_accessor_config: AccessorConfig::new_with_storage_config(storage_config.clone()),
            metadata_accessor_config: IcebergCatalogConfig::File {
                accessor_config: AccessorConfig::new_with_storage_config(storage_config),
            },
        }
    }
}

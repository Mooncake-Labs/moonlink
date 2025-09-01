use crate::{storage::filesystem::accessor_config::AccessorConfig, StorageConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RestCatalogConfig {
    #[serde(rename = "name")]
    #[serde(default)]
    pub name: String,

    #[serde(rename = "uri")]
    #[serde(default)]
    pub uri: String,

    #[serde(rename = "warehouse")]
    #[serde(default)]
    pub warehouse: String,

    /// Optional configuration properties.
    /// Unknown configs will be ignored.
    ///
    /// - prefix:          Optional URL path prefix to insert after the base URI and API version.
    /// - oauth2-server-uri: Custom OAuth2 server URI. Defaults to: [uri, PATH_V1:"v1", "oauth", "tokens"].join("/")
    /// - token:           Static authentication token used by the client for sending requests.
    /// - credentials:     Client credentials used to fetch a new token.
    ///     - None: No credentials provided.
    ///     - Some(None, client_secret): Only client_secret is provided.
    ///     - Some(Some(client_id), client_secret): Both client_id and client_secret are provided.
    #[serde(rename = "props")]
    #[serde(default)]
    pub props: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GlueCatalogConfig {
    #[serde(rename = "name")]
    #[serde(default)]
    name: Option<String>,

    #[serde(rename = "uri")]
    #[serde(default)]
    uri: Option<String>,

    #[serde(rename = "catalog_id")]
    #[serde(default)]
    catalog_id: Option<String>,

    #[serde(rename = "warehouse")]
    #[serde(default)]
    warehouse: String,

    #[serde(rename = "props")]
    #[serde(default)]
    props: HashMap<String, String>,
}

pub type FileCatalogConfig = AccessorConfig;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum IcebergCatalogConfig {
    #[serde(rename = "file")]
    File { accessor_config: FileCatalogConfig },

    #[cfg(feature = "catalog-rest")]
    #[serde(rename = "rest")]
    Rest {
        rest_catalog_config: RestCatalogConfig,
    },

    #[cfg(feature = "catalog-glue")]
    #[serde(rename = "glue")]
    Glue {
        glue_catalog_config: GlueCatalogConfig,
    },
}

impl IcebergCatalogConfig {
    pub fn get_warehouse_uri(&self) -> String {
        match self {
            IcebergCatalogConfig::File { accessor_config } => accessor_config.get_root_path(),
            #[cfg(feature = "catalog-rest")]
            IcebergCatalogConfig::Rest {
                rest_catalog_config,
            } => rest_catalog_config.warehouse.clone(),
            #[cfg(feature = "catalog-glue")]
            IcebergCatalogConfig::Glue {
                glue_catalog_config,
            } => glue_catalog_config.warehouse.clone(),
        }
    }

    pub fn get_file_catalog_accessor_config(&self) -> Option<FileCatalogConfig> {
        match self {
            IcebergCatalogConfig::File { accessor_config } => Some(accessor_config.clone()),
            #[cfg(any(feature = "catalog-rest", feature = "catalog-glue"))]
            _ => None,
        }
    }

    #[cfg(feature = "catalog-rest")]
    pub fn get_rest_catalog_config(&self) -> Option<RestCatalogConfig> {
        if let IcebergCatalogConfig::Rest {
            rest_catalog_config,
        } = self
        {
            return Some(rest_catalog_config.clone());
        }
        None
    }

    #[cfg(feature = "catalog-glue")]
    pub fn get_glue_catalog_config(&self) -> Option<GlueCatalogConfig> {
        if let IcebergCatalogConfig::Glue {
            glue_catalog_config,
        } = self
        {
            return Some(glue_catalog_config.clone());
        }
        None
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

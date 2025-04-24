use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::Error as IcebergError;
use iceberg::Result as IcebergResult;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, TableUpdate,
};

/// This module contains the filesystem catalog implementation, which serves for local development and hermetic unit test purpose, so for initial versions, it's focusing more on simplicity and correctness rather than performance.
/// Compared with `MemoryCatalog`, `FileSystemCatalog` could be used in production environment.
///
/// TODO(hjiang):
/// 1. Implement property related functionalities.
/// 2. Implement features necessary for concurrent accesses.
/// 3. The initial version access everything via filesystem, for performance considerartion we should cache metadata in memory.

#[derive(Debug)]
pub struct FileSystemCatalog {
    file_io: FileIO,
    table_name: String,
    warehouse_location: String,
    metadata: Option<TableMetadata>,
    metadata_content: Vec<u8>,
    metadata_file_path: String,
    table: Option<Table>,
}

impl FileSystemCatalog {
    /// Creates a rest catalog from config.
    pub fn new(name: String, warehouse_location: String) -> Self {
        Self {
            file_io: FileIO::from_path(warehouse_location.clone())
                .unwrap()
                .build()
                .unwrap(),
            table_name: name,
            warehouse_location: warehouse_location,
            metadata: None,
            metadata_content: vec![],
            metadata_file_path: "".to_string(),
            table: None,
        }
    }

    pub async fn load_metadata(mut self) -> IcebergResult<Self> {
        let version_hint_path = format!("{}/metadata/version-hint.text", self.warehouse_location);
        let input_file: iceberg::io::InputFile = self.file_io.new_input(&version_hint_path)?;
        let version = String::from_utf8(input_file.read().await?.to_vec()).expect("");
        self.metadata_file_path = format!(
            "{}/metadata/v{}.metadata.json",
            self.warehouse_location, version
        );
        let input_file: iceberg::io::InputFile =
            self.file_io.new_input(&self.metadata_file_path)?;
        self.metadata_content = input_file.read().await?.to_vec();
        let metadata = serde_json::from_slice::<TableMetadata>(&self.metadata_content)?;
        let table_id: TableIdent = TableIdent::from_strs(["default", &self.table_name]).unwrap();
        self.table = Some(
            Table::builder()
                .file_io(self.file_io.clone())
                .metadata_location(self.metadata_file_path.clone())
                .metadata(metadata.clone())
                .identifier(table_id.clone())
                .build()
                .unwrap(),
        );
        self.metadata = Some(metadata);
        Ok(self)
    }

    fn namespace_path(&self, namespace: &NamespaceIdent) -> PathBuf {
        let mut path = PathBuf::from(&self.warehouse_location);
        for part in namespace.as_ref() {
            path.push(part);
        }
        path
    }
}

#[async_trait]
impl Catalog for FileSystemCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        let mut base_path = PathBuf::from(&self.warehouse_location);
        if let Some(ns) = parent {
            for part in ns.as_ref() {
                base_path.push(part);
            }
            if !base_path.exists() {
                return Err(IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Parent namespace {:?} does not exist", ns),
                ));
            }
        }

        let mut namespaces = vec![];
        for entry in std::fs::read_dir(&base_path).map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to read directory: {}", e),
            )
        })? {
            let entry = entry.map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to read entry: {}", e),
                )
            })?;

            let path = entry.path();
            if path.is_dir() {
                let name = entry.file_name().into_string().map_err(|_| {
                    IcebergError::new(
                        iceberg::ErrorKind::Unexpected,
                        "Failed to parse directory name as UTF-8",
                    )
                })?;
                namespaces.push(NamespaceIdent::new(name));
            }
        }

        Ok(namespaces)
    }

    /// Create a new namespace inside the catalog.
    ///
    /// TODO(hjiang): Implement properties handling.
    async fn create_namespace(
        &self,
        namespace: &iceberg::NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<iceberg::Namespace> {
        // Build full path from warehouse_location and namespace components
        let mut path = PathBuf::from(&self.warehouse_location);
        for part in namespace.as_ref().iter() {
            path.push(part);
        }

        // Attempt to create all directories (no-op if already exists)
        std::fs::create_dir_all(&path).map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to create namespace directory: {}", e),
            )
        })?;

        // Return the created namespace
        Ok(Namespace::new(namespace.clone()))
    }

    /// Get a namespace information from the catalog.
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        let path = self.namespace_path(namespace);
        if path.is_dir() {
            Ok(Namespace::new(namespace.clone()))
        } else {
            Err(IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Namespace {:?} does not exist", namespace),
            ))
        }
    }

    /// Check if namespace exists in catalog.
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> IcebergResult<bool> {
        let path = self.namespace_path(namespace);
        Ok(path.is_dir())
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<()> {
        let path = self.namespace_path(namespace);
        if !path.exists() {
            return Err(IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Namespace {:?} does not exist", namespace),
            ));
        }

        std::fs::remove_dir_all(&path).map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("failed to remove namespace directory: {}", e),
            )
        })?;
        Ok(())
    }

    /// List tables from namespace.
    async fn list_tables(&self, _namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        todo!()
    }
    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        todo!()
    }

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        let table_ident = TableIdent::new(namespace.clone(), creation.name.clone());
        // TODO(hjiang): Confirm the location field inside of `TableCreation`.
        let warehouse_location = self.warehouse_location.clone();
        let metadata_path = format!(
            "{}/{}/{}{}",
            warehouse_location,
            namespace.to_url_string(),
            creation.name,
            "/metadata"
        );
        let version_hint_path = format!(
            "{}/{}/{}{}",
            warehouse_location,
            namespace.to_url_string(),
            creation.name,
            "/metadata/version-hint.text"
        );

        // Create the initial table metadata.
        let table_metadata = TableMetadataBuilder::from_table_creation(creation)?.build()?;
        // Write the initial metadata file.
        let metadata_file_path = format!("{metadata_path}/v0.metadata.json");
        let metadata_json = serde_json::to_string(&table_metadata.metadata)?;
        let output = self.file_io.new_output(&metadata_file_path)?;
        output.write(metadata_json.into()).await?;

        // Write the version hint file.
        let version_hint_output = self.file_io.new_output(&version_hint_path)?;
        version_hint_output.write("0".into()).await?;

        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(table_ident)
            .file_io(self.file_io.clone())
            .build()
            .unwrap();
        Ok(table)
    }

    /// Load table from the catalog.
    async fn load_table(&self, _table: &TableIdent) -> IcebergResult<Table> {
        todo!()
    }
    /// Drop a table from the catalog.
    async fn drop_table(&self, _table: &TableIdent) -> IcebergResult<()> {
        todo!()
    }
    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        let mut metadata_path = PathBuf::from(&self.warehouse_location);
        for part in table.namespace().as_ref() {
            metadata_path.push(part);
        }
        metadata_path.push(table.name());
        metadata_path.push("metadata");

        // Check if directory exists
        if !metadata_path.is_dir() {
            return Ok(false);
        }

        // Check if at least one .metadata.json file exists
        let exists = std::fs::read_dir(&metadata_path)
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to read metadata dir: {}", e),
                )
            })?
            .any(|entry| {
                if let Ok(entry) = entry {
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    name.ends_with(".metadata.json")
                } else {
                    false
                }
            });

        Ok(exists)
    }

    /// Rename a table in the catalog.
    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> IcebergResult<()> {
        todo!()
    }
    /// Update a table to the catalog.
    async fn update_table(&self, mut commit: TableCommit) -> IcebergResult<Table> {
        let version = self.metadata.as_ref().unwrap().next_sequence_number();
        let builder = TableMetadataBuilder::new_from_metadata(
            self.metadata.clone().unwrap(),
            Some(self.metadata_file_path.clone()),
        );
        let update = commit.take_updates().to_vec();
        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &update[0] {
            snapshot
        } else {
            unreachable!()
        };
        let metadata = builder
            .add_snapshot(new_snapshot.clone())
            .unwrap()
            .build()
            .unwrap();
        // Write the initial metadata file
        let metadata_file_path = format!(
            "{}/metadata/v{}.metadata.json",
            self.warehouse_location, version
        );
        let metadata_json = serde_json::to_string(&metadata.metadata)?;
        let output = self.file_io.new_output(&metadata_file_path)?;
        output.write(metadata_json.into()).await?;
        let version_hint_path = format!("{}/metadata/version-hint.text", self.warehouse_location);
        let version_hint_output = self.file_io.new_output(&version_hint_path)?;
        version_hint_output
            .write(format!("{version}").into())
            .await?;
        Table::builder()
            .identifier(commit.identifier().clone())
            .file_io(self.file_io.clone())
            .metadata(metadata.metadata)
            .metadata_location(metadata_file_path)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    use iceberg::spec::NestedField;
    use iceberg::spec::PrimitiveType;
    use iceberg::spec::Schema;
    use iceberg::spec::Type as IcebergType;
    use iceberg::Catalog;
    use iceberg::NamespaceIdent;
    use iceberg::Result as IcebergResult;

    #[tokio::test]
    async fn test_filesystem_catalog_namespace_operations() -> IcebergResult<()> {
        let temp_dir = TempDir::new().expect("tempdir failed");
        let warehouse_path = temp_dir.path().to_str().unwrap();
        let catalog = FileSystemCatalog::new(
            /*table_name=*/ "".to_string(),
            warehouse_path.to_string(),
        );
        let namespace = NamespaceIdent::from_strs(&["default", "ns"])?;

        // List namespace before creation.
        let namespaces = catalog.list_namespaces(/*parent=*/ None).await?;
        assert!(
            namespaces.is_empty(),
            "No namespaces should exist before creation"
        );

        // Ensure namespace does not exist.
        let exists = catalog.namespace_exists(&namespace).await?;
        assert!(!exists, "Namespace should not exist before creation");

        // Create namespace and check.
        catalog
            .create_namespace(&namespace, /*properties=*/ HashMap::new())
            .await?;
        let exists = catalog.namespace_exists(&namespace).await?;
        assert!(exists, "Namespace should exist after creation");

        // List all existing namespaces.
        let namespaces = catalog
            .list_namespaces(
                /*parent=*/ Some(&NamespaceIdent::from_strs(&["default"]).unwrap()),
            )
            .await?;
        assert_eq!(namespaces.len(), 1, "There should be one root namespace");
        assert_eq!(
            namespaces[0],
            NamespaceIdent::from_strs(&["ns"]).unwrap(),
            "Namespace should match created one"
        );

        let namespaces = catalog.list_namespaces(/*parent=*/ None).await?;
        assert_eq!(
            namespaces.len(),
            1,
            "There should be one sub-namespace under root namespace"
        );
        assert_eq!(
            namespaces[0],
            NamespaceIdent::from_strs(&["default"]).unwrap(),
            "Namespace should match created one"
        );

        // Get the namespace and check.
        let ns = catalog.get_namespace(&namespace).await?;
        assert_eq!(ns.name(), &namespace, "Namespace should match created one");

        // Drop the namespace and check.
        catalog.drop_namespace(&namespace).await?;
        let exists = catalog.namespace_exists(&namespace).await?;
        assert!(!exists, "Namespace should not exist after drop");

        Ok(())
    }

    #[tokio::test]
    async fn test_filesystem_catalog_table_operations() -> IcebergResult<()> {
        let temp_dir = TempDir::new().expect("tempdir failed");
        let warehouse_path = temp_dir.path().to_str().unwrap();
        let catalog = FileSystemCatalog::new(
            /*table_name=*/ "".to_string(),
            warehouse_path.to_string(),
        );

        // Define namespace and table.
        let namespace = NamespaceIdent::from_strs(&["default"])?;
        let table_name = "test_table".to_string();
        let table_ident = TableIdent::new(namespace.clone(), table_name.clone());

        // Ensure table does not exist
        let table_exists = catalog.table_exists(&table_ident).await?;
        assert!(!table_exists, "Table should not exist before creation");

        // Create table and check.
        let field = NestedField::required(
            /*id=*/ 0,
            "field_name".to_string(),
            IcebergType::Primitive(PrimitiveType::Int),
        );
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![Arc::new(field)])
            .build()?;
        let table_creation = TableCreation::builder()
            .name(table_name.clone())
            .location(format!(
                "file:///tmp/iceberg-test/{}/{}",
                namespace.to_url_string(),
                table_name
            ))
            .schema(schema)
            .build();
        catalog.create_namespace(&namespace, HashMap::new()).await?;
        catalog.create_table(&namespace, table_creation).await?;

        let table_exists = catalog.table_exists(&table_ident).await?;
        assert!(table_exists, "Table should exist after creation");

        Ok(())
    }
}

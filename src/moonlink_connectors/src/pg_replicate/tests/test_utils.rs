use crate::pg_replicate::clients::postgres::build_tls_connector;
use crate::pg_replicate::table::{TableName, TableSchema};
use crate::pg_replicate::ReplicationClient;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_postgres::connect;
use tokio_postgres::types::PgLsn;

#[cfg(feature = "test-tls")]
const DEFAULT_DB_URL: &str =
    "postgresql://postgres:postgres@postgres:5432/postgres?sslmode=verify-full";

#[cfg(not(feature = "test-tls"))]
const DEFAULT_DB_URL: &str =
    "postgresql://postgres:postgres@postgres:5432/postgres?sslmode=disable";

pub fn database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB_URL.to_string())
}

pub async fn setup_connection() -> tokio_postgres::Client {
    let database_url = database_url();
    let tls = build_tls_connector().unwrap();
    let (client, connection) = connect(&database_url, tls).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Postgres connection error: {e}");
        }
    });
    client
}

pub async fn create_replication_client() -> ReplicationClient {
    let url = database_url();
    let (mut replication_client, connection) =
        ReplicationClient::connect(&url, true).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Replication connection error: {e}");
        }
    });
    replication_client
}

/// Fetch the `TableSchema` for a given table name within the `public` schema,
/// using an optional publication context for column filtering.
pub async fn fetch_table_schema(publication: &str, table_name_str: &str) -> TableSchema {
    let url = database_url();
    let tls = build_tls_connector().unwrap();
    let (schema_pg_client, schema_conn) = connect(&url, tls).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = schema_conn.await {
            eprintln!("Schema connection error: {e}");
        }
    });
    let mut schema_client = ReplicationClient::from_client(schema_pg_client);
    let table_name = TableName {
        schema: "public".to_string(),
        name: table_name_str.to_string(),
    };
    let src_table_id = schema_client
        .get_src_table_id(&table_name)
        .await
        .unwrap()
        .expect(&format!("missing table id for table {table_name}"));
    schema_client
        .get_table_schema(src_table_id, table_name, Some(publication))
        .await
        .unwrap()
}

/// Spawns a background SQL executor that can be used to submit arbitrary SQL
/// in desired order with optional delays between them.
pub fn spawn_sql_executor(database_url: String) -> mpsc::UnboundedSender<String> {
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        let tls = build_tls_connector().unwrap();
        let (bg_client, bg_connection) = connect(&database_url, tls).await.unwrap();
        tokio::spawn(async move {
            bg_connection.await.unwrap();
        });

        while let Some(sql) = rx.recv().await {
            bg_client
                .simple_query(&sql)
                .await
                .expect(&format!("SQL statement execution {} failed", sql));
        }
    });
    tx
}

/// Helper to set replica identity FULL on a table.
pub async fn set_replica_identity_full(client: &tokio_postgres::Client, table_name: &str) {
    client
        .simple_query(&format!("ALTER TABLE {table_name} REPLICA IDENTITY FULL;"))
        .await
        .unwrap();
}

/// Helper to create a publication for a single table.
pub async fn create_publication_for_table(
    client: &tokio_postgres::Client,
    publication: &str,
    table_name: &str,
) {
    client
        .simple_query(&format!(
            "CREATE PUBLICATION {publication} FOR TABLE {table_name};"
        ))
        .await
        .unwrap();
}

/// Create a replication client and ensure a replication slot exists, returning the
/// client and the confirmed flush LSN to start streaming from.
pub async fn create_replication_client_and_slot(slot_name: &str) -> (ReplicationClient, PgLsn) {
    let mut replication_client = create_replication_client().await;
    // https://github.com/supabase/etl/blob/4da956c6b9be8476a1dbe87a4d88689e0671b7c1/etl/docs/Replication%20in%20Postgres.md?plain=1#L70
    replication_client
        .begin_readonly_transaction()
        .await
        .unwrap();
    let slot_info = replication_client
        .get_or_create_slot(slot_name)
        .await
        .unwrap();
    replication_client.commit_txn().await.unwrap();
    (replication_client, slot_info.confirmed_flush_lsn)
}

async fn drop_publication(client: &tokio_postgres::Client, publication: &str) {
    let _ = client
        .simple_query(&format!("DROP PUBLICATION {publication} CASCADE;"))
        .await
        .unwrap();
}

async fn drop_table(client: &tokio_postgres::Client, table_name: &str) {
    let _ = client
        .simple_query(&format!("DROP TABLE {table_name} CASCADE;"))
        .await
        .unwrap();
}

async fn drop_type(client: &tokio_postgres::Client, type_name: &str) {
    let _ = client
        .simple_query(&format!("DROP TYPE {type_name} CASCADE;"))
        .await
        .unwrap();
}

async fn drop_replication_slot(client: &tokio_postgres::Client, slot_name: &str) {
    let _ = client
        .simple_query(&format!(
            "SELECT pg_drop_replication_slot('{slot}');",
            slot = slot_name
        ))
        .await
        .unwrap();
}

pub struct TestResources {
    client: Arc<tokio_postgres::Client>,
    tables: Vec<String>,
    publications: Vec<String>,
    slots: Vec<String>,
    types: Vec<String>,
    sql_tx: Option<mpsc::UnboundedSender<String>>,
}

impl TestResources {
    pub fn new(client: tokio_postgres::Client) -> Self {
        Self {
            client: Arc::new(client),
            tables: Vec::new(),
            publications: Vec::new(),
            slots: Vec::new(),
            types: Vec::new(),
            sql_tx: None,
        }
    }

    pub fn client(&self) -> &tokio_postgres::Client {
        &self.client
    }

    pub fn add_table(&mut self, name: impl Into<String>) {
        self.tables.push(name.into());
    }

    pub fn add_publication(&mut self, name: impl Into<String>) {
        self.publications.push(name.into());
    }

    pub fn add_slot(&mut self, name: impl Into<String>) {
        self.slots.push(name.into());
    }

    pub fn add_type(&mut self, name: impl Into<String>) {
        self.types.push(name.into());
    }

    pub fn set_sql_tx(&mut self, tx: mpsc::UnboundedSender<String>) {
        self.sql_tx = Some(tx);
    }

    async fn cleanup(
        client: Arc<tokio_postgres::Client>,
        slots: Vec<String>,
        publications: Vec<String>,
        tables: Vec<String>,
        types: Vec<String>,
    ) {
        for slot in &slots {
            drop_replication_slot(&client, &slot).await;
        }
        for publication in &publications {
            drop_publication(&client, &publication).await;
        }
        for table in &tables {
            drop_table(&client, &table).await;
        }
        for type_name in &types {
            drop_type(&client, &type_name).await;
        }
    }
}

impl Drop for TestResources {
    fn drop(&mut self) {
        // Drop the SQL sender to stop background executor if still running
        let _ = self.sql_tx.take();

        let client = Arc::clone(&self.client);
        let tables = std::mem::take(&mut self.tables);
        let publications = std::mem::take(&mut self.publications);
        let slots = std::mem::take(&mut self.slots);
        let types = std::mem::take(&mut self.types);

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                TestResources::cleanup(client, slots, publications, tables, types).await;
            });
        });
    }
}

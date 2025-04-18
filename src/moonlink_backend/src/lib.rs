pub use moonlink::Error;

use moonlink::Result;
use moonlink::TableEvent;
use moonlink_connectors::{MoonlinkPostgresSource, PostgresSourceMetadata};
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::RwLock;

pub trait TableIdentifier: Eq + std::hash::Hash + Clone + Send + Sync + 'static {}

pub struct MoonlinkBackend<T: TableIdentifier> {
    ingest_sources: RwLock<Vec<MoonlinkPostgresSource>>,
    table_readers: RwLock<HashMap<T, Sender<TableEvent>>>,
}

impl<T: TableIdentifier> MoonlinkBackend<T> {
    pub fn new() -> Self {
        Self {
            ingest_sources: RwLock::new(Vec::new()),
            table_readers: RwLock::new(HashMap::new()),
        }
    }

    pub async fn create_table(
        &self,
        host: &str,
        port: u16,
        username: &str,
        password: &str,
        database: &str,
        schema: &str,
        table: &str,
        table_id: T,
    ) -> Result<()> {
        let metadata = PostgresSourceMetadata::new(
            host.to_string(),
            port,
            database.to_string(),
            username.to_string(),
            password.to_string(),
        );
        {
            let mut ingest_sources = self.ingest_sources.write().await;
            for ingest_source in ingest_sources.iter_mut() {
                if ingest_source.check_table_belongs_to_source(&metadata) {
                    let sender = ingest_source.add_table(schema, table).await?;
                    self.table_readers.write().await.insert(table_id, sender);
                    return Ok(());
                }
            }
            let mut ingest_source = MoonlinkPostgresSource::new(metadata).await?;
            let sender = ingest_source.add_table(schema, table).await?;
            ingest_sources.push(ingest_source);
            self.table_readers.write().await.insert(table_id, sender);
        }

        return Ok(());
    }

    pub async fn drop_table(&mut self, _table_id: T) -> Result<()> {
        todo!()
    }

    pub async fn scan_table_begin(&self, table_id: T) -> Result<(Vec<String>, Vec<(u32, u32)>)> {
        let table_readers = self.table_readers.read().await;
        let reader = table_readers.get(&table_id).unwrap();
        let (sender, receiver) = oneshot::channel();
        reader
            .send(TableEvent::PrepareRead {
                response_channel: sender,
            })
            .await
            .unwrap();
        let response = receiver.await.unwrap();
        let result = (
            response
                .0
                .iter()
                .map(|p| p.to_string_lossy().to_string())
                .collect(),
            response
                .1
                .iter()
                .map(|(start, end)| (*start as u32, *end as u32))
                .collect(),
        );
        Ok(result)
    }

    pub async fn scan_table_end(&mut self, _table_id: T, _lsn: u64) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl TableIdentifier for &'static str {}
    #[tokio::test]
    async fn test_moonlink_service() {
        let service = MoonlinkBackend::<&'static str>::new();
        // connect to postgres and create a table
        let (client, connection) = tokio_postgres::Config::new()
            .host("localhost")
            .port(5432)
            .user("postgres")
            .password("postgres")
            .dbname("postgres")
            .connect(tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        client.simple_query("DROP TABLE IF EXISTS test; CREATE TABLE test (id bigint PRIMARY KEY, name VARCHAR(255));").await.unwrap();
        println!("created table");
        service
            .create_table(
                "localhost",
                5432,
                "postgres",
                "postgres",
                "postgres",
                "public",
                "test",
                "test",
            )
            .await
            .unwrap();
        client
            .simple_query("INSERT INTO test  VALUES (1 ,'foo');")
            .await
            .unwrap();
        client
            .simple_query("INSERT INTO test  VALUES (2 ,'bar');")
            .await
            .unwrap();
        // wait 2 second
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let (columns, deletions) = service.scan_table_begin("test").await.unwrap();
        println!("columns: {:?}", columns);
        println!("deletions: {:?}", deletions);
    }
}

use moonlink::ReadStateManager;
mod error;

use error::Result;
use moonlink_connectors::MoonlinkPostgresSource;
use std::{collections::HashMap, hash::Hash};
use tokio::sync::RwLock;

pub use error::Error;

pub struct MoonlinkBackend<T: Eq + Hash> {
    ingest_sources: RwLock<Vec<MoonlinkPostgresSource>>,
    table_readers: RwLock<HashMap<T, ReadStateManager>>,
}

impl<T: Eq + Hash> MoonlinkBackend<T> {
    pub fn new() -> Self {
        Self {
            ingest_sources: RwLock::new(Vec::new()),
            table_readers: RwLock::new(HashMap::new()),
        }
    }

    pub async fn create_table(&self, table_id: T, table: &str, uri: &str) -> Result<()> {
        let mut ingest_sources = self.ingest_sources.write().await;
        for ingest_source in ingest_sources.iter_mut() {
            if ingest_source.check_table_belongs_to_source(uri) {
                let reader_state_manager = ingest_source.add_table(table).await?;
                self.table_readers
                        .write()
                        .await
                        .insert(table_id, reader_state_manager);
                return Ok(());
            }
        }
        let mut ingest_source = MoonlinkPostgresSource::new(uri.to_owned()).await?;
        let reader_state_manager = ingest_source.add_table(table).await?;
        ingest_sources.push(ingest_source);
        self.table_readers
                .write()
                .await
                .insert(table_id, reader_state_manager);
        Ok(())
    }

    pub async fn drop_table(&self, _table_id: T) -> Result<()> {
        todo!()
    }

    pub async fn scan_table(&self, table_id: T) -> Result<(Vec<String>, Vec<(u32, u32)>)> {
        let table_readers = self.table_readers.read().await;
        let reader = table_readers.get(&table_id).unwrap();
        let read_state = reader.try_read().await.unwrap();
        let result = (read_state.files.clone(), read_state.deletions.clone());
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::{connect, NoTls};

    #[tokio::test]
    async fn test_moonlink_service() {
        let uri = "postgresql://postgres:postgres@localhost:5432/postgres";
        let service = MoonlinkBackend::<&'static str>::new();
        // connect to postgres and create a table
        let (client, connection) = connect(uri, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        client.simple_query("DROP TABLE IF EXISTS test; CREATE TABLE test (id bigint PRIMARY KEY, name VARCHAR(255));").await.unwrap();
        println!("created table");
        service
            .create_table("test", "public.test", uri)
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
        let (columns, deletions) = service.scan_table("test").await.unwrap();
        println!("columns: {:?}", columns);
        println!("deletions: {:?}", deletions);
    }
}

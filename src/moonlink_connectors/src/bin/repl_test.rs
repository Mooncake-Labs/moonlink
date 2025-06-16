use chrono::Local;
use futures::StreamExt;
use moonlink_connectors::pg_replicate::clients::postgres::ReplicationClient;
use postgres_replication::{
    protocol::{LogicalReplicationMessage, ReplicationMessage},
    LogicalReplicationStream,
};
use std::time::Instant;
use tokio::pin;
use tokio_postgres::{connect, types::PgLsn, NoTls};

const URI: &str = "postgresql://postgres:postgres@postgres:5432/postgres";

async fn setup_replication(
) -> Result<(tokio_postgres::Client, LogicalReplicationStream), Box<dyn std::error::Error>> {
    // Connect to Postgres
    let uri = URI;

    // Create publication using a regular Postgres connection
    let (client, conn) = connect(uri, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Create test table and publication
    client
        .simple_query(
            "DROP TABLE IF EXISTS t;
             CREATE TABLE t (id int PRIMARY KEY, data TEXT);
             DROP PUBLICATION IF EXISTS test_pub;
             CREATE PUBLICATION test_pub FOR TABLE t;",
        )
        .await?;

    // Create replication client
    let repl_client = ReplicationClient::connect_no_tls(uri).await?;

    // Get the replication stream
    let stream = repl_client
        .get_logical_replication_stream("test_pub", "test_slot", PgLsn::from(0))
        .await?;

    Ok((client, stream))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (_client, stream) = setup_replication().await?;
    pin!(stream);
    let mut count = 0;
    let start = Instant::now();

    while let Some(msg) = stream.next().await {
        match msg {
            Ok(msg) => {
                if let ReplicationMessage::XLogData(xlog_data) = msg {
                    if let LogicalReplicationMessage::Insert(_) = xlog_data.into_data() {
                        count += 1;
                        if count % 1_000_000 == 0 {
                            println!(
                                "[{}] Received {} insert messages in {:.2}s",
                                Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                                count,
                                start.elapsed().as_secs_f64()
                            );
                        }
                    }
                }
            }
            Err(e) => eprintln!("Error receiving message: {:?}", e),
        }
    }

    Ok(())
}

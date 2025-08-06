use crate::pg_replicate::postgres_source::{PostgresSource, TableCopyStream};
use crate::pg_replicate::table::{ColumnSchema, LookupKey, SrcTableId, TableName, TableSchema};
use crate::pg_replicate::util::PostgresTableRow;
use crate::Result;
use futures::{pin_mut, Stream, StreamExt};
use moonlink::TableEvent;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;
use tokio_postgres::types::Type;

/// Represents progress information for an ongoing copy.
#[derive(Debug)]
pub struct CopyProgress {
    /// Last WAL LSN processed from the CDC stream.
    pub last_lsn: PgLsn,
    /// Number of rows copied so far.
    pub rows_copied: u64,
}

/// Reads rows from `stream` and sends them to the provided `event_sender`.
pub async fn copy_table_stream_impl(
    table_schema: TableSchema,
    mut stream: TableCopyStream,
    event_sender: &Sender<TableEvent>,
) -> Result<CopyProgress> {
    // `TableEvent::Append` events. These events must be written to the
    // Mooncake table via `event_sender`.
    // TODO: periodically report progress and
    // support cancellation / restart.
    pin_mut!(stream);
    let mut rows_copied = 0u64;
    while let Some(row) = stream.next().await {
        let row = row?;
        if let Err(e) = event_sender
            .send(TableEvent::Append {
                row: PostgresTableRow(row).into(),
                xact_id: None,
                lsn: 0,
                is_copied: true,
                is_recovery: false,
            })
            .await
        {
            tracing::warn!(error = ?e, "failed to send copied row event");
        }
        rows_copied += 1;
    }

    Ok(CopyProgress {
        last_lsn: PgLsn::from(0),
        rows_copied,
    })
}

/// Generic version for testing
#[cfg(test)]
pub async fn copy_table_stream<S>(
    table_schema: TableSchema,
    mut stream: S,
    event_sender: Sender<TableEvent>,
) -> Result<CopyProgress>
where
    S: Stream<Item = Result<crate::pg_replicate::conversions::table_row::TableRow>> + Unpin,
{
    // `TableEvent::Append` events. These events must be written to the
    // Mooncake table via `event_sender`.
    // TODO: periodically report progress and
    // support cancellation / restart.
    let mut rows_copied = 0u64;
    while let Some(row) = stream.next().await {
        let row = row?;
        if let Err(e) = event_sender
            .send(TableEvent::Append {
                row: PostgresTableRow(row).into(),
                xact_id: None,
                lsn: 0,
                is_copied: true,
                is_recovery: false,
            })
            .await
        {
            tracing::warn!(error = ?e, "failed to send copied row event");
        }
        rows_copied += 1;
    }

    Ok(CopyProgress {
        last_lsn: PgLsn::from(0),
        rows_copied,
    })
}

/// Tests for the initial copy functionality
#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_replicate::table::{ColumnSchema, LookupKey, TableName};
    use futures::stream;
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};
    use tokio_postgres::types::Type;

    //----------------------------------------------------------------------
    // Minimal helpers/stubs
    //----------------------------------------------------------------------

    /// Build a simple TableSchema for testing
    fn make_test_schema(name: &str) -> TableSchema {
        TableSchema {
            table_name: TableName {
                schema: "public".to_string(),
                name: name.to_string(),
            },
            src_table_id: 1,
            column_schemas: vec![ColumnSchema {
                name: "id".to_string(),
                typ: Type::INT4,
                modifier: 0,
                nullable: false,
            }],
            lookup_key: LookupKey::FullRow,
        }
    }

    /// Create a simple test error for testing error propagation
    fn make_test_error() -> crate::Error {
        crate::Error::PostgresSourceError {
            source: Arc::new(
                crate::pg_replicate::postgres_source::PostgresSourceError::MissingPublication,
            ),
        }
    }

    //----------------------------------------------------------------------
    // 1. Happy-path – rows are forwarded and counted
    //----------------------------------------------------------------------

    #[tokio::test]
    async fn test_copies_rows_and_reports_progress() {
        // Create a stream of successful TableRow results
        let table_row1 = crate::pg_replicate::conversions::table_row::TableRow {
            values: vec![crate::pg_replicate::conversions::Cell::I32(1)],
        };
        let table_row2 = crate::pg_replicate::conversions::table_row::TableRow {
            values: vec![crate::pg_replicate::conversions::Cell::I32(2)],
        };
        let table_row3 = crate::pg_replicate::conversions::table_row::TableRow {
            values: vec![crate::pg_replicate::conversions::Cell::I32(3)],
        };

        let rows = vec![Ok(table_row1), Ok(table_row2), Ok(table_row3)];
        let stream = stream::iter(rows);

        let (tx, mut rx) = mpsc::channel::<TableEvent>(8);
        let schema = make_test_schema("test");

        let progress = copy_table_stream(schema, Box::pin(stream), tx)
            .await
            .expect("copy failed");

        assert_eq!(progress.rows_copied, 3);

        for expected in 1..=3 {
            let evt = timeout(Duration::from_millis(50), rx.recv())
                .await
                .expect("channel lag")
                .expect("sender closed");
            match evt {
                TableEvent::Append { row, is_copied, .. } => {
                    assert!(is_copied, "flag should be true");
                    // Check that the row has the expected value
                    // Since we can't easily test the internals of MoonlinkRow,
                    // we just verify the event was received correctly
                    assert!(
                        matches!(row.values[0], moonlink::row::RowValue::Int32(v) if v == expected)
                    );
                }
                _ => panic!("unexpected event {evt:?}"),
            }
        }
        assert!(rx.recv().await.is_none(), "no extra events");
    }

    //----------------------------------------------------------------------
    // 2. Empty snapshot – returns zero rows, sends nothing
    //----------------------------------------------------------------------

    #[tokio::test]
    async fn test_handles_empty_stream() {
        let stream = stream::iter(
            vec![] as Vec<Result<crate::pg_replicate::conversions::table_row::TableRow>>
        );
        let (tx, mut rx) = mpsc::channel::<TableEvent>(1);
        let schema = make_test_schema("empty");

        let progress = copy_table_stream(schema, Box::pin(stream), tx)
            .await
            .expect("copy failed");
        assert_eq!(progress.rows_copied, 0);
        assert!(rx.recv().await.is_none(), "no events expected");
    }

    //----------------------------------------------------------------------
    // 3. Stream error – bubbles up to the caller
    //----------------------------------------------------------------------

    #[tokio::test]
    async fn test_propagates_stream_error() {
        let boom = make_test_error();
        let stream = stream::iter(vec![Err(boom)]);
        let (tx, _) = mpsc::channel::<TableEvent>(1);
        let schema = make_test_schema("broken");

        let err = copy_table_stream(schema, Box::pin(stream), tx)
            .await
            .expect_err("expected failure");

        // Just verify we got an error - the exact format may vary
        assert!(err.to_string().contains("Postgres source error"));
    }
}

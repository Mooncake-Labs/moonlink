use std::{
    collections::HashMap,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH},
};

use futures::{ready, Stream};
use pin_project_lite::pin_project;
use postgres_replication::LogicalReplicationStream;
use thiserror::Error;
use tokio_postgres::{tls::NoTlsStream, types::PgLsn, Connection, CopyOutStream, Socket};
use tracing::{debug, error, info_span, warn, Instrument};

use crate::pg_replicate::{
    clients::postgres::{ReplicationClient, ReplicationClientError},
    conversions::{
        cdc_event::{CdcEvent, CdcEventConversionError, CdcEventConverter},
        table_row::{TableRow, TableRowConversionError, TableRowConverter},
    },
    table::{ColumnSchema, SrcTableId, TableName, TableSchema},
};

pub enum TableNamesFrom {
    Vec(Vec<TableName>),
    Publication(String),
}

#[derive(Debug, Error)]
pub enum PostgresSourceError {
    #[error("cdc stream can only be started with a publication")]
    MissingPublication,

    #[error("cdc stream can only be started with a slot_name")]
    MissingSlotName,

    #[error("replication client error: {0}")]
    ReplicationClient(#[from] ReplicationClientError),

    #[error("tokio postgres error: {0}")]
    TokioPostgres(#[from] tokio_postgres::Error),

    #[error("cdc stream error: {0}")]
    CdcStream(#[from] CdcStreamError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct PostgresSource {
    replication_client: ReplicationClient,
    table_schemas: HashMap<SrcTableId, TableSchema>,
    slot_name: Option<String>,
    publication: Option<String>,
    confirmed_flush_lsn: PgLsn,
    uri: String,
}

/// Configuration needed to create a CDC stream
#[derive(Clone, Debug)]
pub struct CdcStreamConfig {
    pub publication: String,
    pub slot_name: String,
    pub confirmed_flush_lsn: PgLsn,
    pub table_schemas: HashMap<SrcTableId, TableSchema>,
}

impl PostgresSource {
    pub async fn new(
        uri: &str,
        slot_name: Option<String>,
        table_names_from: TableNamesFrom,
        replication_mode: bool,
    ) -> Result<PostgresSource, PostgresSourceError> {
        let (mut replication_client, connection) =
            ReplicationClient::connect_no_tls(uri, replication_mode).await?;
        tokio::spawn(
            Self::drive_connection(connection).instrument(info_span!("postgres_client_monitor")),
        );
        if replication_mode {
            replication_client.begin_readonly_transaction().await?;
        }
        let mut confirmed_flush_lsn = PgLsn::from(0);
        if let Some(ref slot_name) = slot_name {
            confirmed_flush_lsn = replication_client
                .get_or_create_slot(slot_name)
                .await?
                .confirmed_flush_lsn;
        }
        let (table_names, publication) =
            Self::get_table_names_and_publication(&replication_client, table_names_from).await?;
        let table_schemas = replication_client
            .get_table_schemas(&table_names, publication.as_deref())
            .await?;
        Ok(PostgresSource {
            replication_client,
            table_schemas,
            publication,
            slot_name,
            confirmed_flush_lsn,
            uri: uri.to_string(),
        })
    }

    fn publication(&self) -> Option<&String> {
        self.publication.as_ref()
    }

    fn slot_name(&self) -> Option<&String> {
        self.slot_name.as_ref()
    }

    async fn drive_connection(connection: Connection<Socket, NoTlsStream>) {
        if let Err(e) = connection.await {
            warn!("connection error: {}", e);
        }
    }

    pub async fn add_table_to_publication(
        &mut self,
        table_name: &TableName,
    ) -> Result<(), PostgresSourceError> {
        self.replication_client
            .add_table_to_publication(table_name)
            .await?;
        Ok(())
    }

    pub async fn get_row_count(
        &mut self,
        table_name: &TableName,
    ) -> Result<i64, PostgresSourceError> {
        let row_count = self.replication_client.get_row_count(table_name).await?;
        Ok(row_count)
    }

    async fn get_table_names_and_publication(
        replication_client: &ReplicationClient,
        table_names_from: TableNamesFrom,
    ) -> Result<(Vec<TableName>, Option<String>), ReplicationClientError> {
        Ok(match table_names_from {
            TableNamesFrom::Vec(table_names) => (table_names, None),
            TableNamesFrom::Publication(publication) => {
                if !replication_client.publication_exists(&publication).await? {
                    return Err(ReplicationClientError::MissingPublication(
                        publication.to_string(),
                    ));
                }
                (
                    replication_client
                        .get_publication_table_names(&publication)
                        .await?,
                    Some(publication),
                )
            }
        })
    }

    pub async fn get_table_schemas(&self) -> HashMap<SrcTableId, TableSchema> {
        self.table_schemas.clone()
    }

    pub async fn fetch_table_schema(
        &mut self,
        table_name: &str,
        publication: Option<&str>,
    ) -> Result<TableSchema, PostgresSourceError> {
        // Open new connection to get table schema
        let (mut replication_client, connection) =
            ReplicationClient::connect_no_tls(&self.uri, false).await?;
        tokio::spawn(
            Self::drive_connection(connection).instrument(info_span!("postgres_client_monitor")),
        );
        replication_client.begin_readonly_transaction().await?;
        let (schema, name) = TableName::parse_schema_name(table_name);
        let table_schema = replication_client
            .get_table_schema(TableName { schema, name }, publication)
            .await?;
        // Add the table schema to the source so that we can use it to convert cdc events.
        self.table_schemas
            .insert(table_schema.src_table_id, table_schema.clone());
        debug!(table_name, "fetched table schema");
        Ok(table_schema)
    }

    /// Get table name from table id.
    /// Precondition: table already exists in pg source, otherwise it panics.
    pub fn get_table_name_from_id(&self, table_id: u32) -> String {
        self.table_schemas
            .get(&table_id)
            .unwrap()
            .table_name
            .get_schema_name()
    }

    /// Remove table schema from source, and return table name.
    /// Precondition: table already exists in pg source, otherwise it panics.
    pub fn remove_table_schema(&mut self, table_id: u32) -> String {
        let table_schema = self.table_schemas.remove(&table_id).unwrap();
        table_schema.table_name.get_schema_name()
    }

    pub async fn get_table_copy_stream(
        &mut self,
        table_name: &TableName,
        column_schemas: &[ColumnSchema],
    ) -> Result<TableCopyStream, PostgresSourceError> {
        debug!("starting table copy stream for table {table_name}");

        let (stream, lsn) = self
            .replication_client
            .get_table_copy_stream(table_name, column_schemas)
            .await
            .map_err(PostgresSourceError::ReplicationClient)?;

        Ok(TableCopyStream {
            stream,
            column_schemas: column_schemas.to_vec(),
            start_lsn: lsn,
        })
    }

    pub async fn commit_transaction(&mut self) -> Result<(), PostgresSourceError> {
        self.replication_client
            .commit_txn()
            .await
            .map_err(PostgresSourceError::ReplicationClient)?;
        Ok(())
    }

    /// Extract the configuration needed to create a CDC stream
    pub fn get_cdc_stream_config(&self) -> Result<CdcStreamConfig, PostgresSourceError> {
        let publication = self
            .publication()
            .ok_or(PostgresSourceError::MissingPublication)?
            .clone();
        let slot_name = self
            .slot_name()
            .ok_or(PostgresSourceError::MissingSlotName)?
            .clone();

        Ok(CdcStreamConfig {
            publication,
            slot_name,
            confirmed_flush_lsn: self.confirmed_flush_lsn,
            table_schemas: self.table_schemas.clone(),
        })
    }

    /// Create a CDC stream from a configuration and replication client
    pub async fn create_cdc_stream(
        mut replication_client: ReplicationClient,
        config: CdcStreamConfig,
    ) -> Result<CdcStream, PostgresSourceError> {
        debug!("creating cdc stream");

        let stream = replication_client
            .get_logical_replication_stream(
                &config.publication,
                &config.slot_name,
                config.confirmed_flush_lsn,
            )
            .await
            .map_err(PostgresSourceError::ReplicationClient)?;

        const TIME_SEC_CONVERSION: u64 = 946_684_800;
        let postgres_epoch = UNIX_EPOCH + Duration::from_secs(TIME_SEC_CONVERSION);

        Ok(CdcStream {
            stream,
            table_schemas: config.table_schemas,
            postgres_epoch,
        })
    }
}

#[derive(Debug, Error)]
pub enum TableCopyStreamError {
    #[error("tokio_postgres error: {0}")]
    TokioPostgresError(#[from] tokio_postgres::Error),

    #[error("conversion error: {0}")]
    ConversionError(TableRowConversionError),
}

pin_project! {
    #[must_use = "streams do nothing unless polled"]
    pub struct TableCopyStream {
        #[pin]
        stream: CopyOutStream,
        column_schemas: Vec<ColumnSchema>,
        pub(crate) start_lsn: PgLsn,
    }
}

impl Stream for TableCopyStream {
    type Item = Result<TableRow, TableCopyStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match ready!(this.stream.poll_next(cx)) {
            Some(Ok(row)) => match TableRowConverter::try_from(&row, this.column_schemas) {
                Ok(row) => Poll::Ready(Some(Ok(row))),
                Err(e) => {
                    let e = TableCopyStreamError::ConversionError(e);
                    error!(error = ?e, "failed to convert table row");
                    Poll::Ready(Some(Err(e)))
                }
            },
            Some(Err(e)) => {
                error!(error = ?e, "table copy stream error");
                Poll::Ready(Some(Err(e.into())))
            }
            None => Poll::Ready(None),
        }
    }
}

#[derive(Debug, Error)]
pub enum CdcStreamError {
    #[error("tokio_postgres error: {0}")]
    TokioPostgresError(#[from] tokio_postgres::Error),

    #[error("cdc event conversion error: {0}")]
    CdcEventConversion(#[from] CdcEventConversionError),
}

pin_project! {
    #[must_use = "streams do nothing unless polled"]
    pub struct CdcStream {
        #[pin]
        stream: LogicalReplicationStream,
        table_schemas: HashMap<SrcTableId, TableSchema>,
        postgres_epoch: SystemTime,
    }
}

#[derive(Debug, Error)]
pub enum StatusUpdateError {
    #[error("system time error: {0}")]
    SystemTime(#[from] SystemTimeError),

    #[error("tokio_postgres error: {0}")]
    TokioPostgres(#[from] tokio_postgres::Error),
}

impl CdcStream {
    pub async fn send_status_update(
        self: Pin<&mut Self>,
        lsn: PgLsn,
    ) -> Result<(), StatusUpdateError> {
        debug!(lsn = u64::from(lsn), "sending status update");
        let this = self.project();
        let ts = this.postgres_epoch.elapsed()?.as_micros() as i64;
        this.stream
            .standby_status_update(lsn, lsn, lsn, ts, 0)
            .await?;

        Ok(())
    }

    pub fn add_table_schema(self: Pin<&mut Self>, schema: TableSchema) {
        let this = self.project();
        this.table_schemas.insert(schema.src_table_id, schema);
    }

    pub fn remove_table_schema(self: Pin<&mut Self>, src_table_id: SrcTableId) {
        let this = self.project();
        this.table_schemas.remove(&src_table_id);
    }
}

impl Stream for CdcStream {
    type Item = Result<CdcEvent, CdcStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match ready!(this.stream.poll_next(cx)) {
            Some(Ok(msg)) => match CdcEventConverter::try_from(msg, &this.table_schemas) {
                Ok(row) => Poll::Ready(Some(Ok(row))),
                Err(e) => Poll::Ready(Some(Err(e.into()))),
            },
            Some(Err(e)) => Poll::Ready(Some(Err(e.into()))),
            None => Poll::Ready(None),
        }
    }
}

use crate::postgres::util::table_schema_to_arrow_schema;
use crate::postgres::util::PostgresTableRow;
use async_trait::async_trait;
use moonlink::{TableEvent, TableHandler};
use pg_replicate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::{
        sinks::{BatchSink, InfallibleSinkError},
        PipelineResumptionState,
    },
    table::{TableId, TableSchema},
};
use std::collections::{HashMap, HashSet};
use std::fs::create_dir_all;
use std::path::PathBuf;
use tokio::sync::mpsc::Sender;
use tokio_postgres::types::PgLsn;

pub struct Sink {
    table_handlers: HashMap<TableId, TableHandler>,
    event_senders: HashMap<TableId, Sender<TableEvent>>,
    reader_notifier: Sender<Sender<TableEvent>>,
    base_path: PathBuf,
}

impl Sink {
    pub fn new(reader_notifier: Sender<Sender<TableEvent>>, base_path: PathBuf) -> Self {
        let event_senders = HashMap::new();
        Self {
            table_handlers: HashMap::new(),
            event_senders,
            reader_notifier,
            base_path,
        }
    }
}

#[async_trait]
impl BatchSink for Sink {
    type Error = InfallibleSinkError;

    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        Ok(PipelineResumptionState {
            copied_tables: HashSet::new(),
            last_lsn: PgLsn::from(0),
        })
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error> {
        let table_handlers = &mut self.table_handlers;
        for (table_id, table_schema) in table_schemas {
            let table_path =
                PathBuf::from(&self.base_path).join(table_schema.table_name.to_string());
            create_dir_all(&table_path).unwrap();
            let table_handler = TableHandler::new(
                table_schema_to_arrow_schema(&table_schema),
                table_schema.table_name.to_string(),
                table_id as u64,
                table_path,
            );
            self.event_senders
                .insert(table_id, table_handler.get_event_sender());
            self.reader_notifier
                .send(table_handler.get_event_sender())
                .await
                .unwrap();
            table_handlers.insert(table_id, table_handler);
        }
        Ok(())
    }

    async fn write_table_rows(
        &mut self,
        rows: Vec<TableRow>,
        table_id: TableId,
    ) -> Result<(), Self::Error> {
        let event_sender = self.event_senders.get_mut(&table_id).unwrap();
        for row in rows {
            event_sender
                .send(TableEvent::Append {
                    row: PostgresTableRow(row).into(),
                })
                .await
                .unwrap();
        }
        event_sender
            .send(TableEvent::Commit { lsn: 0 })
            .await
            .unwrap();
        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        // TODO: support multiple tables in a transaction
        //
        let mut table_id_in_transaction: Option<u32> = None;
        let mut lsn_in_transaction: Option<u64> = None;

        for event in events {
            println!("Received CDC event: {:?}", event);
            match event {
                CdcEvent::Begin(begin_body) => {
                    lsn_in_transaction = Some(begin_body.final_lsn());
                }
                CdcEvent::Commit(commit_body) => {
                    if let Some(table_id) = table_id_in_transaction {
                        let event_sender = self.event_senders.get_mut(&table_id).unwrap();
                        event_sender
                            .send(TableEvent::Commit {
                                lsn: commit_body.commit_lsn(),
                            })
                            .await
                            .unwrap();
                        table_id_in_transaction = None;
                    }
                }
                CdcEvent::Insert((table_id, table_row)) => {
                    if let Some(prev_id) = table_id_in_transaction {
                        assert!(
                            prev_id == table_id,
                            "Multiple tables in a transaction are not supported"
                        );
                    }
                    table_id_in_transaction = Some(table_id);
                    let event_sender = self.event_senders.get_mut(&table_id).unwrap();
                    event_sender
                        .send(TableEvent::Append {
                            row: PostgresTableRow(table_row).into(),
                        })
                        .await
                        .unwrap();
                }
                CdcEvent::Update((table_id, old_table_row, new_table_row)) => {
                    if let Some(prev_id) = table_id_in_transaction {
                        assert!(
                            prev_id == table_id,
                            "Multiple tables in a transaction are not supported"
                        );
                    }
                    table_id_in_transaction = Some(table_id);
                    let event_sender = self.event_senders.get_mut(&table_id).unwrap();
                    event_sender
                        .send(TableEvent::Delete {
                            row: PostgresTableRow(old_table_row.unwrap()).into(),
                            lsn: (lsn_in_transaction.unwrap()),
                        })
                        .await
                        .unwrap();
                    event_sender
                        .send(TableEvent::Append {
                            row: PostgresTableRow(new_table_row).into(),
                        })
                        .await
                        .unwrap();
                }
                CdcEvent::Delete((table_id, table_row)) => {
                    if let Some(prev_id) = table_id_in_transaction {
                        assert!(
                            prev_id == table_id,
                            "Multiple tables in a transaction are not supported"
                        );
                    }
                    table_id_in_transaction = Some(table_id);
                    let event_sender = self.event_senders.get_mut(&table_id).unwrap();
                    event_sender
                        .send(TableEvent::Delete {
                            row: PostgresTableRow(table_row).into(),
                            lsn: (lsn_in_transaction.unwrap()),
                        })
                        .await
                        .unwrap();
                }
                CdcEvent::Relation(relation_body) => println!("Relation {relation_body:?}"),
                CdcEvent::Type(type_body) => println!("Type {type_body:?}"),
                CdcEvent::KeepAliveRequested { .. } => {}
            }
        }
        Ok(PgLsn::from(0))
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        println!("table {table_id} copied");
        Ok(())
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        println!("table {table_id} truncated");
        Ok(())
    }
}

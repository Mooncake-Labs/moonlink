use async_trait::async_trait;
use pg_replicate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::{
        sinks::{BatchSink, InfallibleSinkError},
        PipelineResumptionState,
    },
    table::{TableId, TableSchema},
};
use std::collections::{HashMap, HashSet};
use tokio_postgres::types::PgLsn;

pub struct Sink;

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
        println!("{table_schemas:?}");
        Ok(())
    }

    async fn write_table_rows(
        &mut self,
        rows: Vec<TableRow>,
        _table_id: TableId,
    ) -> Result<(), Self::Error> {
        for row in rows {
            println!("{row:?}");
        }
        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        for event in events {
            match &event {
                CdcEvent::Begin(begin_body) => println!("Begin {begin_body:?}"),
                CdcEvent::Commit(commit_body) => println!("Commit {commit_body:?}"),
                CdcEvent::Insert((table_id, table_row)) => {
                    println!("Insert {table_id:?} {table_row:?}")
                }
                CdcEvent::Update((table_id, old_table_row, new_table_row)) => {
                    println!("Update {table_id:?} {old_table_row:?} {new_table_row:?}")
                }
                CdcEvent::Delete((table_id, table_row)) => {
                    println!("Delete {table_id:?} {table_row:?}")
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

use crate::storage::memtable::MemTable;
use crate::util::table_schema_to_arrow_schema;
use async_trait::async_trait;
use pg_replicate::conversions::Cell;
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
pub struct Sink {
    tables: HashMap<TableId, MemTable>,
}

impl Sink {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
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
        for (table_id, table_schema) in table_schemas {
            self.tables.insert(
                table_id,
                MemTable::new(table_schema_to_arrow_schema(&table_schema)),
            );
        }
        Ok(())
    }

    async fn write_table_rows(
        &mut self,
        rows: Vec<TableRow>,
        _table_id: TableId,
    ) -> Result<(), Self::Error> {
        for row in rows {
            if let Cell::I64(id) = row.values[0] {
                self.tables
                    .get_mut(&_table_id)
                    .unwrap()
                    .append(id, &row)
                    .unwrap();
            } else {
                println!("Invalid primary key type: {:?}", row.values[0]);
            }
        }
        println!(
            "{:?}",
            self.tables
                .get(&_table_id)
                .unwrap()
                .column_store
                .export()
                .unwrap()
        );
        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        for event in events {
            match &event {
                CdcEvent::Begin(begin_body) => println!("Begin {begin_body:?}"),
                CdcEvent::Commit(commit_body) => println!("Commit {commit_body:?}"),
                CdcEvent::Insert((table_id, table_row)) => {
                    if let Cell::I64(id) = table_row.values[0] {
                        self.tables
                            .get_mut(table_id)
                            .unwrap()
                            .append(id, table_row)
                            .unwrap();
                    } else {
                        println!("Invalid primary key type: {:?}", table_row.values[0]);
                    }
                }
                CdcEvent::Update((table_id, old_table_row, new_table_row)) => {
                    if let Cell::I64(id) = old_table_row.as_ref().unwrap().values[0] {
                        self.tables.get_mut(table_id).unwrap().delete(id);
                    } else {
                        println!(
                            "Invalid primary key type: {:?}",
                            old_table_row.as_ref().unwrap().values[0]
                        );
                    }
                    if let Cell::I64(id) = new_table_row.values[0] {
                        self.tables
                            .get_mut(table_id)
                            .unwrap()
                            .append(id, new_table_row)
                            .unwrap();
                    } else {
                        println!("Invalid primary key type: {:?}", new_table_row.values[0]);
                    }
                }
                CdcEvent::Delete((table_id, table_row)) => {
                    if let Cell::I64(id) = table_row.values[0] {
                        self.tables.get_mut(table_id).unwrap().delete(id);
                    } else {
                        println!("Invalid primary key type: {:?}", table_row.values[0]);
                    }
                }
                CdcEvent::Relation(relation_body) => println!("Relation {relation_body:?}"),
                CdcEvent::Type(type_body) => println!("Type {type_body:?}"),
                CdcEvent::KeepAliveRequested { .. } => {}
            }
        }
        for table in self.tables.values_mut() {
            println!("{:?}", table.column_store.export().unwrap());
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

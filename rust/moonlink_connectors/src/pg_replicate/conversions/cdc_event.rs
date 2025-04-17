use core::str;
use std::{collections::HashMap, str::Utf8Error};

use postgres_replication::protocol::{
    BeginBody, CommitBody, DeleteBody, InsertBody, LogicalReplicationMessage, RelationBody,
    ReplicationMessage, StreamAbortBody, StreamCommitBody, StreamStartBody, StreamStopBody,
    TupleData, TypeBody, UpdateBody,
};
use thiserror::Error;

use crate::pg_replicate::{
    pipeline::batching::BatchBoundary,
    table::{ColumnSchema, TableId, TableSchema},
};

use super::{
    table_row::TableRow,
    text::{FromTextError, TextFormatConverter},
    Cell,
};

#[derive(Debug, Error)]
pub enum CdcEventConversionError {
    #[error("message not supported")]
    MessageNotSupported,

    #[error("unknown replication message")]
    UnknownReplicationMessage,

    #[error("binary format not yet supported")]
    BinaryFormatNotSupported,

    #[error("unsupported type: {0}")]
    UnsupportedType(String),

    #[error("missing tuple in delete body")]
    MissingTupleInDeleteBody,

    #[error("schema missing for table id {0}")]
    MissingSchema(TableId),

    #[error("from bytes error: {0}")]
    FromBytes(#[from] FromTextError),

    #[error("invalid string value")]
    InvalidStr(#[from] Utf8Error),
}

pub struct CdcEventConverter;

impl CdcEventConverter {
    fn try_from_tuple_data_slice(
        column_schemas: &[ColumnSchema],
        tuple_data: &[TupleData],
    ) -> Result<TableRow, CdcEventConversionError> {
        let mut values = Vec::with_capacity(column_schemas.len());

        for (i, column_schema) in column_schemas.iter().enumerate() {
            let cell = match &tuple_data[i] {
                TupleData::Null => Cell::Null,
                TupleData::UnchangedToast => TextFormatConverter::default_value(&column_schema.typ),
                TupleData::Text(bytes) => {
                    let str = str::from_utf8(&bytes[..])?;
                    TextFormatConverter::try_from_str(&column_schema.typ, str)?
                }
            };
            values.push(cell);
        }

        Ok(TableRow { values })
    }

    fn try_from_insert_body(
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        insert_body: InsertBody,
    ) -> Result<CdcEvent, CdcEventConversionError> {
        let row =
            Self::try_from_tuple_data_slice(column_schemas, insert_body.tuple().tuple_data())?;

        Ok(CdcEvent::Insert((table_id, row, insert_body.xid())))
    }

    //TODO: handle when identity columns are changed
    fn try_from_update_body(
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        update_body: UpdateBody,
    ) -> Result<CdcEvent, CdcEventConversionError> {
        let old_row = update_body
            .old_tuple()
            .map(|tuple| Self::try_from_tuple_data_slice(column_schemas, tuple.tuple_data()))
            .transpose()?;
        let new_row =
            Self::try_from_tuple_data_slice(column_schemas, update_body.new_tuple().tuple_data())?;

        Ok(CdcEvent::Update((
            table_id,
            old_row,
            new_row,
            update_body.xid(),
        )))
    }

    fn try_from_delete_body(
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        delete_body: DeleteBody,
    ) -> Result<CdcEvent, CdcEventConversionError> {
        let tuple = delete_body
            .key_tuple()
            .or(delete_body.old_tuple())
            .ok_or(CdcEventConversionError::MissingTupleInDeleteBody)?;

        let row = Self::try_from_tuple_data_slice(column_schemas, tuple.tuple_data())?;

        Ok(CdcEvent::Delete((table_id, row, delete_body.xid())))
    }

    pub fn try_from(
        value: ReplicationMessage<LogicalReplicationMessage>,
        table_schemas: &HashMap<TableId, TableSchema>,
    ) -> Result<CdcEvent, CdcEventConversionError> {
        match value {
            ReplicationMessage::XLogData(xlog_data) => match xlog_data.into_data() {
                LogicalReplicationMessage::Begin(begin_body) => Ok(CdcEvent::Begin(begin_body)),
                LogicalReplicationMessage::Commit(commit_body) => Ok(CdcEvent::Commit(commit_body)),
                LogicalReplicationMessage::Origin(_) => {
                    Err(CdcEventConversionError::MessageNotSupported)
                }
                LogicalReplicationMessage::Relation(relation_body) => {
                    Ok(CdcEvent::Relation(relation_body))
                }
                LogicalReplicationMessage::Type(type_body) => Ok(CdcEvent::Type(type_body)),
                LogicalReplicationMessage::Insert(insert_body) => {
                    let table_id = insert_body.rel_id();
                    let column_schemas = &table_schemas
                        .get(&table_id)
                        .ok_or(CdcEventConversionError::MissingSchema(table_id))?
                        .column_schemas;
                    Ok(Self::try_from_insert_body(
                        table_id,
                        column_schemas,
                        insert_body,
                    )?)
                }
                LogicalReplicationMessage::Update(update_body) => {
                    let table_id = update_body.rel_id();
                    let column_schemas = &table_schemas
                        .get(&table_id)
                        .ok_or(CdcEventConversionError::MissingSchema(table_id))?
                        .column_schemas;
                    Ok(Self::try_from_update_body(
                        table_id,
                        column_schemas,
                        update_body,
                    )?)
                }
                LogicalReplicationMessage::Delete(delete_body) => {
                    let table_id = delete_body.rel_id();
                    let column_schemas = &table_schemas
                        .get(&table_id)
                        .ok_or(CdcEventConversionError::MissingSchema(table_id))?
                        .column_schemas;
                    Ok(Self::try_from_delete_body(
                        table_id,
                        column_schemas,
                        delete_body,
                    )?)
                }
                LogicalReplicationMessage::Truncate(_) => {
                    Err(CdcEventConversionError::MessageNotSupported)
                }
                LogicalReplicationMessage::StreamStart(stream_start_body) => {
                    Ok(CdcEvent::StreamStart(stream_start_body))
                }
                LogicalReplicationMessage::StreamStop(stream_stop_body) => {
                    Ok(CdcEvent::StreamStop(stream_stop_body))
                }
                LogicalReplicationMessage::StreamCommit(stream_commit_body) => {
                    Ok(CdcEvent::StreamCommit(stream_commit_body))
                }
                LogicalReplicationMessage::StreamAbort(stream_abort_body) => {
                    Ok(CdcEvent::StreamAbort(stream_abort_body))
                }
                _ => Err(CdcEventConversionError::UnknownReplicationMessage),
            },
            ReplicationMessage::PrimaryKeepAlive(keep_alive) => Ok(CdcEvent::KeepAliveRequested {
                reply: keep_alive.reply() == 1,
            }),
            _ => Err(CdcEventConversionError::UnknownReplicationMessage),
        }
    }
}

#[derive(Debug)]
pub enum CdcEvent {
    Begin(BeginBody),
    Commit(CommitBody),
    Insert((TableId, TableRow, Option<u32>)),
    Update((TableId, Option<TableRow>, TableRow, Option<u32>)),
    Delete((TableId, TableRow, Option<u32>)),
    Relation(RelationBody),
    Type(TypeBody),
    KeepAliveRequested { reply: bool },
    StreamStart(StreamStartBody),
    StreamStop(StreamStopBody),
    StreamCommit(StreamCommitBody),
    StreamAbort(StreamAbortBody),
}

impl BatchBoundary for CdcEvent {
    fn is_last_in_batch(&self) -> bool {
        matches!(
            self,
            CdcEvent::Commit(_)
                | CdcEvent::StreamCommit(_)
                | CdcEvent::StreamStop(_)
                | CdcEvent::StreamAbort(_)
                | CdcEvent::KeepAliveRequested { reply: _ }
        )
    }
}

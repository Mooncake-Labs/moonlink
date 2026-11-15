use core::str;
use std::collections::HashMap;

use postgres_replication::protocol::{
    BeginBody, CommitBody, DeleteBody, InsertBody, LogicalReplicationMessage, PrimaryKeepAliveBody,
    RelationBody, ReplicationMessage, StreamAbortBody, StreamCommitBody, StreamStartBody,
    StreamStopBody, TupleData, TypeBody, UpdateBody,
};
use thiserror::Error;

use crate::pg_replicate::table::{ColumnSchema, SrcTableId, TableSchema};

use super::{table_row::TableRow, text::TextFormatConverter, Cell};

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

    /// Mirrors identify rows by all their columns, so updates and deletes need the complete old
    /// row, which postgres only sends under REPLICA IDENTITY FULL (set per partition).
    #[error("{operation} on the source table did not include the complete old row; the table (and each of its partitions) must have REPLICA IDENTITY FULL")]
    MissingOldRow {
        src_table_id: SrcTableId,
        operation: &'static str,
    },

    /// An unchanged out-of-line (TOAST) value with no old row to take it from.
    #[error("column {column}: unchanged TOAST value without the old row")]
    UnresolvedToast {
        src_table_id: SrcTableId,
        column: String,
    },

    #[error("schema missing for table id {0}")]
    MissingSchema(SrcTableId),

    #[error("table id {src_table_id} sent {received} columns, its schema has {expected}")]
    TupleShapeMismatch {
        src_table_id: SrcTableId,
        expected: usize,
        received: usize,
    },

    /// A value Postgres sent that moonlink cannot represent faithfully.
    #[error("column {column} ({type_name}): cannot convert {value}: {reason}")]
    InvalidValue {
        src_table_id: SrcTableId,
        column: String,
        type_name: String,
        value: String,
        reason: String,
    },
}

impl CdcEventConversionError {
    /// The table a conversion error belongs to, if it is specific to one table. Such errors fail
    /// that table only; the rest of the stream is unaffected.
    pub fn src_table_id(&self) -> Option<SrcTableId> {
        match self {
            Self::TupleShapeMismatch { src_table_id, .. }
            | Self::InvalidValue { src_table_id, .. }
            | Self::MissingOldRow { src_table_id, .. }
            | Self::UnresolvedToast { src_table_id, .. } => Some(*src_table_id),
            _ => None,
        }
    }
}

/// Values are quoted in errors to help users find the offending row; keep the message bounded.
fn excerpt(value: &str) -> String {
    const MAX_CHARS: usize = 64;
    if value.chars().count() <= MAX_CHARS {
        format!("'{value}'")
    } else {
        let head: String = value.chars().take(MAX_CHARS).collect();
        format!("'{head}...'")
    }
}

pub struct CdcEventConverter;

impl CdcEventConverter {
    /// Decode a tuple. `unchanged_from` supplies the values of columns postgres reports as
    /// unchanged TOAST ('u'), which it only does for the new row of an UPDATE.
    fn try_from_tuple_data_slice(
        src_table_id: SrcTableId,
        column_schemas: &[ColumnSchema],
        tuple_data: &[TupleData],
        unchanged_from: Option<&TableRow>,
    ) -> Result<TableRow, CdcEventConversionError> {
        if tuple_data.len() != column_schemas.len() {
            return Err(CdcEventConversionError::TupleShapeMismatch {
                src_table_id,
                expected: column_schemas.len(),
                received: tuple_data.len(),
            });
        }
        let mut values = Vec::with_capacity(column_schemas.len());

        for (i, column_schema) in column_schemas.iter().enumerate() {
            let cell = match &tuple_data[i] {
                TupleData::Null => Cell::Null,
                TupleData::UnchangedToast => match unchanged_from {
                    Some(old_row) => old_row.values[i].clone(),
                    None => {
                        return Err(CdcEventConversionError::UnresolvedToast {
                            src_table_id,
                            column: column_schema.name.clone(),
                        })
                    }
                },
                TupleData::Text(bytes) => {
                    let invalid =
                        |value: String, reason: String| CdcEventConversionError::InvalidValue {
                            src_table_id,
                            column: column_schema.name.clone(),
                            type_name: column_schema.typ.name().to_string(),
                            value,
                            reason,
                        };
                    let text = str::from_utf8(&bytes[..]).map_err(|e| {
                        invalid(excerpt(&String::from_utf8_lossy(&bytes[..])), e.to_string())
                    })?;
                    TextFormatConverter::try_from_str(&column_schema.typ, text)
                        .map_err(|e| invalid(excerpt(text), e.to_string()))?
                }
            };
            values.push(cell);
        }

        Ok(TableRow { values })
    }

    fn try_from_insert_body(
        src_table_id: SrcTableId,
        column_schemas: &[ColumnSchema],
        insert_body: InsertBody,
    ) -> Result<CdcEvent, CdcEventConversionError> {
        let row = Self::try_from_tuple_data_slice(
            src_table_id,
            column_schemas,
            insert_body.tuple().tuple_data(),
            None,
        )?;

        Ok(CdcEvent::Insert((src_table_id, row, insert_body.xid())))
    }

    // TODO: handle when identity columns are changed
    fn try_from_update_body(
        src_table_id: SrcTableId,
        column_schemas: &[ColumnSchema],
        update_body: UpdateBody,
    ) -> Result<CdcEvent, CdcEventConversionError> {
        // Under REPLICA IDENTITY FULL the old row is complete (detoasted), so it can fill the new
        // row's unchanged TOAST columns. A key-only old row ('K') is not enough to find the row.
        let old_tuple = update_body
            .old_tuple()
            .ok_or(CdcEventConversionError::MissingOldRow {
                src_table_id,
                operation: "UPDATE",
            })?;
        let old_row = Self::try_from_tuple_data_slice(
            src_table_id,
            column_schemas,
            old_tuple.tuple_data(),
            None,
        )?;
        let new_row = Self::try_from_tuple_data_slice(
            src_table_id,
            column_schemas,
            update_body.new_tuple().tuple_data(),
            Some(&old_row),
        )?;

        Ok(CdcEvent::Update((
            src_table_id,
            Some(old_row),
            new_row,
            update_body.xid(),
        )))
    }

    fn try_from_delete_body(
        src_table_id: SrcTableId,
        column_schemas: &[ColumnSchema],
        delete_body: DeleteBody,
    ) -> Result<CdcEvent, CdcEventConversionError> {
        let tuple = delete_body
            .old_tuple()
            .ok_or(CdcEventConversionError::MissingOldRow {
                src_table_id,
                operation: "DELETE",
            })?;

        let row = Self::try_from_tuple_data_slice(
            src_table_id,
            column_schemas,
            tuple.tuple_data(),
            None,
        )?;

        Ok(CdcEvent::Delete((src_table_id, row, delete_body.xid())))
    }

    pub fn try_from(
        value: ReplicationMessage<LogicalReplicationMessage>,
        table_schemas: &HashMap<SrcTableId, TableSchema>,
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
            ReplicationMessage::PrimaryKeepAlive(primary_keepalive_body) => {
                Ok(CdcEvent::PrimaryKeepAlive(primary_keepalive_body))
            }
            _ => Err(CdcEventConversionError::UnknownReplicationMessage),
        }
    }
}

#[derive(Debug)]
pub enum CdcEvent {
    Begin(BeginBody),
    Commit(CommitBody),
    Insert((SrcTableId, TableRow, Option<u32>)),
    Update((SrcTableId, Option<TableRow>, TableRow, Option<u32>)),
    Delete((SrcTableId, TableRow, Option<u32>)),
    Relation(RelationBody),
    Type(TypeBody),
    PrimaryKeepAlive(PrimaryKeepAliveBody),
    StreamStart(StreamStartBody),
    StreamStop(StreamStopBody),
    StreamCommit(StreamCommitBody),
    StreamAbort(StreamAbortBody),
    /// The table can no longer be mirrored (e.g. its schema changed); it enters the errored state.
    TableUnsupported {
        src_table_id: SrcTableId,
        reason: String,
    },
}

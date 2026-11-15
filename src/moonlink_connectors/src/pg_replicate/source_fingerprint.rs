//! What a mirror was built against, so changes the replication stream does not show are caught.
//!
//! pgoutput only describes a table by column names, types and typmods, and only when it next
//! sends a change for it. It never shows a column that was dropped and re-added with the same
//! shape (the existing rows lose their values), generated columns, partitions attached with rows
//! or detached, or any change made while moonlink was stopped. The fingerprint records the
//! source table's oid, every live column (attnum, name, type, typmod, generated) and its
//! partitions; it is persisted when the mirror is created, compared at recovery, and polled while
//! replicating. Any difference errors the mirror, except new partitions that hold nothing the
//! mirror lacks: every row they hold was written after they joined the table, so it was published
//! through it.

use crate::errored_tables::table_file_path;
use crate::pg_replicate::table::{LookupKey, SrcTableId, TableSchema};
use moonlink::MooncakeTableId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::io::ErrorKind;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, SimpleQueryMessage};
use tracing::warn;

const FINGERPRINT_DIRECTORY: &str = "source_fingerprints";

/// Version of the values a mirror stores. Mirrors without a fingerprint were written by an older
/// version (1), whose json, character(n), unconstrained numeric and array values differ from what
/// this version produces for the same source rows.
pub const VALUE_FORMAT_VERSION: u32 = 2;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceColumn {
    pub attnum: i32,
    pub name: String,
    pub type_oid: u32,
    pub typmod: i32,
    pub generated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceFingerprint {
    pub value_format_version: u32,
    pub relid: SrcTableId,
    pub columns: Vec<SourceColumn>,
    /// Every member of the partition tree below the table (empty for a plain table).
    pub partitions: BTreeSet<u32>,
}

/// How the source compares with a recorded fingerprint.
#[derive(Debug, PartialEq, Eq)]
pub enum Drift {
    Unchanged,
    /// Only new partitions whose rows were all published: adopt `current`. `new_partitions` are
    /// their names.
    NewEmptyPartitions {
        current: SourceFingerprint,
        new_partitions: Vec<String>,
    },
    /// The mirror cannot follow; the reason is user-facing.
    Changed(String),
}

fn rows(messages: Vec<SimpleQueryMessage>) -> Vec<tokio_postgres::SimpleQueryRow> {
    messages
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect()
}

fn field<T: std::str::FromStr>(row: &tokio_postgres::SimpleQueryRow, idx: usize) -> T
where
    T::Err: std::fmt::Debug,
{
    row.get(idx)
        .expect("catalog column is not null")
        .parse()
        .expect("catalog column parses")
}

/// The source table's current fingerprint, or None if no table has that oid any more.
pub async fn fetch(
    client: &Client,
    relid: SrcTableId,
) -> Result<Option<SourceFingerprint>, tokio_postgres::Error> {
    let exists = rows(
        client
            .simple_query(&format!("SELECT 1 FROM pg_class WHERE oid = {relid};"))
            .await?,
    );
    if exists.is_empty() {
        return Ok(None);
    }
    let columns = rows(
        client
            .simple_query(&format!(
                "SELECT attnum, attname, atttypid, atttypmod, attgenerated <> '' FROM pg_attribute \
                 WHERE attrelid = {relid} AND attnum > 0 AND NOT attisdropped ORDER BY attnum;"
            ))
            .await?,
    )
    .iter()
    .map(|row| SourceColumn {
        attnum: field(row, 0),
        name: row.get(1).unwrap_or_default().to_string(),
        type_oid: field(row, 2),
        typmod: field(row, 3),
        generated: row.get(4) == Some("t"),
    })
    .collect();
    let partitions = rows(
        client
            .simple_query(&format!(
                "SELECT relid::oid FROM pg_partition_tree({relid}::oid::regclass) \
                 WHERE relid <> {relid}::oid::regclass;"
            ))
            .await?,
    )
    .iter()
    .map(|row| field(row, 0))
    .collect();
    Ok(Some(SourceFingerprint {
        value_format_version: VALUE_FORMAT_VERSION,
        relid,
        columns,
        partitions,
    }))
}

/// Name of a relation, and whether it holds no rows; None if it no longer exists.
async fn emptiness(
    client: &Client,
    relid: u32,
) -> Result<Option<(String, bool)>, tokio_postgres::Error> {
    let name = rows(
        client
            .simple_query(&format!(
                "SELECT oid::regclass::text FROM pg_class WHERE oid = {relid};"
            ))
            .await?,
    );
    let Some(name) = name.first().and_then(|row| row.get(0)).map(str::to_string) else {
        return Ok(None);
    };
    let any = rows(
        client
            .simple_query(&format!("SELECT EXISTS (SELECT 1 FROM {name});"))
            .await?,
    );
    let empty = any.first().and_then(|row| row.get(0)) == Some("f");
    Ok(Some((name, empty)))
}

/// Name of a partition, and whether it holds rows written before it was attached. Those were never
/// published through the table; rows written after the attach (in later transactions, or later in
/// the attaching one) were. None if it is no longer a partition.
async fn rows_from_before_attach(
    client: &Client,
    relid: u32,
) -> Result<Option<(String, bool)>, tokio_postgres::Error> {
    let attach = rows(
        client
            .simple_query(&format!(
                "SELECT c.oid::regclass::text, i.xmin::text, i.cmin::text FROM pg_class c \
                 JOIN pg_inherits i ON i.inhrelid = c.oid WHERE c.oid = {relid};"
            ))
            .await?,
    );
    let Some(row) = attach.first() else {
        return Ok(None);
    };
    let (name, xmin, cmin) = (
        row.get(0).unwrap_or_default().to_string(),
        row.get(1).unwrap_or_default().to_string(),
        row.get(2).unwrap_or_default().to_string(),
    );
    // age() orders transaction ids (frozen rows are the oldest).
    let older = rows(
        client
            .simple_query(&format!(
                "SELECT EXISTS (SELECT 1 FROM {name} WHERE age(xmin) > age('{xmin}'::xid) \
                 OR (xmin = '{xmin}'::xid AND cmin::text::bigint < {cmin}));"
            ))
            .await?,
    );
    Ok(Some((
        name,
        older.first().and_then(|r| r.get(0)) == Some("t"),
    )))
}

/// Compare the source with `expected`.
pub async fn drift(
    client: &Client,
    expected: &SourceFingerprint,
) -> Result<Drift, tokio_postgres::Error> {
    let Some(current) = fetch(client, expected.relid).await? else {
        return Ok(Drift::Changed(
            "the source table was dropped (or dropped and re-created)".to_string(),
        ));
    };
    if current.columns != expected.columns {
        return Ok(Drift::Changed(format!(
            "{}; schema changes are not supported",
            describe_column_change(&expected.columns, &current.columns)
        )));
    }
    for removed in expected.partitions.difference(&current.partitions) {
        match emptiness(client, *removed).await? {
            Some((_, true)) => {}
            Some((name, false)) => {
                return Ok(Drift::Changed(format!(
                    "partition {name} was detached with its rows"
                )))
            }
            None => {
                return Ok(Drift::Changed(
                    "a partition was dropped (its rows left the table without being replicated)"
                        .to_string(),
                ))
            }
        }
    }
    let mut new_partitions = Vec::new();
    for added in current.partitions.difference(&expected.partitions) {
        match rows_from_before_attach(client, *added).await? {
            Some((name, false)) => new_partitions.push(name),
            Some((name, true)) => {
                return Ok(Drift::Changed(format!(
                    "partition {name} was attached with rows that were never replicated"
                )))
            }
            // Attached and dropped again in between: the next comparison sees the final state.
            None => {}
        }
    }
    if current == *expected {
        return Ok(Drift::Unchanged);
    }
    Ok(Drift::NewEmptyPartitions {
        current,
        new_partitions,
    })
}

/// The same wording as the replication stream's schema check, plus what only the catalog shows.
fn describe_column_change(old: &[SourceColumn], new: &[SourceColumn]) -> String {
    let names = |columns: &[SourceColumn]| {
        columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    };
    if names(old) != names(new) {
        return format!(
            "its columns changed from ({}) to ({})",
            names(old),
            names(new)
        );
    }
    let changed = |differs: fn(&SourceColumn, &SourceColumn) -> bool| -> Vec<&str> {
        old.iter()
            .zip(new)
            .filter(|(o, n)| differs(o, n))
            .map(|(o, _)| o.name.as_str())
            .collect()
    };
    let readded = changed(|o, n| o.attnum != n.attnum);
    if !readded.is_empty() {
        return format!(
            "column(s) {} were dropped and added again (existing rows lost their values)",
            readded.join(", ")
        );
    }
    let generated = changed(|o, n| o.generated != n.generated);
    if !generated.is_empty() {
        return format!("column(s) {} became generated", generated.join(", "));
    }
    format!(
        "the type of column(s) {} changed",
        changed(|o, n| o != n).join(", ")
    )
}

/// Columns of a keyless table whose stored values changed format since version 1. Rows are
/// identified by every column in such a mirror, so old rows would no longer match their updates
/// and deletes.
pub fn columns_with_changed_value_format(schema: &TableSchema) -> Vec<String> {
    if !matches!(schema.lookup_key, LookupKey::FullRow) {
        return Vec::new();
    }
    schema
        .column_schemas
        .iter()
        .filter(|c| {
            let stable = matches!(
                c.typ,
                Type::BOOL
                    | Type::INT2
                    | Type::INT4
                    | Type::INT8
                    | Type::FLOAT4
                    | Type::FLOAT8
                    | Type::TEXT
                    | Type::VARCHAR
                    | Type::NAME
                    | Type::DATE
                    | Type::TIME
                    | Type::TIMESTAMP
                    | Type::TIMESTAMPTZ
                    | Type::UUID
                    | Type::BYTEA
                    | Type::OID
            ) || (c.typ == Type::NUMERIC && c.modifier != -1);
            !stable
        })
        .map(|c| c.name.clone())
        .collect()
}

pub async fn write(
    base_path: &str,
    mooncake_table_id: &MooncakeTableId,
    fingerprint: &SourceFingerprint,
) -> std::io::Result<()> {
    let path = table_file_path(base_path, FINGERPRINT_DIRECTORY, mooncake_table_id);
    tokio::fs::create_dir_all(path.parent().unwrap()).await?;
    let tmp = path.with_extension("tmp");
    tokio::fs::write(&tmp, serde_json::to_vec(fingerprint)?).await?;
    tokio::fs::rename(&tmp, &path).await
}

/// The recorded fingerprint; None for a mirror written by version 1 (or an unreadable file).
pub async fn read(
    base_path: &str,
    mooncake_table_id: &MooncakeTableId,
) -> Option<SourceFingerprint> {
    let path = table_file_path(base_path, FINGERPRINT_DIRECTORY, mooncake_table_id);
    let bytes = tokio::fs::read(&path).await.ok()?;
    match serde_json::from_slice(&bytes) {
        Ok(fingerprint) => Some(fingerprint),
        Err(e) => {
            warn!(error = %e, table = %mooncake_table_id, "unreadable source fingerprint");
            None
        }
    }
}

pub async fn remove(base_path: &str, mooncake_table_id: &MooncakeTableId) {
    let path = table_file_path(base_path, FINGERPRINT_DIRECTORY, mooncake_table_id);
    if let Err(e) = tokio::fs::remove_file(path).await {
        if e.kind() != ErrorKind::NotFound {
            warn!(error = %e, table = %mooncake_table_id, "failed to remove source fingerprint");
        }
    }
}

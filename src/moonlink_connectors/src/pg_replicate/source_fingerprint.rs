//! What a mirror was built against, so changes the replication stream does not show are caught.
//!
//! pgoutput only describes a table by column names, types and typmods, and only when it next
//! sends a change for it. It never shows a column that was dropped and re-added with the same
//! shape (the existing rows lose their values), generated columns, partitions attached with rows
//! or detached, or any change made while moonlink was stopped. The fingerprint records the
//! source table's oid, every live column (attnum, name, type, typmod, generated) and its
//! partitions; it is persisted when the mirror is created, compared at recovery, and polled while
//! replicating. Any difference errors the mirror, except a new partition that is still empty when
//! it is first seen (created ahead of use): it is adopted, and its replica identity set. A
//! partition that already holds rows, or any partition that leaves the table (detached or
//! dropped), errors the mirror: whether those rows were ever replicated cannot be told from the
//! catalog. So does an unlogged or foreign partition, whose changes are never decoded, and an
//! inheritance child of a regular table.

use crate::errored_tables::table_file_path;
use crate::pg_replicate::table::{LookupKey, SrcTableId, TableSchema};
use moonlink::MooncakeTableId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
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
    /// Each member's pg_inherits row version (xmin). DETACH deletes that row and ATTACH inserts a
    /// new one, so a partition detached and attached again between two checks shows here.
    #[serde(default)]
    pub attachments: BTreeMap<u32, String>,
}

/// How the source compares with a recorded fingerprint.
#[derive(Debug, PartialEq, Eq)]
pub enum Drift {
    Unchanged,
    /// Only new, empty partitions: adopt `current`. `new_partitions` are their names.
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
    let attachments = rows(
        client
            .simple_query(&format!(
                "SELECT inhrelid, xmin::text FROM pg_inherits WHERE inhrelid IN \
                 (SELECT relid FROM pg_partition_tree({relid}::oid::regclass) \
                  WHERE relid <> {relid}::oid::regclass);"
            ))
            .await?,
    )
    .iter()
    .map(|row| (field(row, 0), row.get(1).unwrap_or_default().to_string()))
    .collect();
    Ok(Some(SourceFingerprint {
        value_format_version: VALUE_FORMAT_VERSION,
        relid,
        columns,
        partitions,
        attachments,
    }))
}

/// Whether a regular table has inheritance children (partitions of a partitioned table are
/// published through it and do not count).
async fn has_inheritance_children(
    client: &Client,
    relid: SrcTableId,
) -> Result<bool, tokio_postgres::Error> {
    let found = rows(
        client
            .simple_query(&format!(
                "SELECT 1 FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhparent \
                 WHERE i.inhparent = {relid} AND c.relkind = 'r' LIMIT 1;"
            ))
            .await?,
    );
    Ok(!found.is_empty())
}

/// A partition whose changes logical decoding never sees (unlogged, or a foreign table), as a
/// user-facing description; None if every partition is a regular permanent table. Partitioned
/// members have no storage (their rows are in their leaves, checked themselves), so their own
/// persistence does not matter.
pub async fn unreplicated_partition(
    client: &Client,
    relid: SrcTableId,
) -> Result<Option<String>, tokio_postgres::Error> {
    let found = rows(
        client
            .simple_query(&format!(
                "SELECT c.oid::regclass::text, c.relkind = 'f' FROM \
                 pg_partition_tree({relid}::oid::regclass) t JOIN pg_class c ON c.oid = t.relid \
                 WHERE t.relid <> {relid}::oid::regclass \
                 AND ((c.relpersistence <> 'p' AND c.relkind <> 'p') OR c.relkind = 'f') ORDER BY 1 LIMIT 1;"
            ))
            .await?,
    );
    Ok(found.first().map(|row| {
        let name = row.get(0).unwrap_or_default();
        let what = if row.get(1) == Some("t") {
            "a foreign table"
        } else {
            "unlogged"
        };
        format!("partition {name} is {what}, so postgres never sends its changes")
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
    // By size, not by reading it: the mirror's role may lack SELECT on a partition another role
    // created, and row-level security could hide its rows. Pages left by deleted rows make it
    // look non-empty, which errors the mirror (loud, never silent).
    let size = rows(
        client
            .simple_query(&format!(
                "SELECT COALESCE(SUM(pg_relation_size(relid)), 0) FROM \
                 pg_partition_tree({relid}::oid::regclass) WHERE isleaf;"
            ))
            .await?,
    );
    let empty = size.first().and_then(|row| row.get(0)) == Some("0");
    Ok(Some((name, empty)))
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
    if has_inheritance_children(client, expected.relid).await? {
        return Ok(Drift::Changed(
            "it gained inheritance children, whose changes are not replicated with it (and \
             which a query of the table includes)"
                .to_string(),
        ));
    }
    if let Some(partition) = unreplicated_partition(client, expected.relid).await? {
        return Ok(Drift::Changed(format!(
            "{partition}; only regular (logged) partitions are supported"
        )));
    }
    // Rows left the table when a partition was detached; re-attaching it does not bring the
    // mirror's copy of them in line with what it holds now.
    let reattached = expected.attachments.iter().any(|(partition, xmin)| {
        current
            .attachments
            .get(partition)
            .is_some_and(|now| now != xmin)
    });
    if reattached || !expected.partitions.is_subset(&current.partitions) {
        return Ok(Drift::Changed(
            "a partition was detached (whether or not attached again) or dropped; partition \
             changes other than adding empty partitions are not supported"
                .to_string(),
        ));
    }
    let mut new_partitions = Vec::new();
    for added in current.partitions.difference(&expected.partitions) {
        match emptiness(client, *added).await? {
            Some((name, true)) => new_partitions.push(name),
            Some((name, false)) => {
                return Ok(Drift::Changed(format!(
                    "partition {name} already held rows when it was added; only empty partitions \
                     can be added"
                )))
            }
            // Added and dropped again in between: the next comparison sees the final state.
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

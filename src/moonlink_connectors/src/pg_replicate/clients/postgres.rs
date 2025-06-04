use std::collections::{HashMap, HashSet};

use pg_escape::{quote_identifier, quote_literal};
use postgres_replication::LogicalReplicationStream;
use thiserror::Error;
use tokio_postgres::{
    config::ReplicationMode,
    types::{Kind, PgLsn, Type},
    Client as PostgresClient, Config, CopyOutStream, NoTls, SimpleQueryMessage, SimpleQueryRow,
};
use tracing::{info, warn};

use crate::pg_replicate::conversions::text::TextFormatConverter;
use crate::pg_replicate::table::{ColumnSchema, LookupKey, TableId, TableName, TableSchema};

pub struct SlotInfo {
    pub confirmed_flush_lsn: PgLsn,
}

/// A client for Postgres logical replication
pub struct ReplicationClient {
    postgres_client: PostgresClient,
    in_txn: bool,
}

#[derive(Debug, Error)]
pub enum ReplicationClientError {
    #[error("tokio_postgres error: {0}")]
    TokioPostgresError(#[from] tokio_postgres::Error),

    #[error("column {0} is missing from table {1}")]
    MissingColumn(String, String),

    #[error("publication {0} doesn't exist")]
    MissingPublication(String),

    #[error("oid column is not a valid u32")]
    OidColumnNotU32,

    #[error("replica identity '{0}' not supported")]
    ReplicaIdentityNotSupported(String),

    #[error("type modifier column is not a valid u32")]
    TypeModifierColumnNotI32,

    #[error("column {0}'s type with oid {1} in relation {2} is not supported")]
    UnsupportedType(String, u32, String),

    #[error("table {0} doesn't exist")]
    MissingTable(TableName),

    #[error("not a valid PgLsn")]
    InvalidPgLsn,

    #[error("failed to create slot")]
    FailedToCreateSlot,
}

impl ReplicationClient {
    /// Connect to a postgres database in logical replication mode without TLS
    pub async fn connect_no_tls(uri: &str) -> Result<ReplicationClient, ReplicationClientError> {
        info!("connecting to postgres");

        let mut config = uri.parse::<Config>()?;
        config.replication_mode(ReplicationMode::Logical);
        let (postgres_client, connection) = config.connect(NoTls).await?;

        tokio::spawn(async move {
            info!("waiting for connection to terminate");
            if let Err(e) = connection.await {
                warn!("connection error: {}", e);
            }
        });

        info!("successfully connected to postgres");

        Ok(ReplicationClient {
            postgres_client,
            in_txn: false,
        })
    }

    /// Starts a read-only trasaction with repeatable read isolation level
    pub async fn begin_readonly_transaction(&mut self) -> Result<(), ReplicationClientError> {
        self.postgres_client
            .simple_query("abort; begin read only isolation level repeatable read;")
            .await?;
        self.in_txn = true;
        Ok(())
    }

    /// Commits a transaction
    pub async fn commit_txn(&mut self) -> Result<(), ReplicationClientError> {
        if self.in_txn {
            self.postgres_client.simple_query("commit;").await?;
            self.in_txn = false;
        }
        Ok(())
    }

    async fn rollback_txn(&mut self) -> Result<(), ReplicationClientError> {
        if self.in_txn {
            self.postgres_client.simple_query("rollback;").await?;
            self.in_txn = false;
        }
        Ok(())
    }

    /// Returns a [CopyOutStream] for a table
    pub async fn get_table_copy_stream(
        &self,
        table_name: &TableName,
        column_schemas: &[ColumnSchema],
    ) -> Result<CopyOutStream, ReplicationClientError> {
        let column_list = column_schemas
            .iter()
            .map(|col| quote_identifier(&col.name))
            .collect::<Vec<_>>()
            .join(", ");

        let copy_query = format!(
            r#"COPY {} ({column_list}) TO STDOUT WITH (FORMAT text);"#,
            table_name.as_quoted_identifier(),
        );

        let stream = self.postgres_client.copy_out_simple(&copy_query).await?;

        Ok(stream)
    }

    /// Returns a vector of columns of a table, optionally filtered by a publication's column list
    pub async fn get_column_schemas(
        &self,
        table_id: TableId,
        table_name: &TableName,
        publication: Option<&str>,
    ) -> Result<Vec<ColumnSchema>, ReplicationClientError> {
        let (pub_cte, pub_pred) = if let Some(publication) = publication {
            (
                format!(
                    "with pub_attrs as (
                        select unnest(r.prattrs) AS pub_attnum
                        from pg_publication_rel r
                        left join pg_publication p on r.prpubid = p.oid
                        where p.pubname = {}
                        and r.prrelid = {}
                    )",
                    quote_literal(publication),
                    table_id
                ),
                "and (
                    case (select count(*) from pub_attrs)
                    when 0 then true
                    else (a.attnum in (select pub_attnum from pub_attrs))
                    end
                )",
            )
        } else {
            ("".into(), "")
        };

        let column_info_query = format!(
            "{}
            select a.attname,
                a.atttypid,
                a.atttypmod,
                a.attnotnull,
                coalesce(i.indisprimary, false) as primary
            from pg_attribute a
            left join pg_index i
                on a.attrelid = i.indrelid
                and a.attnum = any(i.indkey)
                and i.indisprimary = true
            where a.attnum > 0::int2
            and not a.attisdropped
            and a.attgenerated = ''
            and a.attrelid = {}
            {}
            order by a.attnum
            ",
            pub_cte, table_id, pub_pred
        );

        let mut column_schemas = vec![];

        for message in self
            .postgres_client
            .simple_query(&column_info_query)
            .await?
        {
            if let SimpleQueryMessage::Row(row) = message {
                let name = row
                    .try_get("attname")?
                    .ok_or(ReplicationClientError::MissingColumn(
                        "attname".to_string(),
                        "pg_attribute".to_string(),
                    ))?
                    .to_string();

                let type_oid = row
                    .try_get("atttypid")?
                    .ok_or(ReplicationClientError::MissingColumn(
                        "atttypid".to_string(),
                        "pg_attribute".to_string(),
                    ))?
                    .parse()
                    .map_err(|_| ReplicationClientError::OidColumnNotU32)?;

                // Fail fast on any type that we are not able to parse in try_from_str.
                let typ = Type::from_oid(type_oid).ok_or_else(|| {
                    ReplicationClientError::UnsupportedType(
                        name.clone(),
                        type_oid,
                        table_name.to_string(),
                    )
                })?;

                if !TextFormatConverter::is_supported_type(&typ) {
                    return Err(ReplicationClientError::UnsupportedType(
                        name.clone(),
                        type_oid,
                        table_name.to_string(),
                    ));
                }

                let modifier = row
                    .try_get("atttypmod")?
                    .ok_or(ReplicationClientError::MissingColumn(
                        "atttypmod".to_string(),
                        "pg_attribute".to_string(),
                    ))?
                    .parse()
                    .map_err(|_| ReplicationClientError::TypeModifierColumnNotI32)?;

                let nullable =
                    row.try_get("attnotnull")?
                        .ok_or(ReplicationClientError::MissingColumn(
                            "attnotnull".to_string(),
                            "pg_attribute".to_string(),
                        ))?
                        == "f";

                column_schemas.push(ColumnSchema {
                    name,
                    typ,
                    modifier,
                    nullable,
                })
            }
        }

        Ok(column_schemas)
    }

    async fn fetch_lookup_key(
        &self,
        table_id: TableId,
        published_column_names: HashSet<String>,
    ) -> Result<Option<LookupKey>, ReplicationClientError> {
        let index_rows = self.fetch_index_rows(table_id).await?;

        for index_row in index_rows {
            let index_name = match index_row.get("index_name") {
                Some(name) => name.to_string(),
                None => continue,
            };

            let indkey = match index_row.get("indkey") {
                Some(key) => key,
                None => continue,
            };

            let column_infos = self.fetch_index_columns(table_id, indkey).await?;

            let mut columns = Vec::new();
            if column_infos.iter().any(|(_, not_null)| !not_null) {
                continue;
            }

            for (col_name, _) in &column_infos {
                columns.push(col_name.clone());
            }

            let all_columns_published = columns
                .iter()
                .all(|name| published_column_names.contains(name));

            if all_columns_published {
                return Ok(Some(LookupKey::Key {
                    name: index_name,
                    columns,
                }));
            }
        }

        Ok(None)
    }

    /// Finds a valid index for lookup key
    /// must be unique, not partial, not deferrable, and include only columns marked NOT NULL.
    /// Follows same definition as PG replica identity [https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY]
    async fn fetch_index_rows(
        &self,
        table_id: TableId,
    ) -> Result<Vec<SimpleQueryRow>, ReplicationClientError> {
        let query = format!(
            "
            SELECT
                c2.relname AS index_name,
                i.indkey,
                i.indisunique,
                i.indisprimary,
                i.indpred IS NOT NULL AS is_partial,
                COALESCE(con.condeferrable, false) AS is_deferrable
            FROM pg_index i
            JOIN pg_class c1 ON c1.oid = i.indrelid
            JOIN pg_class c2 ON c2.oid = i.indexrelid
            LEFT JOIN pg_constraint con ON con.conindid = i.indexrelid
            WHERE c1.oid = {}
            AND (i.indisunique OR i.indisprimary)
            AND i.indpred IS NULL
            AND (con.condeferrable IS NULL OR con.condeferrable = false)
            ORDER BY i.indisprimary DESC, c2.relname
            ",
            table_id
        );

        let result = self.postgres_client.simple_query(&query).await?;

        Ok(result
            .into_iter()
            .filter_map(|msg| match msg {
                SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .collect())
    }

    async fn fetch_index_columns(
        &self,
        table_id: TableId,
        indkey: &str,
    ) -> Result<Vec<(String, bool)>, ReplicationClientError> {
        let query = format!(
            "
            SELECT a.attname, a.attnotnull
            FROM pg_attribute a
            WHERE a.attrelid = {}
            AND a.attnum = ANY(string_to_array('{}', ' ')::smallint[])
            ORDER BY array_position(string_to_array('{}', ' ')::smallint[], a.attnum)
            ",
            table_id, indkey, indkey
        );

        let result = self.postgres_client.simple_query(&query).await?;

        Ok(result
            .into_iter()
            .filter_map(|msg| match msg {
                SimpleQueryMessage::Row(row) => {
                    let name = row.get("attname")?.to_string();
                    let not_null = row.get("attnotnull") == Some("t");
                    Some((name, not_null))
                }
                _ => None,
            })
            .collect())
    }

    pub async fn get_lookup_key(
        &self,
        table_id: TableId,
        column_schemas: &Vec<ColumnSchema>,
    ) -> Result<LookupKey, ReplicationClientError> {
        let column_names: HashSet<String> =
            column_schemas.iter().map(|cs| cs.name.clone()).collect();
        if let Some(unique_index_key) = self.fetch_lookup_key(table_id, column_names).await? {
            return Ok(unique_index_key);
        }

        // default to full row
        Ok(LookupKey::FullRow)
    }

    pub async fn get_table_schemas(
        &self,
        table_names: &[TableName],
        publication: Option<&str>,
    ) -> Result<HashMap<TableId, TableSchema>, ReplicationClientError> {
        let mut table_schemas = HashMap::new();

        for table_name in table_names {
            let table_schema = self
                .get_table_schema(table_name.clone(), publication)
                .await?;
            table_schemas.insert(table_schema.table_id, table_schema);
        }

        Ok(table_schemas)
    }

    pub async fn get_table_schema(
        &self,
        table_name: TableName,
        publication: Option<&str>,
    ) -> Result<TableSchema, ReplicationClientError> {
        let table_id = self
            .get_table_id(&table_name)
            .await?
            .ok_or(ReplicationClientError::MissingTable(table_name.clone()))?;

        let column_schemas = self
            .get_column_schemas(table_id, &table_name, publication)
            .await?;
        let lookup_key = self.get_lookup_key(table_id, &column_schemas).await?;

        let table_schema = TableSchema {
            table_name,
            table_id,
            column_schemas,
            lookup_key,
        };
        Ok(table_schema)
    }

    /// Returns the table id (called relation id in Postgres) of a table
    /// Also checks whether the replica identity is default or full and
    /// returns an error if not.
    pub async fn get_table_id(
        &self,
        table: &TableName,
    ) -> Result<Option<TableId>, ReplicationClientError> {
        let quoted_schema = quote_literal(&table.schema);
        let quoted_name = quote_literal(&table.name);

        let table_info_query = format!(
            "select c.oid,
                c.relreplident
            from pg_class c
            join pg_namespace n
                on (c.relnamespace = n.oid)
            where n.nspname = {}
                and c.relname = {}
            ",
            quoted_schema, quoted_name
        );

        for message in self.postgres_client.simple_query(&table_info_query).await? {
            if let SimpleQueryMessage::Row(row) = message {
                let replica_identity =
                    row.try_get("relreplident")?
                        .ok_or(ReplicationClientError::MissingColumn(
                            "relreplident".to_string(),
                            "pg_class".to_string(),
                        ))?;

                if !(replica_identity == "f") {
                    return Err(ReplicationClientError::ReplicaIdentityNotSupported(
                        replica_identity.to_string(),
                    ));
                }

                let oid: u32 = row
                    .try_get("oid")?
                    .ok_or(ReplicationClientError::MissingColumn(
                        "oid".to_string(),
                        "pg_class".to_string(),
                    ))?
                    .parse()
                    .map_err(|_| ReplicationClientError::OidColumnNotU32)?;
                return Ok(Some(oid));
            }
        }

        Ok(None)
    }

    /// Returns the slot info of an existing slot. The slot info currently only has the
    /// confirmed_flush_lsn column of the pg_replication_slots table.
    async fn get_slot(&self, slot_name: &str) -> Result<Option<SlotInfo>, ReplicationClientError> {
        let query = format!(
            r#"select confirmed_flush_lsn from pg_replication_slots where slot_name = {};"#,
            quote_literal(slot_name)
        );

        let query_result = self.postgres_client.simple_query(&query).await?;

        for res in &query_result {
            if let SimpleQueryMessage::Row(row) = res {
                let confirmed_flush_lsn = row
                    .get("confirmed_flush_lsn")
                    .ok_or(ReplicationClientError::MissingColumn(
                        "confirmed_flush_lsn".to_string(),
                        "pg_replication_slots".to_string(),
                    ))?
                    .parse()
                    .map_err(|_| ReplicationClientError::InvalidPgLsn)?;

                return Ok(Some(SlotInfo {
                    confirmed_flush_lsn,
                }));
            }
        }

        Ok(None)
    }

    /// Creates a logical replication slot. This will only succeed if the postgres connection
    /// is in logical replication mode. Otherwise it will fail with the following error:
    /// `syntax error at or near "CREATE_REPLICATION_SLOT"``
    ///
    /// Returns the consistent_point column as slot info.
    async fn create_slot(&self, slot_name: &str) -> Result<SlotInfo, ReplicationClientError> {
        let query = format!(
            r#"CREATE_REPLICATION_SLOT {} LOGICAL pgoutput USE_SNAPSHOT"#,
            quote_identifier(slot_name)
        );
        let results = self.postgres_client.simple_query(&query).await?;

        for result in results {
            if let SimpleQueryMessage::Row(row) = result {
                let consistent_point: PgLsn = row
                    .get("consistent_point")
                    .ok_or(ReplicationClientError::MissingColumn(
                        "consistent_point".to_string(),
                        "create_replication_slot".to_string(),
                    ))?
                    .parse()
                    .map_err(|_| ReplicationClientError::InvalidPgLsn)?;
                return Ok(SlotInfo {
                    confirmed_flush_lsn: consistent_point,
                });
            }
        }
        Err(ReplicationClientError::FailedToCreateSlot)
    }

    /// Either return the slot info of an existing slot or creates a new
    /// slot and returns its slot info.
    pub async fn get_or_create_slot(
        &mut self,
        slot_name: &str,
    ) -> Result<SlotInfo, ReplicationClientError> {
        if let Some(slot_info) = self.get_slot(slot_name).await? {
            Ok(slot_info)
        } else {
            self.rollback_txn().await?;
            self.begin_readonly_transaction().await?;
            Ok(self.create_slot(slot_name).await?)
        }
    }

    /// Returns all table names in a publication
    pub async fn get_publication_table_names(
        &self,
        publication: &str,
    ) -> Result<Vec<TableName>, ReplicationClientError> {
        let publication_query = format!(
            "select schemaname, tablename from pg_publication_tables where pubname = {};",
            quote_literal(publication)
        );

        let mut table_names = vec![];
        for msg in self
            .postgres_client
            .simple_query(&publication_query)
            .await?
        {
            if let SimpleQueryMessage::Row(row) = msg {
                let schema = row
                    .get(0)
                    .ok_or(ReplicationClientError::MissingColumn(
                        "schemaname".to_string(),
                        "pg_publication_tables".to_string(),
                    ))?
                    .to_string();

                let name = row
                    .get(1)
                    .ok_or(ReplicationClientError::MissingColumn(
                        "tablename".to_string(),
                        "pg_publication_tables".to_string(),
                    ))?
                    .to_string();

                table_names.push(TableName { schema, name })
            }
        }

        Ok(table_names)
    }

    pub async fn publication_exists(
        &self,
        publication: &str,
    ) -> Result<bool, ReplicationClientError> {
        let publication_exists_query = format!(
            "select 1 as exists from pg_publication where pubname = {};",
            quote_literal(publication)
        );
        for msg in self
            .postgres_client
            .simple_query(&publication_exists_query)
            .await?
        {
            if let SimpleQueryMessage::Row(_) = msg {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub async fn get_logical_replication_stream(
        &self,
        publication: &str,
        slot_name: &str,
        start_lsn: PgLsn,
    ) -> Result<LogicalReplicationStream, ReplicationClientError> {
        let options = format!(
            r#"("proto_version" '2', "publication_names" {}, "streaming" 'on')"#,
            quote_literal(publication),
        );

        let query = format!(
            r#"START_REPLICATION SLOT {} LOGICAL {} {}"#,
            quote_identifier(slot_name),
            start_lsn,
            options
        );

        let copy_stream = self
            .postgres_client
            .copy_both_simple::<bytes::Bytes>(&query)
            .await?;

        let stream = LogicalReplicationStream::new(copy_stream, Some(2));

        Ok(stream)
    }
}

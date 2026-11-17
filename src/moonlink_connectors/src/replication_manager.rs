use crate::errored_tables::{self, ErroredTable};
use crate::pg_replicate::source_fingerprint;
use crate::pg_replicate::table::SrcTableId;
use crate::pg_replicate::{PostgresConnection, SourceIdentity, RETRY_TASK_LIMIT};
use crate::ReplicationConnection;
use crate::{Error, Result};
use moonlink::{
    MooncakeTableId, MoonlinkTableConfig, ObjectStorageCache, ReadStateManager, TableEventManager,
};
use moonlink::{ReadStateFilepathRemap, TableStatus, TableStatusReader};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

/// How long a request waits in all for earlier connections to the same database to finish
/// shutting down, before it fails with a retryable error. Longer than [`RETRY_TASK_LIMIT`], so a
/// shutdown that panicked (its retries end within that) or that finishes slowly is waited out.
pub const SHUTDOWN_WAIT: Duration = Duration::from_secs(45);

/// A shutdown still running after this is taken as hung: it is reported and no longer waited for,
/// so it cannot block new mirrors of its database until moonlink restarts.
const SHUTDOWN_TASK_LIMIT: Duration = Duration::from_secs(120);

/// How a background shutdown ended: of a replication connection, or of an errored mirror's
/// source. It drops its database's publication and slot and finishes its retried removals from
/// the publication, which a new connection to that database shares.
#[derive(Clone, Copy, Debug)]
enum ShutdownEnd {
    Finished,
    /// It panicked: retries it started may still run until [`RETRY_TASK_LIMIT`] after this.
    Panicked(Instant),
}

type ShutdownWatch = watch::Receiver<Option<ShutdownEnd>>;

fn shutdown_ended(end: Option<ShutdownEnd>) -> bool {
    match end {
        Some(ShutdownEnd::Finished) => true,
        Some(ShutdownEnd::Panicked(at)) => at.elapsed() >= RETRY_TASK_LIMIT,
        None => false,
    }
}

/// The background shutdowns a new connection to a database has to wait for.
pub struct PendingShutdowns(Vec<ShutdownWatch>);

impl PendingShutdowns {
    /// Whether all of them ended (checked without waiting).
    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|watch| shutdown_ended(*watch.borrow()))
    }

    /// Wait until they ended, at most [`SHUTDOWN_WAIT`]. Cancel-safe: nothing is consumed.
    pub async fn wait(self) -> Result<()> {
        self.wait_until(Instant::now() + SHUTDOWN_WAIT).await
    }

    /// Wait until they ended, or fail with a retryable error at `deadline`. Cancel-safe.
    pub async fn wait_until(mut self, deadline: Instant) -> Result<()> {
        let all_ended = async {
            for watch in self.0.iter_mut() {
                loop {
                    let end = *watch.borrow_and_update();
                    if let Some(ShutdownEnd::Panicked(at)) = end {
                        tokio::time::sleep_until(at + RETRY_TASK_LIMIT).await;
                        break;
                    }
                    // Ended, or its watcher is gone (the runtime is shutting down).
                    if shutdown_ended(end) || watch.changed().await.is_err() {
                        break;
                    }
                }
            }
        };
        tokio::time::timeout_at(deadline, all_ended)
            .await
            .map_err(|_| Error::source_shutting_down("this database"))
    }
}

/// Manage replication sources, one per source database.
///
/// Connections are keyed by [`SourceIdentity::key`], not by the connection string: each database
/// has a single replication slot and publication, so two spellings of the same database (another
/// user, host alias, options) must share one connection instead of taking the slot over from it.
///
/// This struct abstracts the lifecycle of `MoonlinkPostgresSource` and
/// provides a single entry point to add new tables to a running
/// replication. A new replication will automatically be started when a
/// table is added for a URI that is not currently being replicated.
pub struct ReplicationManager {
    /// Maps from source key to replication connection.
    connections: HashMap<String, ReplicationConnection>,
    /// Maps every connection string seen so far to its source key.
    uri_keys: HashMap<String, String>,
    /// Maps from mooncake table id to (source key, source table id).
    table_info: HashMap<MooncakeTableId, (String, SrcTableId)>,
    /// Recovered mirrors that are not served (errored before, or failed to recover).
    errored_tables: HashMap<MooncakeTableId, ErroredTable>,
    /// Mirrors that could not be restored for a transient reason, retried until they can, with
    /// their connection strings. The slot of their database is held meanwhile, so they replay
    /// exactly once restored.
    pending_recoveries: HashMap<MooncakeTableId, String>,
    /// Hold every slot (while recovering: a mirror not restored yet must find its changes).
    hold_all_slots: bool,
    /// Base directory for mooncake tables.
    table_base_path: String,
    /// Object storage cache.
    object_storage_cache: ObjectStorageCache,
    /// Background shutdowns per source key (see [`ShutdownEnd`]).
    shutdowns: HashMap<String, Vec<ShutdownWatch>>,
    /// The source keys of the connection strings of background shutdowns, so a request can wait
    /// for them by connection string before it takes the manager lock.
    shutdown_uri_keys: HashMap<String, String>,
}

impl ReplicationManager {
    pub fn new(table_base_path: String, object_storage_cache: ObjectStorageCache) -> Self {
        Self {
            connections: HashMap::new(),
            uri_keys: HashMap::new(),
            table_info: HashMap::new(),
            errored_tables: HashMap::new(),
            pending_recoveries: HashMap::new(),
            hold_all_slots: false,
            table_base_path,
            object_storage_cache,
            shutdowns: HashMap::new(),
            shutdown_uri_keys: HashMap::new(),
        }
    }

    /// The background shutdowns a new connection for `src_uri` has to wait for, as far as this
    /// connection string is known. For waiting without holding the manager lock.
    pub fn pending_shutdowns_for_uri(&self, src_uri: &str) -> PendingShutdowns {
        let key = self
            .uri_keys
            .get(src_uri)
            .or_else(|| self.shutdown_uri_keys.get(src_uri));
        self.pending_shutdowns(key.map(String::as_str))
    }

    /// The background shutdowns of `src_uri`'s database, found by its source key (contacting the
    /// source the first time), whatever connection string they were for. For checking under the
    /// manager lock without waiting there.
    pub async fn pending_shutdowns_for_source(
        &mut self,
        src_uri: &str,
    ) -> Result<PendingShutdowns> {
        let key = self.source_key(src_uri).await?;
        Ok(self.pending_shutdowns(Some(&key)))
    }

    fn pending_shutdowns(&self, key: Option<&str>) -> PendingShutdowns {
        PendingShutdowns(
            key.and_then(|key| self.shutdowns.get(key))
                .cloned()
                .unwrap_or_default(),
        )
    }

    /// Track a background shutdown of the database with source key `key`.
    fn track_shutdown(&mut self, key: &str, uris: Vec<String>, task: JoinHandle<Result<()>>) {
        self.prune_shutdowns();
        let (tx, rx) = watch::channel(None);
        tokio::spawn(async move {
            let mut task = task;
            let end = match tokio::time::timeout(SHUTDOWN_TASK_LIMIT, &mut task).await {
                Ok(Ok(Ok(()))) => ShutdownEnd::Finished,
                Ok(Ok(Err(e))) => {
                    warn!(error = ?e, "a replication connection shut down with an error");
                    ShutdownEnd::Finished
                }
                Ok(Err(e)) => {
                    warn!(error = ?e, "a replication connection's shutdown task failed");
                    ShutdownEnd::Panicked(Instant::now())
                }
                Err(_) => {
                    error!(
                        "a replication connection's shutdown is still running after \
                         {SHUTDOWN_TASK_LIMIT:?}; new mirrors of its database no longer wait for it, \
                         and its publication or slot may need to be dropped by hand"
                    );
                    ShutdownEnd::Finished
                }
            };
            let _ = tx.send(Some(end));
        });
        self.shutdowns.entry(key.to_string()).or_default().push(rx);
        for uri in uris {
            self.shutdown_uri_keys.insert(uri, key.to_string());
        }
    }

    /// Forget background shutdowns that ended.
    fn prune_shutdowns(&mut self) {
        self.shutdowns.retain(|_, watches| {
            watches.retain(|watch| !shutdown_ended(*watch.borrow()));
            !watches.is_empty()
        });
        let shutdowns = &self.shutdowns;
        self.shutdown_uri_keys
            .retain(|_, key| shutdowns.contains_key(key));
    }

    /// Resolve a connection string to its source key, contacting the source the first time.
    async fn source_key(&mut self, src_uri: &str) -> Result<String> {
        if let Some(key) = self.uri_keys.get(src_uri) {
            return Ok(key.clone());
        }
        let key = SourceIdentity::identify(src_uri).await?.key();
        self.uri_keys.insert(src_uri.to_string(), key.clone());
        Ok(key)
    }

    pub async fn get_or_create_connection(
        &mut self,
        src_uri: &str,
    ) -> Result<&mut ReplicationConnection> {
        let key = self.source_key(src_uri).await?;
        if !self.connections.contains_key(&key) {
            debug!(%src_uri, "creating replication connection");
            // An earlier connection to this database may still be shutting down: dropping its
            // publication and slot, or retrying the removal of a dropped mirror's table from the
            // publication. Both are shared with the new connection. Callers usually waited for
            // this before taking the manager lock already (see pending_shutdowns_for_uri).
            self.pending_shutdowns(Some(&key)).wait().await?;
            self.prune_shutdowns();

            tokio::fs::create_dir_all(&self.table_base_path).await?;
            let base_path = tokio::fs::canonicalize(&self.table_base_path).await?;
            let replication_connection = ReplicationConnection::new(
                src_uri.to_string(),
                base_path.to_str().unwrap().to_string(),
                self.object_storage_cache.clone(),
            )
            .await?;
            self.connections.insert(key.clone(), replication_connection);
        }
        let replication_connection = self.connections.get_mut(&key).unwrap();
        if replication_connection.uri() != src_uri {
            info!(
                "reusing the replication connection already open for this database; \
                 its connection string is used for the new table too"
            );
        }
        Ok(replication_connection)
    }

    /// Add a table to be replicated from the given `uri`.
    ///
    /// If replication for this `uri` is not yet running a new replication
    /// source will be created.
    ///
    /// Returns the LSN the rows of a new mirror's initial copy are committed at, if it copied any;
    /// they are lost on a restart until a snapshot at that LSN is persisted.
    ///
    /// # Arguments
    ///
    /// * secret_entry: secret necessary to access object storage, use local filesystem if not assigned.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_table(
        &mut self,
        src_uri: &str,
        mooncake_table_id: MooncakeTableId,
        table_name: &str,
        moonlink_table_config: &mut MoonlinkTableConfig,
        read_state_filepath_remap: ReadStateFilepathRemap,
        is_recovery: bool,
    ) -> Result<Option<u64>> {
        debug!(%src_uri, table_name, "adding table through manager");

        // Error handling: don't allow duplicate mooncake table id be registered.
        if self.table_info.contains_key(&mooncake_table_id)
            || self.errored_tables.contains_key(&mooncake_table_id)
        {
            return Err(Error::repl_duplicate_table(mooncake_table_id.to_string()));
        }

        let key = self.source_key(src_uri).await?;
        self.get_or_create_connection(src_uri).await?;
        // Before replication can confirm anything for a new connection.
        self.refresh_slot_holds();
        let added = match self
            .check_can_add(&key, src_uri, table_name, is_recovery)
            .await
        {
            Ok(()) => {
                let replication_connection = self.connections.get_mut(&key).unwrap();
                if !replication_connection.replication_started() {
                    replication_connection.start_replication().await?;
                }
                replication_connection
                    .add_table_replication(
                        table_name,
                        &mooncake_table_id,
                        moonlink_table_config,
                        read_state_filepath_remap,
                        is_recovery,
                    )
                    .await
            }
            Err(e) => Err(e),
        };
        let (src_table_id, initial_copy_lsn) = match added {
            Ok(added) => added,
            Err(e) => {
                // A failed first mirror must not leave a replication slot behind that holds the
                // source's WAL forever (unless a mirror waiting to be restored needs it).
                let connection = self.connections.get(&key).unwrap();
                if connection.table_count() == 0 && !is_recovery {
                    let drop_all = !self.pending_mirror_needs_slot(&key);
                    self.shutdown_connection_by_key(&key, drop_all);
                }
                return Err(e);
            }
        };

        assert!(self
            .table_info
            .insert(mooncake_table_id.clone(), (key, src_table_id))
            .is_none());

        debug!(src_table_id, "table added through manager");

        Ok(initial_copy_lsn)
    }

    /// Checks before adding a table through the connection of `key`: a second connection string
    /// must be able to read the table itself, and a source table whose old mirror is still an
    /// errored (or pending) placeholder must have that dropped first: a new mirror next to it
    /// could re-apply history the slot replays for the old one.
    async fn check_can_add(
        &mut self,
        key: &str,
        src_uri: &str,
        table_name: &str,
        is_recovery: bool,
    ) -> Result<()> {
        let connection_uri = self.connections.get(key).unwrap().uri().to_string();
        if connection_uri != src_uri {
            crate::pg_replicate::check_role_can_read_table(src_uri, table_name).await?;
        }
        if is_recovery {
            return Ok(());
        }
        let placeholders: Vec<(MooncakeTableId, String)> = self
            .errored_tables
            .iter()
            .filter(|(_, errored)| self.uri_matches_key(&errored.src_uri, key))
            .map(|(id, errored)| (id.clone(), errored.src_table_name.clone()))
            .collect();
        if placeholders.is_empty() {
            return Ok(());
        }
        let connection = self.connections.get_mut(key).unwrap();
        let Some(oid) = connection.source_table_oid(table_name).await? else {
            return Ok(());
        };
        for (old_mirror, old_table) in placeholders {
            if connection.source_table_oid(&old_table).await? == Some(oid) {
                return Err(Error::unsupported_source_table(
                    table_name,
                    &format!(
                        "its earlier mirror {old_mirror} is errored; drop that mirror first, then \
                         create the new one"
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Message of a mirror persisted as errored by an earlier run, if any.
    pub async fn persisted_errored_message(
        &self,
        mooncake_table_id: &MooncakeTableId,
    ) -> Option<String> {
        errored_tables::read_marker(&self.table_base_path, mooncake_table_id).await
    }

    /// Register a recovered mirror that is not served: every request on it returns `message`, and
    /// dropping it deletes its data. With `persist`, it stays errored across restarts.
    pub async fn add_errored_table(
        &mut self,
        mooncake_table_id: MooncakeTableId,
        errored: ErroredTable,
        persist: bool,
    ) {
        if persist {
            if let Err(e) = errored_tables::write_marker(
                &self.table_base_path,
                &mooncake_table_id,
                &errored.message,
            )
            .await
            {
                warn!(error = %e, table = %mooncake_table_id, "failed to persist errored-table marker");
            }
        }
        self.errored_tables.insert(mooncake_table_id, errored);
    }

    /// Register a mirror that could not be restored for a transient reason (see
    /// [`Self::pending_recoveries`]); requests on it return `errored.message` until it is
    /// restored by [`Self::begin_retry`] / [`Self::end_retry`].
    pub fn add_pending_table(&mut self, mooncake_table_id: MooncakeTableId, errored: ErroredTable) {
        self.pending_recoveries
            .insert(mooncake_table_id.clone(), errored.src_uri.clone());
        self.errored_tables.insert(mooncake_table_id, errored);
        self.refresh_slot_holds();
    }

    pub fn pending_table_ids(&self) -> Vec<MooncakeTableId> {
        self.pending_recoveries.keys().cloned().collect()
    }

    /// Mirrors that are not served after a restart: (id, connection string, pending).
    pub fn placeholders(&self) -> Vec<(MooncakeTableId, String, bool)> {
        let mut placeholders: Vec<_> = self
            .errored_tables
            .iter()
            .map(|(id, errored)| {
                (
                    id.clone(),
                    errored.src_uri.clone(),
                    self.pending_recoveries.contains_key(id),
                )
            })
            .collect();
        placeholders.sort_by(|a, b| a.0.to_string().cmp(&b.0.to_string()));
        placeholders
    }

    /// Whether a replication connection is open for the source `key`.
    pub fn has_connection(&self, key: &str) -> bool {
        self.connections.contains_key(key)
    }

    /// Unregister a pending mirror's placeholder so it can be added again; its slot stays held
    /// until [`Self::end_retry`].
    pub fn begin_retry(&mut self, mooncake_table_id: &MooncakeTableId) -> Option<ErroredTable> {
        if !self.pending_recoveries.contains_key(mooncake_table_id) {
            return None;
        }
        self.errored_tables.remove(mooncake_table_id)
    }

    /// Finish a retry: restored (`None`), still failing for a transient reason, or failed for good
    /// (persisted as errored).
    pub async fn end_retry(
        &mut self,
        mooncake_table_id: MooncakeTableId,
        failure: Option<(ErroredTable, /*transient=*/ bool)>,
    ) {
        match failure {
            None => {
                self.pending_recoveries.remove(&mooncake_table_id);
            }
            Some((errored, true)) => {
                self.errored_tables.insert(mooncake_table_id, errored);
            }
            Some((errored, false)) => {
                self.pending_recoveries.remove(&mooncake_table_id);
                self.add_errored_table(mooncake_table_id, errored, /*persist=*/ true)
                    .await;
            }
        }
        self.refresh_slot_holds();
    }

    /// Whether `uri` names the source `key`: by its resolved identity, or, for a source not
    /// reached yet, by the database it names alone (its port may be mapped, e.g. a container or
    /// a pooler; holding a slot too many is the safe mistake).
    fn uri_matches_key(&self, uri: &str, key: &str) -> bool {
        if let Some(resolved) = self.uri_keys.get(uri) {
            return resolved == key;
        }
        let Ok(config) = uri.parse::<tokio_postgres::Config>() else {
            return true;
        };
        let database = config
            .get_dbname()
            .or_else(|| config.get_user())
            .unwrap_or_default()
            .to_string();
        let key_database = key.splitn(3, '/').nth(2);
        key_database == Some(database.as_str())
    }

    /// Hold every slot until [`Self::set_hold_all_slots`] is called with false (during recovery,
    /// before every mirror is either restored or pending).
    pub fn set_hold_all_slots(&mut self, hold: bool) {
        self.hold_all_slots = hold;
        self.refresh_slot_holds();
    }

    /// Whether a mirror waiting to be restored may replay from the slot of the source `key`: it
    /// must be neither advanced nor dropped meanwhile.
    fn pending_mirror_needs_slot(&self, key: &str) -> bool {
        self.pending_recoveries
            .values()
            .any(|uri| self.uri_matches_key(uri, key))
    }

    /// Hold the slot of every connection whose database has a pending mirror; release the rest.
    fn refresh_slot_holds(&self) {
        for (key, connection) in &self.connections {
            connection.set_slot_hold(self.hold_all_slots || self.pending_mirror_needs_slot(key));
        }
    }

    /// After dropping a placeholder: take its table out of the publication, and if no live
    /// mirror of its database remains, drop the database's slot and publication as dropping the
    /// last live mirror would (placeholders never resume, so nothing needs them). Best effort.
    async fn release_errored_source(
        &mut self,
        errored: &ErroredTable,
        src_table_id: Option<SrcTableId>,
    ) {
        let key = match self.source_key(&errored.src_uri).await {
            Ok(key) => key,
            Err(e) => {
                warn!(error = ?e, "source of the dropped mirror is unreachable; slot left in place");
                return;
            }
        };
        let needed_by_pending_mirror = self.pending_mirror_needs_slot(&key);
        let name_has_other_placeholder = self.errored_tables.values().any(|other| {
            other.src_table_name == errored.src_table_name
                && self.uri_matches_key(&other.src_uri, &key)
        });
        let live_connection = self.connections.get_mut(&key);
        if let Some(connection) = live_connection {
            let src_table_id = match src_table_id {
                Some(src_table_id) => Some(src_table_id),
                // A mirror recorded before source fingerprints: its table is the one that has its
                // name now.
                // Unless another mirror of this database waiting to be restored is for that name.
                None if name_has_other_placeholder => None,
                None => match connection.source_table_oid(&errored.src_table_name).await {
                    Ok(src_table_id) => src_table_id,
                    Err(e) => {
                        warn!(error = ?e, "cannot look up the dropped mirror's source table; leaving it in the publication");
                        None
                    }
                },
            };
            let Some(src_table_id) = src_table_id else {
                return;
            };
            // The same source table can be mirrored again while the placeholder exists; that
            // mirror still needs it published.
            if connection.mirrors_source_oid(src_table_id) {
                return;
            }
            if let Err(e) = connection.remove_table_from_publication(src_table_id).await {
                warn!(error = ?e, "failed to remove the dropped mirror's table from the publication");
            }
            return;
        }
        if needed_by_pending_mirror {
            return;
        }
        // In the background, tracked like a connection's shutdown: a new connection to this
        // database waits for it.
        // The whole publication is dropped, so the table needs no removal of its own.
        let src_uri = errored.src_uri.clone();
        let cleanup = tokio::spawn(async move {
            let mut source = PostgresConnection::new(src_uri).await?;
            source.shutdown(/*drop_all=*/ true).await
        });
        self.track_shutdown(&key, vec![errored.src_uri.clone()], cleanup);
    }

    fn check_not_errored(&self, mooncake_table_id: &MooncakeTableId) -> Result<()> {
        match self.errored_tables.get(mooncake_table_id) {
            Some(errored) => Err(Error::table_errored(&errored.message)),
            None => Ok(()),
        }
    }

    /// Start streaming changes from `src_uri` once recovery has registered all its mirrors.
    pub async fn start_replication(&mut self, src_uri: &str) -> Result<()> {
        let key = self.source_key(src_uri).await?;
        let Some(connection) = self.connections.get_mut(&key) else {
            return Err(Error::table_not_found(src_uri.to_string()));
        };
        if !connection.replication_started() {
            connection.start_replication().await?;
        }
        connection.start_streaming().await
    }

    /// Drop table specified by the given table id.
    /// If the table is not tracked, logs a message and returns successfully.
    /// Return whether the table is tracked by moonlink.
    pub async fn drop_table(&mut self, mooncake_table_id: &MooncakeTableId) -> Result<bool> {
        if let Some(errored) = self.errored_tables.remove(mooncake_table_id) {
            self.pending_recoveries.remove(mooncake_table_id);
            self.refresh_slot_holds();
            // Best effort: the mirror is gone from moonlink either way.
            let _ = errored_tables::drop_iceberg_table_files(&errored.iceberg_table_config).await;
            errored_tables::remove_marker(&self.table_base_path, mooncake_table_id).await;
            // The source table's oid, so its publication entry is removed only if it is that
            // table's, and not one re-created under the same name.
            let src_table_id = source_fingerprint::read(&self.table_base_path, mooncake_table_id)
                .await
                .map(|fingerprint| fingerprint.relid);
            source_fingerprint::remove(&self.table_base_path, mooncake_table_id).await;
            self.release_errored_source(&errored, src_table_id).await;
            return Ok(true);
        }
        let (table_uri, src_table_id) = match self.table_info.get(mooncake_table_id) {
            Some(info) => info.clone(),
            None => {
                debug!("attempted to drop table that is not tracked by moonlink - table may already be dropped");
                return Ok(false);
            }
        };
        debug!(src_table_id, %table_uri, "dropping table through manager");
        let repl_conn = self.connections.get_mut(&table_uri).unwrap();
        match repl_conn.drop_table(mooncake_table_id, src_table_id).await {
            Ok(()) => {
                // Clear manager mapping after successful drop
                self.table_info.remove(mooncake_table_id);
                if repl_conn.table_count() == 0 {
                    let drop_all = !self.pending_mirror_needs_slot(&table_uri);
                    self.shutdown_connection_by_key(&table_uri, drop_all);
                }

                debug!(src_table_id, "table dropped through manager");
                Ok(true)
            }
            Err(e) => Err(e),
        }
    }

    pub fn get_table_reader(
        &self,
        mooncake_table_id: &MooncakeTableId,
    ) -> Result<Arc<ReadStateManager>> {
        self.check_not_errored(mooncake_table_id)?;
        let (src_table_id, connection) = self.get_replication_connection(mooncake_table_id)?;
        Ok(connection.get_table_reader(mooncake_table_id, src_table_id))
    }

    pub fn get_table_state_reader(
        &self,
        mooncake_table_id: &MooncakeTableId,
    ) -> Result<&TableStatusReader> {
        self.check_not_errored(mooncake_table_id)?;
        let (src_table_id, connection) = self.get_replication_connection(mooncake_table_id)?;
        Ok(connection.get_table_status_reader(mooncake_table_id, src_table_id))
    }

    /// Return mapping from mooncake table id to its table status readers.
    pub fn get_table_status_readers(&self) -> HashMap<MooncakeTableId, &TableStatusReader> {
        let mut table_state_readers = HashMap::with_capacity(self.connections.len());
        for (_, (src_uri, _)) in self.table_info.iter() {
            let cur_repl_conn = self.connections.get(src_uri).unwrap_or_else(|| {
                panic!("replication connection with uri {src_uri} should exist.")
            });

            let table_status_readers = cur_repl_conn.get_table_status_readers();
            for (cur_mooncake_table_id, cur_table_status_reader) in table_status_readers.into_iter()
            {
                // Multiple mooncake tables could reference to one single replication connection, so duplicate key expected.
                table_state_readers.insert(cur_mooncake_table_id, cur_table_status_reader);
            }
        }
        table_state_readers
    }

    /// Whether `mooncake_table_id` is still the mirror whose status `status` follows, rather than
    /// gone or replaced by a mirror re-created under its name.
    pub fn is_same_mirror(
        &mut self,
        mooncake_table_id: &MooncakeTableId,
        status: &watch::Receiver<TableStatus>,
    ) -> bool {
        let Some((uri, src_table_id)) = self.table_info.get(mooncake_table_id) else {
            return false;
        };
        let src_table_id = *src_table_id;
        let Some(connection) = self.connections.get_mut(uri) else {
            return false;
        };
        connection
            .get_table_event_manager(mooncake_table_id, src_table_id)
            .subscribe_table_status()
            .same_channel(status)
    }

    pub fn get_table_event_manager(
        &mut self,
        mooncake_table_id: &MooncakeTableId,
    ) -> Result<&mut TableEventManager> {
        self.check_not_errored(mooncake_table_id)?;
        let (uri, src_table_id) = self
            .table_info
            .get(mooncake_table_id)
            .ok_or_else(|| Error::table_not_found(mooncake_table_id.to_string()))?;
        let connection = self
            .connections
            .get_mut(uri)
            // Directly panic: table connection uri existence here is an invariant.
            .unwrap_or_else(|| panic!("connection {uri} not found"));
        Ok(connection.get_table_event_manager(mooncake_table_id, *src_table_id))
    }

    /// Gracefully shutdown a replication connection by its URI.
    /// If postgres drop all is false, then we will not drop the PostgreSQL publication and replication slot,
    /// which allows for recovery from the PostgreSQL replication slot.
    pub fn shutdown_connection(&mut self, uri: &str, postgres_drop_all: bool) {
        if let Some(key) = self.uri_keys.get(uri).cloned() {
            self.shutdown_connection_by_key(&key, postgres_drop_all);
        }
    }

    fn shutdown_connection_by_key(&mut self, key: &str, postgres_drop_all: bool) {
        if let Some(conn) = self.connections.remove(key) {
            let shutdown_handle = conn.shutdown(postgres_drop_all);
            let uris = self
                .uri_keys
                .iter()
                .filter(|(_, k)| k.as_str() == key)
                .map(|(uri, _)| uri.clone())
                .collect();
            self.track_shutdown(key, uris, shutdown_handle);
            self.table_info.retain(|_, (k, _)| k != key);
            self.uri_keys.retain(|_, k| k != key);
        }
    }

    /// Get replication connection by mooncake table id.
    fn get_replication_connection(
        &self,
        mooncake_table_id: &MooncakeTableId,
    ) -> Result<(SrcTableId, &ReplicationConnection)> {
        let (uri, src_table_id) = self
            .table_info
            .get(mooncake_table_id)
            .ok_or_else(|| Error::table_not_found(mooncake_table_id.to_string()))?;
        let connection = self
            .connections
            .get(uri)
            // Directly panic: table connection uri existence here is an invariant.
            .unwrap_or_else(|| panic!("connection {uri} not found"));
        Ok((*src_table_id, connection))
    }
}

#[cfg(test)]
mod shutdown_wait_tests {
    use super::*;
    use crate::error::{error_struct_has_cause, is_transient_cause};

    fn manager(dir: &tempfile::TempDir) -> ReplicationManager {
        let base = dir.path().to_str().unwrap().to_string();
        ReplicationManager::new(
            base.clone(),
            ObjectStorageCache::new(moonlink::ObjectStorageCacheConfig::new(
                1 << 20,
                format!("{base}/cache"),
                true,
            )),
        )
    }

    /// A shutdown of `key` that has not ended, and the sender that ends it.
    fn pending(manager: &mut ReplicationManager, key: &str) -> watch::Sender<Option<ShutdownEnd>> {
        let (tx, rx) = watch::channel(None);
        manager
            .shutdowns
            .entry(key.to_string())
            .or_default()
            .push(rx);
        tx
    }

    #[tokio::test(start_paused = true)]
    async fn a_panicked_shutdown_is_waited_out_until_its_retries_have_ended() {
        let (_tx, rx) = watch::channel(Some(ShutdownEnd::Panicked(Instant::now())));
        let start = Instant::now();
        PendingShutdowns(vec![rx]).wait().await.unwrap();
        assert_eq!(start.elapsed(), RETRY_TASK_LIMIT);
    }

    #[tokio::test(start_paused = true)]
    async fn a_shutdown_that_never_ends_fails_the_wait_with_a_retryable_error() {
        let (_tx, rx) = watch::channel(None);
        let start = Instant::now();
        let err = PendingShutdowns(vec![rx]).wait().await.unwrap_err();
        assert_eq!(start.elapsed(), SHUTDOWN_WAIT);
        assert_eq!(
            err.error_struct().status,
            moonlink_error::ErrorStatus::Temporary
        );
        // So a recovery that hits it retries instead of persisting the mirror as errored.
        assert!(error_struct_has_cause(
            err.error_struct(),
            &is_transient_cause
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn a_dropped_wait_leaves_the_shutdown_to_wait_for() {
        let dir = tempfile::tempdir().unwrap();
        let mut manager = manager(&dir);
        let tx = pending(&mut manager, "a");
        let first = manager.pending_shutdowns(Some("a")).wait();
        assert!(tokio::time::timeout(Duration::from_secs(1), first)
            .await
            .is_err());
        assert!(!manager.pending_shutdowns(Some("a")).is_empty());
        tx.send(Some(ShutdownEnd::Finished)).unwrap();
        let start = Instant::now();
        manager.pending_shutdowns(Some("a")).wait().await.unwrap();
        assert_eq!(start.elapsed(), Duration::ZERO);
        manager.prune_shutdowns();
        assert!(manager.shutdowns.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_shutdown_of_another_database_is_not_waited_for() {
        let dir = tempfile::tempdir().unwrap();
        let mut manager = manager(&dir);
        let _tx = pending(&mut manager, "a");
        assert!(manager.pending_shutdowns(Some("b")).is_empty());
        let start = Instant::now();
        manager.pending_shutdowns(Some("b")).wait().await.unwrap();
        assert_eq!(start.elapsed(), Duration::ZERO);
        assert!(!manager.pending_shutdowns(Some("a")).is_empty());
    }
}

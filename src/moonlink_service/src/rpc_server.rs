use arrow_ipc::writer::StreamWriter;
use dashmap::DashMap;
use moonlink_backend::{MoonlinkBackend, ReadState};
use moonlink_rpc::moonlink::{rpc_server::Rpc, *};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct RpcService {
    backend: Arc<MoonlinkBackend>,
    // TODO
    read_states: DashMap<(String, String), Arc<ReadState>>,
}

impl RpcService {
    pub fn new(backend: Arc<MoonlinkBackend>) -> Self {
        Self {
            backend,
            read_states: DashMap::new(),
        }
    }
}

#[tonic::async_trait]
impl Rpc for RpcService {
    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let CreateSnapshotRequest {
            database,
            table,
            lsn,
        } = request.into_inner();
        self.backend
            .create_snapshot(database, table, lsn)
            .await
            .unwrap();
        Ok(Response::new(CreateSnapshotResponse {}))
    }

    async fn create_table(
        &self,
        request: Request<CreateTableRequest>,
    ) -> Result<Response<CreateTableResponse>, Status> {
        let CreateTableRequest {
            database,
            table,
            src,
            src_uri,
            table_config,
        } = request.into_inner();
        self.backend
            .create_table(database, table, src, src_uri, table_config, None)
            .await
            .unwrap();
        Ok(Response::new(CreateTableResponse {}))
    }

    async fn drop_table(
        &self,
        request: Request<DropTableRequest>,
    ) -> Result<Response<DropTableResponse>, Status> {
        let DropTableRequest { database, table } = request.into_inner();
        self.backend.drop_table(database, table).await.unwrap();
        Ok(Response::new(DropTableResponse {}))
    }

    async fn get_table_schema(
        &self,
        request: Request<GetTableSchemaRequest>,
    ) -> Result<Response<GetTableSchemaResponse>, Status> {
        let GetTableSchemaRequest { database, table } = request.into_inner();
        let schema = self
            .backend
            .get_table_schema(database, table)
            .await
            .unwrap();
        let writer = StreamWriter::try_new(Vec::new(), &schema).unwrap();
        Ok(Response::new(GetTableSchemaResponse {
            schema: writer.into_inner().unwrap(),
        }))
    }

    async fn list_tables(
        &self,
        _request: Request<ListTablesRequest>,
    ) -> Result<Response<ListTablesResponse>, Status> {
        unimplemented!();
    }

    async fn optimize_table(
        &self,
        request: Request<OptimizeTableRequest>,
    ) -> Result<Response<OptimizeTableResponse>, Status> {
        let OptimizeTableRequest {
            database,
            table,
            mode,
        } = request.into_inner();
        self.backend
            .optimize_table(database, table, &mode)
            .await
            .unwrap();
        Ok(Response::new(OptimizeTableResponse {}))
    }

    async fn scan_table_begin(
        &self,
        request: Request<ScanTableBeginRequest>,
    ) -> Result<Response<ScanTableBeginResponse>, Status> {
        let ScanTableBeginRequest {
            database,
            table,
            lsn,
        } = request.into_inner();
        let read_state = self
            .backend
            .scan_table(database.clone(), table.clone(), Some(lsn))
            .await
            .unwrap();
        let metadata = read_state.data.clone();
        self.read_states.insert((database, table), read_state);
        Ok(Response::new(ScanTableBeginResponse { metadata }))
    }

    async fn scan_table_end(
        &self,
        request: Request<ScanTableEndRequest>,
    ) -> Result<Response<ScanTableEndResponse>, Status> {
        let ScanTableEndRequest { database, table } = request.into_inner();
        self.read_states.remove(&(database, table));
        Ok(Response::new(ScanTableEndResponse {}))
    }
}

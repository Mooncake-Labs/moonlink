use crate::error::Result;
use crate::table_metadata::{DeletionVector, MooncakeTableMetadata, PositionDelete};
use arrow::datatypes::SchemaRef;
use arrow_ipc::reader::StreamReader;
use async_trait::async_trait;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DFSchema, DataFusionError};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::{
    DefaultParquetFileReaderFactory, ParquetAccessPlan,
};
use datafusion::datasource::physical_plan::{
    FileMeta, FileScanConfigBuilder, ParquetFileReaderFactory, ParquetSource,
};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use moonlink_rpc::{get_table_schema, scan_table_begin, scan_table_end};
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use roaring::RoaringTreemap;
use std::any::Any;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio::net::UnixStream;

#[derive(Debug)]
pub struct MooncakeTableProvider {
    schema: SchemaRef,
    scan: Arc<MooncakeTableScan>,
}

impl MooncakeTableProvider {
    pub async fn try_new(uri: &str, schema: String, table: String, lsn: u64) -> Result<Self> {
        let mut stream = UnixStream::connect(uri).await?;
        let table_schema = get_table_schema(&mut stream, schema.clone(), table.clone()).await?;
        let table_schema = StreamReader::try_new(table_schema.as_slice(), None)?.schema();
        let scan = Arc::new(MooncakeTableScan::try_new(stream, schema, table, lsn).await?);
        Ok(Self {
            schema: table_schema,
            scan,
        })
    }
}

#[async_trait]
impl TableProvider for MooncakeTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let schema = DFSchema::try_from(self.schema())?;
        let predicate = conjunction(filters.to_vec())
            .map(|predicate| state.create_physical_expr(predicate, &schema))
            .transpose()?;
        let mut source = ParquetSource::default();
        if let Some(predicate) = predicate {
            source = source.with_predicate(predicate);
        }
        let url = ObjectStoreUrl::local_filesystem();
        let store = state.runtime_env().object_store(&url)?;
        let reader_factory = Arc::new(MooncakeParquetFileReaderFactory::new(
            store,
            Arc::clone(&self.scan),
        ));
        let source = Arc::new(source.with_parquet_file_reader_factory(reader_factory));
        let mut config_builder = FileScanConfigBuilder::new(url, self.schema(), source)
            .with_projection(projection.cloned())
            .with_limit(limit);

        let MooncakeTableMetadata {
            data_files,
            puffin_files,
            deletion_vectors,
            position_deletes,
        } = &self.scan.metadata;
        let mut deletion_vector_number = 0;
        let mut position_delete_number = 0;
        for (data_file_number, data_file) in data_files.iter().enumerate() {
            let mut deleted_rows = RoaringTreemap::new();
            if deletion_vector_number < deletion_vectors.len() {
                let DeletionVector {
                    data_file_number: _data_file_number,
                    puffin_file_number,
                    offset,
                    size,
                } = deletion_vectors[deletion_vector_number];
                if data_file_number == _data_file_number as usize {
                    deletion_vector_number += 1;
                    let mut puffin_file =
                        File::open(&puffin_files[puffin_file_number as usize]).await?;
                    // | 4-byte length | 4-byte magic | buffer | 4-byte CRC-32 |
                    puffin_file.seek(SeekFrom::Start(offset as u64 + 8)).await?;
                    let mut buffer = vec![0u8; size as usize - 12];
                    puffin_file.read_exact(&mut buffer).await?;
                    deleted_rows = RoaringTreemap::deserialize_from(buffer.as_slice())?;
                }
            }
            while position_delete_number < position_deletes.len() {
                let PositionDelete {
                    data_file_number: _data_file_number,
                    data_file_row_number,
                } = position_deletes[position_delete_number];
                if data_file_number < _data_file_number as usize {
                    break;
                }
                position_delete_number += 1;
                deleted_rows.insert(data_file_row_number as u64);
            }

            let file = File::open(data_file).await?;
            let size = file.metadata().await?.len();
            let stream_builder = ParquetRecordBatchStreamBuilder::new(file).await?;
            let mut access_plan =
                ParquetAccessPlan::new_all(stream_builder.metadata().num_row_groups());
            let mut data_file_row_number = 0;
            for (row_group_number, row_group) in
                stream_builder.metadata().row_groups().iter().enumerate()
            {
                let row_group_row_number = data_file_row_number + row_group.num_rows();
                let mut selectors = vec![];
                while data_file_row_number < row_group_row_number {
                    let data_file_row_number_start = data_file_row_number;
                    let is_deleted = deleted_rows.contains(data_file_row_number as u64);
                    while data_file_row_number < row_group_row_number
                        && deleted_rows.contains(data_file_row_number as u64) == is_deleted
                    {
                        data_file_row_number += 1;
                    }
                    let row_count = data_file_row_number - data_file_row_number_start;
                    if is_deleted {
                        selectors.push(RowSelector::skip(row_count as usize));
                    } else {
                        selectors.push(RowSelector::select(row_count as usize));
                    }
                }
                access_plan.scan_selection(row_group_number, RowSelection::from(selectors));
            }
            let file = PartitionedFile::new(data_file, size).with_extensions(Arc::new(access_plan));
            config_builder = config_builder.with_file(file);
        }
        Ok(DataSourceExec::from_data_source(config_builder.build()))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

#[derive(Debug)]
struct MooncakeParquetFileReaderFactory {
    inner: DefaultParquetFileReaderFactory,
    // DEVNOTE: Keep it alive for the entire table scan
    _scan: Arc<MooncakeTableScan>,
}

impl MooncakeParquetFileReaderFactory {
    fn new(store: Arc<dyn ObjectStore>, _scan: Arc<MooncakeTableScan>) -> Self {
        Self {
            inner: DefaultParquetFileReaderFactory::new(store),
            _scan,
        }
    }
}

impl ParquetFileReaderFactory for MooncakeParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>, DataFusionError> {
        self.inner
            .create_reader(partition_index, file_meta, metadata_size_hint, metrics)
    }
}

#[derive(Debug)]
struct MooncakeTableScan {
    stream: Option<UnixStream>,
    schema: String,
    table: String,
    metadata: MooncakeTableMetadata,
}

impl MooncakeTableScan {
    async fn try_new(
        mut stream: UnixStream,
        schema: String,
        table: String,
        lsn: u64,
    ) -> Result<Self> {
        let metadata = scan_table_begin(&mut stream, schema.clone(), table.clone(), lsn).await?;
        let metadata = bincode::decode_from_slice(&metadata, bincode::config::standard())?.0;
        Ok(Self {
            stream: Some(stream),
            schema,
            table,
            metadata,
        })
    }
}

impl Drop for MooncakeTableScan {
    fn drop(&mut self) {
        let stream = self.stream.take();
        let schema = std::mem::take(&mut self.schema);
        let table = std::mem::take(&mut self.table);
        tokio::spawn(async move {
            let mut stream = stream.expect("stream should be set by try_new");
            if let Err(e) = scan_table_end(&mut stream, schema, table).await {
                eprintln!("scan_table_end error: {e}");
            }
        });
    }
}

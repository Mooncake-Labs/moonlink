use iceberg::Result as IcebergResult;
use iceberg::io::FileIOBuilder;
use iceberg::puffin::PuffinReader;
use iceberg::table::Table as IcebergTable;

struct IcebergTableManager {
    iceberg_table: Option<IcebergTable>,
}

impl IcebergTableManager {
    async fn do_something(&self) {}
}

async fn f() {}

async fn create_iceberg_table_manager() -> IcebergTableManager {
    async {
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let input_file = file_io.new_input("/tmp/iceberg/table").unwrap();
        let puffin_reader = PuffinReader::new(input_file);
        let puffin_file_metadata: &iceberg::puffin::FileMetadata =
            puffin_reader.file_metadata().await.unwrap();
    }.await;
    IcebergTableManager {
        iceberg_table: None
    }
}

async fn fake_main() -> IcebergResult<()> {
    let handle = tokio::spawn(async move {
        let mgr = create_iceberg_table_manager().await;
        mgr.do_something().await;
    });

    handle.await.unwrap();
    Ok(())
}

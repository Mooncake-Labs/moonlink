//! duckdb_mooncake binary-searches the ScanTableBegin payload's deletion vectors and positional
//! deletes by `data_file_number` (std::lower_bound) and only reads the first deletion vector per
//! data file, so [`ReadState::new`] must emit both arrays sorted regardless of input order.

use moonlink::ReadState;
use moonlink_table_metadata::{DeletionVector, MooncakeTableMetadata, PositionDelete};
use std::sync::Arc;

fn dv(data_file_number: u32, puffin_file_number: u32, offset: u32, size: u32) -> DeletionVector {
    DeletionVector {
        data_file_number,
        puffin_file_number,
        offset,
        size,
    }
}

fn pd(data_file_number: u32, data_file_row_number: u32) -> PositionDelete {
    PositionDelete {
        data_file_number,
        data_file_row_number,
    }
}

#[tokio::test]
async fn test_read_state_sorts_deletes_by_data_file() {
    let data_files: Vec<String> = (0..4).map(|i| format!("/data/{i}.parquet")).collect();
    let read_state = ReadState::new(
        data_files.clone(),
        /*puffin_cache_handles=*/ vec![],
        vec![
            dv(3, 0, 100, 40),
            dv(0, 1, 4, 30),
            dv(2, 0, 4, 50),
            dv(0, 0, 60, 30),
        ],
        vec![pd(2, 9), pd(0, 5), pd(3, 1), pd(0, 2), pd(2, 0)],
        /*associated_files=*/ vec![],
        /*cache_handles=*/ vec![],
        Arc::new(|path| path),
    );

    let (metadata, _): (MooncakeTableMetadata, usize) =
        bincode::decode_from_slice(&read_state.data, bincode::config::standard()).unwrap();
    assert_eq!(metadata.data_files, data_files);
    assert_eq!(
        metadata.deletion_vectors,
        vec![
            dv(0, 0, 60, 30),
            dv(0, 1, 4, 30),
            dv(2, 0, 4, 50),
            dv(3, 0, 100, 40)
        ]
    );
    assert_eq!(
        metadata.position_deletes,
        vec![pd(0, 2), pd(0, 5), pd(2, 0), pd(2, 9), pd(3, 1)]
    );
}

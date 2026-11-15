//! Golden bytes for the `ScanTableBegin` payload.
//!
//! duckdb_mooncake reinterprets this buffer as `uint32_t*` (all integers u32 native-endian) and
//! performs no version check, so any drift silently breaks scans. Golden generated from upstream
//! main 401c1d1. Set `WIRE_GOLDEN_PRINT=1` to print the actual hex.

#![cfg(target_endian = "little")]

use moonlink_table_metadata::{DeletionVector, MooncakeTableMetadata, PositionDelete};

/// Same bincode config as `moonlink::union_read::read_state`.
const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

const GOLDEN: &str = "02000000000000001d0000003a00000001000000000000001a000000010000000100000000000000040000000403020102000000000000000700000001000000efcdab002f70675f6d6f6f6e63616b652f742f646174612d302e706172717565742f70675f6d6f6f6e63616b652f742f646174612d312e706172717565742f70675f6d6f6f6e63616b652f742f64762d302e70756666696e";

fn sample_metadata() -> MooncakeTableMetadata {
    MooncakeTableMetadata {
        data_files: vec![
            "/pg_mooncake/t/data-0.parquet".to_string(),
            "/pg_mooncake/t/data-1.parquet".to_string(),
        ],
        puffin_files: vec!["/pg_mooncake/t/dv-0.puffin".to_string()],
        deletion_vectors: vec![DeletionVector {
            data_file_number: 1,
            puffin_file_number: 0,
            offset: 4,
            size: 0x0102_0304,
        }],
        position_deletes: vec![
            PositionDelete {
                data_file_number: 0,
                data_file_row_number: 7,
            },
            PositionDelete {
                data_file_number: 1,
                data_file_row_number: 0x00AB_CDEF,
            },
        ],
    }
}

/// Leading u32 words of the encoded [`sample_metadata`], as duckdb_mooncake indexes them.
#[rustfmt::skip]
const EXPECTED_HEADER_WORDS: [u32; 19] = [
    2, 0, 29, 58,             // data files: count, offsets[count + 1]
    1, 0, 26,                 // puffin files: count, offsets[count + 1]
    1, 1, 0, 4, 0x0102_0304,  // deletion vectors: count, {data, puffin, offset, size}
    2, 0, 7, 1, 0x00AB_CDEF,  // position deletes: count, {data, row}
    u32::from_le_bytes(*b"/pg_"), u32::from_le_bytes(*b"moon"), // start of data file paths
];

#[test]
fn test_scan_payload_wire_format() {
    let metadata = sample_metadata();
    let bytes = bincode::encode_to_vec(&metadata, BINCODE_CONFIG).unwrap();
    let actual: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    if std::env::var_os("WIRE_GOLDEN_PRINT").is_some() {
        println!("{actual}");
    }
    assert_eq!(actual, GOLDEN, "ScanTableBegin payload drift");

    // The fixed-width header is what the C++ side indexes into; spell it out independently.
    let words: Vec<u32> = bytes[..4 * EXPECTED_HEADER_WORDS.len()]
        .chunks_exact(4)
        .map(|c| u32::from_ne_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(words, EXPECTED_HEADER_WORDS);
    assert_eq!(bytes.len(), 4 * 17 + 29 * 2 + 26);

    let (decoded, len): (MooncakeTableMetadata, usize) =
        bincode::decode_from_slice(&bytes, BINCODE_CONFIG).unwrap();
    assert_eq!(len, bytes.len());
    assert_eq!(decoded, metadata);
}

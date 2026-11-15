//! Golden bytes for the moonlink RPC wire format.
//!
//! duckdb_mooncake (including the prebuilt extension pg_mooncake downloads) pins moonlink_rpc by git
//! and performs no version check, so any drift here silently breaks scans. Goldens were generated
//! from upstream main 401c1d1; they must only change together with every client.
//!
//! Set `WIRE_GOLDEN_PRINT=1` to print the actual hex of every case.

#![cfg(target_endian = "little")]

use moonlink_error::{ErrorStatus, ErrorStruct};
use moonlink_rpc::{read, write, Request, RpcResult, Table};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Frame `value` exactly like the real RPC path (u32 native-endian length + bincode standard payload).
async fn frame<S: Serialize>(value: &S) -> Vec<u8> {
    let mut buf = Vec::new();
    write(&mut buf, value).await.unwrap();
    buf
}

async fn unframe<D: DeserializeOwned>(bytes: &[u8]) -> D {
    let mut reader = bytes;
    let value = read(&mut reader).await.unwrap();
    assert!(reader.is_empty(), "trailing bytes after frame");
    value
}

/// Compare each case with its golden by position, requiring matching labels.
fn check(cases: &[(&str, Vec<u8>)], goldens: &[(&str, &str)]) {
    let print = std::env::var_os("WIRE_GOLDEN_PRINT").is_some();
    let mut mismatches = Vec::new();
    for (i, (name, actual)) in cases.iter().enumerate() {
        let actual = to_hex(actual);
        if print {
            println!("(\"{name}\", \"{actual}\"),");
        }
        match goldens.get(i) {
            Some((label, golden)) if label == name && actual == *golden => {}
            Some((label, golden)) => mismatches.push(format!(
                "{name} (golden label {label}):\n  golden {golden}\n  actual {actual}"
            )),
            None => mismatches.push(format!("{name}: no golden\n  actual {actual}")),
        }
    }
    if goldens.len() > cases.len() {
        mismatches.push(format!(
            "{} goldens without a case",
            goldens.len() - cases.len()
        ));
    }
    assert!(
        mismatches.is_empty(),
        "wire format drift:\n{}",
        mismatches.join("\n")
    );
}

const DATABASE: &str = "postgres";
const TABLE: &str = "public.orders";
/// Larger than u32::MAX so the varint encoding of u64 LSNs is covered.
const LSN: u64 = 0x1_6B3A_0F28;

fn s(value: &str) -> String {
    value.to_string()
}

/// Wire index and name of a request. Exhaustive on purpose: a new variant does not compile until
/// it gets a label here, a sample in [`sample_requests`] and a golden in [`REQUEST_GOLDENS`].
fn request_label(request: &Request) -> &'static str {
    match request {
        Request::CreateSnapshot { .. } => "0 CreateSnapshot",
        Request::CreateTable { .. } => "1 CreateTable",
        Request::DropTable { .. } => "2 DropTable",
        Request::GetParquetMetadatas { .. } => "3 GetParquetMetadatas",
        Request::GetTableSchema { .. } => "4 GetTableSchema",
        Request::ListTables { .. } => "5 ListTables",
        Request::LoadFiles { .. } => "6 LoadFiles",
        Request::OptimizeTable { .. } => "7 OptimizeTable",
        Request::ScanTableBegin { .. } => "8 ScanTableBegin",
        Request::ScanTableEnd { .. } => "9 ScanTableEnd",
    }
}

/// One sample per variant, in declaration order. The discriminant on the wire is the variant index.
fn sample_requests() -> Vec<(&'static str, Request)> {
    vec![
        (
            "0 CreateSnapshot",
            Request::CreateSnapshot {
                database: s(DATABASE),
                table: s(TABLE),
                lsn: LSN,
            },
        ),
        (
            "1 CreateTable",
            Request::CreateTable {
                database: s(DATABASE),
                table: s(TABLE),
                src: s("public.orders_src"),
                src_uri: s("postgresql://localhost:5432/postgres"),
                table_config: s(r#"{"mooncake":{"append_only":true}}"#),
            },
        ),
        (
            "2 DropTable",
            Request::DropTable {
                database: s(DATABASE),
                table: s(TABLE),
            },
        ),
        (
            "3 GetParquetMetadatas",
            Request::GetParquetMetadatas {
                data_files: vec![s("a.parquet"), s("b.parquet")],
            },
        ),
        (
            "4 GetTableSchema",
            Request::GetTableSchema {
                database: s(DATABASE),
                table: s(TABLE),
            },
        ),
        ("5 ListTables", Request::ListTables {}),
        (
            "6 LoadFiles",
            Request::LoadFiles {
                database: s(DATABASE),
                table: s(TABLE),
                files: vec![s("/tmp/load.parquet")],
            },
        ),
        (
            "7 OptimizeTable",
            Request::OptimizeTable {
                database: s(DATABASE),
                table: s(TABLE),
                mode: s("full"),
            },
        ),
        (
            "8 ScanTableBegin",
            Request::ScanTableBegin {
                database: s(DATABASE),
                table: s(TABLE),
                lsn: LSN,
            },
        ),
        (
            "9 ScanTableEnd",
            Request::ScanTableEnd {
                database: s(DATABASE),
                table: s(TABLE),
            },
        ),
    ]
}

const REQUEST_GOLDENS: &[(&str, &str)] = &[
    ("0 CreateSnapshot", "210000000008706f7374677265730d7075626c69632e6f7264657273fd280f3a6b01000000"),
    ("1 CreateTable", "710000000108706f7374677265730d7075626c69632e6f7264657273117075626c69632e6f72646572735f73726324706f737467726573716c3a2f2f6c6f63616c686f73743a353433322f706f737467726573217b226d6f6f6e63616b65223a7b22617070656e645f6f6e6c79223a747275657d7d"),
    ("2 DropTable", "180000000208706f7374677265730d7075626c69632e6f7264657273"),
    ("3 GetParquetMetadatas", "16000000030209612e7061727175657409622e70617271756574"),
    ("4 GetTableSchema", "180000000408706f7374677265730d7075626c69632e6f7264657273"),
    ("5 ListTables", "0100000005"),
    ("6 LoadFiles", "2b0000000608706f7374677265730d7075626c69632e6f726465727301112f746d702f6c6f61642e70617271756574"),
    ("7 OptimizeTable", "1d0000000708706f7374677265730d7075626c69632e6f72646572730466756c6c"),
    ("8 ScanTableBegin", "210000000808706f7374677265730d7075626c69632e6f7264657273fd280f3a6b01000000"),
    ("9 ScanTableEnd", "180000000908706f7374677265730d7075626c69632e6f7264657273"),
];

#[tokio::test]
async fn test_request_wire_format() {
    let requests = sample_requests();
    let mut cases = Vec::new();
    for (name, request) in requests.iter() {
        assert_eq!(*name, request_label(request), "sample label mismatch");
        let bytes = frame(request).await;
        // Round trip through the real decode path.
        let decoded: Request = unframe(&bytes).await;
        assert_eq!(format!("{decoded:?}"), format!("{request:?}"));
        cases.push((*name, bytes));
    }
    // One sample per variant, in wire order.
    let labels: Vec<&str> = cases.iter().map(|(name, _)| *name).collect();
    for (i, label) in labels.iter().enumerate() {
        assert!(
            label.starts_with(&format!("{i} ")),
            "sample {label} out of order"
        );
    }
    check(&cases, REQUEST_GOLDENS);
}

fn sample_error(status: ErrorStatus) -> ErrorStruct {
    ErrorStruct {
        message: s("backend error"),
        status,
        source: Some(Arc::new(anyhow::anyhow!("table not found"))),
        location: Some(s("src/moonlink_service/src/rpc_server.rs:1:1")),
    }
}

fn sample_tables() -> Vec<Table> {
    vec![
        Table {
            database: s(DATABASE),
            table: s(TABLE),
            cardinality: 1_000_000,
            commit_lsn: LSN,
            flush_lsn: Some(LSN - 1),
            iceberg_warehouse_location: s("pg_mooncake/postgres/public.orders"),
        },
        Table {
            database: s(DATABASE),
            table: s("public.empty"),
            cardinality: 0,
            commit_lsn: 0,
            flush_lsn: None,
            iceberg_warehouse_location: s(""),
        },
    ]
}

const RESPONSE_GOLDENS: &[(&str, &str)] = &[
    ("Ok(())", "0100000000"),
    ("Err(Temporary)", "4d000000010d6261636b656e64206572726f7200010f7461626c65206e6f7420666f756e64012a7372632f6d6f6f6e6c696e6b5f736572766963652f7372632f7270635f7365727665722e72733a313a31"),
    ("Err(Permanent)", "4d000000010d6261636b656e64206572726f7201010f7461626c65206e6f7420666f756e64012a7372632f6d6f6f6e6c696e6b5f736572766963652f7372632f7270635f7365727665722e72733a313a31"),
    ("Err(no source, no location)", "150000000110696e76616c696420617267756d656e74010000"),
    ("Ok(Vec<u8>)", "07000000000500017f80ff"),
    ("Ok(Vec<Vec<u8>>)", "0700000000020301020300"),
    ("Ok(Vec<Table>)", "6e000000000208706f7374677265730d7075626c69632e6f7264657273fc40420f00fd280f3a6b0100000001fd270f3a6b010000002270675f6d6f6f6e63616b652f706f7374677265732f7075626c69632e6f726465727308706f7374677265730c7075626c69632e656d70747900000000"),
    ("Ok(Vec<u8> 300 bytes)", "3001000000fb2c01000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fa000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f30"),
];

#[tokio::test]
async fn test_response_wire_format() {
    let mut cases: Vec<(&str, Vec<u8>)> = Vec::new();

    let unit_ok: RpcResult<()> = Ok(());
    cases.push(("Ok(())", frame(&unit_ok).await));
    for (name, status) in [
        ("Err(Temporary)", ErrorStatus::Temporary),
        ("Err(Permanent)", ErrorStatus::Permanent),
    ] {
        let err: RpcResult<()> = Err(sample_error(status));
        let bytes = frame(&err).await;
        let decoded: RpcResult<()> = unframe(&bytes).await;
        let decoded = decoded.unwrap_err();
        let expected = sample_error(status);
        assert_eq!(decoded.message, expected.message);
        assert_eq!(decoded.status, expected.status);
        assert_eq!(decoded.location, expected.location);
        assert_eq!(
            decoded.source.map(|e| e.to_string()),
            Some(s("table not found"))
        );
        cases.push((name, bytes));
    }
    let no_source: RpcResult<()> = Err(ErrorStruct {
        message: s("invalid argument"),
        status: ErrorStatus::Permanent,
        source: None,
        location: None,
    });
    cases.push(("Err(no source, no location)", frame(&no_source).await));

    let bytes_ok: RpcResult<Vec<u8>> = Ok(vec![0x00, 0x01, 0x7f, 0x80, 0xff]);
    cases.push(("Ok(Vec<u8>)", frame(&bytes_ok).await));
    let decoded: RpcResult<Vec<u8>> = unframe(&cases.last().unwrap().1).await;
    assert_eq!(decoded.unwrap(), bytes_ok.clone().unwrap());

    let metadatas_ok: RpcResult<Vec<Vec<u8>>> = Ok(vec![vec![1, 2, 3], vec![]]);
    cases.push(("Ok(Vec<Vec<u8>>)", frame(&metadatas_ok).await));

    let tables_ok: RpcResult<Vec<Table>> = Ok(sample_tables());
    cases.push(("Ok(Vec<Table>)", frame(&tables_ok).await));
    let decoded: RpcResult<Vec<Table>> = unframe(&cases.last().unwrap().1).await;
    assert_eq!(
        format!("{:?}", decoded.unwrap()),
        format!("{:?}", sample_tables())
    );

    // Real ScanTableBegin/GetTableSchema payloads are usually 251..=65535 bytes: bincode's 0xfb + u16 length tier.
    let large_ok: RpcResult<Vec<u8>> = Ok((0..300u32).map(|i| (i % 251) as u8).collect());
    let bytes = frame(&large_ok).await;
    assert_eq!(
        &bytes[..8],
        &[0x30, 0x01, 0x00, 0x00, 0x00, 0xfb, 0x2c, 0x01]
    );
    let decoded: RpcResult<Vec<u8>> = unframe(&bytes).await;
    assert_eq!(decoded.unwrap(), large_ok.clone().unwrap());
    cases.push(("Ok(Vec<u8> 300 bytes)", bytes));

    check(&cases, RESPONSE_GOLDENS);
}

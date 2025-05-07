use super::test_utils::*;
use crate::storage::mooncake_table::Identity;
use std::time::Instant;

#[tokio::test]
async fn perf_write_mooncake_table() {
    // Only run in release mode
    if cfg!(debug_assertions) {
        eprintln!("perf_write_mooncake_table skipped in debug mode");
        return;
    }

    // Parameters
    const TOTAL_ROWS: i32 = 10_000_000; // Adjust as needed for your machine/CI
    const BATCH_SIZE: i32 = 100_000;

    let context = TestContext::new("bench_write");
    let mut table = test_table(&context, "bench_table", Identity::SinglePrimitiveKey(0));

    let mut current_id = 1;
    let mut total_appended = 0;

    // Pre-generate all batches
    let mut all_batches = Vec::new();
    while current_id <= TOTAL_ROWS {
        let batch_end = std::cmp::min(current_id + BATCH_SIZE, TOTAL_ROWS + 1);
        let rows = batch_rows(current_id, batch_end - current_id);
        all_batches.push((current_id, rows));
        current_id = batch_end;
    }

    // Now start timing only the write/commit/flush
    let start = Instant::now();

    for (batch_id, rows) in all_batches.into_iter() {
        total_appended += rows.len();
        append_rows(&mut table, rows).expect("append failed");
        table.commit(batch_id as u64);
    }

    // Flush the final batch
    let flush_handle = table.flush(TOTAL_ROWS as u64);
    flush_handle.await.expect("flush failed");

    let elapsed = start.elapsed();
    println!(
        "Wrote {} rows in {:?} ({:.2} rows/sec)",
        total_appended,
        elapsed,
        total_appended as f64 / elapsed.as_secs_f64()
    );
}

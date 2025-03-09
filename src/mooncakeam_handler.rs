use core::ffi::c_void;
use pgrx::prelude::*;
use std::sync::LazyLock;

// HACK: bypass table AM check in heap_getnext()
#[pg_guard]
unsafe extern "C" fn mooncake_index_build_range_scan(
    table_rel: pg_sys::Relation,
    index_rel: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
    allow_sync: bool,
    anyvisible: bool,
    progress: bool,
    start_blockno: pg_sys::BlockNumber,
    numblocks: pg_sys::BlockNumber,
    callback: pg_sys::IndexBuildCallback,
    callback_state: *mut c_void,
    scan: pg_sys::TableScanDesc,
) -> f64 {
    (*table_rel).rd_tableam = pg_sys::GetHeapamTableAmRoutine();
    let res = ((*(*table_rel).rd_tableam).index_build_range_scan.unwrap())(
        table_rel,
        index_rel,
        index_info,
        allow_sync,
        anyvisible,
        progress,
        start_blockno,
        numblocks,
        callback,
        callback_state,
        scan,
    );
    (*table_rel).rd_tableam = &*MOONCAKEAM;
    res
}

// HACK: bypass table AM check in heap_getnext()
#[pg_guard]
unsafe extern "C" fn mooncake_index_validate_scan(
    table_rel: pg_sys::Relation,
    index_rel: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
    snapshot: pg_sys::Snapshot,
    state: *mut pg_sys::ValidateIndexState,
) {
    (*table_rel).rd_tableam = pg_sys::GetHeapamTableAmRoutine();
    ((*(*table_rel).rd_tableam).index_validate_scan.unwrap())(
        table_rel, index_rel, index_info, snapshot, state,
    );
    (*table_rel).rd_tableam = &*MOONCAKEAM;
}

static MOONCAKEAM: LazyLock<pg_sys::TableAmRoutine> = LazyLock::new(|| {
    let heapam = unsafe { &*pg_sys::GetHeapamTableAmRoutine() };
    pg_sys::TableAmRoutine {
        index_build_range_scan: Some(mooncake_index_build_range_scan),
        index_validate_scan: Some(mooncake_index_validate_scan),
        ..*heapam
    }
});

#[pg_extern(sql = "
CREATE FUNCTION mooncakeam_handler(internal) RETURNS table_am_handler LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
CREATE ACCESS METHOD mooncake TYPE TABLE HANDLER mooncakeam_handler;
")]
extern "C" fn mooncakeam_handler(
    _fcinfo: pg_sys::FunctionCallInfo,
) -> PgBox<pg_sys::TableAmRoutine> {
    unsafe { PgBox::from_pg(&*MOONCAKEAM as *const _ as *mut _) }
}

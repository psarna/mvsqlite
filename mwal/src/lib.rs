#![allow(non_snake_case)]
#![allow(clippy::not_unsafe_ptr_arg_deref)]

mod ffi;

use crate::ffi::{libsql_wal_methods, sqlite3_file, sqlite3_vfs, PgHdr, Wal};
use std::ffi::c_void;
use std::sync::Arc;
use std::collections::HashMap;

// Just heuristics, but should work for ~100% of cases
fn is_regular(vfs: *const sqlite3_vfs) -> bool {
    let vfs = unsafe { std::ffi::CStr::from_ptr((*vfs).zName) }
        .to_str()
        .unwrap_or("[error]");
    tracing::trace!("VFS: {}", vfs);
    vfs.starts_with("unix") || vfs.starts_with("win32")
}

fn instantiate_mvfs() -> mvfs::MultiVersionVfs {
    let mut sector_size: usize = 8192;
    let mut force_http2 = false;

    if let Ok(s) = std::env::var("MVSQLITE_SECTOR_SIZE") {
        let requested_ss = s
            .parse::<usize>()
            .expect("MVSQLITE_SECTOR_SIZE must be a usize");
        if ![4096, 8192, 16384, 32768].contains(&requested_ss) {
            panic!("MVSQLITE_SECTOR_SIZE must be one of 4096, 8192, 16384, 32768");
        }
        sector_size = requested_ss;
        tracing::debug!(requested = requested_ss, "setting sector size",);
    }
    /* FIXME
    if let Ok(s) = std::env::var("MVSQLITE_PAGE_CACHE_SIZE") {
        let requested = s
            .parse::<usize>()
            .expect("MVSQLITE_PAGE_CACHE_SIZE must be a usize");
        vfs::PAGE_CACHE_SIZE.store(requested, Ordering::Relaxed);
        tracing::debug!(requested, "setting page cache size",);
    }
    if let Ok(s) = std::env::var("MVSQLITE_WRITE_CHUNK_SIZE") {
        let requested = s
            .parse::<usize>()
            .expect("MVSQLITE_WRITE_CHUNK_SIZE must be a usize");
        vfs::WRITE_CHUNK_SIZE.store(requested, Ordering::Relaxed);
        tracing::debug!(requested, "setting write chunk size",);
    }
    if let Ok(s) = std::env::var("MVSQLITE_PREFETCH_DEPTH") {
        let requested = s
            .parse::<usize>()
            .expect("MVSQLITE_PREFETCH_DEPTH must be a usize");
        vfs::PREFETCH_DEPTH.store(requested, Ordering::Relaxed);
        tracing::debug!(requested, "setting prefetch depth",);
    }
    */
    if let Ok(s) = std::env::var("MVSQLITE_FORCE_HTTP2") {
        if s.as_str() == "1" {
            force_http2 = true;
            tracing::debug!("enabling forced http2");
        }
    }

    let timeout_secs = if let Ok(s) = std::env::var("MVSQLITE_HTTP_TIMEOUT_SECS") {
        let requested = s
            .parse::<u64>()
            .expect("MVSQLITE_HTTP_TIMEOUT_SECS must be a u64");
        tracing::debug!(requested, "configuring http timeout secs");
        requested
    } else {
        10
    };

    let mut db_name_map: HashMap<String, String> = HashMap::new();
    if let Ok(s) = std::env::var("MVSQLITE_DB_NAME_MAP") {
        for mapping in s.split(',').map(|x| x.trim()).filter(|x| !x.is_empty()) {
            let mut parts = mapping.splitn(2, '=');
            let from = parts.next().unwrap();
            let to = parts.next().unwrap();
            db_name_map.insert(from.to_string(), to.to_string());
        }
        tracing::debug!(num_entries = db_name_map.len(), "configuring db name map");
    }
    let db_name_map = Arc::new(db_name_map);

    let mut lock_owner: Option<String> = None;
    if let Ok(s) = std::env::var("MVSQLITE_LOCK_OWNER") {
        if !s.is_empty() {
            tracing::debug!(lock_owner = s, "configuring lock owner");
            lock_owner = Some(s);
        }
    }

    let build_http_client = move || {
        let mut builder = reqwest::ClientBuilder::new();
        builder = builder.timeout(std::time::Duration::from_secs(timeout_secs));
        if force_http2 {
            builder = builder.http2_prior_knowledge();
        }
        builder.build().expect("failed to build http client")
    };

    let data_plane = std::env::var("MVSQLITE_DATA_PLANE").expect("MVSQLITE_DATA_PLANE is not set");

    // FIXME:
    //let http_client = if opts.fork_tolerant {
    //    AbstractHttpClient::Builder(Arc::new(Box::new(build_http_client)))
    //} else {
    let http_client = mvfs::vfs::AbstractHttpClient::Prebuilt(build_http_client());
    //};

    mvfs::MultiVersionVfs {
        data_plane: data_plane.clone(),
        sector_size,
        http_client: http_client.clone(),
        db_name_map: db_name_map.clone(),
        lock_owner: lock_owner.clone(),
        fork_tolerant: false,
    }
}

pub extern "C" fn xOpen(
    vfs: *const sqlite3_vfs,
    db_file: *mut sqlite3_file,
    wal_name: *const i8,
    no_shm_mode: i32,
    max_size: i64,
    methods: *mut libsql_wal_methods,
    wal: *mut *mut Wal,
) -> i32 {
    tracing::debug!("Opening WAL {}", unsafe {
        std::ffi::CStr::from_ptr(wal_name).to_str().unwrap()
    });
    if !is_regular(vfs) {
        tracing::error!("mWAL is currently only supported for regular VFS");
        return ffi::SQLITE_CANTOPEN;
    }
    let orig_methods = unsafe { &*(*methods).underlying_methods };
    let rc = (orig_methods.xOpen)(vfs, db_file, wal_name, no_shm_mode, max_size, methods, wal);
    if rc != ffi::SQLITE_OK {
        return rc;
    }
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            tracing::error!("Failed to initialize async runtime: {}", e);
            return ffi::SQLITE_CANTOPEN;
        }
    };

    let mvfs = instantiate_mvfs();

    let path = unsafe {
        match std::ffi::CStr::from_ptr(wal_name).to_str() {
            Ok(path) if path.len() >= 4 => &path[..path.len() - 4],
            Ok(path) => path,
            Err(e) => {
                tracing::error!("Failed to parse the main database path: {}", e);
                return ffi::SQLITE_CANTOPEN;
            }
        }
    };

    let context = ffi::MvfsContext { mvfs, runtime };
    unsafe { (*(*wal)).mvfs_context = Box::leak(Box::new(context)) };

    ffi::SQLITE_OK
}

fn get_orig_methods(wal: *mut Wal) -> &'static libsql_wal_methods {
    unsafe { &*((*(*wal).wal_methods).underlying_methods) }
}

fn get_mvfs_context(wal: *mut Wal) -> &'static mut ffi::MvfsContext {
    unsafe { &mut *((*wal).mvfs_context) }
}

pub extern "C" fn xClose(
    wal: *mut Wal,
    db: *mut c_void,
    sync_flags: i32,
    n_buf: i32,
    z_buf: *mut u8,
) -> i32 {
    tracing::debug!("Closing wal");
    let orig_methods = get_orig_methods(wal);
    let _mvfs_box = unsafe { Box::from_raw((*wal).mvfs_context) };

    (orig_methods.xClose)(wal, db, sync_flags, n_buf, z_buf)
}

pub extern "C" fn xLimit(wal: *mut Wal, limit: i64) {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xLimit)(wal, limit)
}

pub extern "C" fn xBeginReadTransaction(wal: *mut Wal, changed: *mut i32) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xBeginReadTransaction)(wal, changed)
}

pub extern "C" fn xEndReadTransaction(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xEndReadTransaction)(wal)
}

pub extern "C" fn xFindFrame(wal: *mut Wal, pgno: i32, frame: *mut i32) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xFindFrame)(wal, pgno, frame)
}

pub extern "C" fn xReadFrame(wal: *mut Wal, frame: u32, n_out: i32, p_out: *mut u8) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xReadFrame)(wal, frame, n_out, p_out)
}

pub extern "C" fn xDbSize(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xDbSize)(wal)
}

pub extern "C" fn xBeginWriteTransaction(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xBeginWriteTransaction)(wal)
}

pub extern "C" fn xEndWriteTransaction(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xEndWriteTransaction)(wal)
}

pub extern "C" fn xUndo(
    wal: *mut Wal,
    func: extern "C" fn(*mut c_void, i32) -> i32,
    ctx: *mut c_void,
) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xUndo)(wal, func, ctx)
}

pub extern "C" fn xSavepoint(wal: *mut Wal, wal_data: *mut u32) {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xSavepoint)(wal, wal_data)
}

pub extern "C" fn xSavepointUndo(wal: *mut Wal, wal_data: *mut u32) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xSavepointUndo)(wal, wal_data)
}

pub extern "C" fn xFrames(
    wal: *mut Wal,
    page_size: u32,
    page_headers: *const PgHdr,
    size_after: u32,
    is_commit: i32,
    sync_flags: i32,
) -> i32 {
    let _ctx = get_mvfs_context(wal);
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xFrames)(
        wal,
        page_size,
        page_headers,
        size_after,
        is_commit,
        sync_flags,
    )
}

#[tracing::instrument(skip(wal, db, busy_handler, busy_arg))]
pub extern "C" fn xCheckpoint(
    wal: *mut Wal,
    db: *mut c_void,
    emode: i32,
    busy_handler: extern "C" fn(busy_param: *mut c_void) -> i32,
    busy_arg: *const c_void,
    sync_flags: i32,
    n_buf: i32,
    z_buf: *mut u8,
    frames_in_wal: *mut i32,
    backfilled_frames: *mut i32,
) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xCheckpoint)(
        wal,
        db,
        emode,
        busy_handler,
        busy_arg,
        sync_flags,
        n_buf,
        z_buf,
        frames_in_wal,
        backfilled_frames,
    )
}

pub extern "C" fn xCallback(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xCallback)(wal)
}

pub extern "C" fn xExclusiveMode(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xExclusiveMode)(wal)
}

pub extern "C" fn xHeapMemory(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xHeapMemory)(wal)
}

pub extern "C" fn xFile(wal: *mut Wal) -> *const c_void {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xFile)(wal)
}

pub extern "C" fn xDb(wal: *mut Wal, db: *const c_void) {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xDb)(wal, db)
}

pub extern "C" fn xPathnameLen(orig_len: i32) -> i32 {
    orig_len + 4
}

pub extern "C" fn xGetPathname(buf: *mut u8, orig: *const u8, orig_len: i32) {
    unsafe { std::ptr::copy(orig, buf, orig_len as usize) }
    unsafe { std::ptr::copy("-wal".as_ptr(), buf.offset(orig_len as isize), 4) }
}

pub extern "C" fn xPreMainDbOpen(_methods: *mut libsql_wal_methods, path: *const i8) -> i32 {
    tracing::info!("Opening {:?} soon", unsafe { std::ffi::CStr::from_ptr(path) });
    ffi::SQLITE_OK
}

#[no_mangle]
pub extern "C" fn mwal_init() {
    tracing_subscriber::fmt::init();
    tracing::debug!("mWAL module initialized");
}

#[tracing::instrument]
#[no_mangle]
pub extern "C" fn mwal_methods(
    underlying_methods: *const libsql_wal_methods,
) -> *const libsql_wal_methods {
    let vwal_name: *const u8 = "mwal\0".as_ptr();

    Box::into_raw(Box::new(libsql_wal_methods {
        iVersion: 1,
        xOpen,
        xClose,
        xLimit,
        xBeginReadTransaction,
        xEndReadTransaction,
        xFindFrame,
        xReadFrame,
        xDbSize,
        xBeginWriteTransaction,
        xEndWriteTransaction,
        xUndo,
        xSavepoint,
        xSavepointUndo,
        xFrames,
        xCheckpoint,
        xCallback,
        xExclusiveMode,
        xHeapMemory,
        snapshot_get_stub: std::ptr::null(),
        snapshot_open_stub: std::ptr::null(),
        snapshot_recover_stub: std::ptr::null(),
        snapshot_check_stub: std::ptr::null(),
        snapshot_unlock_stub: std::ptr::null(),
        framesize_stub: std::ptr::null(),
        xFile,
        write_lock_stub: std::ptr::null(),
        xDb,
        xPathnameLen,
        xGetPathname,
        xPreMainDbOpen,
        name: vwal_name,
        b_uses_shm: 0,
        p_next: std::ptr::null(),
        underlying_methods,
    }))
}

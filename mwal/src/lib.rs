#![allow(non_snake_case)]
#![allow(clippy::not_unsafe_ptr_arg_deref)]

mod ffi;

use crate::ffi::{libsql_wal_methods, sqlite3_file, sqlite3_vfs, PgHdr, Wal};
use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::Arc;

fn instantiate_mvfs() -> mvfs::MultiVersionVfs {
    let mut sector_size: usize = 4096;
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
    /* FIXME: accept all these parameters
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

    // FIXME: accept this parameter
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

const WAL_NORMAL_MODE: u8 = 0;
const WAL_HEAPMEMORY_MODE: u8 = 2;

pub extern "C" fn xOpen(
    vfs: *const sqlite3_vfs,
    _db_file: *mut sqlite3_file,
    wal_name: *const i8,
    no_shm_mode: i32,
    max_size: i64,
    methods: *mut libsql_wal_methods,
    wal: *mut *mut Wal,
) -> i32 {
    tracing::debug!("Opening WAL {}", unsafe {
        std::ffi::CStr::from_ptr(wal_name).to_str().unwrap()
    });

    let mvfs = instantiate_mvfs();

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
    let name = match path.rfind('/') {
        Some(index) => path[index + 1..].to_string(),
        None => path.to_string(),
    };

    tracing::debug!("Database name: {}", name);
    let mut connection = match mvfs.open(&name, false) {
        // TODO: verify what's the map_name parameter for exactly
        Ok(conn) => conn,
        Err(e) => {
            tracing::error!("Failed to connect to mvstore: {}", e);
            return ffi::SQLITE_CANTOPEN;
        }
    };

    let db_size = runtime.block_on(async {
        if connection
            .lock(mvfs::types::LockKind::Shared)
            .await
            .is_err()
        {
            return 0;
        }
        let size = connection.size().await.unwrap_or(0);
        connection.unlock(mvfs::types::LockKind::None).await.ok();
        size
    });
    let n_pages = (db_size / mvfs.sector_size as u64) as u32;
    tracing::debug!(
        "Number of pages detected: {} (db size: {})",
        n_pages,
        db_size
    );

    let context = ffi::MvfsContext {
        mvfs,
        connection,
        runtime,
    };

    let exclusive_mode = if no_shm_mode != 0 {
        WAL_HEAPMEMORY_MODE
    } else {
        WAL_NORMAL_MODE
    };

    let page_size_u16 = if context.mvfs.sector_size == 65536 {
        1
    } else {
        context.mvfs.sector_size
    } as u16;
    // Allocate the WAL structure
    let new_wal = Box::into_raw(Box::new(ffi::Wal {
        vfs,
        db_fd: std::ptr::null_mut(),
        wal_fd: std::ptr::null_mut(),
        callback_value: 0,
        max_wal_size: max_size,
        wi_data: 0,
        size_first_block: 0,
        ap_wi_data: std::ptr::null_mut(),
        page_size: context.mvfs.sector_size as u32,
        read_lock: 0,
        sync_flags: 0,
        exclusive_mode,
        write_lock: 0,
        checkpoint_lock: 0,
        read_only: 0,
        truncate_on_commit: 0,
        sync_header: 0,
        pad_to_section_boundary: 0,
        b_shm_unreliable: 1,
        hdr: ffi::WalIndexHdr {
            version: 1,
            unused: 0,
            change: 0,
            is_init: 0,
            big_endian_checksum: 0,
            page_size: page_size_u16,
            last_valid_frame: n_pages,
            n_pages,
            frame_checksum: [0, 0],
            salt: [0, 0],
            checksum: [0, 0],
        },
        min_frame: 0,
        recalculate_checksums: 0,
        wal_name,
        n_checkpoints: 0,
        lock_error: 0,
        p_snapshot: std::ptr::null(),
        p_db: std::ptr::null(),
        wal_methods: methods,
        mvfs_context: Box::leak(Box::new(context)),
    }));

    unsafe { *wal = new_wal }
    tracing::trace!("Opened WAL at {:?}", new_wal);

    ffi::SQLITE_OK
}

fn get_mvfs_context(wal: *mut Wal) -> &'static mut ffi::MvfsContext {
    unsafe { &mut *((*wal).mvfs_context) }
}

pub extern "C" fn xClose(
    wal: *mut Wal,
    _db: *mut c_void,
    _sync_flags: i32,
    _n_buf: i32,
    _z_buf: *mut u8,
) -> i32 {
    tracing::debug!("Closing wal");
    let _mvfs_box = unsafe { Box::from_raw((*wal).mvfs_context) };
    let _wal_box = unsafe { Box::from_raw(wal) };
    ffi::SQLITE_OK
}

pub extern "C" fn xLimit(_wal: *mut Wal, _limit: i64) {
    tracing::debug!("xLimit");
}

pub extern "C" fn xBeginReadTransaction(wal: *mut Wal, changed: *mut i32) -> i32 {
    tracing::debug!("BeginReadTransaction");
    let ctx = get_mvfs_context(wal);

    unsafe { *changed = 1 }

    ctx.runtime.block_on(async {
        match ctx.connection.lock(mvfs::types::LockKind::Shared).await {
            Err(e) => {
                tracing::error!("BeginReadTransaction failed: {}", e);
                return ffi::SQLITE_IOERR_LOCK;
            }
            Ok(true) => ffi::SQLITE_OK,
            Ok(false) => ffi::SQLITE_BUSY,
        }
    })
}

pub extern "C" fn xEndReadTransaction(wal: *mut Wal) -> i32 {
    tracing::debug!("EndReadTransaction");
    let ctx = get_mvfs_context(wal);

    ctx.runtime.block_on(async {
        match ctx.connection.unlock(mvfs::types::LockKind::None).await {
            Err(e) => {
                tracing::error!("EndReadTransaction failed: {}", e);
                return ffi::SQLITE_IOERR_UNLOCK;
            }
            Ok(true) => ffi::SQLITE_OK,
            Ok(false) => ffi::SQLITE_BUSY,
        }
    })
}

pub extern "C" fn xFindFrame(wal: *mut Wal, pgno: i32, frame: *mut i32) -> i32 {
    tracing::trace!("FindFrame for {}", pgno);
    if unsafe { (*wal).hdr.n_pages } > 0 {
        unsafe { *frame = pgno };
    }
    ffi::SQLITE_OK
}

pub extern "C" fn xReadFrame(wal: *mut Wal, frame: u32, n_out: i32, p_out: *mut u8) -> i32 {
    tracing::trace!("ReadFrame {}", frame);
    let ctx = get_mvfs_context(wal);
    let page_size = ctx.mvfs.sector_size as u32;
    assert!(n_out as u32 >= page_size);
    let buf = unsafe { std::slice::from_raw_parts_mut(p_out, n_out as usize) };
    ctx.runtime.block_on(async {
        if let Err(e) = ctx
            .connection
            .read_exact_at(buf, page_offset(frame as i32, page_size))
            .await
        {
            tracing::error!("Read failed: {}", e);
            return ffi::SQLITE_IOERR_READ;
        }
        ffi::SQLITE_OK
    })
}

pub extern "C" fn xDbSize(wal: *mut Wal) -> u32 {
    tracing::debug!("Dbsize: {}", unsafe { (*wal).hdr.n_pages });
    unsafe { (*wal).hdr.n_pages }
}

pub extern "C" fn xBeginWriteTransaction(wal: *mut Wal) -> i32 {
    tracing::trace!("BeginWriteTransaction");
    let ctx = get_mvfs_context(wal);

    ctx.runtime.block_on(async {
        // FIXME: reserved and pending locks should be taken into account as well
        match ctx.connection.lock(mvfs::types::LockKind::Exclusive).await {
            Err(e) => {
                tracing::error!("BeginWriteTransaction failed: {}", e);
                return ffi::SQLITE_IOERR_LOCK;
            }
            Ok(true) => ffi::SQLITE_OK,
            Ok(false) => ffi::SQLITE_BUSY,
        }
    })
}

pub extern "C" fn xEndWriteTransaction(wal: *mut Wal) -> i32 {
    tracing::trace!("EndWriteTransaction");
    let ctx = get_mvfs_context(wal);

    ctx.runtime.block_on(async {
        match ctx.connection.unlock(mvfs::types::LockKind::None).await {
            Err(e) => {
                tracing::error!("EndWriteTransaction failed: {}", e);
                return ffi::SQLITE_IOERR_UNLOCK;
            }
            Ok(true) => ffi::SQLITE_OK,
            Ok(false) => ffi::SQLITE_BUSY,
        }
    })
}

pub extern "C" fn xUndo(
    _wal: *mut Wal,
    _func: extern "C" fn(*mut c_void, i32) -> i32,
    _ctx: *mut c_void,
) -> i32 {
    tracing::debug!("xUndo");
    ffi::SQLITE_OK
}

pub extern "C" fn xSavepoint(_wal: *mut Wal, _wal_data: *mut u32) {
    tracing::error!("xSavepoint is not implemented yet");
}

pub extern "C" fn xSavepointUndo(_wal: *mut Wal, _wal_data: *mut u32) -> i32 {
    tracing::error!("xSavepointUndo is not implemented yet");
    ffi::SQLITE_MISUSE
}

fn page_offset(pgno: i32, page_size: u32) -> u64 {
    ((pgno as u32 - 1) * page_size) as u64
}

pub extern "C" fn xFrames(
    wal: *mut Wal,
    page_size: u32,
    page_headers: *const PgHdr,
    size_after: u32,
    is_commit: i32,
    _sync_flags: i32,
) -> i32 {
    tracing::trace!("xFrames called, is_commit = {}", is_commit);
    let ctx = get_mvfs_context(wal);

    let rc = ctx.runtime.block_on(async {
        for (pgno, data) in ffi::PageHdrIter::new(page_headers, page_size as usize) {
            if let Err(e) = ctx
                .connection
                .write_all_at(&data, page_offset(pgno, page_size))
                .await
            {
                tracing::error!("Failed to write frames: {}", e);
                return ffi::SQLITE_IOERR_WRITE;
            };
            tracing::trace!("Written page {}", pgno);
        }
        if is_commit != 0 {
            tracing::trace!("Confirming commit");
            ctx.connection.confirm_commit();
        }
        ffi::SQLITE_OK
    });
    if rc != ffi::SQLITE_OK {
        return rc;
    }
    if size_after > 0 {
        unsafe { (*wal).hdr.n_pages = size_after }
    }
    rc
}

pub extern "C" fn xCheckpoint(
    _wal: *mut Wal,
    _db: *mut c_void,
    _emode: i32,
    _busy_handler: extern "C" fn(busy_param: *mut c_void) -> i32,
    _busy_arg: *const c_void,
    _sync_flags: i32,
    _n_buf: i32,
    _z_buf: *mut u8,
    _frames_in_wal: *mut i32,
    _backfilled_frames: *mut i32,
) -> i32 {
    tracing::trace!("Skipping a checkpoint");
    ffi::SQLITE_OK
}

pub extern "C" fn xCallback(_wal: *mut Wal) -> i32 {
    tracing::trace!("xCallback");
    ffi::SQLITE_MISUSE
}

pub extern "C" fn xExclusiveMode(_wal: *mut Wal) -> i32 {
    tracing::trace!("xExclusiveMode - returning 0");
    0
}

pub extern "C" fn xHeapMemory(_wal: *mut Wal) -> i32 {
    tracing::trace!("xHeapMemory");
    42
}

pub extern "C" fn xFile(_wal: *mut Wal) -> *const c_void {
    tracing::error!("xFile not implemented");
    std::ptr::null()
}

pub extern "C" fn xDb(_wal: *mut Wal, _db: *const c_void) {
    tracing::error!("xDb not implemented");
}

pub extern "C" fn xPathnameLen(orig_len: i32) -> i32 {
    orig_len + 4
}

pub extern "C" fn xGetPathname(buf: *mut u8, orig: *const u8, orig_len: i32) {
    unsafe { std::ptr::copy(orig, buf, orig_len as usize) }
    unsafe { std::ptr::copy("-wal".as_ptr(), buf.offset(orig_len as isize), 4) }
}

pub extern "C" fn xPreMainDbOpen(_methods: *mut libsql_wal_methods, path: *const i8) -> i32 {
    tracing::trace!("Database {:?} is going to be soon", unsafe {
        std::ffi::CStr::from_ptr(path)
    });
    ffi::SQLITE_OK
}

#[no_mangle]
pub extern "C" fn mwal_init() {
    tracing_subscriber::fmt::init();
    tracing::info!("mWAL module initialized");
}

#[tracing::instrument]
#[no_mangle]
pub extern "C" fn mwal_methods() -> *const libsql_wal_methods {
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
    }))
}

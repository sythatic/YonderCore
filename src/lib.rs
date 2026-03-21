//! lib.rs — YonderCore static library entry point
//!
//! This file is a pure coordinator.  All logic lives in the submodules:
//!
//! | Module           | Owns                                                    |
//! |------------------|---------------------------------------------------------|
//! | `stops_db`       | `GTFSStop`, `StopsDatabase`, R-tree, CSV parsing, FFI.   |
//! | `tile_cache`     | `TileCacheCore`, rkyv on-disk cache (internals only).    |
//! | `gtfs_rt`        | `GtfsRtCore`, protobuf parsing, enriched vehicles, FFI.  |
//! | `gtfs_static`    | `GtfsStaticStore` registry, ZIP parsing, interpolation, FFI. |
//! | `shapes_editor`  | `ShapesEditorStore` registry, shapes.txt editing, FFI.   |
//!
//! `tile_cache` exposes only a Rust-level API; its `tile_cache_*` FFI wrappers
//! live here because the module is intentionally sealed against modification.
//! All other modules own their own FFI.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;

// ── Modules ───────────────────────────────────────────────────────────────────

mod geo;
mod stops_db;
mod tile_cache;
mod gtfs_rt;
mod gtfs_static;
mod shapes_editor;

pub use stops_db::*;
pub use tile_cache::*;
pub use gtfs_rt::*;
pub use gtfs_static::*;
pub use shapes_editor::*;

// ── Library probe ─────────────────────────────────────────────────────────────

/// Returns a static greeting string to confirm the library loaded correctly.
/// Free with `free_rust_string`.
#[no_mangle]
pub extern "C" fn hello_from_rust() -> *const c_char {
    CString::new("Library initialized successfully")
        .unwrap_or_else(|_| CString::new("Error").unwrap())
        .into_raw()
}

/// Free a string returned by `hello_from_rust` (or any other Rust-allocated
/// `*const c_char` that was produced via `CString::into_raw`).
#[no_mangle]
pub extern "C" fn free_rust_string(ptr: *mut c_char) {
    if ptr.is_null() { return; }
    unsafe { let _ = CString::from_raw(ptr); }
}

// ── TileCacheCore FFI ─────────────────────────────────────────────────────────
//
// tile_cache.rs is sealed (no modifications allowed), so its FFI wrappers live
// here rather than in the module itself.  Every function is a thin delegation
// to the Rust-level `TileCacheCore` API.

/// Create a new tile cache backed by the directory at `path`.
/// Returns NULL on error.  Free with `tile_cache_free`.
#[no_mangle]
pub extern "C" fn tile_cache_new(path: *const c_char) -> *mut TileCacheCore {
    if path.is_null() { return std::ptr::null_mut(); }
    unsafe {
        let path_str = match CStr::from_ptr(path).to_str() {
            Ok(s)  => s,
            Err(_) => return std::ptr::null_mut(),
        };
        match TileCacheCore::new(path_str.into()) {
            Ok(cache) => Box::into_raw(Box::new(cache)),
            Err(_)    => std::ptr::null_mut(),
        }
    }
}

/// Save tile data to disk.
/// `is_negative != 0` writes a negative-cache sentinel (no data bytes needed).
/// Returns 1 on success, 0 on error.
#[no_mangle]
pub extern "C" fn tile_cache_save(
    cache:           *mut TileCacheCore,
    cache_key:       *const c_char,
    data:            *const u8,
    data_len:        usize,
    expiration_secs: i64,
    is_negative:     i32,
) -> i32 {
    if cache.is_null() || cache_key.is_null() { return 0; }
    if data_len > 0 && data.is_null()         { return 0; }
    unsafe {
        let cache = &*cache;
        let key_str = match CStr::from_ptr(cache_key).to_str() { Ok(s) => s, Err(_) => return 0 };
        let data_slice = if data_len > 0 && !data.is_null() {
            std::slice::from_raw_parts(data, data_len)
        } else {
            &[]
        };
        let meta = if is_negative != 0 {
            TileMeta::new_negative(expiration_secs)
        } else {
            TileMeta::new_success(expiration_secs, data_len as u32)
        };
        match cache.save_tile(key_str, data_slice, &meta) { Ok(_) => 1, Err(_) => 0 }
    }
}

/// Load tile data from disk.
/// On success sets `*out_data_len`, `*out_http_status`, and `*out_is_negative`
/// (pass NULL for any you don't need) and returns a heap-allocated byte slice.
/// Free with `tile_cache_free_data`.  Returns NULL on cache miss or error.
#[no_mangle]
pub extern "C" fn tile_cache_load(
    cache:           *const TileCacheCore,
    cache_key:       *const c_char,
    out_data_len:    *mut usize,
    out_http_status: *mut u16,
    out_is_negative: *mut i32,
) -> *mut u8 {
    if cache.is_null() || cache_key.is_null() || out_data_len.is_null() {
        return std::ptr::null_mut();
    }
    unsafe {
        let cache   = &*cache;
        let key_str = match CStr::from_ptr(cache_key).to_str() {
            Ok(s)  => s,
            Err(_) => return std::ptr::null_mut(),
        };
        match cache.load_tile(key_str) {
            Some(ct) => {
                *out_data_len = ct.data.len();
                if !out_http_status.is_null() { *out_http_status = ct.meta.http_status; }
                if !out_is_negative.is_null() { *out_is_negative = if ct.meta.is_negative { 1 } else { 0 }; }
                let mut data = ct.data;
                let ptr = data.as_mut_ptr();
                std::mem::forget(data);
                ptr
            }
            None => std::ptr::null_mut(),
        }
    }
}

/// Free tile data returned by `tile_cache_load`.
#[no_mangle]
pub extern "C" fn tile_cache_free_data(data: *mut u8, len: usize) {
    if data.is_null() || len == 0 { return; }
    unsafe { let _ = Vec::from_raw_parts(data, len, len); }
}

/// Remove a single tile from the cache by key.
/// Returns 1 on success, 0 on error.
#[no_mangle]
pub extern "C" fn tile_cache_remove(cache: *mut TileCacheCore, cache_key: *const c_char) -> i32 {
    if cache.is_null() || cache_key.is_null() { return 0; }
    unsafe {
        let key_str = match CStr::from_ptr(cache_key).to_str() { Ok(s) => s, Err(_) => return 0 };
        match (&*cache).delete_tile(key_str) { Ok(_) => 1, Err(_) => 0 }
    }
}

/// Returns 1 if the tile exists and has not expired.
#[no_mangle]
pub extern "C" fn tile_cache_is_valid(cache: *const TileCacheCore, cache_key: *const c_char) -> i32 {
    if cache.is_null() || cache_key.is_null() { return 0; }
    unsafe {
        let key_str = match CStr::from_ptr(cache_key).to_str() { Ok(s) => s, Err(_) => return 0 };
        if (&*cache).is_valid(key_str) { 1 } else { 0 }
    }
}

/// Delete all expired tiles.  Returns the number of tiles removed.
#[no_mangle]
pub extern "C" fn tile_cache_cleanup_expired(cache: *mut TileCacheCore) -> usize {
    if cache.is_null() { return 0; }
    unsafe { (&*cache).clear_expired().unwrap_or(0) as usize }
}

/// Total on-disk size of the cache in bytes.
#[no_mangle]
pub extern "C" fn tile_cache_size(cache: *const TileCacheCore) -> u64 {
    if cache.is_null() { return 0; }
    unsafe { (&*cache).cache_size().unwrap_or(0) }
}

/// Number of tiles currently in the cache.
#[no_mangle]
pub extern "C" fn tile_cache_count(cache: *const TileCacheCore) -> usize {
    if cache.is_null() { return 0; }
    unsafe { (&*cache).tile_count().unwrap_or(0) }
}

/// Remove all tiles from the cache.  Returns 1 on success, 0 on error.
#[no_mangle]
pub extern "C" fn tile_cache_clear_all(cache: *mut TileCacheCore) -> i32 {
    if cache.is_null() { return 0; }
    unsafe { match (&*cache).clear_all() { Ok(_) => 1, Err(_) => 0 } }
}

/// Record a memory-layer cache hit for statistics.
#[no_mangle]
pub extern "C" fn tile_cache_record_memory_hit(cache: *const TileCacheCore) {
    if cache.is_null() { return; }
    unsafe { (&*cache).record_memory_hit(); }
}

/// Record a network fetch for statistics.
#[no_mangle]
pub extern "C" fn tile_cache_record_network_fetch(cache: *const TileCacheCore) {
    if cache.is_null() { return; }
    unsafe { (&*cache).record_network_fetch(); }
}

/// Record a cache miss for statistics.
#[no_mangle]
pub extern "C" fn tile_cache_record_cache_miss(cache: *const TileCacheCore) {
    if cache.is_null() { return; }
    unsafe { (&*cache).record_cache_miss(); }
}

/// Read out all cache statistics counters.
/// Any output pointer may be NULL if that counter is not needed.
#[no_mangle]
pub extern "C" fn tile_cache_get_stats(
    cache:               *const TileCacheCore,
    out_memory_hits:     *mut u64,
    out_disk_hits:       *mut u64,
    out_network_fetches: *mut u64,
    out_cache_misses:    *mut u64,
    out_expired_tiles:   *mut u64,
) {
    if cache.is_null() { return; }
    unsafe {
        let stats = (&*cache).statistics();
        if !out_memory_hits.is_null()     { *out_memory_hits     = stats.memory_hits;     }
        if !out_disk_hits.is_null()       { *out_disk_hits       = stats.disk_hits;       }
        if !out_network_fetches.is_null() { *out_network_fetches = stats.network_fetches; }
        if !out_cache_misses.is_null()    { *out_cache_misses    = stats.cache_misses;    }
        if !out_expired_tiles.is_null()   { *out_expired_tiles   = stats.expired_tiles;   }
    }
}

/// Reset all statistics counters to zero.
#[no_mangle]
pub extern "C" fn tile_cache_reset_stats(cache: *mut TileCacheCore) {
    if cache.is_null() { return; }
    unsafe { (&*cache).reset_statistics(); }
}

/// Free a `TileCacheCore` allocated by `tile_cache_new`.
#[no_mangle]
pub extern "C" fn tile_cache_free(cache: *mut TileCacheCore) {
    if !cache.is_null() { unsafe { let _ = Box::from_raw(cache); } }
}

/// Write a negative-cache sentinel for `cache_key` with a `ttl_seconds` TTL.
/// Returns 1 on success, 0 on error.
#[no_mangle]
pub extern "C" fn tile_cache_save_negative(
    cache:      *mut TileCacheCore,
    cache_key:  *const c_char,
    ttl_seconds: i64,
) -> i32 {
    if cache.is_null() || cache_key.is_null() { return 0; }
    unsafe {
        let key = match CStr::from_ptr(cache_key).to_str() { Ok(s) => s, Err(_) => return 0 };
        match (&*cache).save_negative_cache(key, ttl_seconds) { Ok(_) => 1, Err(_) => 0 }
    }
}

/// Returns 1 if `cache_key` is a valid, unexpired negative-cache entry.
#[no_mangle]
pub extern "C" fn tile_cache_is_negative(
    cache:     *const TileCacheCore,
    cache_key: *const c_char,
) -> i32 {
    if cache.is_null() || cache_key.is_null() { return 0; }
    unsafe {
        let key = match CStr::from_ptr(cache_key).to_str() { Ok(s) => s, Err(_) => return 0 };
        if (&*cache).is_negative_cache(key) { 1 } else { 0 }
    }
}

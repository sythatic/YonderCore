use std::ffi::{CString, CStr};
use std::os::raw::c_char;
use std::sync::Arc;
use parking_lot::RwLock;  // No lock-poisoning on panic; faster than std RwLock
use std::collections::HashMap;
use csv::ReaderBuilder;
use rstar::{RTree, AABB, primitives::GeomWithData};

// MARK: - Modules

mod tile_cache;
mod gtfs_rt;

pub use tile_cache::*;
pub use gtfs_rt::*;

// MARK: - Thread-local Last Error
//
// Any FFI function that fails can store a diagnostic string here.
// The C/Swift side retrieves it with yonder_last_error().

use std::cell::RefCell;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

/// Store an error message in the thread-local slot.
fn set_last_error(msg: impl Into<Vec<u8>>) {
    let cstring = CString::new(msg).unwrap_or_else(|_| {
        CString::new("(error message contained interior null byte)").unwrap()
    });
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = Some(cstring);
    });
}

/// Return a pointer to the last error string for the calling thread, or NULL
/// if no error has occurred. The string is valid until the next YonderCore call
/// on this thread. Do NOT free the returned pointer.
#[no_mangle]
pub extern "C" fn yonder_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|s| s.as_ptr())
            .unwrap_or(std::ptr::null())
    })
}

/// Clear the thread-local last error.
#[no_mangle]
pub extern "C" fn yonder_clear_error() {
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

// MARK: - Core Data Structures

/// A GTFS stop with all required fields — Box<str> is smaller than String for
/// immutable string data (no spare capacity pointer).
#[derive(Debug, Clone)]
pub struct GTFSStop {
    pub id: Box<str>,
    pub name: Box<str>,
    pub url: Option<Box<str>>,
    pub lat: f64,
    pub lon: f64,
    pub providers: Vec<Box<str>>,
}

impl GTFSStop {
    #[inline(always)]
    pub fn has_valid_coordinates(&self) -> bool {
        self.lat >= -90.0 && self.lat <= 90.0 && self.lon >= -180.0 && self.lon <= 180.0
    }

    pub fn new(
        id: String,
        name: String,
        url: Option<String>,
        lat: f64,
        lon: f64,
        providers: Vec<String>,
    ) -> Result<Self, &'static str> {
        if lat < -90.0 || lat > 90.0 {
            return Err("Invalid latitude");
        }
        if lon < -180.0 || lon > 180.0 {
            return Err("Invalid longitude");
        }
        Ok(Self {
            id: id.into_boxed_str(),
            name: name.into_boxed_str(),
            url: url.map(|s| s.into_boxed_str()),
            lat,
            lon,
            providers: providers.into_iter().map(|s| s.into_boxed_str()).collect(),
        })
    }
}

// MARK: - C-compatible Stop Struct
//
// Replaces the old pipe-delimited char** approach. Swift/C receives typed fields
// and is not required to parse strings — removing the risk of misparse when stop
// names contain the delimiter character.
//
// Ownership: all pointer fields are owned by Rust and must be freed by calling
// stops_db_free_stop_array(ptr, count). Never free individual fields directly.

/// C-compatible representation of a GTFS stop.
#[repr(C)]
pub struct CStop {
    /// Owned UTF-8 C string. Never null.
    pub id: *mut c_char,
    /// Owned UTF-8 C string. Never null.
    pub name: *mut c_char,
    pub lat: f64,
    pub lon: f64,
    /// Owned UTF-8 C string, or NULL if not present.
    pub url: *mut c_char,
    /// Owned UTF-8 C string of comma-joined provider IDs, or NULL if none.
    pub providers: *mut c_char,
}

impl CStop {
    fn from_gtfs(stop: &GTFSStop) -> Option<Self> {
        let id = CString::new(stop.id.as_ref()).ok()?.into_raw();
        let name = CString::new(stop.name.as_ref()).ok().map(|s| s.into_raw()).unwrap_or_else(|| {
            // If name conversion fails, free id and return None — handled by caller.
            // We use a sentinel to signal failure without panicking.
            std::ptr::null_mut()
        });

        if name.is_null() {
            // Free id before bailing.
            unsafe { drop(CString::from_raw(id)); }
            return None;
        }

        let url = stop.url.as_deref()
            .and_then(|s| CString::new(s).ok())
            .map(|s| s.into_raw())
            .unwrap_or(std::ptr::null_mut());

        let providers = if stop.providers.is_empty() {
            std::ptr::null_mut()
        } else {
            let joined = stop.providers.iter()
                .map(|s| s.as_ref())
                .collect::<Vec<&str>>()
                .join(",");
            CString::new(joined).ok().map(|s| s.into_raw()).unwrap_or(std::ptr::null_mut())
        };

        Some(CStop { id, name, lat: stop.lat, lon: stop.lon, url, providers })
    }

    /// Free all owned string pointers. Consumes self to prevent double-free.
    unsafe fn free_owned(self) {
        if !self.id.is_null() { drop(CString::from_raw(self.id)); }
        if !self.name.is_null() { drop(CString::from_raw(self.name)); }
        if !self.url.is_null() { drop(CString::from_raw(self.url)); }
        if !self.providers.is_null() { drop(CString::from_raw(self.providers)); }
    }
}

// MARK: - R-tree Spatial Index

type StopPoint = GeomWithData<[f64; 2], usize>;

// MARK: - Thread-Safe Stops Database

pub struct StopsDatabase {
    inner: Arc<RwLock<StopsDatabaseInner>>,
}

struct StopsDatabaseInner {
    stops: Vec<GTFSStop>,
    spatial_index: RTree<StopPoint>,
    id_index: HashMap<Box<str>, usize>,
}

impl StopsDatabase {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(StopsDatabaseInner {
                stops: Vec::with_capacity(10000),
                spatial_index: RTree::new(),
                id_index: HashMap::with_capacity(10000),
            }))
        }
    }

    /// Load stops from a CSV file (appends to existing stops).
    pub fn load_from_csv(&self, csv_path: &str) -> Result<usize, String> {
        let new_stops = parse_gtfs_csv_optimized(csv_path)?;
        let count = new_stops.len();

        let mut inner = self.inner.write();
        let start_idx = inner.stops.len();

        for (i, stop) in new_stops.into_iter().enumerate() {
            let idx = start_idx + i;
            inner.id_index.insert(stop.id.clone(), idx);
            if stop.has_valid_coordinates() {
                inner.spatial_index.insert(GeomWithData::new([stop.lon, stop.lat], idx));
            }
            inner.stops.push(stop);
        }

        Ok(count)
    }

    /// Load multiple CSV files at once with a single bulk R-tree build.
    /// Prefer over repeated load_from_csv calls for large datasets.
    pub fn load_from_csv_bulk(&self, csv_paths: &[&str]) -> Result<usize, String> {
        let mut all_new_stops: Vec<GTFSStop> = Vec::new();
        for path in csv_paths {
            let mut stops = parse_gtfs_csv_optimized(path)?;
            all_new_stops.append(&mut stops);
        }

        let total = all_new_stops.len();

        let mut inner = self.inner.write();
        let start_idx = inner.stops.len();
        let mut new_points = Vec::with_capacity(total);

        for (i, stop) in all_new_stops.into_iter().enumerate() {
            let idx = start_idx + i;
            inner.id_index.insert(stop.id.clone(), idx);
            if stop.has_valid_coordinates() {
                new_points.push(GeomWithData::new([stop.lon, stop.lat], idx));
            }
            inner.stops.push(stop);
        }

        if !new_points.is_empty() {
            let mut all_points = Vec::with_capacity(inner.stops.len());
            for (idx, stop) in inner.stops.iter().enumerate() {
                if stop.has_valid_coordinates() {
                    all_points.push(GeomWithData::new([stop.lon, stop.lat], idx));
                }
            }
            inner.spatial_index = RTree::bulk_load(all_points);
        }

        Ok(total)
    }

    pub fn stops_near(&self, lat: f64, lon: f64, radius_meters: f64) -> Result<Vec<GTFSStop>, String> {
        if lat < -90.0 || lat > 90.0 || lon < -180.0 || lon > 180.0 {
            return Err("Invalid coordinates".to_string());
        }

        let inner = self.inner.read();

        let lat_rad = lat.to_radians();
        let meters_per_degree_lat = 111_132.92 - 559.82 * (2.0 * lat_rad).cos()
            + 1.175 * (4.0 * lat_rad).cos() - 0.0023 * (6.0 * lat_rad).cos();
        let meters_per_degree_lon = 111_412.84 * lat_rad.cos()
            - 93.5 * (3.0 * lat_rad).cos() + 0.118 * (5.0 * lat_rad).cos();

        let radius_lat = radius_meters / meters_per_degree_lat;
        let radius_lon = radius_meters / meters_per_degree_lon.abs().max(0.00001);

        let envelope = AABB::from_corners(
            [lon - radius_lon, lat - radius_lat],
            [lon + radius_lon, lat + radius_lat],
        );

        let mut results = Vec::with_capacity(100);
        for point in inner.spatial_index.locate_in_envelope(&envelope) {
            let stop = &inner.stops[point.data];
            if haversine_distance(lat, lon, stop.lat, stop.lon) <= radius_meters {
                results.push(stop.clone());
            }
        }

        results.shrink_to_fit();
        Ok(results)
    }

    pub fn find_by_id(&self, id: &str) -> Result<Option<GTFSStop>, String> {
        let inner = self.inner.read();
        Ok(inner.id_index.get(id).map(|&idx| inner.stops[idx].clone()))
    }

    pub fn get_all(&self) -> Result<Vec<GTFSStop>, String> {
        let inner = self.inner.read();
        Ok(inner.stops.clone())
    }

    pub fn count(&self) -> usize {
        self.inner.read().stops.len()
    }

    pub fn clear(&self) -> Result<(), String> {
        let mut inner = self.inner.write();
        inner.stops.clear();
        inner.spatial_index = RTree::new();
        inner.id_index.clear();
        Ok(())
    }
}

// MARK: - CSV Parsing

#[inline]
fn parse_comma_separated_providers(input: &str) -> Vec<String> {
    input.split(',')
        .filter_map(|p| {
            let trimmed = p.trim();
            if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
        })
        .collect()
}

fn parse_gtfs_csv_optimized(path: &str) -> Result<Vec<GTFSStop>, String> {
    // Pass the File directly — the csv crate has its own 8 MB internal buffer.
    // Wrapping in BufReader would add a redundant 64 KB buffer on top for no gain.
    let file = std::fs::File::open(path)
        .map_err(|e| format!("YonderCore: Failed to open CSV: {}", e))?;

    let mut reader = ReaderBuilder::new()
        .buffer_capacity(8 * 1024 * 1024)
        .from_reader(file);

    let headers = reader.headers()
        .map_err(|e| format!("YonderCore: Failed to read headers: {}", e))?;

    let id_idx = find_header_index(&headers, "stop_id")
        .ok_or("YonderCore: Missing stop_id column")?;
    let name_idx = find_header_index(&headers, "stop_name")
        .ok_or("YonderCore: Missing stop_name column")?;
    let lat_idx = find_header_index(&headers, "stop_lat")
        .ok_or("YonderCore: Missing stop_lat column")?;
    let lon_idx = find_header_index(&headers, "stop_lon")
        .ok_or("YonderCore: Missing stop_lon column")?;

    let url_idx = find_header_index(&headers, "stop_url");
    let providers_idx = find_header_index(&headers, "providers");
    let agency_idx = find_header_index(&headers, "agency_id");

    let mut stops = Vec::with_capacity(10000);
    let mut line_num = 1;

    for result in reader.records() {
        line_num += 1;

        let record = result
            .map_err(|e| format!("YonderCore: Failed to parse line {}: {}", line_num, e))?;

        let id = record.get(id_idx)
            .ok_or_else(|| format!("Missing stop_id at line {}", line_num))?
            .trim();
        if id.is_empty() { continue; }

        let name = record.get(name_idx)
            .ok_or_else(|| format!("Missing stop_name at line {}", line_num))?
            .trim();
        if name.is_empty() { continue; }

        let lat_str = record.get(lat_idx)
            .ok_or_else(|| format!("Missing stop_lat at line {}", line_num))?
            .trim();
        let lat: f64 = lat_str.parse()
            .map_err(|_| format!("Invalid latitude '{}' at line {}", lat_str, line_num))?;

        let lon_str = record.get(lon_idx)
            .ok_or_else(|| format!("Missing stop_lon at line {}", line_num))?
            .trim();
        let lon: f64 = lon_str.parse()
            .map_err(|_| format!("Invalid longitude '{}' at line {}", lon_str, line_num))?;

        if lat < -90.0 || lat > 90.0 || lon < -180.0 || lon > 180.0 { continue; }

        let url = url_idx.and_then(|idx| {
            record.get(idx).and_then(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
            })
        });

        let providers: Vec<String> = if let Some(idx) = providers_idx {
            record.get(idx).map(|s| parse_comma_separated_providers(s)).unwrap_or_default()
        } else if let Some(idx) = agency_idx {
            record.get(idx)
                .and_then(|s| {
                    let trimmed = s.trim();
                    if trimmed.is_empty() { None } else { Some(parse_comma_separated_providers(trimmed)) }
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        match GTFSStop::new(id.to_string(), name.to_string(), url, lat, lon, providers) {
            Ok(stop) => stops.push(stop),
            Err(_) => continue,
        }
    }

    stops.shrink_to_fit();
    Ok(stops)
}

#[inline]
fn find_header_index(headers: &csv::StringRecord, name: &str) -> Option<usize> {
    headers.iter().position(|h| h.trim().eq_ignore_ascii_case(name))
}

// MARK: - Distance Calculation

/// Haversine great-circle distance in metres.
/// Uses atan2 for better numerical stability near antipodal points.
#[inline(always)]
fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_METERS: f64 = 6_371_000.0;
    const DEG_TO_RAD: f64 = std::f64::consts::PI / 180.0;

    let lat1_rad = lat1 * DEG_TO_RAD;
    let lat2_rad = lat2 * DEG_TO_RAD;
    let delta_lat = (lat2 - lat1) * DEG_TO_RAD;
    let delta_lon = (lon2 - lon1) * DEG_TO_RAD;

    let sin_dlat = (delta_lat * 0.5).sin();
    let sin_dlon = (delta_lon * 0.5).sin();

    let a = sin_dlat * sin_dlat + lat1_rad.cos() * lat2_rad.cos() * sin_dlon * sin_dlon;
    // atan2 form — numerically stable for all distances including antipodal.
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    EARTH_RADIUS_METERS * c
}

// MARK: - Helper: Convert stops slice to a heap-allocated CStop array

/// Convert a slice of GTFSStop into a C-compatible CStop array.
/// Returns NULL and sets out_count = 0 on failure, cleaning up all allocations.
unsafe fn stops_to_c_array(stops: &[GTFSStop], out_count: *mut usize) -> *mut CStop {
    let mut result: Vec<CStop> = Vec::with_capacity(stops.len());

    for stop in stops {
        match CStop::from_gtfs(stop) {
            Some(c_stop) => result.push(c_stop),
            None => {
                // Conversion failed; free everything allocated so far.
                for c in result {
                    c.free_owned();
                }
                *out_count = 0;
                return std::ptr::null_mut();
            }
        }
    }

    *out_count = result.len();
    let boxed = result.into_boxed_slice();
    Box::into_raw(boxed) as *mut CStop
}

// MARK: - FFI String Helper

/// Convert a non-null `*const c_char` to a `&str`, recording a last-error and
/// returning `None` on invalid UTF-8. Eliminates the repeated match arms that
/// appear in every FFI function that accepts a C string argument.
///
/// # Safety
/// ptr must be a valid, non-null, null-terminated C string for lifetime a`.
unsafe fn cstr_to_str<'a>(ptr: *const c_char, context: &str) -> Option<&'a str> {
    match CStr::from_ptr(ptr).to_str() {
        Ok(s) => Some(s),
        Err(e) => {
            set_last_error(format!("{}: {}", context, e));
            None
        }
    }
}

// MARK: - Test Functions

/// Returns a static diagnostic string. The pointer is valid for the lifetime of
/// the process. Do NOT free it.
#[no_mangle]
pub extern "C" fn hello_from_rust() -> *const c_char {
    // Static CString — no heap allocation, no leak risk.
    static MSG: &[u8] = b"Library initialized successfully\0";
    MSG.as_ptr() as *const c_char
}

/// Free a string that was explicitly allocated by Rust for the caller.
/// Only use for strings where the documentation says to call this.
#[no_mangle]
pub extern "C" fn free_rust_string(ptr: *mut c_char) {
    if ptr.is_null() { return; }
    unsafe { drop(CString::from_raw(ptr)); }
}

// MARK: - StopsDatabase FFI

#[no_mangle]
pub extern "C" fn stops_db_new() -> *mut StopsDatabase {
    Box::into_raw(Box::new(StopsDatabase::new()))
}

/// Load stops from a CSV file.
/// Returns the number of stops loaded (saturated to i32::MAX), or -1 on error.
/// On error, call yonder_last_error() for a description.
#[no_mangle]
pub extern "C" fn stops_db_load_csv(db: *mut StopsDatabase, path: *const c_char) -> i32 {
    if db.is_null() || path.is_null() { return -1; }

    unsafe {
        let db = &*db;
        let path_str = match CStr::from_ptr(path).to_str() {
            Ok(s) => s,
            Err(e) => {
                set_last_error(format!("Invalid UTF-8 path: {}", e));
                return -1;
            }
        };

        match db.load_from_csv(path_str) {
            Ok(count) => count.min(i32::MAX as usize) as i32,
            Err(e) => {
                set_last_error(e);
                -1
            }
        }
    }
}

/// Find stops within radius_meters of (lat, lon).
/// Returns a heap-allocated array of CStop structs. Free with stops_db_free_stop_array.
/// Returns NULL on error (check yonder_last_error).
#[no_mangle]
pub extern "C" fn stops_db_find_near(
    db: *const StopsDatabase,
    lat: f64,
    lon: f64,
    radius_meters: f64,
    out_count: *mut usize,
) -> *mut CStop {
    if db.is_null() || out_count.is_null() { return std::ptr::null_mut(); }

    unsafe {
        let db = &*db;
        match db.stops_near(lat, lon, radius_meters) {
            Ok(stops) => stops_to_c_array(&stops, out_count),
            Err(e) => {
                set_last_error(e);
                *out_count = 0;
                std::ptr::null_mut()
            }
        }
    }
}

/// Get all stops.
/// Returns a heap-allocated array of CStop structs. Free with stops_db_free_stop_array.
/// Returns NULL on error (check yonder_last_error).
#[no_mangle]
pub extern "C" fn stops_db_get_all(
    db: *const StopsDatabase,
    out_count: *mut usize,
) -> *mut CStop {
    if db.is_null() || out_count.is_null() { return std::ptr::null_mut(); }

    unsafe {
        let db = &*db;
        match db.get_all() {
            Ok(stops) => stops_to_c_array(&stops, out_count),
            Err(e) => {
                set_last_error(e);
                *out_count = 0;
                std::ptr::null_mut()
            }
        }
    }
}

/// Find a stop by its stop_id. Returns a single-element array (count=1) if found,
/// or NULL with count=0 if not found. Free with stops_db_free_stop_array.
#[no_mangle]
pub extern "C" fn stops_db_find_by_id(
    db: *const StopsDatabase,
    stop_id: *const c_char,
    out_count: *mut usize,
) -> *mut CStop {
    if db.is_null() || stop_id.is_null() || out_count.is_null() {
        return std::ptr::null_mut();
    }

    unsafe {
        let db = &*db;
        let id_str = match CStr::from_ptr(stop_id).to_str() {
            Ok(s) => s,
            Err(e) => {
                set_last_error(format!("Invalid UTF-8 stop_id: {}", e));
                *out_count = 0;
                return std::ptr::null_mut();
            }
        };

        match db.find_by_id(id_str) {
            Ok(Some(stop)) => stops_to_c_array(std::slice::from_ref(&stop), out_count),
            Ok(None) => {
                *out_count = 0;
                std::ptr::null_mut()
            }
            Err(e) => {
                set_last_error(e);
                *out_count = 0;
                std::ptr::null_mut()
            }
        }
    }
}

/// Free a CStop array returned by stops_db_find_near, stops_db_get_all, or stops_db_find_by_id.
/// `count` must match the value written to out_count.
#[no_mangle]
pub extern "C" fn stops_db_free_stop_array(stops: *mut CStop, count: usize) {
    if stops.is_null() || count == 0 { return; }
    unsafe {
        let boxed = Box::from_raw(std::ptr::slice_from_raw_parts_mut(stops, count));
        for stop in Vec::from(boxed) {
            stop.free_owned();
        }
    }
}

/// Get stop count.
#[no_mangle]
pub extern "C" fn stops_db_count(db: *const StopsDatabase) -> usize {
    if db.is_null() { return 0; }
    unsafe { (&*db).count() }
}

/// Free the stops database.
#[no_mangle]
pub extern "C" fn stops_db_free(db: *mut StopsDatabase) {
    if !db.is_null() {
        unsafe { drop(Box::from_raw(db)); }
    }
}

// MARK: - TileCacheCore FFI

#[no_mangle]
pub extern "C" fn tile_cache_new(path: *const c_char) -> *mut TileCacheCore {
    if path.is_null() { return std::ptr::null_mut(); }

    unsafe {
        let path_str = match CStr::from_ptr(path).to_str() {
            Ok(s) => s,
            Err(e) => {
                set_last_error(format!("Invalid UTF-8 path: {}", e));
                return std::ptr::null_mut();
            }
        };

        match TileCacheCore::new(path_str.into()) {
            Ok(cache) => Box::into_raw(Box::new(cache)),
            Err(e) => {
                set_last_error(format!("Failed to create tile cache: {}", e));
                std::ptr::null_mut()
            }
        }
    }
}

/// Save tile data to cache with the given expiration TTL.
/// Returns 1 on success, 0 on failure (check yonder_last_error).
///
/// For negative cache entries (e.g. 404 responses), use tile_cache_save_negative instead.
#[no_mangle]
pub extern "C" fn tile_cache_save(
    cache: *mut TileCacheCore,
    cache_key: *const c_char,
    data: *const u8,
    data_len: usize,
    expiration_secs: i64,
) -> i32 {
    if cache.is_null() || cache_key.is_null() { return 0; }
    if data_len > 0 && data.is_null() { return 0; }

    unsafe {
        let cache = &*cache;
        let key_str = match cstr_to_str(cache_key, "Invalid UTF-8 cache key") {
            Some(s) => s,
            None => return 0,
        };

        let data_slice = if data_len > 0 && !data.is_null() {
            std::slice::from_raw_parts(data, data_len)
        } else {
            &[]
        };

        let meta = TileMeta::new_success(expiration_secs, data_len as u32);
        match cache.save_tile(key_str, data_slice, &meta) {
            Ok(_) => 1,
            Err(e) => {
                set_last_error(format!("tile_cache_save failed: {}", e));
                0
            }
        }
    }
}

/// Save a negative cache entry (e.g. for a 404 response).
/// Returns 1 on success, 0 on failure (check yonder_last_error).
#[no_mangle]
pub extern "C" fn tile_cache_save_negative(
    cache: *mut TileCacheCore,
    cache_key: *const c_char,
    ttl_seconds: i64,
) -> i32 {
    if cache.is_null() || cache_key.is_null() { return 0; }

    unsafe {
        let cache = &*cache;
        let key = match cstr_to_str(cache_key, "Invalid UTF-8 cache key") {
            Some(s) => s,
            None => return 0,
        };

        match cache.save_negative_cache(key, ttl_seconds) {
            Ok(_) => 1,
            Err(e) => {
                set_last_error(format!("tile_cache_save_negative failed: {}", e));
                0
            }
        }
    }
}

/// Load a tile. Returns a pointer to the tile data (length written to out_data_len).
/// Free the returned data with tile_cache_free_data(ptr, *out_data_len).
/// Returns NULL if the tile is not cached or is expired.
#[no_mangle]
pub extern "C" fn tile_cache_load(
    cache: *const TileCacheCore,
    cache_key: *const c_char,
    out_data_len: *mut usize,
    out_http_status: *mut u16,
    out_is_negative: *mut i32,
) -> *mut u8 {
    if cache.is_null() || cache_key.is_null() || out_data_len.is_null() {
        return std::ptr::null_mut();
    }

    unsafe {
        let cache = &*cache;
        let key_str = match CStr::from_ptr(cache_key).to_str() {
            Ok(s) => s,
            Err(e) => {
                set_last_error(format!("Invalid UTF-8 cache key: {}", e));
                return std::ptr::null_mut();
            }
        };

        match cache.load_tile(key_str) {
            Some(cached_tile) => {
                *out_data_len = cached_tile.data.len();
                if !out_http_status.is_null() {
                    *out_http_status = cached_tile.meta.http_status;
                }
                if !out_is_negative.is_null() {
                    *out_is_negative = if cached_tile.meta.is_negative { 1 } else { 0 };
                }
                let boxed: Box<[u8]> = cached_tile.data.into_boxed_slice();
                Box::into_raw(boxed) as *mut u8
            }
            None => std::ptr::null_mut(),
        }
    }
}

/// Free tile data returned by tile_cache_load.
/// `len` must be the exact value written to out_data_len.
#[no_mangle]
pub extern "C" fn tile_cache_free_data(data: *mut u8, len: usize) {
    if data.is_null() || len == 0 { return; }
    unsafe {
        drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(data, len)));
    }
}

#[no_mangle]
pub extern "C" fn tile_cache_remove(cache: *mut TileCacheCore, cache_key: *const c_char) -> i32 {
    if cache.is_null() || cache_key.is_null() { return 0; }
    unsafe {
        let cache = &*cache;
        let key_str = match CStr::from_ptr(cache_key).to_str() {
            Ok(s) => s,
            Err(e) => { set_last_error(format!("Invalid UTF-8: {}", e)); return 0; }
        };
        match cache.delete_tile(key_str) {
            Ok(_) => 1,
            Err(e) => { set_last_error(format!("tile_cache_remove failed: {}", e)); 0 }
        }
    }
}

#[no_mangle]
pub extern "C" fn tile_cache_is_valid(cache: *const TileCacheCore, cache_key: *const c_char) -> i32 {
    if cache.is_null() || cache_key.is_null() { return 0; }
    unsafe {
        let cache = &*cache;
        let key_str = match CStr::from_ptr(cache_key).to_str() {
            Ok(s) => s,
            Err(_) => return 0,
        };
        if cache.is_valid(key_str) { 1 } else { 0 }
    }
}

#[no_mangle]
pub extern "C" fn tile_cache_is_negative(cache: *const TileCacheCore, cache_key: *const c_char) -> i32 {
    if cache.is_null() || cache_key.is_null() { return 0; }
    unsafe {
        let cache = &*cache;
        let key_str = match CStr::from_ptr(cache_key).to_str() {
            Ok(s) => s,
            Err(_) => return 0,
        };
        if cache.is_negative_cache(key_str) { 1 } else { 0 }
    }
}

/// Clean up expired tiles. Returns the number of distinct tiles removed.
/// Return type is uint64_t — matches the Rust u64 exactly on all platforms.
#[no_mangle]
pub extern "C" fn tile_cache_cleanup_expired(cache: *mut TileCacheCore) -> u64 {
    if cache.is_null() { return 0; }
    unsafe {
        let cache = &*cache;
        cache.clear_expired().unwrap_or_else(|e| {
            set_last_error(format!("tile_cache_cleanup_expired failed: {}", e));
            0
        })
    }
}

#[no_mangle]
pub extern "C" fn tile_cache_size(cache: *const TileCacheCore) -> u64 {
    if cache.is_null() { return 0; }
    unsafe { (&*cache).cache_size().unwrap_or(0) }
}

#[no_mangle]
pub extern "C" fn tile_cache_count(cache: *const TileCacheCore) -> usize {
    if cache.is_null() { return 0; }
    unsafe { (&*cache).tile_count().unwrap_or(0) }
}

#[no_mangle]
pub extern "C" fn tile_cache_clear_all(cache: *mut TileCacheCore) -> i32 {
    if cache.is_null() { return 0; }
    unsafe {
        match (&*cache).clear_all() {
            Ok(_) => 1,
            Err(e) => { set_last_error(format!("tile_cache_clear_all failed: {}", e)); 0 }
        }
    }
}

/// Get cache statistics. All out_ pointers are optional (maybe NULL).
/// Statistics managed by Rust — do not try to increment them from Swift.
#[no_mangle]
pub extern "C" fn tile_cache_get_stats(
    cache: *const TileCacheCore,
    out_memory_hits: *mut u64,
    out_disk_hits: *mut u64,
    out_network_fetches: *mut u64,
    out_cache_misses: *mut u64,
    out_expired_tiles: *mut u64,
) {
    if cache.is_null() { return; }
    unsafe {
        let stats = (&*cache).statistics();
        if !out_memory_hits.is_null()    { *out_memory_hits    = stats.memory_hits; }
        if !out_disk_hits.is_null()      { *out_disk_hits      = stats.disk_hits; }
        if !out_network_fetches.is_null(){ *out_network_fetches= stats.network_fetches; }
        if !out_cache_misses.is_null()   { *out_cache_misses   = stats.cache_misses; }
        if !out_expired_tiles.is_null()  { *out_expired_tiles  = stats.expired_tiles; }
    }
}

#[no_mangle]
pub extern "C" fn tile_cache_reset_stats(cache: *mut TileCacheCore) {
    if cache.is_null() { return; }
    unsafe { (&*cache).reset_statistics(); }
}

#[no_mangle]
pub extern "C" fn tile_cache_free(cache: *mut TileCacheCore) {
    if !cache.is_null() {
        unsafe { drop(Box::from_raw(cache)); }
    }
}

// MARK: - GTFS-RT FFI

#[no_mangle]
pub extern "C" fn gtfs_rt_new() -> *mut GtfsRtCore {
    Box::into_raw(Box::new(GtfsRtCore::new()))
}

/// Parse GTFS-RT protobuf data.
/// Returns 0 on success, -1 on error (check yonder_last_error).
#[no_mangle]
pub extern "C" fn gtfs_rt_parse(
    core: *mut GtfsRtCore,
    data: *const u8,
    data_len: usize,
) -> i32 {
    if core.is_null() || data.is_null() { return -1; }

    unsafe {
        let core = &*core;
        let data_slice = std::slice::from_raw_parts(data, data_len);
        match core.parse(data_slice) {
            Ok(_) => 0,
            Err(e) => {
                set_last_error(e);
                -1
            }
        }
    }
}

/// Get all vehicles as a CVehicle array.
/// The caller must free the result with gtfs_rt_free_vehicles(ptr, *out_count).
/// Returns NULL if there are no vehicles or on error.
#[no_mangle]
pub extern "C" fn gtfs_rt_get_vehicles(
    core: *const GtfsRtCore,
    out_count: *mut usize,
) -> *mut CVehicle {
    if core.is_null() || out_count.is_null() { return std::ptr::null_mut(); }

    unsafe {
        let core = &*core;

        let ffi_vehicles = match core.get_vehicles() {
            Ok(v) => v,
            Err(e) => {
                set_last_error(e);
                *out_count = 0;
                return std::ptr::null_mut();
            }
        };

        *out_count = ffi_vehicles.len();
        if ffi_vehicles.is_empty() { return std::ptr::null_mut(); }

        let c_vehicles: Vec<CVehicle> = ffi_vehicles.into_iter().map(CVehicle::from_ffi).collect();
        let boxed = c_vehicles.into_boxed_slice();
        Box::into_raw(boxed) as *mut CVehicle
    }
}

#[no_mangle]
pub extern "C" fn gtfs_rt_vehicle_count(core: *const GtfsRtCore) -> usize {
    if core.is_null() { return 0; }
    unsafe { (&*core).vehicle_count().unwrap_or(0) }
}

/// Free the vehicle array returned by gtfs_rt_get_vehicles.
/// `count` must match the value written to out_count.
#[no_mangle]
pub extern "C" fn gtfs_rt_free_vehicles(vehicles: *mut CVehicle, count: usize) {
    if vehicles.is_null() || count == 0 { return; }

    unsafe {
        // Reconstruct the boxed slice — this gives us unique ownership.
        let boxed = Box::from_raw(std::ptr::slice_from_raw_parts_mut(vehicles, count));
        // free_owned takes self by value, making double-free impossible.
        for vehicle in Vec::from(boxed) {
            vehicle.free_owned();
        }
    }
}

#[no_mangle]
pub extern "C" fn gtfs_rt_free(core: *mut GtfsRtCore) {
    if !core.is_null() {
        unsafe { drop(Box::from_raw(core)); }
    }
}
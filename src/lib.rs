use std::ffi::{CString, CStr};
use std::os::raw::c_char;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use csv::ReaderBuilder;
use rstar::{RTree, AABB, primitives::GeomWithData};

// MARK: - Modules

mod tile_cache;
mod gtfs_rt;
mod gtfs_static;
mod shapes_editor;

pub use tile_cache::*;
pub use gtfs_rt::*;
pub use gtfs_static::*;
pub use shapes_editor::*;

// MARK: - Core Data Structures

/// A GTFS stop with all required fields - optimized with Box<str> for smaller memory footprint
#[derive(Debug, Clone)]
pub struct GTFSStop {
    pub id: Box<str>,          // Box<str> is smaller than String for immutable data
    pub name: Box<str>,
    pub url: Option<Box<str>>,
    pub lat: f64,
    pub lon: f64,
    pub providers: Vec<Box<str>>,
}

impl GTFSStop {
    /// Validate coordinates are within valid ranges (inlined for performance)
    #[inline(always)]
    pub fn has_valid_coordinates(&self) -> bool {
        self.lat >= -90.0 && self.lat <= 90.0 && self.lon >= -180.0 && self.lon <= 180.0
    }

    /// Create a new stop with validation
    pub fn new(
        id: String,
        name: String,
        url: Option<String>,
        lat: f64,
        lon: f64,
        providers: Vec<String>,
    ) -> Result<Self, &'static str> {
        // Validate coordinates
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

/// R-tree point type for spatial indexing
type StopPoint = GeomWithData<[f64; 2], usize>;

/// Thread-safe stops database with spatial index
pub struct StopsDatabase {
    inner: Arc<RwLock<StopsDatabaseInner>>,
}

struct StopsDatabaseInner {
    stops: Vec<GTFSStop>,
    spatial_index: RTree<StopPoint>,
    id_index: HashMap<Box<str>, usize>,  // Fast ID lookup
}

impl StopsDatabase {
    /// Create a new empty database
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(StopsDatabaseInner {
                stops: Vec::with_capacity(10000),  // Pre-allocate for typical dataset
                spatial_index: RTree::new(),
                id_index: HashMap::with_capacity(10000),
            }))
        }
    }

    /// Load stops from a CSV file path (APPENDS to existing stops)
    pub fn load_from_csv(&self, csv_path: &str) -> Result<usize, String> {
        let new_stops = parse_gtfs_csv_optimized(csv_path)?;
        let count = new_stops.len();

        let mut inner = self.inner.write()
            .map_err(|_| "Failed to acquire write lock")?;

        // Store the starting index for new stops
        let start_idx = inner.stops.len();

        // Build new spatial points for the new stops only
        let mut new_points = Vec::with_capacity(count);

        for (i, stop) in new_stops.into_iter().enumerate() {
            let idx = start_idx + i;

            // Add to ID index for fast lookups
            inner.id_index.insert(stop.id.clone(), idx);

            // Add to spatial index if coordinates are valid
            if stop.has_valid_coordinates() {
                new_points.push(GeomWithData::new([stop.lon, stop.lat], idx));
            }

            inner.stops.push(stop);
        }

        // Bulk insert new points into existing R-tree (more efficient than rebuild)
        if !new_points.is_empty() {
            // Unfortunately rstar doesn't have incremental bulk insert, so we need to rebuild
            // But we can optimize by collecting all points at once
            let mut all_points = Vec::with_capacity(inner.stops.len());
            for (idx, stop) in inner.stops.iter().enumerate() {
                if stop.has_valid_coordinates() {
                    all_points.push(GeomWithData::new([stop.lon, stop.lat], idx));
                }
            }
            inner.spatial_index = RTree::bulk_load(all_points);
        }

        Ok(count)
    }

    /// Find stops within a radius (in meters) of a coordinate - optimized version
    pub fn stops_near(&self, lat: f64, lon: f64, radius_meters: f64) -> Result<Vec<GTFSStop>, String> {
        // Validate input coordinates
        if lat < -90.0 || lat > 90.0 || lon < -180.0 || lon > 180.0 {
            return Err("Invalid coordinates".to_string());
        }

        let inner = self.inner.read()
            .map_err(|_| "Failed to acquire read lock")?;

        // More accurate degree conversion based on latitude
        let lat_rad = lat.to_radians();
        let meters_per_degree_lat = 111_132.92 - 559.82 * (2.0 * lat_rad).cos()
            + 1.175 * (4.0 * lat_rad).cos() - 0.0023 * (6.0 * lat_rad).cos();
        let meters_per_degree_lon = 111_412.84 * lat_rad.cos()
            - 93.5 * (3.0 * lat_rad).cos() + 0.118 * (5.0 * lat_rad).cos();

        let radius_lat = radius_meters / meters_per_degree_lat;
        let radius_lon = radius_meters / meters_per_degree_lon.abs().max(0.00001); // Prevent division by zero

        // Create bounding box for initial filtering (FIXED: max calculation)
        let min = [lon - radius_lon, lat - radius_lat];
        let max = [lon + radius_lon, lat + radius_lat];  // Fixed: was incorrectly using minus
        let envelope = AABB::from_corners(min, max);

        // Pre-allocate result vector
        let mut results = Vec::with_capacity(100);

        // Query R-tree and filter by exact distance
        for point in inner.spatial_index.locate_in_envelope(&envelope) {
            let stop = &inner.stops[point.data];
            let distance = haversine_distance_optimized(lat, lon, stop.lat, stop.lon);
            if distance <= radius_meters {
                results.push(stop.clone());
            }
        }

        results.shrink_to_fit(); // Release excess capacity
        Ok(results)
    }

    /// Find a stop by ID - now O(1) with HashMap
    pub fn find_by_id(&self, id: &str) -> Result<Option<GTFSStop>, String> {
        let inner = self.inner.read()
            .map_err(|_| "Failed to acquire read lock")?;

        Ok(inner.id_index.get(id).map(|&idx| inner.stops[idx].clone()))
    }

    /// Get all stops
    pub fn get_all(&self) -> Result<Vec<GTFSStop>, String> {
        let inner = self.inner.read()
            .map_err(|_| "Failed to acquire read lock")?;
        Ok(inner.stops.clone())
    }

    /// Get total number of stops
    pub fn count(&self) -> usize {
        self.inner.read().map(|inner| inner.stops.len()).unwrap_or(0)
    }

    /// Clear all stops (useful for testing)
    pub fn clear(&self) -> Result<(), String> {
        let mut inner = self.inner.write()
            .map_err(|_| "Failed to acquire write lock")?;
        inner.stops.clear();
        inner.spatial_index = RTree::new();
        inner.id_index.clear();
        Ok(())
    }
}

// MARK: - Optimized CSV Parsing

/// Helper function to parse comma-separated providers
#[inline]
fn parse_comma_separated_providers(input: &str) -> Vec<String> {
    input.split(',')
        .filter_map(|p| {
            let trimmed = p.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .collect()
}

fn parse_gtfs_csv_optimized(path: &str) -> Result<Vec<GTFSStop>, String> {
    // Use BufReader for better I/O performance
    let file = std::fs::File::open(path)
        .map_err(|e| format!("YonderCore: Failed to open CSV: {}", e))?;

    let mut reader = ReaderBuilder::new()
        .buffer_capacity(8 * 1024 * 1024) // 8MB buffer for better performance
        .from_reader(std::io::BufReader::with_capacity(64 * 1024, file));

    // Get headers and find column indices
    let headers = reader.headers()
        .map_err(|e| format!("YonderCore: Failed to read headers: {}", e))?;

    // Cache column indices
    let id_idx = find_header_index(&headers, "stop_id")
        .ok_or("YonderCore: Missing stop_id column")?;
    let name_idx = find_header_index(&headers, "stop_name")
        .ok_or("YonderCore: Missing stop_name column")?;
    let lat_idx = find_header_index(&headers, "stop_lat")
        .ok_or("YonderCore: Missing stop_lat column")?;
    let lon_idx = find_header_index(&headers, "stop_lon")
        .ok_or("YonderCore: Missing stop_lon column")?;

    // Optional columns
    let url_idx = find_header_index(&headers, "stop_url");
    let providers_idx = find_header_index(&headers, "providers");
    let agency_idx = find_header_index(&headers, "agency_id");

    let mut stops = Vec::with_capacity(10000); // Pre-allocate
    let mut line_num = 1;

    for result in reader.records() {
        line_num += 1;

        let record = result
            .map_err(|e| format!("YonderCore: Failed to parse line {}: {}", line_num, e))?;

        // Parse required fields with better error context
        let id = record.get(id_idx)
            .ok_or_else(|| format!("Missing stop_id at line {}", line_num))?
            .trim();

        if id.is_empty() {
            continue; // Skip empty IDs
        }

        let name = record.get(name_idx)
            .ok_or_else(|| format!("Missing stop_name at line {}", line_num))?
            .trim();

        if name.is_empty() {
            continue; // Skip empty names
        }

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

        // Skip invalid coordinates
        if lat < -90.0 || lat > 90.0 || lon < -180.0 || lon > 180.0 {
            continue;
        }

        // Optional URL field
        let url = url_idx.and_then(|idx| {
            record.get(idx).and_then(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            })
        });

        // Parse providers - check "providers" column first, then fall back to "agency_id"
        // This matches the Swift implementation's priority
        let providers: Vec<String> = if let Some(idx) = providers_idx {
            // "providers" column exists - parse as comma-separated list
            record.get(idx)
                .map(|s| parse_comma_separated_providers(s))
                .unwrap_or_default()
        } else if let Some(idx) = agency_idx {
            // Fall back to "agency_id" column - treat as single provider (matching Swift)
            record.get(idx)
                .and_then(|s| {
                    let trimmed = s.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        // Split by comma to handle multi-agency stops
                        Some(parse_comma_separated_providers(trimmed))
                    }
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        // Use the new constructor for validation
        match GTFSStop::new(id.to_string(), name.to_string(), url, lat, lon, providers) {
            Ok(stop) => stops.push(stop),
            Err(_) => continue, // Skip invalid stops
        }
    }

    stops.shrink_to_fit(); // Release excess capacity
    Ok(stops)
}

// Helper function to find header index (case-insensitive)
#[inline]
fn find_header_index(headers: &csv::StringRecord, name: &str) -> Option<usize> {
    headers.iter()
        .position(|h| h.trim().eq_ignore_ascii_case(name))
}

// MARK: - Optimized Distance Calculation

/// Calculate distance using optimized Haversine formula with fast approximations
#[inline(always)]
fn haversine_distance_optimized(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_METERS: f64 = 6_371_000.0;
    const DEG_TO_RAD: f64 = std::f64::consts::PI / 180.0;

    let lat1_rad = lat1 * DEG_TO_RAD;
    let lat2_rad = lat2 * DEG_TO_RAD;
    let delta_lat = (lat2 - lat1) * DEG_TO_RAD;
    let delta_lon = (lon2 - lon1) * DEG_TO_RAD;

    let sin_delta_lat = (delta_lat * 0.5).sin();
    let sin_delta_lon = (delta_lon * 0.5).sin();

    let a = sin_delta_lat * sin_delta_lat
        + lat1_rad.cos() * lat2_rad.cos() * sin_delta_lon * sin_delta_lon;

    // Using atan2 directly is more accurate than the original formula
    let c = 2.0 * a.sqrt().asin();

    EARTH_RADIUS_METERS * c
}

// MARK: - Safe FFI Functions

/// Test function - returns a greeting string
#[no_mangle]
pub extern "C" fn hello_from_rust() -> *const c_char {
    let greeting = CString::new("Library initialized successfully")
        .unwrap_or_else(|_| CString::new("Error").unwrap());
    greeting.into_raw()
}

/// Free a string allocated by Rust
#[no_mangle]
pub extern "C" fn free_rust_string(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let _ = CString::from_raw(ptr);
    }
}

// MARK: - Helper Functions for FFI

/// Convert a raw C string pointer to a `&str` borrow.
///
/// Returns `None` when the pointer is null or the bytes are not valid UTF-8,
/// allowing call sites to avoid repeating the same five-line match block.
///
/// # Safety
/// `ptr` must either be null or point to a null-terminated C string that remains
/// valid for the duration of the returned borrow.
#[inline]
unsafe fn cstr_to_str<'a>(ptr: *const c_char) -> Option<&'a str> {
    if ptr.is_null() {
        return None;
    }
    CStr::from_ptr(ptr).to_str().ok()
}

/// Helper function to convert stops to C string array
/// Returns NULL on error, cleaning up any allocated strings
unsafe fn stops_to_c_string_array(stops: &[GTFSStop], out_count: *mut usize) -> *mut *mut c_char {
    let mut result = Vec::with_capacity(stops.len());

    for stop in stops {
        let url_str = stop.url.as_deref().unwrap_or("");
        let providers_str = stop.providers.iter()
            .map(|s| s.as_ref())
            .collect::<Vec<&str>>()
            .join(",");

        let formatted = format!(
            "{}|{}|{}|{}|{}|{}",
            stop.id, stop.name, stop.lat, stop.lon, url_str, providers_str
        );

        match CString::new(formatted) {
            Ok(c_str) => result.push(c_str.into_raw()),
            Err(_) => {
                // Clean up already allocated strings
                for ptr in result {
                    let _ = CString::from_raw(ptr);
                }
                return std::ptr::null_mut();
            }
        }
    }

    *out_count = result.len();
    let ptr = result.as_mut_ptr();
    std::mem::forget(result);
    ptr
}

// MARK: - Thread-Safe FFI Interface for StopsDatabase

/// Create a new stops database
#[no_mangle]
pub extern "C" fn stops_db_new() -> *mut StopsDatabase {
    Box::into_raw(Box::new(StopsDatabase::new()))
}

/// Load stops from a CSV file (thread-safe)
/// Returns the number of stops loaded, or -1 on error
#[no_mangle]
pub extern "C" fn stops_db_load_csv(db: *mut StopsDatabase, path: *const c_char) -> i32 {
    if db.is_null() || path.is_null() {
        return -1;
    }

    unsafe {
        let db = &*db;

        // Safely convert C string to Rust string
        let path_str = match CStr::from_ptr(path).to_str() {
            Ok(s) => s,
            Err(_) => return -1,
        };

        match db.load_from_csv(path_str) {
            Ok(count) => count as i32,
            Err(_) => -1,
        }
    }
}

/// Find stops near a location (thread-safe)
/// Returns array of formatted strings, or NULL on error
#[no_mangle]
pub extern "C" fn stops_db_find_near(
    db: *const StopsDatabase,
    lat: f64,
    lon: f64,
    radius_meters: f64,
    out_count: *mut usize,
) -> *mut *mut c_char {
    if db.is_null() || out_count.is_null() {
        return std::ptr::null_mut();
    }

    unsafe {
        let db = &*db;

        match db.stops_near(lat, lon, radius_meters) {
            Ok(stops) => stops_to_c_string_array(&stops, out_count),
            Err(_) => {
                *out_count = 0;
                std::ptr::null_mut()
            }
        }
    }
}

/// Get all stops (thread-safe)
#[no_mangle]
pub extern "C" fn stops_db_get_all(
    db: *const StopsDatabase,
    out_count: *mut usize,
) -> *mut *mut c_char {
    if db.is_null() || out_count.is_null() {
        return std::ptr::null_mut();
    }

    unsafe {
        let db = &*db;

        match db.get_all() {
            Ok(stops) => stops_to_c_string_array(&stops, out_count),
            Err(_) => {
                *out_count = 0;
                std::ptr::null_mut()
            }
        }
    }
}

/// Free results array
#[no_mangle]
pub extern "C" fn stops_db_free_results(results: *mut *mut c_char, count: usize) {
    if results.is_null() {
        return;
    }

    unsafe {
        let results = Vec::from_raw_parts(results, count, count);
        for ptr in results {
            if !ptr.is_null() {
                let _ = CString::from_raw(ptr);
            }
        }
    }
}

/// Get stop count (thread-safe)
#[no_mangle]
pub extern "C" fn stops_db_count(db: *const StopsDatabase) -> usize {
    if db.is_null() {
        return 0;
    }

    unsafe {
        let db = &*db;
        db.count()
    }
}

/// Find the single nearest stop to (lat, lon) using the R-tree spatial index.
///
/// Uses the same bounding-box + Haversine approach as `stops_db_find_near` but
/// returns only the single closest stop, avoiding a full deserialize-all on the
/// Swift side. On success sets `*out_count` = 1 and returns a 1-element `char**`
/// array in the same pipe-delimited format as `stops_db_find_near`.
/// Returns NULL with `*out_count` = 0 if no stops are loaded or coordinates are invalid.
/// Free with `stops_db_free_results`.
#[no_mangle]
pub extern "C" fn stops_db_find_nearest(
    db: *const StopsDatabase,
    lat: f64,
    lon: f64,
    out_count: *mut usize,
) -> *mut *mut c_char {
    if db.is_null() || out_count.is_null() {
        return std::ptr::null_mut();
    }

    unsafe {
        let db = &*db;

        // Validate coordinates
        if lat < -90.0 || lat > 90.0 || lon < -180.0 || lon > 180.0 {
            *out_count = 0;
            return std::ptr::null_mut();
        }

        let inner = match db.inner.read() {
            Ok(guard) => guard,
            Err(_) => {
                *out_count = 0;
                return std::ptr::null_mut();
            }
        };

        if inner.stops.is_empty() {
            *out_count = 0;
            return std::ptr::null_mut();
        }

        // Use nearest_neighbor from the R-tree — O(log n), no radius needed.
        let nearest_point = inner.spatial_index.nearest_neighbor(&[lon, lat]);

        let stop = match nearest_point {
            Some(p) => &inner.stops[p.data],
            None => {
                *out_count = 0;
                return std::ptr::null_mut();
            }
        };

        stops_to_c_string_array(std::slice::from_ref(stop), out_count)
    }
}

/// Find a single stop by its stop_id. Uses the O(1) HashMap index.
/// Returns a single-element `char**` array (same format as `stops_db_find_near`)
/// with `*out_count` set to 1, or NULL with `*out_count` = 0 if not found.
/// Free with `stops_db_free_results`.
#[no_mangle]
pub extern "C" fn stops_db_find_by_id(
    db: *const StopsDatabase,
    stop_id: *const c_char,
    out_count: *mut usize,
) -> *mut *mut c_char {
    if db.is_null() || stop_id.is_null() || out_count.is_null() {
        return std::ptr::null_mut();
    }

    unsafe {
        let db = &*db;

        let Some(id_str) = cstr_to_str(stop_id) else {
            *out_count = 0;
            return std::ptr::null_mut();
        };

        match db.find_by_id(id_str) {
            Ok(Some(stop)) => stops_to_c_string_array(std::slice::from_ref(&stop), out_count),
            _ => {
                *out_count = 0;
                std::ptr::null_mut()
            }
        }
    }
}

/// Find all stops whose providers list contains the given provider string.
/// Returns a `char**` array in the same format as `stops_db_find_near`.
/// Free with `stops_db_free_results`.
#[no_mangle]
pub extern "C" fn stops_db_find_by_provider(
    db: *const StopsDatabase,
    provider: *const c_char,
    out_count: *mut usize,
) -> *mut *mut c_char {
    if db.is_null() || provider.is_null() || out_count.is_null() {
        return std::ptr::null_mut();
    }

    unsafe {
        let db = &*db;

        let Some(provider_str) = cstr_to_str(provider) else {
            *out_count = 0;
            return std::ptr::null_mut();
        };

        match db.get_all() {
            Ok(stops) => {
                let matching: Vec<GTFSStop> = stops
                    .into_iter()
                    .filter(|s| s.providers.iter().any(|p| p.as_ref() == provider_str))
                    .collect();
                stops_to_c_string_array(&matching, out_count)
            }
            Err(_) => {
                *out_count = 0;
                std::ptr::null_mut()
            }
        }
    }
}

/// Free the stops database
#[no_mangle]
pub extern "C" fn stops_db_free(db: *mut StopsDatabase) {
    if !db.is_null() {
        unsafe {
            let _ = Box::from_raw(db);
        }
    }
}

// MARK: - FFI Interface for TileCacheCore

/// Create a new tile cache
#[no_mangle]
pub extern "C" fn tile_cache_new(path: *const c_char) -> *mut TileCacheCore {
    if path.is_null() {
        return std::ptr::null_mut();
    }

    unsafe {
        let path_str = match CStr::from_ptr(path).to_str() {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        };

        match TileCacheCore::new(path_str.into()) {
            Ok(cache) => Box::into_raw(Box::new(cache)),
            Err(_) => std::ptr::null_mut(),
        }
    }
}

/// Save a tile to disk (with validation)
#[no_mangle]
pub extern "C" fn tile_cache_save(
    cache: *mut TileCacheCore,
    cache_key: *const c_char,
    data: *const u8,
    data_len: usize,
    expiration_secs: i64,
    is_negative: i32,
) -> i32 {
    if cache.is_null() || cache_key.is_null() {
        return 0;
    }

    // Validate data pointer if data_len > 0
    if data_len > 0 && data.is_null() {
        return 0;
    }

    unsafe {
        let cache = &*cache;

        let key_str = match CStr::from_ptr(cache_key).to_str() {
            Ok(s) => s,
            Err(_) => return 0,
        };

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

        match cache.save_tile(key_str, data_slice, &meta) {
            Ok(_) => 1,
            Err(_) => 0,
        }
    }
}

/// Load a tile from disk (with validation)
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
            Err(_) => return std::ptr::null_mut(),
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

                // Transfer ownership of data to caller
                let mut data = cached_tile.data;
                let ptr = data.as_mut_ptr();
                std::mem::forget(data);
                ptr
            }
            None => std::ptr::null_mut(),
        }
    }
}

/// Free tile data returned by tile_cache_load
#[no_mangle]
pub extern "C" fn tile_cache_free_data(data: *mut u8, len: usize) {
    if data.is_null() || len == 0 {
        return;
    }
    unsafe {
        let _ = Vec::from_raw_parts(data, len, len);
    }
}

/// Remove a tile from cache
#[no_mangle]
pub extern "C" fn tile_cache_remove(
    cache: *mut TileCacheCore,
    cache_key: *const c_char,
) -> i32 {
    if cache.is_null() || cache_key.is_null() {
        return 0;
    }

    unsafe {
        let cache = &*cache;

        let key_str = match CStr::from_ptr(cache_key).to_str() {
            Ok(s) => s,
            Err(_) => return 0,
        };

        match cache.delete_tile(key_str) {
            Ok(_) => 1,
            Err(_) => 0,
        }
    }
}

/// Check if a tile exists and is valid
#[no_mangle]
pub extern "C" fn tile_cache_is_valid(
    cache: *const TileCacheCore,
    cache_key: *const c_char,
) -> i32 {
    if cache.is_null() || cache_key.is_null() {
        return 0;
    }

    unsafe {
        let cache = &*cache;

        let key_str = match CStr::from_ptr(cache_key).to_str() {
            Ok(s) => s,
            Err(_) => return 0,
        };

        if cache.is_valid(key_str) { 1 } else { 0 }
    }
}

/// Clean up expired tiles
#[no_mangle]
pub extern "C" fn tile_cache_cleanup_expired(cache: *mut TileCacheCore) -> usize {
    if cache.is_null() {
        return 0;
    }

    unsafe {
        let cache = &*cache;
        cache.clear_expired().unwrap_or(0)
            as usize
    }
}

/// Get current cache size in bytes
#[no_mangle]
pub extern "C" fn tile_cache_size(cache: *const TileCacheCore) -> u64 {
    if cache.is_null() {
        return 0;
    }

    unsafe {
        let cache = &*cache;
        cache.cache_size().unwrap_or(0)
    }
}

/// Get number of tiles in cache
#[no_mangle]
pub extern "C" fn tile_cache_count(cache: *const TileCacheCore) -> usize {
    if cache.is_null() {
        return 0;
    }

    unsafe {
        let cache = &*cache;
        cache.tile_count().unwrap_or(0)
    }
}

/// Clear all tiles from cache
#[no_mangle]
pub extern "C" fn tile_cache_clear_all(cache: *mut TileCacheCore) -> i32 {
    if cache.is_null() {
        return 0;
    }

    unsafe {
        let cache = &*cache;
        match cache.clear_all() {
            Ok(_) => 1,
            Err(_) => 0,
        }
    }
}

/// Record a memory cache hit
#[no_mangle]
pub extern "C" fn tile_cache_record_memory_hit(cache: *const TileCacheCore) {
    if cache.is_null() {
        return;
    }
    unsafe {
        let cache = &*cache;
        cache.record_memory_hit();
    }
}

/// Record a network fetch
#[no_mangle]
pub extern "C" fn tile_cache_record_network_fetch(cache: *const TileCacheCore) {
    if cache.is_null() {
        return;
    }
    unsafe {
        let cache = &*cache;
        cache.record_network_fetch();
    }
}

/// Record a cache miss
#[no_mangle]
pub extern "C" fn tile_cache_record_cache_miss(cache: *const TileCacheCore) {
    if cache.is_null() {
        return;
    }
    unsafe {
        let cache = &*cache;
        cache.record_cache_miss();
    }
}

/// Get cache statistics
#[no_mangle]
pub extern "C" fn tile_cache_get_stats(
    cache: *const TileCacheCore,
    out_memory_hits: *mut u64,
    out_disk_hits: *mut u64,
    out_network_fetches: *mut u64,
    out_cache_misses: *mut u64,
    out_expired_tiles: *mut u64,
) {
    if cache.is_null() {
        return;
    }

    unsafe {
        let cache = &*cache;
        let stats = cache.statistics();

        if !out_memory_hits.is_null() {
            *out_memory_hits = stats.memory_hits;
        }
        if !out_disk_hits.is_null() {
            *out_disk_hits = stats.disk_hits;
        }
        if !out_network_fetches.is_null() {
            *out_network_fetches = stats.network_fetches;
        }
        if !out_cache_misses.is_null() {
            *out_cache_misses = stats.cache_misses;
        }
        if !out_expired_tiles.is_null() {
            *out_expired_tiles = stats.expired_tiles;
        }
    }
}

/// Reset cache statistics
#[no_mangle]
pub extern "C" fn tile_cache_reset_stats(cache: *mut TileCacheCore) {
    if cache.is_null() {
        return;
    }
    unsafe {
        let cache = &*cache;
        cache.reset_statistics();
    }
}

/// Free the tile cache
#[no_mangle]
pub extern "C" fn tile_cache_free(cache: *mut TileCacheCore) {
    if !cache.is_null() {
        unsafe {
            let _ = Box::from_raw(cache);
        }
    }
}

/// Save a negative cache entry
#[no_mangle]
pub extern "C" fn tile_cache_save_negative(
    cache: *mut TileCacheCore,
    cache_key: *const c_char,
    ttl_seconds: i64,
) -> i32 {
    if cache.is_null() || cache_key.is_null() {
        return 0;
    }

    unsafe {
        let cache = &*cache;

        let key = match CStr::from_ptr(cache_key).to_str() {
            Ok(s) => s,
            Err(_) => return 0,
        };

        match cache.save_negative_cache(key, ttl_seconds) {
            Ok(_) => 1,
            Err(_) => 0,
        }
    }
}

/// Check if entry is a negative cache
#[no_mangle]
pub extern "C" fn tile_cache_is_negative(
    cache: *const TileCacheCore,
    cache_key: *const c_char,
) -> i32 {
    if cache.is_null() || cache_key.is_null() {
        return 0;
    }

    unsafe {
        let cache = &*cache;

        let key = match CStr::from_ptr(cache_key).to_str() {
            Ok(s) => s,
            Err(_) => return 0,
        };

        if cache.is_negative_cache(key) { 1 } else { 0 }
    }
}

// MARK: - GTFS-RT FFI Functions

/// Create a new GTFS-RT manager
#[no_mangle]
pub extern "C" fn gtfs_rt_new() -> *mut GtfsRtCore {
    Box::into_raw(Box::new(GtfsRtCore::new()))
}

/// Parse GTFS-RT protobuf data
/// Returns 0 on success, -1 on error
#[no_mangle]
pub extern "C" fn gtfs_rt_parse(
    core: *mut GtfsRtCore,
    data: *const u8,
    data_len: usize,
) -> i32 {
    if core.is_null() || data.is_null() {
        return -1;
    }

    unsafe {
        let core = &*core;
        let data_slice = std::slice::from_raw_parts(data, data_len);

        match core.parse(data_slice) {
            Ok(_) => 0,
            Err(_) => -1,
        }
    }
}

/// Get all vehicles
/// Returns pointer to array of FFIVehicle structs
/// out_count is set to the number of vehicles
#[no_mangle]
pub extern "C" fn gtfs_rt_get_vehicles(
    core: *const GtfsRtCore,
    out_count: *mut usize,
) -> *mut FFIVehicle {
    if core.is_null() || out_count.is_null() {
        return std::ptr::null_mut();
    }

    unsafe {
        let core = &*core;

        let vehicles = match core.get_vehicles() {
            Ok(v) => v,
            Err(_) => {
                *out_count = 0;
                return std::ptr::null_mut();
            }
        };

        *out_count = vehicles.len();

        if vehicles.is_empty() {
            return std::ptr::null_mut();
        }

        let boxed = vehicles.into_boxed_slice();
        Box::into_raw(boxed) as *mut FFIVehicle
    }
}

/// Get count of vehicles
#[no_mangle]
pub extern "C" fn gtfs_rt_vehicle_count(core: *const GtfsRtCore) -> usize {
    if core.is_null() {
        return 0;
    }

    unsafe {
        let core = &*core;
        core.vehicle_count().unwrap_or(0)
    }
}

/// Free vehicle array returned by gtfs_rt_get_vehicles
#[no_mangle]
pub extern "C" fn gtfs_rt_free_vehicles(vehicles: *mut FFIVehicle, count: usize) {
    if vehicles.is_null() || count == 0 {
        return;
    }

    unsafe {
        // Create slice to iterate over
        let vehicles_slice = std::slice::from_raw_parts_mut(vehicles, count);

        // Free each string field.
        // NOTE: label is allocated via CString::into_raw() in vehicle_to_ffi()
        // and must be freed here alongside id, route_id, and trip_id.
        for vehicle in vehicles_slice.iter() {
            if !vehicle.id.is_null() {
                let _ = CString::from_raw(vehicle.id as *mut c_char);
            }
            if !vehicle.route_id.is_null() {
                let _ = CString::from_raw(vehicle.route_id as *mut c_char);
            }
            if !vehicle.trip_id.is_null() {
                let _ = CString::from_raw(vehicle.trip_id as *mut c_char);
            }
            if !vehicle.label.is_null() {
                let _ = CString::from_raw(vehicle.label as *mut c_char);
            }
        }

        // Reconstruct the boxed slice from the raw pointer
        let boxed_slice = Box::from_raw(std::ptr::slice_from_raw_parts_mut(vehicles, count));
        drop(boxed_slice);
    }
}

/// Free GTFS-RT manager
#[no_mangle]
pub extern "C" fn gtfs_rt_free(core: *mut GtfsRtCore) {
    if !core.is_null() {
        unsafe {
            let _ = Box::from_raw(core);
        }
    }
}

/// Look up trip update data for a given trip_id.
/// Returns a heap-allocated TripUpdateSummary on success, or null if not found.
/// Free with gtfs_rt_free_trip_update().
#[no_mangle]
pub extern "C" fn gtfs_rt_get_trip_update(
    core: *const GtfsRtCore,
    trip_id: *const c_char,
) -> *mut TripUpdateSummary {
    if core.is_null() || trip_id.is_null() {
        return std::ptr::null_mut();
    }

    unsafe {
        let core = &*core;
        let trip_id_str = match CStr::from_ptr(trip_id).to_str() {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        };

        match core.get_trip_update(trip_id_str) {
            Ok(Some(summary)) => Box::into_raw(Box::new(summary)),
            _ => std::ptr::null_mut(),
        }
    }
}

/// Free a TripUpdateSummary returned by gtfs_rt_get_trip_update.
/// next_stop_id is a CString allocated by Rust via into_raw() and is
/// explicitly freed here before dropping the struct — no leak occurs.
#[no_mangle]
pub extern "C" fn gtfs_rt_free_trip_update(summary: *mut TripUpdateSummary) {
    if summary.is_null() {
        return;
    }

    unsafe {
        let s = &*summary;
        if !s.next_stop_id.is_null() {
            let _ = CString::from_raw(s.next_stop_id as *mut c_char);
        }
        let _ = Box::from_raw(summary);
    }
}

// MARK: - GTFS-RT Enriched Vehicle FFI Functions

/// Return all active vehicles enriched with TripUpdate data in a single FFI call.
///
/// `now_eastern` = now_unix + TimeZone("America/New_York").secondsFromGMT(now)
/// Only vehicles whose trip passes the schedule-window gate are included.
/// The gate returns 1 (pass) while stop_times are still loading, so no vehicles
/// are incorrectly hidden during startup.
///
/// Replaces the previous pattern of N×gtfs_static_is_trip_active +
/// N×gtfs_rt_get_trip_update calls per update cycle.
///
/// Free the result with gtfs_rt_free_enriched_vehicles().
#[no_mangle]
pub extern "C" fn gtfs_rt_get_active_enriched_vehicles(
    core: *const GtfsRtCore,
    now_eastern: i64,
    out_count: *mut usize,
) -> *mut FFIEnrichedVehicle {
    if core.is_null() || out_count.is_null() {
        return std::ptr::null_mut();
    }

    unsafe {
        let core = &*core;

        let vehicles = match core.get_active_enriched_vehicles(now_eastern) {
            Ok(v) => v,
            Err(_) => {
                *out_count = 0;
                return std::ptr::null_mut();
            }
        };

        *out_count = vehicles.len();

        if vehicles.is_empty() {
            return std::ptr::null_mut();
        }

        let boxed = vehicles.into_boxed_slice();
        Box::into_raw(boxed) as *mut FFIEnrichedVehicle
    }
}

/// Free the array returned by gtfs_rt_get_active_enriched_vehicles.
///
/// Frees every heap-allocated C string field (id, route_id, trip_id, label,
/// next_stop_id) before dropping the slice, mirroring the allocation performed
/// inside GtfsRtCore::get_active_enriched_vehicles → vehicle_to_ffi /
/// get_trip_update_inner.
#[no_mangle]
pub extern "C" fn gtfs_rt_free_enriched_vehicles(
    vehicles: *mut FFIEnrichedVehicle,
    count: usize,
) {
    if vehicles.is_null() || count == 0 {
        return;
    }

    unsafe {
        let slice = std::slice::from_raw_parts(vehicles, count);

        for v in slice.iter() {
            if !v.id.is_null() {
                let _ = CString::from_raw(v.id as *mut c_char);
            }
            if !v.route_id.is_null() {
                let _ = CString::from_raw(v.route_id as *mut c_char);
            }
            if !v.trip_id.is_null() {
                let _ = CString::from_raw(v.trip_id as *mut c_char);
            }
            if !v.label.is_null() {
                let _ = CString::from_raw(v.label as *mut c_char);
            }
            if !v.next_stop_id.is_null() {
                let _ = CString::from_raw(v.next_stop_id as *mut c_char);
            }
        }

        let _ = Box::from_raw(std::ptr::slice_from_raw_parts_mut(vehicles, count));
    }
}
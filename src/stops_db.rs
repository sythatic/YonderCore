//! stops_db.rs — GTFS stops database with R-tree spatial index
//!
//! Owns the `GTFSStop` type, `StopsDatabase` implementation, CSV parsing,
//! haversine distance, and all `stops_db_*` FFI functions.
//!
//! # Instance model
//!
//! `StopsDatabase` is fully instance-based — Swift allocates one or more
//! databases via `stops_db_new()` and passes the pointer to every subsequent
//! call.  There is no global state; multiple databases can coexist (e.g. one
//! per agency, or one shared database loaded from multiple CSVs via repeated
//! `stops_db_load_csv` calls, which append rather than replace).

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::sync::{Arc, RwLock};

use csv::ReaderBuilder;
use rstar::{AABB, RTree, primitives::GeomWithData};

use crate::geo::haversine_m;

// ── Data structures ───────────────────────────────────────────────────────────

/// A GTFS stop.  Uses `Box<str>` instead of `String` for a smaller per-stop
/// memory footprint on immutable data.
#[derive(Debug, Clone)]
pub struct GTFSStop {
    pub id:        Box<str>,
    pub name:      Box<str>,
    pub url:       Option<Box<str>>,
    pub lat:       f64,
    pub lon:       f64,
    pub providers: Vec<Box<str>>,
}

impl GTFSStop {
    #[inline(always)]
    pub fn has_valid_coordinates(&self) -> bool {
        self.lat >= -90.0 && self.lat <= 90.0
            && self.lon >= -180.0 && self.lon <= 180.0
    }

    pub fn new(
        id:        String,
        name:      String,
        url:       Option<String>,
        lat:       f64,
        lon:       f64,
        providers: Vec<String>,
    ) -> Result<Self, &'static str> {
        if lat < -90.0 || lat > 90.0  { return Err("Invalid latitude");  }
        if lon < -180.0 || lon > 180.0 { return Err("Invalid longitude"); }
        Ok(Self {
            id:        id.into_boxed_str(),
            name:      name.into_boxed_str(),
            url:       url.map(|s| s.into_boxed_str()),
            lat,
            lon,
            providers: providers.into_iter().map(|s| s.into_boxed_str()).collect(),
        })
    }
}

type StopPoint = GeomWithData<[f64; 2], usize>;

/// Thread-safe stops database with R-tree spatial index and O(1) ID lookup.
pub struct StopsDatabase {
    inner: Arc<RwLock<StopsDatabaseInner>>,
}

struct StopsDatabaseInner {
    stops:         Vec<GTFSStop>,
    spatial_index: RTree<StopPoint>,
    id_index:      HashMap<Box<str>, usize>,
}

impl StopsDatabase {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(StopsDatabaseInner {
                stops:         Vec::with_capacity(10_000),
                spatial_index: RTree::new(),
                id_index:      HashMap::with_capacity(10_000),
            })),
        }
    }

    /// Load stops from a CSV file, **appending** to any already-loaded stops.
    /// Returns the number of newly added stops, or an error string.
    pub fn load_from_csv(&self, csv_path: &str) -> Result<usize, String> {
        let new_stops = parse_stops_csv(csv_path)?;
        let count = new_stops.len();

        let mut inner = self.inner.write()
            .map_err(|_| "Failed to acquire write lock")?;

        let start_idx = inner.stops.len();

        for (i, stop) in new_stops.into_iter().enumerate() {
            let idx = start_idx + i;
            inner.id_index.insert(stop.id.clone(), idx);
            inner.stops.push(stop);
        }

        // Rebuild the spatial index in bulk — rstar has no incremental insert.
        let all_points: Vec<StopPoint> = inner.stops.iter()
            .enumerate()
            .filter(|(_, s)| s.has_valid_coordinates())
            .map(|(idx, s)| GeomWithData::new([s.lon, s.lat], idx))
            .collect();
        inner.spatial_index = RTree::bulk_load(all_points);

        Ok(count)
    }

    /// Find all stops within `radius_meters` of `(lat, lon)`.
    pub fn stops_near(&self, lat: f64, lon: f64, radius_meters: f64) -> Result<Vec<GTFSStop>, String> {
        if lat < -90.0 || lat > 90.0 || lon < -180.0 || lon > 180.0 {
            return Err("Invalid coordinates".to_string());
        }

        let inner = self.inner.read()
            .map_err(|_| "Failed to acquire read lock")?;

        let lat_rad = lat.to_radians();
        let meters_per_degree_lat = 111_132.92
            - 559.82  * (2.0 * lat_rad).cos()
            + 1.175   * (4.0 * lat_rad).cos()
            - 0.0023  * (6.0 * lat_rad).cos();
        let meters_per_degree_lon = (111_412.84 * lat_rad.cos()
            - 93.5 * (3.0 * lat_rad).cos()
            + 0.118 * (5.0 * lat_rad).cos())
            .abs()
            .max(0.00001);

        let radius_lat = radius_meters / meters_per_degree_lat;
        let radius_lon = radius_meters / meters_per_degree_lon;

        let envelope = AABB::from_corners(
            [lon - radius_lon, lat - radius_lat],
            [lon + radius_lon, lat + radius_lat],
        );

        let mut results = Vec::with_capacity(100);
        for point in inner.spatial_index.locate_in_envelope(&envelope) {
            let stop = &inner.stops[point.data];
            if haversine_m(lat, lon, stop.lat, stop.lon) <= radius_meters {
                results.push(stop.clone());
            }
        }
        results.shrink_to_fit();
        Ok(results)
    }

    /// Find a stop by its `stop_id` in O(1).
    pub fn find_by_id(&self, id: &str) -> Result<Option<GTFSStop>, String> {
        let inner = self.inner.read()
            .map_err(|_| "Failed to acquire read lock")?;
        Ok(inner.id_index.get(id).map(|&idx| inner.stops[idx].clone()))
    }

    pub fn get_all(&self) -> Result<Vec<GTFSStop>, String> {
        let inner = self.inner.read()
            .map_err(|_| "Failed to acquire read lock")?;
        Ok(inner.stops.clone())
    }

    pub fn count(&self) -> usize {
        self.inner.read().map(|i| i.stops.len()).unwrap_or(0)
    }

    pub fn clear(&self) -> Result<(), String> {
        let mut inner = self.inner.write()
            .map_err(|_| "Failed to acquire write lock")?;
        inner.stops.clear();
        inner.spatial_index = RTree::new();
        inner.id_index.clear();
        Ok(())
    }
}

// ── CSV parsing ───────────────────────────────────────────────────────────────

fn parse_stops_csv(path: &str) -> Result<Vec<GTFSStop>, String> {
    let file = std::fs::File::open(path)
        .map_err(|e| format!("YonderCore: Failed to open CSV: {}", e))?;

    let mut reader = ReaderBuilder::new()
        .buffer_capacity(8 * 1024 * 1024)
        .from_reader(std::io::BufReader::with_capacity(64 * 1024, file));

    let headers = reader.headers()
        .map_err(|e| format!("YonderCore: Failed to read headers: {}", e))?;

    let id_idx   = col_index(&headers, "stop_id")  .ok_or("YonderCore: Missing stop_id column")?;
    let name_idx = col_index(&headers, "stop_name") .ok_or("YonderCore: Missing stop_name column")?;
    let lat_idx  = col_index(&headers, "stop_lat")  .ok_or("YonderCore: Missing stop_lat column")?;
    let lon_idx  = col_index(&headers, "stop_lon")  .ok_or("YonderCore: Missing stop_lon column")?;
    let url_idx       = col_index(&headers, "stop_url");
    let providers_idx = col_index(&headers, "providers");
    let agency_idx    = col_index(&headers, "agency_id");

    let mut stops = Vec::with_capacity(10_000);
    let mut line_num = 1usize;

    for result in reader.records() {
        line_num += 1;
        let record = result
            .map_err(|e| format!("YonderCore: Failed to parse line {}: {}", line_num, e))?;

        let id = record.get(id_idx)
            .ok_or_else(|| format!("Missing stop_id at line {}", line_num))?.trim();
        if id.is_empty() { continue; }

        let name = record.get(name_idx)
            .ok_or_else(|| format!("Missing stop_name at line {}", line_num))?.trim();
        if name.is_empty() { continue; }

        let lat_str = record.get(lat_idx)
            .ok_or_else(|| format!("Missing stop_lat at line {}", line_num))?.trim();
        let lat: f64 = lat_str.parse()
            .map_err(|_| format!("Invalid latitude '{}' at line {}", lat_str, line_num))?;

        let lon_str = record.get(lon_idx)
            .ok_or_else(|| format!("Missing stop_lon at line {}", line_num))?.trim();
        let lon: f64 = lon_str.parse()
            .map_err(|_| format!("Invalid longitude '{}' at line {}", lon_str, line_num))?;

        if lat < -90.0 || lat > 90.0 || lon < -180.0 || lon > 180.0 { continue; }

        let url = url_idx.and_then(|i| record.get(i)).and_then(|s| {
            let t = s.trim(); if t.is_empty() { None } else { Some(t.to_string()) }
        });

        let providers: Vec<String> = if let Some(i) = providers_idx {
            record.get(i).map(parse_providers).unwrap_or_default()
        } else if let Some(i) = agency_idx {
            record.get(i).and_then(|s| {
                let t = s.trim(); if t.is_empty() { None } else { Some(parse_providers(t)) }
            }).unwrap_or_default()
        } else {
            Vec::new()
        };

        if let Ok(stop) = GTFSStop::new(id.to_string(), name.to_string(), url, lat, lon, providers) {
            stops.push(stop);
        }
    }

    stops.shrink_to_fit();
    Ok(stops)
}

#[inline]
fn parse_providers(input: &str) -> Vec<String> {
    input.split(',')
        .filter_map(|p| { let t = p.trim(); if t.is_empty() { None } else { Some(t.to_string()) } })
        .collect()
}

/// Case-insensitive column index lookup.
#[inline]
fn col_index(headers: &csv::StringRecord, name: &str) -> Option<usize> {
    headers.iter().position(|h| h.trim().eq_ignore_ascii_case(name))
}

// ── FFI helpers ───────────────────────────────────────────────────────────────

/// Convert a raw C string pointer to a `&str` borrow.
/// Returns `None` when the pointer is null or the bytes are not valid UTF-8.
///
/// # Safety
/// `ptr` must be null or point to a valid null-terminated C string that outlives
/// the returned borrow.
#[inline]
pub(crate) unsafe fn cstr_to_str<'a>(ptr: *const c_char) -> Option<&'a str> {
    if ptr.is_null() { return None; }
    CStr::from_ptr(ptr).to_str().ok()
}

/// Serialize a slice of `GTFSStop` into a heap-allocated `char**` array.
/// Each element is a pipe-delimited string: `id|name|lat|lon|url|providers`.
/// Returns NULL on allocation failure, cleaning up any already-allocated strings.
unsafe fn stops_to_c_string_array(
    stops:     &[GTFSStop],
    out_count: *mut usize,
) -> *mut *mut c_char {
    let mut result: Vec<*mut c_char> = Vec::with_capacity(stops.len());

    for stop in stops {
        let url_str       = stop.url.as_deref().unwrap_or("");
        let providers_str = stop.providers.iter()
            .map(|s| s.as_ref())
            .collect::<Vec<&str>>()
            .join(",");

        let formatted = format!(
            "{}|{}|{}|{}|{}|{}",
            stop.id, stop.name, stop.lat, stop.lon, url_str, providers_str
        );

        match CString::new(formatted) {
            Ok(cs) => result.push(cs.into_raw()),
            Err(_) => {
                for ptr in result { let _ = CString::from_raw(ptr); }
                return std::ptr::null_mut();
            }
        }
    }

    *out_count = result.len();
    let ptr = result.as_mut_ptr();
    std::mem::forget(result);
    ptr
}

// ── FFI ───────────────────────────────────────────────────────────────────────

/// Create a new stop database.
#[no_mangle]
pub extern "C" fn stops_db_new() -> *mut StopsDatabase {
    Box::into_raw(Box::new(StopsDatabase::new()))
}

/// Load stops from a CSV file into `db` (appends; does not replace).
/// Returns the number of stops loaded, or -1 on error.
#[no_mangle]
pub extern "C" fn stops_db_load_csv(db: *mut StopsDatabase, path: *const c_char) -> i32 {
    if db.is_null() || path.is_null() { return -1; }
    unsafe {
        let db = &*db;
        let path_str = match CStr::from_ptr(path).to_str() { Ok(s) => s, Err(_) => return -1 };
        match db.load_from_csv(path_str) { Ok(n) => n as i32, Err(_) => -1 }
    }
}

/// Find stops within `radius_meters` of `(lat, lon)`.
/// Returns a `char**` of pipe-delimited strings; free with `stops_db_free_results`.
#[no_mangle]
pub extern "C" fn stops_db_find_near(
    db:            *const StopsDatabase,
    lat:           f64,
    lon:           f64,
    radius_meters: f64,
    out_count:     *mut usize,
) -> *mut *mut c_char {
    if db.is_null() || out_count.is_null() { return std::ptr::null_mut(); }
    unsafe {
        let db = &*db;
        match db.stops_near(lat, lon, radius_meters) {
            Ok(stops) => stops_to_c_string_array(&stops, out_count),
            Err(_)    => { *out_count = 0; std::ptr::null_mut() }
        }
    }
}

/// Get all stops.  Free with `stops_db_free_results`.
#[no_mangle]
pub extern "C" fn stops_db_get_all(
    db:        *const StopsDatabase,
    out_count: *mut usize,
) -> *mut *mut c_char {
    if db.is_null() || out_count.is_null() { return std::ptr::null_mut(); }
    unsafe {
        let db = &*db;
        match db.get_all() {
            Ok(stops) => stops_to_c_string_array(&stops, out_count),
            Err(_)    => { *out_count = 0; std::ptr::null_mut() }
        }
    }
}

/// Free a `char**` array returned by any `stops_db_*` function.
#[no_mangle]
pub extern "C" fn stops_db_free_results(results: *mut *mut c_char, count: usize) {
    if results.is_null() { return; }
    unsafe {
        let v = Vec::from_raw_parts(results, count, count);
        for ptr in v { if !ptr.is_null() { let _ = CString::from_raw(ptr); } }
    }
}

/// Total number of stops currently held in `db`.
#[no_mangle]
pub extern "C" fn stops_db_count(db: *const StopsDatabase) -> usize {
    if db.is_null() { return 0; }
    unsafe { (&*db).count() }
}

/// Find the single nearest stop using the R-tree (O(log n)).
/// Returns a 1-element `char**` or NULL.  Free with `stops_db_free_results`.
#[no_mangle]
pub extern "C" fn stops_db_find_nearest(
    db:        *const StopsDatabase,
    lat:       f64,
    lon:       f64,
    out_count: *mut usize,
) -> *mut *mut c_char {
    if db.is_null() || out_count.is_null() { return std::ptr::null_mut(); }
    unsafe {
        if lat < -90.0 || lat > 90.0 || lon < -180.0 || lon > 180.0 {
            *out_count = 0;
            return std::ptr::null_mut();
        }

        let db    = &*db;
        let inner = match db.inner.read() {
            Ok(g)  => g,
            Err(_) => { *out_count = 0; return std::ptr::null_mut(); }
        };

        if inner.stops.is_empty() { *out_count = 0; return std::ptr::null_mut(); }

        let stop = match inner.spatial_index.nearest_neighbor(&[lon, lat]) {
            Some(p) => &inner.stops[p.data],
            None    => { *out_count = 0; return std::ptr::null_mut(); }
        };

        stops_to_c_string_array(std::slice::from_ref(stop), out_count)
    }
}

/// Find a single stop by `stop_id` using the O(1) HashMap index.
/// Returns a 1-element `char**` or NULL.  Free with `stops_db_free_results`.
#[no_mangle]
pub extern "C" fn stops_db_find_by_id(
    db:        *const StopsDatabase,
    stop_id:   *const c_char,
    out_count: *mut usize,
) -> *mut *mut c_char {
    if db.is_null() || stop_id.is_null() || out_count.is_null() {
        return std::ptr::null_mut();
    }
    unsafe {
        let Some(id_str) = cstr_to_str(stop_id) else {
            *out_count = 0; return std::ptr::null_mut();
        };
        match (&*db).find_by_id(id_str) {
            Ok(Some(stop)) => stops_to_c_string_array(std::slice::from_ref(&stop), out_count),
            _ => { *out_count = 0; std::ptr::null_mut() }
        }
    }
}

/// Find all stops whose `providers` list contains `provider`.
/// Returns a `char**` array.  Free with `stops_db_free_results`.
#[no_mangle]
pub extern "C" fn stops_db_find_by_provider(
    db:        *const StopsDatabase,
    provider:  *const c_char,
    out_count: *mut usize,
) -> *mut *mut c_char {
    if db.is_null() || provider.is_null() || out_count.is_null() {
        return std::ptr::null_mut();
    }
    unsafe {
        let Some(prov) = cstr_to_str(provider) else {
            *out_count = 0; return std::ptr::null_mut();
        };
        match (&*db).get_all() {
            Ok(stops) => {
                let matching: Vec<GTFSStop> = stops.into_iter()
                    .filter(|s| s.providers.iter().any(|p| p.as_ref() == prov))
                    .collect();
                stops_to_c_string_array(&matching, out_count)
            }
            Err(_) => { *out_count = 0; std::ptr::null_mut() }
        }
    }
}

/// Free a `StopsDatabase` allocated by `stops_db_new`.
#[no_mangle]
pub extern "C" fn stops_db_free(db: *mut StopsDatabase) {
    if !db.is_null() { unsafe { let _ = Box::from_raw(db); } }
}

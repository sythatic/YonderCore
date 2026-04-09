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
    pub id:            Box<str>,
    pub name:          Box<str>,
    pub url:           Option<Box<str>>,
    pub lat:           f64,
    pub lon:           f64,
    pub providers:     Vec<Box<str>>,
    /// stop_code: public-facing identifier shown on signs and timetables.
    /// Distinct from stop_id (internal database key).  May be empty.
    pub code:          Option<Box<str>>,
    /// location_type:
    ///   0 (or absent) = routable stop / platform — the only type passengers board at.
    ///   1 = station (parent of stops)
    ///   2 = entrance / exit
    ///   3 = generic node (path waypoint)
    ///   4 = boarding area
    /// Best practice: only show location_type=0 stops to passengers.
    pub location_type: u8,
    /// stop_timezone: IANA timezone name override for this stop.
    /// When set, it overrides the agency timezone for local time display.
    /// Relevant for routes that cross timezone boundaries (e.g. Amtrak long-distance).
    pub timezone:      Option<Box<str>>,
}

impl GTFSStop {
    #[inline(always)]
    pub fn has_valid_coordinates(&self) -> bool {
        self.lat >= -90.0 && self.lat <= 90.0
            && self.lon >= -180.0 && self.lon <= 180.0
    }

    /// Returns true for stops that passengers can actually board at.
    /// Per GTFS spec and best practices, only location_type=0 (or absent)
    /// stops are routable boarding points.  Stations (1), entrances (2),
    /// generic nodes (3), and boarding areas (4) should not appear as
    /// "next stop" candidates or in spatial passenger-facing queries.
    #[inline(always)]
    pub fn is_boardable(&self) -> bool {
        self.location_type == 0
    }

    pub fn new(
        id:            String,
        name:          String,
        url:           Option<String>,
        lat:           f64,
        lon:           f64,
        providers:     Vec<String>,
        code:          Option<String>,
        location_type: u8,
        timezone:      Option<String>,
    ) -> Result<Self, &'static str> {
        if lat < -90.0 || lat > 90.0  { return Err("Invalid latitude");  }
        if lon < -180.0 || lon > 180.0 { return Err("Invalid longitude"); }
        Ok(Self {
            id:            id.into_boxed_str(),
            name:          name.into_boxed_str(),
            url:           url.map(|s| s.into_boxed_str()),
            lat,
            lon,
            providers:     providers.into_iter().map(|s| s.into_boxed_str()).collect(),
            code:          code.filter(|s| !s.is_empty()).map(|s| s.into_boxed_str()),
            location_type,
            timezone:      timezone.filter(|s| !s.is_empty()).map(|s| s.into_boxed_str()),
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

impl Default for StopsDatabase {
    fn default() -> Self { Self::new() }
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
    ///
    /// # Spatial index note
    ///
    /// Each call inserts new stops into the R-tree incrementally.  Incremental
    /// insertion is O(log N) per stop but produces a less balanced tree than
    /// `bulk_load`.  When loading multiple CSVs, call
    /// [`rebuild_spatial_index`][Self::rebuild_spatial_index] once after all
    /// feeds have been loaded to rebuild an optimally balanced tree in one
    /// O(N log N) pass.  The FFI counterpart is `stops_db_rebuild_index`.
    pub fn load_from_csv(&self, csv_path: &str) -> Result<usize, String> {
        let new_stops = parse_stops_csv(csv_path)?;
        let count = new_stops.len();

        let mut inner = self.inner.write()
            .map_err(|_| "Failed to acquire write lock")?;

        let start_idx = inner.stops.len();

        for (i, stop) in new_stops.into_iter().enumerate() {
            let idx = start_idx + i;
            // Incremental R-tree insert — O(log N) per stop.
            // Call rebuild_spatial_index() after all feeds are loaded for a
            // bulk_load-balanced tree if query performance is critical.
            if stop.has_valid_coordinates() {
                inner.spatial_index.insert(GeomWithData::new([stop.lon, stop.lat], idx));
            }
            inner.id_index.insert(stop.id.clone(), idx);
            inner.stops.push(stop);
        }

        Ok(count)
    }

    /// Rebuild the spatial index using `bulk_load` for an optimally balanced
    /// R-tree.  Call this once after loading all feeds to amortise the cost of
    /// incremental insertions done by repeated [`load_from_csv`][Self::load_from_csv]
    /// calls.
    pub fn rebuild_spatial_index(&self) -> Result<(), String> {
        let mut inner = self.inner.write()
            .map_err(|_| "Failed to acquire write lock")?;
        let all_points: Vec<StopPoint> = inner.stops.iter()
            .enumerate()
            .filter(|(_, s)| s.has_valid_coordinates())
            .map(|(idx, s)| GeomWithData::new([s.lon, s.lat], idx))
            .collect();
        inner.spatial_index = RTree::bulk_load(all_points);
        Ok(())
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

    /// Return only stops whose `providers` list contains `provider`.
    /// Holds the read lock for the full scan but clones only matching stops.
    pub fn find_by_provider(&self, provider: &str) -> Result<Vec<GTFSStop>, String> {
        let inner = self.inner.read()
            .map_err(|_| "Failed to acquire read lock")?;
        Ok(inner.stops.iter()
            .filter(|s| s.providers.iter().any(|p| p.as_ref() == provider))
            .cloned()
            .collect())
    }

    /// Return only stops whose `stop_code` equals `code`.
    /// Holds the read lock for the full scan but clones only matching stops.
    pub fn find_by_code(&self, code: &str) -> Result<Vec<GTFSStop>, String> {
        let inner = self.inner.read()
            .map_err(|_| "Failed to acquire read lock")?;
        Ok(inner.stops.iter()
            .filter(|s| s.code.as_deref() == Some(code))
            .cloned()
            .collect())
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
    let url_idx           = col_index(&headers, "stop_url");
    let providers_idx     = col_index(&headers, "providers");
    let agency_idx        = col_index(&headers, "agency_id");
    // New GTFS columns
    let code_idx          = col_index(&headers, "stop_code");
    let location_type_idx = col_index(&headers, "location_type");
    let timezone_idx      = col_index(&headers, "stop_timezone");

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

        let url = url_idx.and_then(|i| record.get(i)).and_then(|s| nonempty_str(s));

        let providers: Vec<String> = if let Some(i) = providers_idx {
            record.get(i).map(parse_providers).unwrap_or_default()
        } else if let Some(i) = agency_idx {
            record.get(i).and_then(|s| nonempty_str(s).map(|t| parse_providers(&t)))
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        // stop_code: public-facing stop identifier (shown on signs/apps).
        let code = code_idx.and_then(|i| record.get(i)).and_then(|s| nonempty_str(s));

        // location_type: 0=stop, 1=station, 2=entrance, 3=node, 4=boarding area.
        // Default to 0 (routable stop) when absent, per GTFS spec.
        let location_type: u8 = location_type_idx
            .and_then(|i| record.get(i))
            .and_then(|s| s.trim().parse::<u8>().ok())
            .unwrap_or(0);

        // stop_timezone: IANA timezone override (e.g. "America/Chicago").
        let timezone = timezone_idx.and_then(|i| record.get(i)).and_then(|s| nonempty_str(s));

        if let Ok(stop) = GTFSStop::new(
            id.to_string(), name.to_string(), url, lat, lon, providers,
            code, location_type, timezone,
        ) {
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

/// Trim a CSV field and return `Some(owned_string)` when non-empty, `None` otherwise.
/// Eliminates the repeated inline `and_then(|s| { let t = s.trim(); … })` pattern.
#[inline]
fn nonempty_str(s: &str) -> Option<String> {
    let t = s.trim();
    if t.is_empty() { None } else { Some(t.to_string()) }
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
/// Each element is a pipe-delimited string:
///   `id|name|lat|lon|url|providers|stop_code|location_type|timezone`
///
/// Fields 7–9 (stop_code, location_type, timezone) are new; existing Swift
/// parsers that only read fields 1–6 are unaffected.
///
/// Returns NULL on allocation failure, cleaning up any already-allocated strings.
unsafe fn stops_to_c_string_array(
    stops:     &[GTFSStop],
    out_count: *mut usize,
) -> *mut *mut c_char {
    if stops.is_empty() {
        *out_count = 0;
        return std::ptr::null_mut();
    }
    let mut result: Vec<*mut c_char> = Vec::with_capacity(stops.len());

    for stop in stops {
        let url_str       = stop.url.as_deref().unwrap_or("");
        let providers_str = stop.providers.iter()
            .map(|s| s.as_ref())
            .collect::<Vec<&str>>()
            .join(",");
        let code_str      = stop.code.as_deref().unwrap_or("");
        let timezone_str  = stop.timezone.as_deref().unwrap_or("");

        let formatted = format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}",
            stop.id, stop.name, stop.lat, stop.lon,
            url_str, providers_str,
            code_str, stop.location_type, timezone_str
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
    // Use into_boxed_slice so capacity == length; Vec::from_raw_parts with a
    // mismatched capacity would be undefined behaviour in the free function.
    let mut boxed: Box<[*mut c_char]> = result.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);
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
        match db.load_from_csv(path_str) {
            Ok(n)  => i32::try_from(n).unwrap_or(i32::MAX),
            Err(_) => -1,
        }
    }
}

/// Rebuild the spatial R-tree index using `bulk_load` for optimal query
/// performance.  Call this once after loading all feeds via repeated
/// `stops_db_load_csv` calls.  Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn stops_db_rebuild_index(db: *mut StopsDatabase) -> i32 {
    if db.is_null() { return -1; }
    unsafe {
        match (&*db).rebuild_spatial_index() {
            Ok(())  => 0,
            Err(_)  => -1,
        }
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
        // Reconstruct the Box<[*mut c_char]> produced by stops_to_c_string_array.
        let boxed = Box::from_raw(std::slice::from_raw_parts_mut(results, count));
        for ptr in boxed.iter() { if !ptr.is_null() { let _ = CString::from_raw(*ptr); } }
    }
}

/// Total number of stops currently held in `db`.
#[no_mangle]
pub extern "C" fn stops_db_count(db: *const StopsDatabase) -> usize {
    if db.is_null() { return 0; }
    unsafe { (&*db).count() }
}

/// Remove all stops from `db`, resetting it to an empty state.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn stops_db_clear(db: *mut StopsDatabase) -> i32 {
    if db.is_null() { return -1; }
    unsafe {
        match (&*db).clear() {
            Ok(()) => 0,
            Err(_) => -1,
        }
    }
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
        match (&*db).find_by_provider(prov) {
            Ok(matching) => stops_to_c_string_array(&matching, out_count),
            Err(_)       => { *out_count = 0; std::ptr::null_mut() }
        }
    }
}

/// Find all stops within `radius_meters` of `(lat, lon)` that are boardable
/// (location_type == 0).  Filters out station nodes, entrances, and boarding
/// areas so only actual passenger boarding points are returned.
/// Free with `stops_db_free_results`.
#[no_mangle]
pub extern "C" fn stops_db_find_near_boardable(
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
            Ok(stops) => {
                let boardable: Vec<GTFSStop> = stops.into_iter()
                    .filter(|s| s.is_boardable())
                    .collect();
                stops_to_c_string_array(&boardable, out_count)
            }
            Err(_) => { *out_count = 0; std::ptr::null_mut() }
        }
    }
}

/// Find a stop by its public-facing `stop_code` (the code shown on signs and
/// timetables — distinct from the internal `stop_id`).
/// Returns a `char**` array (may have multiple hits if the same code appears
/// across different providers).  Free with `stops_db_free_results`.
#[no_mangle]
pub extern "C" fn stops_db_find_by_code(
    db:        *const StopsDatabase,
    stop_code: *const c_char,
    out_count: *mut usize,
) -> *mut *mut c_char {
    if db.is_null() || stop_code.is_null() || out_count.is_null() {
        return std::ptr::null_mut();
    }
    unsafe {
        let Some(code_str) = cstr_to_str(stop_code) else {
            *out_count = 0; return std::ptr::null_mut();
        };
        match (&*db).find_by_code(code_str) {
            Ok(matching) => stops_to_c_string_array(&matching, out_count),
            Err(_)       => { *out_count = 0; std::ptr::null_mut() }
        }
    }
}

/// Free a `StopsDatabase` allocated by `stops_db_new`.
#[no_mangle]
pub extern "C" fn stops_db_free(db: *mut StopsDatabase) {
    if !db.is_null() { unsafe { let _ = Box::from_raw(db); } }
}

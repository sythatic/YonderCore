/// GTFS Static Lookup — parses Amtrak's GTFS.zip via HTTP range requests.
///
/// The ZIP format stores its Central Directory at the end of the file, so we
/// can read just `trips.txt` and `routes.txt` without downloading the whole
/// archive:
///
///   1. Swift fetches the last 65 KB  →  `gtfs_static_feed_eocd()`
///      Rust finds the Central Directory, returns the byte ranges needed for
///      `trips.txt` and `routes.txt` back to Swift.
///
///   2. Swift issues two HTTP Range requests for those byte ranges
///      →  `gtfs_static_feed_file("trips", data, len)`
///         `gtfs_static_feed_file("routes", data, len)`
///      Rust inflates and parses each file.
///
///   3. Swift calls `gtfs_static_lookup(trip_id)` at display time.
///      Rust returns a `GTFSStaticResult { train_number, route_name }`.
///
/// Thread safety: all state lives behind a `RwLock`; parsing is write-locked,
/// lookups are read-locked.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::io::Read;
use std::os::raw::c_char;
use std::sync::{Mutex, OnceLock};

use flate2::read::DeflateDecoder;

// ── Singleton ────────────────────────────────────────────────────────────────

static STORE: OnceLock<Mutex<GtfsStaticStore>> = OnceLock::new();

fn store() -> &'static Mutex<GtfsStaticStore> {
    STORE.get_or_init(|| Mutex::new(GtfsStaticStore::new()))
}

// ── Data model ───────────────────────────────────────────────────────────────

struct GtfsStaticStore {
    /// route_id → route_long_name  e.g. "NEC" → "Northeast Regional"
    route_names: HashMap<String, String>,
    /// trip_id → (train_number, route_id)  e.g. "168-amtrak_…" → ("168", "NEC")
    trips: HashMap<String, (String, String)>,
    /// Pending central-directory entries indexed by filename
    cd_entries: HashMap<String, CdEntry>,
}

#[derive(Debug, Clone)]
struct CdEntry {
    /// Offset of the local file header from the start of the ZIP
    local_header_offset: u64,
    /// Compressed size of the file data
    compressed_size: u64,
    /// Uncompressed size
    uncompressed_size: u64,
    /// Compression method (0 = stored, 8 = deflated)
    method: u16,
}

impl GtfsStaticStore {
    fn new() -> Self {
        Self {
            route_names: HashMap::new(),
            trips: HashMap::new(),
            cd_entries: HashMap::new(),
        }
    }

    /// Returns (train_number, route_long_name) for a realtime trip_id.
    ///
    /// GTFS-RT feeds encode trip_id differently per agency:
    ///   • Amtrak national:  bare integer              e.g. "241507"
    ///   • Gold Runner/SJJPA: "YYYY-MM-DD_AMTK_{id}"  e.g. "2026-02-27_AMTK_704"
    ///
    /// Strategy:
    ///   1. Exact match — handles Amtrak bare integers directly.
    ///   2. Last underscore-separated token — handles any prefixed format
    ///      regardless of how many date/agency components precede the number.
    ///      "2026-02-27_AMTK_704" → "704"
    ///      "2026-02-27_704"      → "704"
    ///      "20260228_704"        → "704"
    fn lookup(&self, trip_id: &str) -> Option<(String, String)> {
        // Helper: resolve a trips-table hit to (train_num, route_long_name)
        let resolve = |train_num: &String, route_id: &String| -> (String, String) {
            let route_name = self.route_names
                .get(route_id)
                .cloned()
                .unwrap_or_else(|| route_id.clone());
            (train_num.clone(), route_name)
        };

        // 1. Exact match — fastest path, handles Amtrak bare integers.
        if let Some((train_num, route_id)) = self.trips.get(trip_id) {
            return Some(resolve(train_num, route_id));
        }

        // 2. Last underscore-separated token — strips any leading date/agency
        //    prefix regardless of separator style or number of components.
        if trip_id.contains('_') {
            if let Some(last) = trip_id.split('_').last() {
                if let Some((train_num, route_id)) = self.trips.get(last) {
                    return Some(resolve(train_num, route_id));
                }
            }
        }

        None
    }
}

// ── ZIP parsing ──────────────────────────────────────────────────────────────

/// Minimum size of the End-of-Central-Directory record (no comment).
const EOCD_MIN_SIZE: usize = 22;
/// EOCD signature
const EOCD_SIG: u32 = 0x06054b50;
/// Central Directory File Header signature
const CDFH_SIG: u32 = 0x02014b50;

/// Parse the End-of-Central-Directory block and populate `cd_entries`.
/// `data` is the raw tail bytes fetched by Swift (up to 65 KB).
/// Returns a list of (filename, byte_range_start, byte_range_len) that Swift
/// must fetch, restricted to files we care about.
fn parse_eocd(data: &[u8]) -> Result<Vec<(String, u64, u64)>, String> {
    // Scan backwards for the EOCD signature
    let eocd_offset = data
        .windows(4)
        .rposition(|w| u32::from_le_bytes(w.try_into().unwrap()) == EOCD_SIG)
        .ok_or("EOCD signature not found")?;

    let eocd = &data[eocd_offset..];
    if eocd.len() < EOCD_MIN_SIZE {
        return Err("EOCD too short".into());
    }

    let cd_size = u32::from_le_bytes(eocd[12..16].try_into().unwrap()) as u64;
    let cd_offset = u32::from_le_bytes(eocd[16..20].try_into().unwrap()) as u64;

    // The central directory immediately precedes the EOCD in the file.
    // In our tail buffer, eocd_offset bytes from the start of data correspond
    // to (file_size - tail_len + eocd_offset) bytes from the start of the file.
    // We don't know file_size, but we know cd_offset and cd_size absolutely.
    // If the CD fits in our tail buffer, parse it directly.
    let tail_start_in_file = {
        // The EOCD sits at (file_size - tail_len + eocd_offset_in_tail).
        // We know cd_offset from EOCD; tail_len = data.len().
        // eocd_in_file = cd_offset + cd_size  (CD immediately before EOCD)
        // → tail_start_in_file = eocd_in_file - eocd_offset
        let eocd_in_file = cd_offset + cd_size;
        eocd_in_file.saturating_sub(eocd_offset as u64)
    };

    let cd_offset_in_buf = (cd_offset - tail_start_in_file) as usize;
    if cd_offset_in_buf + cd_size as usize > data.len() {
        return Err(format!(
            "Central Directory not in tail buffer: need offset {} size {} but have {}",
            cd_offset_in_buf, cd_size, data.len()
        ));
    }

    let cd_data = &data[cd_offset_in_buf..cd_offset_in_buf + cd_size as usize];
    let mut pos = 0usize;
    let mut entries: HashMap<String, CdEntry> = HashMap::new();

    while pos + 46 <= cd_data.len() {
        let sig = u32::from_le_bytes(cd_data[pos..pos + 4].try_into().unwrap());
        if sig != CDFH_SIG {
            break;
        }
        let method = u16::from_le_bytes(cd_data[pos + 10..pos + 12].try_into().unwrap());
        let compressed_size = u32::from_le_bytes(cd_data[pos + 20..pos + 24].try_into().unwrap()) as u64;
        let uncompressed_size = u32::from_le_bytes(cd_data[pos + 24..pos + 28].try_into().unwrap()) as u64;
        let fname_len = u16::from_le_bytes(cd_data[pos + 28..pos + 30].try_into().unwrap()) as usize;
        let extra_len = u16::from_le_bytes(cd_data[pos + 30..pos + 32].try_into().unwrap()) as usize;
        let comment_len = u16::from_le_bytes(cd_data[pos + 32..pos + 34].try_into().unwrap()) as usize;
        let local_header_offset = u32::from_le_bytes(cd_data[pos + 42..pos + 46].try_into().unwrap()) as u64;

        pos += 46;
        if pos + fname_len > cd_data.len() {
            break;
        }
        let fname = String::from_utf8_lossy(&cd_data[pos..pos + fname_len]).into_owned();
        pos += fname_len + extra_len + comment_len;

        entries.insert(
            fname,
            CdEntry { local_header_offset, compressed_size, uncompressed_size, method },
        );
    }

    // Filter to files we need; compute byte range = local header + 30 + fname + extra + data
    // We'll ask Swift for a conservative range: local_header_offset + 30 + fname_max + compressed_size
    let targets = ["trips.txt", "routes.txt"];
    let mut ranges = Vec::new();

    for target in &targets {
        if let Some(entry) = entries.get(*target) {
            // Local file header: 30 bytes fixed + variable fname + variable extra.
            // The local extra length is INDEPENDENT of the CD extra length — this is
            // a well-known ZIP quirk. Amtrak's ZIP uses local extras of up to ~36 bytes
            // (e.g. for NTFS timestamps). We add 256 bytes of slop to cover any local
            // fname + extra combination, then the full compressed payload.
            let fetch_start = entry.local_header_offset;
            let fetch_len = 30 + 256 + entry.compressed_size;
            ranges.push((target.to_string(), fetch_start, fetch_len));
        }
    }

    // Store entries for later use when files are fed in
    {
        let mut s = store().lock().unwrap();
        s.cd_entries = entries;
    }

    Ok(ranges)
}

/// Decompress a raw local-file-entry byte slice (starts at the local file header).
fn decompress_local_entry(data: &[u8], entry: &CdEntry) -> Result<Vec<u8>, String> {
    // Local file header: sig(4) ver(2) flags(2) method(2) time(2) date(2) crc(4)
    //   comp_size(4) uncomp_size(4) fname_len(2) extra_len(2) = 30 bytes fixed
    if data.len() < 30 {
        return Err("Local header too short".into());
    }
    let sig = u32::from_le_bytes(data[0..4].try_into().unwrap());
    if sig != 0x04034b50 {
        return Err(format!("Bad local file header signature: {:08x}", sig));
    }
    let fname_len = u16::from_le_bytes(data[26..28].try_into().unwrap()) as usize;
    let extra_len = u16::from_le_bytes(data[28..30].try_into().unwrap()) as usize;
    let data_start = 30 + fname_len + extra_len;
    let data_end = data_start + entry.compressed_size as usize;

    if data_end > data.len() {
        return Err(format!(
            "Data slice too short: need {} but have {}",
            data_end, data.len()
        ));
    }

    let compressed = &data[data_start..data_end];

    match entry.method {
        0 => {
            // Stored (no compression)
            Ok(compressed.to_vec())
        }
        8 => {
            // Deflated — use raw deflate (no zlib wrapper)
            let mut decoder = DeflateDecoder::new(compressed);
            let mut out = Vec::with_capacity(entry.uncompressed_size as usize);
            decoder
                .read_to_end(&mut out)
                .map_err(|e| format!("Deflate error: {}", e))?;
            Ok(out)
        }
        m => Err(format!("Unsupported compression method: {}", m)),
    }
}

// ── CSV parsing ──────────────────────────────────────────────────────────────

fn parse_routes(data: &[u8]) -> Result<HashMap<String, String>, String> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(data);

    let headers = rdr
        .headers()
        .map_err(|e| e.to_string())?
        .clone();

    let route_id_idx = headers.iter().position(|h| h == "route_id")
        .ok_or("routes.txt: missing route_id column")?;
    let route_short_idx = headers.iter().position(|h| h == "route_short_name");
    let route_long_idx  = headers.iter().position(|h| h == "route_long_name")
        .ok_or("routes.txt: missing route_long_name column")?;

    let mut map = HashMap::new();
    for result in rdr.records() {
        let record = result.map_err(|e| e.to_string())?;
        let route_id = record.get(route_id_idx).unwrap_or("").trim().to_string();
        let short = route_short_idx.and_then(|i| record.get(i)).unwrap_or("").trim().to_string();
        let long  = record.get(route_long_idx).unwrap_or("").trim().to_string();
        let name  = if !short.is_empty() { short } else { long };
        if !route_id.is_empty() {
            map.insert(route_id, name);
        }
    }
    Ok(map)
}

fn parse_trips(data: &[u8]) -> Result<HashMap<String, (String, String)>, String> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(data);

    let headers = rdr
        .headers()
        .map_err(|e| e.to_string())?
        .clone();

    let trip_id_idx = headers.iter().position(|h| h == "trip_id")
        .ok_or("trips.txt: missing trip_id column")?;
    let route_id_idx = headers.iter().position(|h| h == "route_id")
        .ok_or("trips.txt: missing route_id column")?;
    // trip_short_name is the train number (e.g. "168"); may be absent
    let short_name_idx = headers.iter().position(|h| h == "trip_short_name");

    let mut map = HashMap::new();
    for result in rdr.records() {
        let record = result.map_err(|e| e.to_string())?;
        let trip_id = record.get(trip_id_idx).unwrap_or("").trim().to_string();
        let route_id = record.get(route_id_idx).unwrap_or("").trim().to_string();
        let train_num = short_name_idx
            .and_then(|i| record.get(i))
            .unwrap_or("")
            .trim()
            .to_string();
        // Fallback: use trip_id itself as train number only if trip_short_name is absent.
        let train_num = if train_num.is_empty() {
            trip_id.clone()
        } else {
            train_num
        };
        if !trip_id.is_empty() {
            // Primary key: trip_id — handles bare-integer RT feeds (most Amtrak corridor trains).
            map.insert(trip_id.clone(), (train_num.clone(), route_id.clone()));
            // Secondary key: trip_short_name — handles "_AMTK_{short_name}" RT feeds
            // (long-distance Amtrak: Chief sends "2026-02-27_AMTK_3", last token "3" = short_name).
            // Skip if identical to trip_id (Gold Runner: trip_id == trip_short_name already indexed).
            if !train_num.is_empty() && train_num != trip_id {
                map.entry(train_num.clone()).or_insert((train_num.clone(), route_id));
            }
        }
    }
    Ok(map)
}

// ── FFI Result type ──────────────────────────────────────────────────────────

/// Returned by `gtfs_static_lookup`.  Both pointers may be null if not found.
/// Caller must pass to `gtfs_static_free_result` when done.
#[repr(C)]
pub struct GTFSStaticResult {
    pub train_number: *const c_char,  // e.g. "168"      (null if not found)
    pub route_name:   *const c_char,  // e.g. "Northeast Regional" (null if not found)
}

// ── FFI range-descriptor type ────────────────────────────────────────────────

/// One HTTP range request Swift needs to make.
#[repr(C)]
pub struct GTFSZipRange {
    pub filename:    *const c_char,  // "trips.txt" or "routes.txt"
    pub byte_offset: u64,
    pub byte_length: u64,
}

// ── FFI functions ─────────────────────────────────────────────────────────────

/// Feed the tail bytes of the ZIP (last ~65 KB).
/// On success returns a heap-allocated array of `GTFSZipRange` and sets
/// `*out_count`.  Pass the array to `gtfs_static_free_ranges` when done.
/// Returns null on error (check `*out_count == 0`).
#[no_mangle]
pub extern "C" fn gtfs_static_feed_eocd(
    data: *const u8,
    data_len: usize,
    out_count: *mut usize,
) -> *mut GTFSZipRange {
    unsafe { *out_count = 0 };

    if data.is_null() || data_len == 0 {
        return std::ptr::null_mut();
    }

    let slice = unsafe { std::slice::from_raw_parts(data, data_len) };

    match parse_eocd(slice) {
        Err(e) => {
            eprintln!("gtfs_static_feed_eocd: {}", e);
            std::ptr::null_mut()
        }
        Ok(ranges) => {
            let mut out: Vec<GTFSZipRange> = ranges
                .into_iter()
                .filter_map(|(name, offset, len)| {
                    CString::new(name).ok().map(|cs| GTFSZipRange {
                        filename: cs.into_raw(),
                        byte_offset: offset,
                        byte_length: len,
                    })
                })
                .collect();

            let count = out.len();
            unsafe { *out_count = count };
            let ptr = out.as_mut_ptr();
            std::mem::forget(out);
            ptr
        }
    }
}

/// Feed the raw bytes for one file entry (the slice starting at the local file
/// header).  `filename` must be "trips.txt" or "routes.txt".
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn gtfs_static_feed_file(
    filename: *const c_char,
    data: *const u8,
    data_len: usize,
) -> i32 {
    if filename.is_null() || data.is_null() {
        return -1;
    }

    let fname = unsafe {
        match CStr::from_ptr(filename).to_str() {
            Ok(s) => s.to_string(),
            Err(_) => return -1,
        }
    };

    let slice = unsafe { std::slice::from_raw_parts(data, data_len) };

    let entry = {
        let s = store().lock().unwrap();
        s.cd_entries.get(&fname).cloned()
    };

    let entry = match entry {
        Some(e) => e,
        None => {
            eprintln!(
                "gtfs_static_feed_file: '{}' not in cd_entries (known: {:?})",
                fname,
                store().lock().unwrap().cd_entries.keys().collect::<Vec<_>>()
            );
            return -1;
        }
    };

    let decompressed = match decompress_local_entry(slice, &entry) {
        Ok(d) => d,
        Err(e) => {
            eprintln!(
                "gtfs_static_feed_file({}): decompress error: {} [data_len={}, compressed_size={}, method={}]",
                fname, e, slice.len(), entry.compressed_size, entry.method
            );
            return -1;
        }
    };

    let mut s = store().lock().unwrap();

    match fname.as_str() {
        "routes.txt" => match parse_routes(&decompressed) {
            Ok(map) => {
                s.route_names.extend(map);
                0
            }
            Err(e) => {
                eprintln!("parse_routes: {}", e);
                -1
            }
        },
        "trips.txt" => match parse_trips(&decompressed) {
            Ok(map) => {
                s.trips.extend(map);
                0
            }
            Err(e) => {
                eprintln!("parse_trips: {}", e);
                -1
            }
        },
        _ => -1,
    }
}

/// Look up a realtime trip_id.
/// Always returns a valid (non-null) `GTFSStaticResult` pointer; individual
/// fields may be null if data is unavailable.
/// Caller must pass result to `gtfs_static_free_result`.
#[no_mangle]
pub extern "C" fn gtfs_static_lookup(
    trip_id: *const c_char,
) -> *mut GTFSStaticResult {
    let empty = Box::new(GTFSStaticResult {
        train_number: std::ptr::null(),
        route_name: std::ptr::null(),
    });

    if trip_id.is_null() {
        return Box::into_raw(empty);
    }

    let tid = match unsafe { CStr::from_ptr(trip_id).to_str() } {
        Ok(s) => s,
        Err(_) => return Box::into_raw(empty),
    };

    let s = store().lock().unwrap();
    match s.lookup(tid) {
        None => Box::into_raw(empty),
        Some((train_num, route_name)) => {
            let result = GTFSStaticResult {
                train_number: CString::new(train_num)
                    .map(|s| s.into_raw() as *const c_char)
                    .unwrap_or(std::ptr::null()),
                route_name: if route_name.is_empty() {
                    std::ptr::null()
                } else {
                    CString::new(route_name)
                        .map(|s| s.into_raw() as *const c_char)
                        .unwrap_or(std::ptr::null())
                },
            };
            Box::into_raw(Box::new(result))
        }
    }
}

/// Free a `GTFSStaticResult` returned by `gtfs_static_lookup`.
#[no_mangle]
pub extern "C" fn gtfs_static_free_result(result: *mut GTFSStaticResult) {
    if result.is_null() {
        return;
    }
    unsafe {
        let r = &*result;
        if !r.train_number.is_null() {
            let _ = CString::from_raw(r.train_number as *mut c_char);
        }
        if !r.route_name.is_null() {
            let _ = CString::from_raw(r.route_name as *mut c_char);
        }
        let _ = Box::from_raw(result);
    }
}

/// Free the range array returned by `gtfs_static_feed_eocd`.
#[no_mangle]
pub extern "C" fn gtfs_static_free_ranges(ranges: *mut GTFSZipRange, count: usize) {
    if ranges.is_null() || count == 0 {
        return;
    }
    unsafe {
        let slice = std::slice::from_raw_parts_mut(ranges, count);
        for r in slice.iter() {
            if !r.filename.is_null() {
                let _ = CString::from_raw(r.filename as *mut c_char);
            }
        }
        let _ = Vec::from_raw_parts(ranges, count, count);
    }
}

/// Returns 1 if both routes and trips tables are loaded, 0 otherwise.
#[no_mangle]
pub extern "C" fn gtfs_static_is_loaded() -> i32 {
    let s = store().lock().unwrap();
    if !s.route_names.is_empty() && !s.trips.is_empty() { 1 } else { 0 }
}

/// Evict all loaded data (e.g. before a refresh).
#[no_mangle]
pub extern "C" fn gtfs_static_reset() {
    let mut s = store().lock().unwrap();
    s.route_names.clear();
    s.trips.clear();
    s.cd_entries.clear();
}
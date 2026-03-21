/// GTFS Static Lookup + Shape Interpolation  —  Multi-store edition
///
/// # Multi-store design
///
/// Each GTFS static feed (Amtrak, SEPTA, NJT, …) gets its own
/// `GtfsStaticStore` identified by a `u32` store_id.  Store IDs are handed out
/// by `gtfs_static_store_new()` and released by `gtfs_static_store_free()`.
///
/// # Backward compatibility
///
/// The original singleton functions (`gtfs_static_feed_eocd`,
/// `gtfs_static_feed_file`, `gtfs_static_lookup`, …) are preserved unchanged.
/// They route to a hidden **legacy store** (`LEGACY_STORE_ID = 1`) so existing
/// Amtrak Swift code requires zero changes.
///
/// # Per-store trip-id strategy
///
/// Different agencies encode the human-readable train/run number in different
/// fields.  Each store carries a `TripIdStrategy` that controls how
/// `parse_trips` and `lookup` extract a display label:
///
/// | Strategy        | Agencies          | Source of train number           |
/// |-----------------|-------------------|----------------------------------|
/// | `ShortName`     | Amtrak, SJJPA     | `trip_short_name` (or `trip_id`) |
/// | `SeptaTripId`   | SEPTA Regional    | digits after leading alpha prefix |
/// | `RouteShortName`| NJT, MBTA, etc.   | `route_short_name`               |
/// | `Opaque`        | any               | `trip_id` verbatim               |
///
/// # ZIP loading protocol (unchanged)
///
///   1. Swift calls `gtfs_static_store_feed_eocd(store_id, tail, len, &count)`
///   2. Swift issues HTTP Range requests; calls `gtfs_static_store_feed_file`
///      for each (stops.txt MUST precede stop_times.txt)
///   3. Swift calls `gtfs_static_store_lookup(store_id, trip_id)` for display info
///   4. Swift calls `gtfs_static_store_is_trip_active` to filter pre-departure vehicles
///   5. Swift calls `gtfs_static_store_interpolate` for smooth lat/lon

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::io::Read;
use std::os::raw::c_char;
use std::sync::{Mutex, OnceLock};

use flate2::read::DeflateDecoder;

use crate::geo::haversine_m;

// ── Store ID for the legacy singleton ────────────────────────────────────────

const LEGACY_STORE_ID: u32 = 1;

// ── Trip-ID extraction strategy ──────────────────────────────────────────────

/// Controls how a store extracts a human-readable train/run number from
/// `trips.txt` for display in the UI.
///
/// Set via `gtfs_static_store_set_strategy()` before feeding any files.
/// Defaults to `ShortName` (Amtrak-compatible).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TripIdStrategy {
    /// Use `trip_short_name` as the train number; fall back to `trip_id`.
    /// Correct for Amtrak and Gold Runner / SJJPA.
    ShortName = 0,

    /// SEPTA Regional Rail: `trip_id` is `{LINE}{RUN}_{DATE}_{SID}`.
    /// e.g. `CYN1052_20260201_SID185189` → train_number = `"1052"`,
    /// and `route_short_name` (or `route_long_name`) provides the line name.
    ///
    /// Splitting on `_` and taking the first token gives `CYN1052`.
    /// The leading alphabetic prefix is the line code; the remaining digits
    /// are the run number that `staticInfo.trainNumber` should display.
    SeptaTripId = 1,

    /// Agencies where neither `trip_short_name` nor `trip_id` carry a
    /// meaningful run number (NJT, MBTA commuter rail, etc.).
    /// `route_short_name` is used as the line label; `train_number` will be
    /// the raw `trip_id` (callers should treat it as opaque / hide it).
    RouteShortName = 2,

    /// Fully opaque: expose `trip_id` verbatim as `train_number`.
    /// Useful for debugging and for agencies with truly unique trip_ids.
    Opaque = 3,
}

impl TripIdStrategy {
    fn from_i32(v: i32) -> Self {
        match v {
            1 => TripIdStrategy::SeptaTripId,
            2 => TripIdStrategy::RouteShortName,
            3 => TripIdStrategy::Opaque,
            _ => TripIdStrategy::ShortName,
        }
    }
}

// ── Registry ─────────────────────────────────────────────────────────────────

struct Registry {
    stores:  HashMap<u32, GtfsStaticStore>,
    next_id: u32,
}

impl Registry {
    fn new() -> Self {
        let mut r = Registry { stores: HashMap::new(), next_id: 2 }; // 1 = legacy
        r.stores.insert(LEGACY_STORE_ID, GtfsStaticStore::new(TripIdStrategy::ShortName));
        r
    }

    fn open(&mut self) -> u32 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(2);
        self.stores.insert(id, GtfsStaticStore::new(TripIdStrategy::ShortName));
        id
    }

    fn close(&mut self, id: u32) {
        if id != LEGACY_STORE_ID {
            self.stores.remove(&id);
        }
    }

    fn get(&self, id: u32) -> Option<&GtfsStaticStore> {
        self.stores.get(&id)
    }

    fn get_mut(&mut self, id: u32) -> Option<&mut GtfsStaticStore> {
        self.stores.get_mut(&id)
    }
}

static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();

fn registry() -> &'static Mutex<Registry> {
    REGISTRY.get_or_init(|| Mutex::new(Registry::new()))
}

// ── Data model ───────────────────────────────────────────────────────────────

struct GtfsStaticStore {
    strategy:          TripIdStrategy,
    route_names:       HashMap<String, String>, // route_id → display name
    route_short_names: HashMap<String, String>, // route_id → raw short name
    trips:             HashMap<String, (String, String)>, // trip_id → (train_num, route_id)
    trip_windows:      HashMap<String, (u32, u32)>,
    cd_entries:        HashMap<String, CdEntry>,
    shape_points:      HashMap<String, Vec<ShapePoint>>,
    stop_latlon:       HashMap<String, (f64, f64)>,
    trip_shape_ids:    HashMap<String, String>,
    trip_stop_seqs:    HashMap<String, TripStopSequence>,
    shape_timelines:   HashMap<String, TripShapeTimeline>,
}

#[derive(Debug, Clone)]
struct CdEntry {
    local_header_offset: u64,
    compressed_size:     u64,
    uncompressed_size:   u64,
    method:              u16,
}

impl GtfsStaticStore {
    fn new(strategy: TripIdStrategy) -> Self {
        Self {
            strategy,
            route_names:       HashMap::new(),
            route_short_names: HashMap::new(),
            trips:             HashMap::new(),
            trip_windows:      HashMap::new(),
            cd_entries:        HashMap::new(),
            shape_points:      HashMap::new(),
            stop_latlon:       HashMap::new(),
            trip_shape_ids:    HashMap::new(),
            trip_stop_seqs:    HashMap::new(),
            shape_timelines:   HashMap::new(),
        }
    }

    fn lookup(&self, trip_id: &str) -> Option<(String, String)> {
        let resolve = |train_num: &String, route_id: &String| -> (String, String) {
            let route_name = self.route_names
                .get(route_id)
                .cloned()
                .unwrap_or_else(|| route_id.clone());
            (train_num.clone(), route_name)
        };

        if let Some((train_num, route_id)) = self.trips.get(trip_id) {
            return Some(resolve(train_num, route_id));
        }

        if trip_id.contains('_') {
            if let Some(last) = trip_id.split('_').last() {
                if let Some((train_num, route_id)) = self.trips.get(last) {
                    return Some(resolve(train_num, route_id));
                }
            }
        }

        None
    }

    fn is_trip_active(&self, trip_id: &str, now_eastern: i64) -> bool {
        let canonical = if self.trip_windows.contains_key(trip_id) {
            trip_id.to_string()
        } else if trip_id.contains('_') {
            trip_id.split('_').last().unwrap_or(trip_id).to_string()
        } else {
            trip_id.to_string()
        };

        let Some(&(first_dep, last_arr)) = self.trip_windows.get(canonical.as_str()) else {
            return true;
        };

        const DELAY_BUFFER_SECS: u32 = 2 * 60 * 60;
        const SECS_PER_DAY:      i64 = 86_400;

        let window_end = last_arr.saturating_add(DELAY_BUFFER_SECS);
        for days_ago in 0i64..=3 {
            let midnight  = (now_eastern / SECS_PER_DAY - days_ago) * SECS_PER_DAY;
            let abs_start = midnight + first_dep as i64;
            let abs_end   = midnight + window_end as i64;
            if now_eastern >= abs_start && now_eastern <= abs_end {
                return true;
            }
        }
        false
    }

    fn interpolate_position(&mut self, trip_id: &str, now_eastern: i64) -> Option<(f64, f64)> {
        let canonical = self.resolve_trip_id_for_shape(trip_id)?;

        if !self.shape_timelines.contains_key(canonical.as_str()) {
            let shape_id = self.trip_shape_ids.get(canonical.as_str())?.clone();
            let shape    = self.shape_points.get(&shape_id)?.clone();
            let seq      = self.trip_stop_seqs.get(canonical.as_str())?.clone();
            let timeline = build_timeline(&shape, &seq);
            self.shape_timelines.insert(canonical.clone(), timeline);
        }

        let timeline = self.shape_timelines.get(canonical.as_str())?;
        let since_midnight = now_eastern % 86_400;
        query_position(timeline, since_midnight)
            .or_else(|| query_position(timeline, since_midnight + 86_400))
    }

    fn resolve_trip_id_for_shape(&self, trip_id: &str) -> Option<String> {
        if self.trip_shape_ids.contains_key(trip_id) {
            return Some(trip_id.to_string());
        }
        if trip_id.contains('_') {
            if let Some(last) = trip_id.split('_').last() {
                if self.trip_shape_ids.contains_key(last) {
                    return Some(last.to_string());
                }
            }
        }
        None
    }

    fn is_loaded(&self) -> bool {
        !self.route_names.is_empty() && !self.trips.is_empty() && !self.trip_windows.is_empty()
    }

    fn interpolation_ready(&self) -> bool {
        !self.shape_points.is_empty()
            && !self.stop_latlon.is_empty()
            && !self.trip_stop_seqs.is_empty()
    }

    fn reset(&mut self) {
        let strategy = self.strategy; // preserve strategy
        *self = GtfsStaticStore::new(strategy);
    }
}

// ── Shape interpolation types ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct ShapePoint { lat: f64, lon: f64 }

#[derive(Debug, Clone)]
struct TripStopSequence {
    stop_lats: Vec<f64>,
    stop_lons: Vec<f64>,
    dep_secs:  Vec<u32>,
}

#[derive(Debug, Clone)]
struct TripShapeTimeline {
    points:        Vec<ShapePoint>,
    time_at_point: Vec<i64>,
}

// ── Shape interpolation algorithm (unchanged) ─────────────────────────────────

fn build_timeline(shape: &[ShapePoint], seq: &TripStopSequence) -> TripShapeTimeline {
    let n = shape.len();
    if n == 0 || seq.dep_secs.is_empty() {
        return TripShapeTimeline { points: shape.to_vec(), time_at_point: vec![0; n] };
    }

    let mut cum_dist = vec![0.0f64; n];
    for i in 1..n {
        cum_dist[i] = cum_dist[i - 1]
            + haversine_m(shape[i-1].lat, shape[i-1].lon, shape[i].lat, shape[i].lon);
    }

    let mut time_at_point  = vec![0i64; n];
    time_at_point[0]       = seq.dep_secs[0] as i64;
    let num_stops          = seq.dep_secs.len();
    let mut shape_cursor   = 1usize;
    let mut prev_shape_inx = 0usize;
    let mut prev_time      = seq.dep_secs[0] as i64;

    for stop_idx in 1..num_stops {
        let stop_lat      = seq.stop_lats[stop_idx];
        let stop_lon      = seq.stop_lons[stop_idx];
        let mut next_time = seq.dep_secs[stop_idx] as i64;

        if next_time == prev_time {
            if stop_idx == num_stops - 1 { next_time += 1; } else { continue; }
        }

        let stop_shape_inx = nearest_forward(shape, stop_lat, stop_lon, prev_shape_inx);
        let seg_dist       = cum_dist[stop_shape_inx] - cum_dist[prev_shape_inx];
        if seg_dist == 0.0 { continue; }

        let seg_time = (next_time - prev_time) as f64;
        while shape_cursor <= stop_shape_inx {
            let dist_into_seg = cum_dist[shape_cursor] - cum_dist[prev_shape_inx];
            let frac = (dist_into_seg / seg_dist).clamp(0.0, 1.0);
            time_at_point[shape_cursor] = prev_time + (frac * seg_time) as i64;
            shape_cursor += 1;
        }

        time_at_point[stop_shape_inx] = next_time;
        prev_shape_inx = stop_shape_inx;
        prev_time      = next_time;
    }

    for i in shape_cursor..n { time_at_point[i] = prev_time; }
    TripShapeTimeline { points: shape.to_vec(), time_at_point }
}

fn query_position(timeline: &TripShapeTimeline, secs: i64) -> Option<(f64, f64)> {
    let times  = &timeline.time_at_point;
    let points = &timeline.points;
    if points.is_empty() { return None; }
    let last = points.len() - 1;
    if secs < times[0]    { return None; }
    if secs > times[last] { return None; }

    let seg = times
        .binary_search(&secs)
        .unwrap_or_else(|next| next.saturating_sub(1));
    if seg >= last { let p = &points[last]; return Some((p.lat, p.lon)); }

    let t0 = times[seg];   let t1 = times[seg + 1];
    let p0 = &points[seg]; let p1 = &points[seg + 1];
    let dt   = (t1 - t0) as f64;
    let frac = if dt > 0.0 { ((secs - t0) as f64 / dt).clamp(0.0, 1.0) } else { 0.0 };
    Some((p0.lat + (p1.lat - p0.lat) * frac, p0.lon + (p1.lon - p0.lon) * frac))
}

#[inline]
fn nearest_forward(shape: &[ShapePoint], stop_lat: f64, stop_lon: f64, start_from: usize) -> usize {
    let cos_lat = (stop_lat * std::f64::consts::PI / 180.0).cos();
    let mut best_dist = f64::MAX;
    let mut best_idx  = start_from;
    for (i, p) in shape.iter().enumerate().skip(start_from) {
        let dlat = p.lat - stop_lat;
        let dlon = (p.lon - stop_lon) * cos_lat;
        let d    = dlat * dlat + dlon * dlon;
        if d < best_dist { best_dist = d; best_idx = i; }
    }
    best_idx
}

// ── ZIP parsing ───────────────────────────────────────────────────────────────

const EOCD_MIN_SIZE: usize = 22;
const EOCD_SIG:      u32   = 0x06054b50;
const CDFH_SIG:      u32   = 0x02014b50;

fn parse_eocd_into(data: &[u8], store: &mut GtfsStaticStore) -> Result<Vec<(String, u64, u64)>, String> {
    let eocd_offset = data
        .windows(4)
        .rposition(|w| u32::from_le_bytes(w.try_into().unwrap()) == EOCD_SIG)
        .ok_or("EOCD signature not found")?;

    let eocd = &data[eocd_offset..];
    if eocd.len() < EOCD_MIN_SIZE { return Err("EOCD too short".into()); }

    let cd_size   = u32::from_le_bytes(eocd[12..16].try_into().unwrap()) as u64;
    let cd_offset = u32::from_le_bytes(eocd[16..20].try_into().unwrap()) as u64;
    let tail_start_in_file = (cd_offset + cd_size).saturating_sub(eocd_offset as u64);
    let cd_offset_in_buf   = (cd_offset - tail_start_in_file) as usize;

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
        if sig != CDFH_SIG { break; }
        let method            = u16::from_le_bytes(cd_data[pos + 10..pos + 12].try_into().unwrap());
        let compressed_size   = u32::from_le_bytes(cd_data[pos + 20..pos + 24].try_into().unwrap()) as u64;
        let uncompressed_size = u32::from_le_bytes(cd_data[pos + 24..pos + 28].try_into().unwrap()) as u64;
        let fname_len         = u16::from_le_bytes(cd_data[pos + 28..pos + 30].try_into().unwrap()) as usize;
        let extra_len         = u16::from_le_bytes(cd_data[pos + 30..pos + 32].try_into().unwrap()) as usize;
        let comment_len       = u16::from_le_bytes(cd_data[pos + 32..pos + 34].try_into().unwrap()) as usize;
        let local_header_offset = u32::from_le_bytes(cd_data[pos + 42..pos + 46].try_into().unwrap()) as u64;

        pos += 46;
        if pos + fname_len > cd_data.len() { break; }
        let fname = String::from_utf8_lossy(&cd_data[pos..pos + fname_len]).into_owned();
        pos += fname_len + extra_len + comment_len;
        entries.insert(fname, CdEntry { local_header_offset, compressed_size, uncompressed_size, method });
    }

    let targets = ["trips.txt", "routes.txt", "stop_times.txt", "stops.txt", "shapes.txt"];
    let mut ranges = Vec::new();
    for target in &targets {
        if let Some(entry) = entries.get(*target) {
            ranges.push((target.to_string(), entry.local_header_offset, 30 + 256 + entry.compressed_size));
        }
    }

    store.cd_entries = entries;
    Ok(ranges)
}

fn decompress_local_entry(data: &[u8], entry: &CdEntry) -> Result<Vec<u8>, String> {
    if data.len() < 30 { return Err("Local header too short".into()); }
    let sig = u32::from_le_bytes(data[0..4].try_into().unwrap());
    if sig != 0x04034b50 {
        return Err(format!("Bad local file header signature: {:08x}", sig));
    }
    let fname_len  = u16::from_le_bytes(data[26..28].try_into().unwrap()) as usize;
    let extra_len  = u16::from_le_bytes(data[28..30].try_into().unwrap()) as usize;
    let data_start = 30 + fname_len + extra_len;
    let data_end   = data_start + entry.compressed_size as usize;
    if data_end > data.len() {
        return Err(format!("Data slice too short: need {} but have {}", data_end, data.len()));
    }
    let compressed = &data[data_start..data_end];
    match entry.method {
        0 => Ok(compressed.to_vec()),
        8 => {
            let mut decoder = DeflateDecoder::new(compressed);
            let mut out = Vec::with_capacity(entry.uncompressed_size as usize);
            decoder.read_to_end(&mut out).map_err(|e| format!("Deflate error: {}", e))?;
            Ok(out)
        }
        m => Err(format!("Unsupported compression method: {}", m)),
    }
}

// ── CSV parsing ───────────────────────────────────────────────────────────────

#[inline]
fn parse_gtfs_time(s: &str) -> Option<u32> {
    let s = s.trim();
    if s.len() < 7 { return None; }
    let mut parts = s.split(':');
    let h   = parts.next()?;
    let m   = parts.next()?;
    let sec = parts.next()?;
    if parts.next().is_some() { return None; }
    if m.len() != 2 || sec.len() != 2 { return None; }
    let hh: u32 = h.parse().ok()?;
    let mm: u32 = m.parse().ok()?;
    let ss: u32 = sec.parse().ok()?;
    Some(hh * 3600 + mm * 60 + ss)
}

#[inline]
fn resolve_stop_time(rec: &csv::StringRecord, dep_idx: Option<usize>, arr_idx: Option<usize>) -> Option<u32> {
    dep_idx.and_then(|i| rec.get(i)).and_then(parse_gtfs_time)
        .or_else(|| arr_idx.and_then(|i| rec.get(i)).and_then(parse_gtfs_time))
}

fn parse_stop_times(data: &[u8]) -> Result<HashMap<String, (u32, u32)>, String> {
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(data);
    let headers = rdr.headers().map_err(|e| e.to_string())?.clone();
    let trip_id_idx = headers.iter().position(|h| h == "trip_id")
        .ok_or("stop_times.txt: missing trip_id column")?;
    let arr_idx = headers.iter().position(|h| h == "arrival_time");
    let dep_idx = headers.iter().position(|h| h == "departure_time");
    if arr_idx.is_none() && dep_idx.is_none() {
        return Err("stop_times.txt: missing both arrival_time and departure_time".into());
    }
    let mut map: HashMap<String, (u32, u32)> = HashMap::new();
    for result in rdr.records() {
        let record  = result.map_err(|e| e.to_string())?;
        let trip_id = match record.get(trip_id_idx) {
            Some(s) if !s.trim().is_empty() => s.trim().to_string(),
            _ => continue,
        };
        let Some(t) = resolve_stop_time(&record, dep_idx, arr_idx) else { continue };
        map.entry(trip_id)
            .and_modify(|(first, last)| { if t < *first { *first = t; } else if t > *last { *last = t; } })
            .or_insert((t, t));
    }
    Ok(map)
}

fn parse_stop_sequences(
    data: &[u8], stop_latlon: &HashMap<String, (f64, f64)>,
) -> Result<HashMap<String, TripStopSequence>, String> {
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(data);
    let headers = rdr.headers().map_err(|e| e.to_string())?.clone();
    let trip_id_idx = csv_col(&headers, "trip_id")?;
    let stop_id_idx = csv_col(&headers, "stop_id")?;
    let arr_idx     = headers.iter().position(|h| h == "arrival_time");
    let dep_idx     = headers.iter().position(|h| h == "departure_time");
    let seq_idx     = headers.iter().position(|h| h == "stop_sequence");
    if arr_idx.is_none() && dep_idx.is_none() {
        return Err("stop_times.txt: missing both arrival_time and departure_time".into());
    }
    struct RawRow { trip_id: String, seq: u32, stop_id: String, time: u32 }
    let mut rows: Vec<RawRow> = Vec::new();
    for result in rdr.records() {
        let rec = result.map_err(|e| e.to_string())?;
        let trip_id = match rec.get(trip_id_idx) {
            Some(s) if !s.trim().is_empty() => s.trim().to_string(),
            _ => continue,
        };
        let stop_id = rec.get(stop_id_idx).unwrap_or("").trim().to_string();
        let seq: u32 = seq_idx.and_then(|i| rec.get(i)).and_then(|s| s.trim().parse().ok()).unwrap_or(0);
        let Some(time) = resolve_stop_time(&rec, dep_idx, arr_idx) else { continue };
        rows.push(RawRow { trip_id, seq, stop_id, time });
    }
    rows.sort_by(|a, b| a.trip_id.cmp(&b.trip_id).then(a.seq.cmp(&b.seq)));
    let mut map: HashMap<String, TripStopSequence> = HashMap::new();
    for row in &rows {
        let (lat, lon) = stop_latlon.get(&row.stop_id).copied().unwrap_or((0.0, 0.0));
        let entry = map.entry(row.trip_id.clone()).or_insert_with(|| TripStopSequence {
            stop_lats: Vec::new(), stop_lons: Vec::new(), dep_secs: Vec::new(),
        });
        entry.stop_lats.push(lat);
        entry.stop_lons.push(lon);
        entry.dep_secs.push(row.time);
    }
    map.retain(|_, seq| seq.dep_secs.len() >= 2);
    Ok(map)
}

/// Returns (display_names, short_names).
fn parse_routes(data: &[u8]) -> Result<(HashMap<String, String>, HashMap<String, String>), String> {
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(data);
    let headers = rdr.headers().map_err(|e| e.to_string())?.clone();
    let route_id_idx   = headers.iter().position(|h| h == "route_id")
        .ok_or("routes.txt: missing route_id column")?;
    let route_short_idx = headers.iter().position(|h| h == "route_short_name");
    let route_long_idx  = headers.iter().position(|h| h == "route_long_name")
        .ok_or("routes.txt: missing route_long_name column")?;

    let mut display_names: HashMap<String, String> = HashMap::new();
    let mut short_names:   HashMap<String, String> = HashMap::new();

    for result in rdr.records() {
        let record   = result.map_err(|e| e.to_string())?;
        let route_id = record.get(route_id_idx).unwrap_or("").trim().to_string();
        if route_id.is_empty() { continue; }
        let short = route_short_idx.and_then(|i| record.get(i)).unwrap_or("").trim().to_string();
        let long  = record.get(route_long_idx).unwrap_or("").trim().to_string();
        let display = if !short.is_empty() { short.clone() } else { long };
        display_names.insert(route_id.clone(), display);
        if !short.is_empty() { short_names.insert(route_id, short); }
    }
    Ok((display_names, short_names))
}

/// Parse trips.txt, applying the store's strategy for train-number extraction.
fn parse_trips_with_strategy(
    data:              &[u8],
    strategy:          TripIdStrategy,
    route_short_names: &HashMap<String, String>,
) -> Result<(HashMap<String, (String, String)>, HashMap<String, String>), String> {
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(data);
    let headers = rdr.headers().map_err(|e| e.to_string())?.clone();

    let trip_id_idx    = headers.iter().position(|h| h == "trip_id")
        .ok_or("trips.txt: missing trip_id column")?;
    let route_id_idx   = headers.iter().position(|h| h == "route_id")
        .ok_or("trips.txt: missing route_id column")?;
    let short_name_idx = headers.iter().position(|h| h == "trip_short_name");
    let shape_id_idx   = headers.iter().position(|h| h == "shape_id");

    let mut trips_map:    HashMap<String, (String, String)> = HashMap::new();
    let mut shape_id_map: HashMap<String, String>           = HashMap::new();

    for result in rdr.records() {
        let record     = result.map_err(|e| e.to_string())?;
        let trip_id    = record.get(trip_id_idx).unwrap_or("").trim().to_string();
        let route_id   = record.get(route_id_idx).unwrap_or("").trim().to_string();
        let shape_id   = shape_id_idx.and_then(|i| record.get(i)).unwrap_or("").trim().to_string();
        let short_name = short_name_idx.and_then(|i| record.get(i)).unwrap_or("").trim().to_string();

        if trip_id.is_empty() { continue; }

        let (train_num, secondary_key) =
            extract_train_number(strategy, &trip_id, &short_name, &route_id, route_short_names);

        trips_map.insert(trip_id.clone(), (train_num.clone(), route_id.clone()));
        if let Some(sec) = secondary_key {
            trips_map.entry(sec).or_insert((train_num.clone(), route_id.clone()));
        }
        if !shape_id.is_empty() {
            shape_id_map.insert(trip_id, shape_id);
        }
    }
    Ok((trips_map, shape_id_map))
}

/// Strategy-dispatched train-number extraction.
///
/// Returns `(train_number, secondary_key)`.
/// `secondary_key` is an extra alias to insert into the trips table so that
/// mangled RT trip_ids (e.g. Amtrak's `_AMTK_<short>` prefix) can still hit.
fn extract_train_number(
    strategy:          TripIdStrategy,
    trip_id:           &str,
    short_name:        &str,
    route_id:          &str,
    route_short_names: &HashMap<String, String>,
) -> (String, Option<String>) {
    match strategy {
        TripIdStrategy::ShortName => {
            // Amtrak / Gold Runner: trip_short_name is canonical.
            // Fall back to trip_id (existing behavior).
            let train_num = if !short_name.is_empty() {
                short_name.to_string()
            } else {
                trip_id.to_string()
            };
            let secondary = if !short_name.is_empty() && short_name != trip_id {
                Some(short_name.to_string())
            } else {
                None
            };
            (train_num, secondary)
        }

        TripIdStrategy::SeptaTripId => {
            // SEPTA: trip_id = "CYN1052_20260201_SID185189"
            // First token = "CYN1052"; leading alpha = line code, trailing digits = run.
            let first_token = trip_id.split('_').next().unwrap_or(trip_id);
            let digit_start = first_token.find(|c: char| c.is_ascii_digit())
                .unwrap_or(first_token.len());
            let run_number  = &first_token[digit_start..];
            let train_num   = if run_number.is_empty() { first_token.to_string() } else { run_number.to_string() };
            (train_num, None)
        }

        TripIdStrategy::RouteShortName => {
            // Agencies where the route short name is the meaningful label.
            let train_num = route_short_names
                .get(route_id)
                .cloned()
                .unwrap_or_else(|| trip_id.to_string());
            (train_num, None)
        }

        TripIdStrategy::Opaque => {
            (trip_id.to_string(), None)
        }
    }
}

fn parse_shapes(data: &[u8]) -> Result<HashMap<String, Vec<ShapePoint>>, String> {
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(data);
    let headers = rdr.headers().map_err(|e| e.to_string())?.clone();
    let id_idx  = csv_col(&headers, "shape_id")?;
    let lat_idx = csv_col(&headers, "shape_pt_lat")?;
    let lon_idx = csv_col(&headers, "shape_pt_lon")?;
    let seq_idx = csv_col(&headers, "shape_pt_sequence")?;

    let mut raw: Vec<(String, u32, f64, f64)> = Vec::new();
    for result in rdr.records() {
        let rec      = result.map_err(|e| e.to_string())?;
        let shape_id = rec.get(id_idx).unwrap_or("").trim();
        if shape_id.is_empty() { continue; }
        let seq: u32 = rec.get(seq_idx).unwrap_or("0").trim().parse().unwrap_or(0);
        let lat: f64 = rec.get(lat_idx).unwrap_or("0").trim().parse().unwrap_or(0.0);
        let lon: f64 = rec.get(lon_idx).unwrap_or("0").trim().parse().unwrap_or(0.0);
        if lat == 0.0 && lon == 0.0 { continue; }
        raw.push((shape_id.to_string(), seq, lat, lon));
    }
    raw.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut map: HashMap<String, Vec<ShapePoint>> = HashMap::new();
    for (shape_id, _seq, lat, lon) in raw {
        map.entry(shape_id).or_default().push(ShapePoint { lat, lon });
    }
    Ok(map)
}

fn parse_stops_latlon(data: &[u8]) -> Result<HashMap<String, (f64, f64)>, String> {
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(data);
    let headers = rdr.headers().map_err(|e| e.to_string())?.clone();
    let id_idx  = csv_col(&headers, "stop_id")?;
    let lat_idx = csv_col(&headers, "stop_lat")?;
    let lon_idx = csv_col(&headers, "stop_lon")?;
    let mut map = HashMap::new();
    for result in rdr.records() {
        let rec     = result.map_err(|e| e.to_string())?;
        let stop_id = rec.get(id_idx).unwrap_or("").trim();
        if stop_id.is_empty() { continue; }
        let lat: f64 = rec.get(lat_idx).unwrap_or("0").trim().parse().unwrap_or(0.0);
        let lon: f64 = rec.get(lon_idx).unwrap_or("0").trim().parse().unwrap_or(0.0);
        map.insert(stop_id.to_string(), (lat, lon));
    }
    Ok(map)
}

fn csv_col(headers: &csv::StringRecord, name: &str) -> Result<usize, String> {
    headers.iter().position(|h| h.trim() == name)
        .ok_or_else(|| format!("missing column '{}'", name))
}

// ── Internal file-feed dispatcher ─────────────────────────────────────────────

/// Decompress and ingest one GTFS file into a store.
fn feed_file_into(store: &mut GtfsStaticStore, fname: &str, slice: &[u8]) -> i32 {
    let entry = match store.cd_entries.get(fname).cloned() {
        Some(e) => e,
        None => {
            eprintln!("gtfs_static feed_file: '{}' not in cd_entries (known: {:?})",
                      fname, store.cd_entries.keys().collect::<Vec<_>>());
            return -1;
        }
    };

    let decompressed = match decompress_local_entry(slice, &entry) {
        Ok(d)  => d,
        Err(e) => {
            eprintln!("feed_file({}): decompress error: {} [data_len={}, compressed_size={}, method={}]",
                      fname, e, slice.len(), entry.compressed_size, entry.method);
            return -1;
        }
    };

    match fname {
        "routes.txt" => match parse_routes(&decompressed) {
            Ok((display, short)) => {
                // or_insert: first feed wins on collision (Amtrak is authoritative).
                for (k, v) in display { store.route_names.entry(k).or_insert(v); }
                for (k, v) in short   { store.route_short_names.entry(k).or_insert(v); }
                0
            }
            Err(e) => { eprintln!("parse_routes: {}", e); -1 }
        },

        "trips.txt" => {
            // routes.txt should be fed before trips.txt so route_short_names is
            // already populated when RouteShortName strategy needs it.
            let strategy          = store.strategy;
            let route_short_names = store.route_short_names.clone();
            match parse_trips_with_strategy(&decompressed, strategy, &route_short_names) {
                Ok((trips_map, shape_id_map)) => {
                    for (k, v) in trips_map    { store.trips.entry(k).or_insert(v); }
                    for (k, v) in shape_id_map { store.trip_shape_ids.entry(k).or_insert(v); }
                    0
                }
                Err(e) => { eprintln!("parse_trips: {}", e); -1 }
            }
        },

        "stops.txt" => match parse_stops_latlon(&decompressed) {
            Ok(map) => { store.stop_latlon.extend(map); 0 }
            Err(e)  => { eprintln!("parse_stops_latlon: {}", e); -1 }
        },

        "stop_times.txt" => {
            match parse_stop_times(&decompressed) {
                Ok(map) => { store.trip_windows.extend(map); }
                Err(e)  => { eprintln!("parse_stop_times: {}", e); return -1; }
            }
            let stop_latlon = store.stop_latlon.clone();
            match parse_stop_sequences(&decompressed, &stop_latlon) {
                Ok(map) => { store.trip_stop_seqs.extend(map); 0 }
                Err(e)  => { eprintln!("parse_stop_sequences: {}", e); -1 }
            }
        },

        "shapes.txt" => match parse_shapes(&decompressed) {
            Ok(map) => { store.shape_points.extend(map); 0 }
            Err(e)  => { eprintln!("parse_shapes: {}", e); -1 }
        },

        _ => -1,
    }
}

// ── FFI result types ──────────────────────────────────────────────────────────

/// Returned by `gtfs_static_lookup` / `gtfs_static_store_lookup`.
/// Both pointers may be null.  Free with `gtfs_static_free_result`.
#[repr(C)]
pub struct GTFSStaticResult {
    pub train_number: *const c_char,
    pub route_name:   *const c_char,
}

/// One HTTP range request Swift needs to issue.
#[repr(C)]
pub struct GTFSZipRange {
    pub filename:    *const c_char,
    pub byte_offset: u64,
    pub byte_length: u64,
}

/// Returned by interpolation functions.  Stack-allocated — no free needed.
#[repr(C)]
pub struct InterpolatedPosition {
    pub lat:      f64,
    pub lon:      f64,
    /// 1 = valid; 0 = fall back to raw GPS.
    pub is_valid: i32,
}

// ── FFI helpers ───────────────────────────────────────────────────────────────

fn ranges_to_ffi(ranges: Vec<(String, u64, u64)>, out_count: *mut usize) -> *mut GTFSZipRange {
    let mut out: Vec<GTFSZipRange> = ranges.into_iter()
        .filter_map(|(name, offset, len)| {
            CString::new(name).ok().map(|cs| GTFSZipRange {
                filename: cs.into_raw(), byte_offset: offset, byte_length: len,
            })
        })
        .collect();
    let count = out.len();
    unsafe { *out_count = count };
    let ptr = out.as_mut_ptr();
    std::mem::forget(out);
    ptr
}

fn make_static_result(found: Option<(String, String)>) -> *mut GTFSStaticResult {
    match found {
        None => Box::into_raw(Box::new(GTFSStaticResult {
            train_number: std::ptr::null(),
            route_name:   std::ptr::null(),
        })),
        Some((train_num, route_name)) => {
            Box::into_raw(Box::new(GTFSStaticResult {
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
            }))
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// MARK: - Multi-store FFI  (new API)
// ═══════════════════════════════════════════════════════════════════════════════

/// Allocate a new GTFS static store.
/// Returns a non-zero `store_id`; free with `gtfs_static_store_free`.
/// The store starts with `ShortName` strategy (Amtrak-compatible).
/// Call `gtfs_static_store_set_strategy` before feeding files to change it.
#[no_mangle]
pub extern "C" fn gtfs_static_store_new() -> u32 {
    registry().lock().unwrap().open()
}

/// Release the store identified by `store_id`.
/// Silently ignores unknown or zero IDs.
#[no_mangle]
pub extern "C" fn gtfs_static_store_free(store_id: u32) {
    if store_id == 0 { return; }
    registry().lock().unwrap().close(store_id);
}

/// Set the trip-id extraction strategy for `store_id`.
/// **Call before feeding any files.**
///
/// `strategy` values:
///   0 = ShortName      (Amtrak / Gold Runner — default)
///   1 = SeptaTripId    (SEPTA Regional Rail)
///   2 = RouteShortName (NJT, MBTA, etc.)
///   3 = Opaque         (trip_id verbatim)
#[no_mangle]
pub extern "C" fn gtfs_static_store_set_strategy(store_id: u32, strategy: i32) {
    let mut reg = registry().lock().unwrap();
    if let Some(store) = reg.get_mut(store_id) {
        store.strategy = TripIdStrategy::from_i32(strategy);
    }
}

/// Step 1 of the loading protocol: feed the ZIP tail bytes.
/// Returns a heap-allocated `GTFSZipRange` array; free with `gtfs_static_free_ranges`.
/// Returns null on error.
#[no_mangle]
pub extern "C" fn gtfs_static_store_feed_eocd(
    store_id:  u32,
    data:      *const u8,
    data_len:  usize,
    out_count: *mut usize,
) -> *mut GTFSZipRange {
    unsafe { *out_count = 0 };
    if store_id == 0 || data.is_null() || data_len == 0 { return std::ptr::null_mut(); }

    let slice   = unsafe { std::slice::from_raw_parts(data, data_len) };
    let mut reg = registry().lock().unwrap();
    let Some(store) = reg.get_mut(store_id) else { return std::ptr::null_mut() };

    match parse_eocd_into(slice, store) {
        Ok(ranges) => ranges_to_ffi(ranges, out_count),
        Err(e)     => { eprintln!("gtfs_static_store_feed_eocd: {}", e); std::ptr::null_mut() }
    }
}

/// Step 2 of the loading protocol: feed one GTFS file.
/// Accepted: "routes.txt", "trips.txt", "stops.txt",
///           "stop_times.txt" (stops.txt must precede), "shapes.txt".
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn gtfs_static_store_feed_file(
    store_id: u32,
    filename: *const c_char,
    data:     *const u8,
    data_len: usize,
) -> i32 {
    if store_id == 0 || filename.is_null() || data.is_null() { return -1; }
    let fname = match unsafe { CStr::from_ptr(filename).to_str() } {
        Ok(s)  => s.to_string(),
        Err(_) => return -1,
    };
    let slice   = unsafe { std::slice::from_raw_parts(data, data_len) };
    let mut reg = registry().lock().unwrap();
    let Some(store) = reg.get_mut(store_id) else { return -1 };
    feed_file_into(store, &fname, slice)
}

/// Look up a realtime `trip_id` in `store_id`.
/// Always returns non-null; free with `gtfs_static_free_result`.
#[no_mangle]
pub extern "C" fn gtfs_static_store_lookup(
    store_id: u32,
    trip_id:  *const c_char,
) -> *mut GTFSStaticResult {
    if store_id == 0 || trip_id.is_null() {
        return make_static_result(None);
    }
    let tid = match unsafe { CStr::from_ptr(trip_id).to_str() } {
        Ok(s)  => s,
        Err(_) => return make_static_result(None),
    };
    let reg = registry().lock().unwrap();
    let found = reg.get(store_id).and_then(|s| s.lookup(tid));
    make_static_result(found)
}

/// Returns 1 if routes, trips, and stop_times are loaded in `store_id`.
#[no_mangle]
pub extern "C" fn gtfs_static_store_is_loaded(store_id: u32) -> i32 {
    let reg = registry().lock().unwrap();
    reg.get(store_id).map(|s| if s.is_loaded() { 1 } else { 0 }).unwrap_or(0)
}

/// Returns 1 if `trip_id` is scheduled as active at `now_eastern` in `store_id`.
#[no_mangle]
pub extern "C" fn gtfs_static_store_is_trip_active(
    store_id:    u32,
    trip_id:     *const c_char,
    now_eastern: i64,
) -> i32 {
    if store_id == 0 || trip_id.is_null() { return 0; }
    let tid = match unsafe { CStr::from_ptr(trip_id).to_str() } {
        Ok(s)  => s,
        Err(_) => return 0,
    };
    let reg = registry().lock().unwrap();
    let Some(store) = reg.get(store_id) else { return 0 };
    if store.trip_windows.is_empty() { return 1; }
    if store.is_trip_active(tid, now_eastern) { 1 } else { 0 }
}

/// Compute a smooth interpolated position for `trip_id` at `now_eastern` in `store_id`.
/// `is_valid = 0` means shape data is unavailable — fall back to raw GPS.
#[no_mangle]
pub extern "C" fn gtfs_static_store_interpolate(
    store_id:    u32,
    trip_id:     *const c_char,
    now_eastern: i64,
) -> InterpolatedPosition {
    let null = InterpolatedPosition { lat: 0.0, lon: 0.0, is_valid: 0 };
    if store_id == 0 || trip_id.is_null() { return null; }
    let tid = match unsafe { CStr::from_ptr(trip_id).to_str() } {
        Ok(s)  => s,
        Err(_) => return null,
    };
    let mut reg = registry().lock().unwrap();
    let Some(store) = reg.get_mut(store_id) else { return null };
    match store.interpolate_position(tid, now_eastern) {
        Some((lat, lon)) => InterpolatedPosition { lat, lon, is_valid: 1 },
        None             => null,
    }
}

/// Returns 1 when shape interpolation data is fully loaded for `store_id`.
#[no_mangle]
pub extern "C" fn gtfs_static_store_interpolation_ready(store_id: u32) -> i32 {
    let reg = registry().lock().unwrap();
    reg.get(store_id).map(|s| if s.interpolation_ready() { 1 } else { 0 }).unwrap_or(0)
}

/// Evict all data from `store_id` (preserves strategy).
#[no_mangle]
pub extern "C" fn gtfs_static_store_reset(store_id: u32) {
    let mut reg = registry().lock().unwrap();
    if let Some(store) = reg.get_mut(store_id) { store.reset(); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// MARK: - Legacy singleton FFI  (routes to LEGACY_STORE_ID = 1, zero Swift changes needed)
// ═══════════════════════════════════════════════════════════════════════════════

/// Feed the ZIP tail bytes into the legacy (Amtrak) store.
#[no_mangle]
pub extern "C" fn gtfs_static_feed_eocd(
    data: *const u8, data_len: usize, out_count: *mut usize,
) -> *mut GTFSZipRange {
    gtfs_static_store_feed_eocd(LEGACY_STORE_ID, data, data_len, out_count)
}

/// Feed one GTFS file into the legacy (Amtrak) store.
#[no_mangle]
pub extern "C" fn gtfs_static_feed_file(
    filename: *const c_char, data: *const u8, data_len: usize,
) -> i32 {
    gtfs_static_store_feed_file(LEGACY_STORE_ID, filename, data, data_len)
}

/// Look up a trip_id in the legacy (Amtrak) store.
#[no_mangle]
pub extern "C" fn gtfs_static_lookup(trip_id: *const c_char) -> *mut GTFSStaticResult {
    gtfs_static_store_lookup(LEGACY_STORE_ID, trip_id)
}

/// Free a `GTFSStaticResult` returned by any lookup function.
#[no_mangle]
pub extern "C" fn gtfs_static_free_result(result: *mut GTFSStaticResult) {
    if result.is_null() { return; }
    unsafe {
        let r = &*result;
        if !r.train_number.is_null() { let _ = CString::from_raw(r.train_number as *mut c_char); }
        if !r.route_name.is_null()   { let _ = CString::from_raw(r.route_name   as *mut c_char); }
        let _ = Box::from_raw(result);
    }
}

/// Free a range array returned by any `feed_eocd` function.
#[no_mangle]
pub extern "C" fn gtfs_static_free_ranges(ranges: *mut GTFSZipRange, count: usize) {
    if ranges.is_null() || count == 0 { return; }
    unsafe {
        let slice = std::slice::from_raw_parts_mut(ranges, count);
        for r in slice.iter() {
            if !r.filename.is_null() { let _ = CString::from_raw(r.filename as *mut c_char); }
        }
        let _ = Vec::from_raw_parts(ranges, count, count);
    }
}

/// Returns 1 if the legacy store has all core tables loaded.
#[no_mangle]
pub extern "C" fn gtfs_static_is_loaded() -> i32 {
    gtfs_static_store_is_loaded(LEGACY_STORE_ID)
}

/// Returns 1 if `trip_id` is active at `now_eastern` in the legacy store.
#[no_mangle]
pub extern "C" fn gtfs_static_is_trip_active(trip_id: *const c_char, now_eastern: i64) -> i32 {
    gtfs_static_store_is_trip_active(LEGACY_STORE_ID, trip_id, now_eastern)
}

/// Compute an interpolated position from the legacy store.
#[no_mangle]
pub extern "C" fn gtfs_interpolate_position(trip_id: *const c_char, now_eastern: i64) -> InterpolatedPosition {
    gtfs_static_store_interpolate(LEGACY_STORE_ID, trip_id, now_eastern)
}

/// Returns 1 when shape interpolation data is ready in the legacy store.
#[no_mangle]
pub extern "C" fn gtfs_interpolation_is_ready() -> i32 {
    gtfs_static_store_interpolation_ready(LEGACY_STORE_ID)
}

/// Evict all data from the legacy store.
#[no_mangle]
pub extern "C" fn gtfs_static_reset() {
    gtfs_static_store_reset(LEGACY_STORE_ID)
}
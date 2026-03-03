/// GTFS Static Lookup + Shape Interpolation
///
/// Parses Amtrak's GTFS.zip via HTTP range requests, and computes smooth
/// interpolated vehicle positions between GPS pings.
///
/// # ZIP loading protocol
///
///   1. Swift fetches the last 256 KB  →  `gtfs_static_feed_eocd()`
///      Rust finds the Central Directory, returns byte ranges for all needed
///      files back to Swift.
///
///   2. Swift issues HTTP Range requests for each range:
///         gtfs_static_feed_file("routes.txt",    data, len)
///         gtfs_static_feed_file("trips.txt",     data, len)
///         gtfs_static_feed_file("stops.txt",     data, len)   ← feeds stop lat/lon
///         gtfs_static_feed_file("stop_times.txt",data, len)   ← must follow stops.txt
///         gtfs_static_feed_file("shapes.txt",    data, len)
///
///   3. Swift calls `gtfs_static_lookup(trip_id)` for train number / route name.
///
///   4. Swift calls `gtfs_static_is_trip_active(trip_id, now_eastern)` to filter
///      non-revenue/pre-departure vehicles.
///
///   5. Swift calls `gtfs_interpolate_position(trip_id, now_eastern)` for a smooth
///      lat/lon between GPS updates. Falls back to raw GPS if `is_valid == 0`.
///
/// # Feed order constraint
///
///   stops.txt MUST be fed before stop_times.txt so stop lat/lon can be resolved
///   when building the per-trip stop sequences used for interpolation.
///
/// # Thread safety
///
///   All state lives behind a single `Mutex<GtfsStaticStore>`. Parsing is
///   write-locked; lookups and interpolation are also write-locked (timelines
///   are built and cached lazily on first query).

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
    /// trip_id → (first_departure_secs, last_arrival_secs) from midnight of
    /// the service day. Values can exceed 86400 for overnight/multi-day trips.
    /// Used by `is_trip_active` to reject yard/deadhead/pre-departure vehicles.
    trip_windows: HashMap<String, (u32, u32)>,
    /// Pending central-directory entries indexed by filename
    cd_entries: HashMap<String, CdEntry>,

    // ── Shape interpolation fields ──────────────────────────────────────────

    /// shape_id → ordered polyline points (from shapes.txt)
    shape_points: HashMap<String, Vec<ShapePoint>>,
    /// stop_id → (lat, lon) (from stops.txt; needed to resolve stop positions
    /// when building per-trip stop sequences for shape interpolation)
    stop_latlon: HashMap<String, (f64, f64)>,
    /// static trip_id → shape_id (populated while parsing trips.txt)
    trip_shape_ids: HashMap<String, String>,
    /// static trip_id → full ordered stop sequence with departure times
    /// (from stop_times.txt; requires stop_latlon to be populated first)
    trip_stop_seqs: HashMap<String, TripStopSequence>,
    /// static trip_id → cached TripShapeTimeline (built lazily on first query)
    shape_timelines: HashMap<String, TripShapeTimeline>,
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
            route_names:     HashMap::new(),
            trips:           HashMap::new(),
            trip_windows:    HashMap::new(),
            cd_entries:      HashMap::new(),
            shape_points:    HashMap::new(),
            stop_latlon:     HashMap::new(),
            trip_shape_ids:  HashMap::new(),
            trip_stop_seqs:  HashMap::new(),
            shape_timelines: HashMap::new(),
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

    /// Returns true if `now_eastern` (Unix timestamp already adjusted to Eastern
    /// Time by the caller) falls within the active window of the trip.
    ///
    /// **Caller contract**: `now_eastern` must be `now_unix + eastern_utc_offset_secs`
    /// so that integer division by 86 400 yields the correct Eastern-calendar
    /// midnight. Swift satisfies this with Foundation's DST-aware:
    ///   `Int64(now.timeIntervalSince1970) +
    ///    Int64(TimeZone(identifier: "America/New_York")!.secondsFromGMT(for: now))`
    ///
    /// Strategy:
    /// - Look up the trip's (first_dep, last_arr) in seconds-from-Eastern-midnight.
    /// - For each of the last 4 Eastern midnights, compute absolute [start, end]
    ///   and check whether now_eastern falls within the window (+ 2 h delay buffer).
    ///   This covers same-day trips AND long-distance trains that departed on a
    ///   prior calendar day (California Zephyr ~65 h, Auto Train ~18 h, etc.).
    /// - If the trip has no window data (stop_times not loaded yet), return
    ///   true so vehicles aren't incorrectly hidden before the file loads.
    fn is_trip_active(&self, trip_id: &str, now_eastern: i64) -> bool {
        // Resolve the canonical trip_id (handles prefixed RT formats)
        let canonical = if self.trip_windows.contains_key(trip_id) {
            trip_id.to_string()
        } else if trip_id.contains('_') {
            trip_id
                .split('_')
                .last()
                .unwrap_or(trip_id)
                .to_string()
        } else {
            trip_id.to_string()
        };

        let Some(&(first_dep, last_arr)) = self.trip_windows.get(canonical.as_str()) else {
            // No window data — degrade gracefully (don't hide the vehicle)
            return true;
        };

        const DELAY_BUFFER_SECS: u32 = 2 * 60 * 60; // 2 h buffer for late trains
        const SECS_PER_DAY: i64 = 86_400;

        let window_end = last_arr.saturating_add(DELAY_BUFFER_SECS);

        // Check today's Eastern midnight and up to 3 prior midnights.
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

    // ── Shape interpolation methods ─────────────────────────────────────────

    /// Return an interpolated (lat, lon) for `trip_id` at `now_eastern`.
    ///
    /// `now_eastern` is a Unix timestamp pre-adjusted to Eastern Time by Swift:
    ///   `now_unix + TimeZone("America/New_York").secondsFromGMT(now)`
    ///
    /// The timeline is built once per trip and cached in `shape_timelines`.
    ///
    /// Returns `None` if:
    ///   - shapes.txt or stop_times.txt haven't been fed yet
    ///   - the trip has no shape_id in trips.txt (Thruway bus, etc.)
    ///   - the trip's shape_id doesn't match any entry in shapes.txt
    fn interpolate_position(
        &mut self,
        trip_id:     &str,
        now_eastern: i64,
    ) -> Option<(f64, f64)> {
        let canonical = self.resolve_trip_id_for_shape(trip_id)?;

        // Build and cache the timeline on first use.
        if !self.shape_timelines.contains_key(canonical.as_str()) {
            let shape_id = self.trip_shape_ids.get(canonical.as_str())?.clone();
            let shape    = self.shape_points.get(&shape_id)?.clone();
            let seq      = self.trip_stop_seqs.get(canonical.as_str())?.clone();
            let timeline = build_timeline(&shape, &seq);
            self.shape_timelines.insert(canonical.clone(), timeline);
        }

        let timeline = self.shape_timelines.get(canonical.as_str())?;

        // Convert absolute Eastern timestamp → seconds from Eastern midnight.
        // For overnight trips (stop times > 86 400 s), also try the next-day
        // offset so "25:10:00" stop times can be matched.
        let since_midnight = now_eastern % 86_400;
        query_position(timeline, since_midnight)
            .or_else(|| query_position(timeline, since_midnight + 86_400))
    }

    /// Resolve an RT trip_id to the static key used in `trip_shape_ids`.
    /// Mirrors the same two-step logic as `lookup()`.
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
}

// ── Shape interpolation data types ───────────────────────────────────────────

/// One point on a route shape polyline.
#[derive(Debug, Clone)]
struct ShapePoint {
    lat: f64,
    lon: f64,
}

/// Full ordered stop-time sequence for one trip.
/// Built from stop_times.txt; stop positions resolved via stops.txt.
#[derive(Debug, Clone)]
struct TripStopSequence {
    /// Parallel arrays: stop position and departure time.
    stop_lats: Vec<f64>,
    stop_lons: Vec<f64>,
    /// Departure seconds from midnight. GTFS allows >86400 for overnight trips
    /// (e.g. "25:10:00" = 90600 s for a train arriving the following calendar day).
    dep_secs: Vec<u32>,
}

/// Pre-computed per-shape-point timetable for one trip.
/// Built lazily on the first call to `interpolate_position` for that trip.
#[derive(Debug, Clone)]
struct TripShapeTimeline {
    points: Vec<ShapePoint>,
    /// Parallel to `points`: seconds-from-midnight at which the vehicle is
    /// expected to be at each shape point under the constant-speed assumption.
    time_at_point: Vec<i64>,
}

// ── Shape interpolation core algorithm ───────────────────────────────────────

/// Build the time-per-shape-point table for one trip.
///
/// Given the ordered shape polyline and the ordered stop schedule, produces a
/// `Vec<i64>` parallel to `shape` where `time_at_point[i]` is the
/// seconds-from-midnight when the vehicle is expected to be at shape point i.
///
/// Method:
///   1. Compute cumulative geodesic distance along the polyline.
///   2. For each stop, find the nearest shape point at or after the previous
///      stop's shape point (forward-only search avoids ambiguity on loops).
///   3. Between consecutive stop anchors, distribute time proportionally to
///      distance (constant-speed assumption within each inter-stop segment).
///   4. Snap each anchor point to its exact scheduled time to prevent
///      floating-point drift from accumulating across long trips.
fn build_timeline(shape: &[ShapePoint], seq: &TripStopSequence) -> TripShapeTimeline {
    let n = shape.len();
    if n == 0 || seq.dep_secs.is_empty() {
        return TripShapeTimeline { points: shape.to_vec(), time_at_point: vec![0; n] };
    }

    // Step 1: cumulative geodesic distance along the shape polyline.
    let mut cum_dist = vec![0.0f64; n];
    for i in 1..n {
        cum_dist[i] = cum_dist[i - 1]
            + haversine_m(shape[i-1].lat, shape[i-1].lon, shape[i].lat, shape[i].lon);
    }

    let mut time_at_point  = vec![0i64; n];
    time_at_point[0]       = seq.dep_secs[0] as i64;

    let num_stops          = seq.dep_secs.len();
    let mut shape_cursor   = 1usize;    // next shape index still needing a time
    let mut prev_shape_inx = 0usize;    // shape index of the most recent stop anchor
    let mut prev_time      = seq.dep_secs[0] as i64;

    for stop_idx in 1..num_stops {
        let stop_lat      = seq.stop_lats[stop_idx];
        let stop_lon      = seq.stop_lons[stop_idx];
        let mut next_time = seq.dep_secs[stop_idx] as i64;

        // Guard: consecutive stops with identical scheduled times (e.g. loop
        // routes, data errors) would give infinite speed. Skip as an anchor
        // except for the last stop, where we nudge by 1 s so the train still
        // moves to the terminus.
        if next_time == prev_time {
            if stop_idx == num_stops - 1 { next_time += 1; } else { continue; }
        }

        // Step 2: snap this stop to its nearest forward shape point.
        let stop_shape_inx = nearest_forward(shape, stop_lat, stop_lon, prev_shape_inx);

        let seg_dist = cum_dist[stop_shape_inx] - cum_dist[prev_shape_inx];

        // Guard: two consecutive stops snap to the same shape point (sparse
        // polyline). Skip; the next stop's segment will cover this region.
        if seg_dist == 0.0 { continue; }

        let seg_time = (next_time - prev_time) as f64;

        // Step 3: fill time_at_point for indices in (prev_shape_inx, stop_shape_inx].
        while shape_cursor <= stop_shape_inx {
            let dist_into_seg = cum_dist[shape_cursor] - cum_dist[prev_shape_inx];
            let frac = (dist_into_seg / seg_dist).clamp(0.0, 1.0);
            time_at_point[shape_cursor] = prev_time + (frac * seg_time) as i64;
            shape_cursor += 1;
        }

        // Step 4: snap the anchor point to the exact scheduled time.
        time_at_point[stop_shape_inx] = next_time;

        prev_shape_inx = stop_shape_inx;
        prev_time      = next_time;
    }

    // Clamp any shape points past the last stop anchor to the last scheduled
    // time. A query past the terminus returns the terminus location.
    for i in shape_cursor..n {
        time_at_point[i] = prev_time;
    }

    TripShapeTimeline { points: shape.to_vec(), time_at_point }
}

/// Query the interpolated (lat, lon) at `secs_from_midnight`.
///
/// Clamps to the shape endpoints if the time is before the first stop or after
/// the last stop. Returns `None` only for an empty shape.
fn query_position(timeline: &TripShapeTimeline, secs: i64) -> Option<(f64, f64)> {
    let times  = &timeline.time_at_point;
    let points = &timeline.points;
    if points.is_empty() { return None; }

    let last = points.len() - 1;

    if secs <= times[0]    { let p = &points[0];    return Some((p.lat, p.lon)); }
    if secs >= times[last] { let p = &points[last]; return Some((p.lat, p.lon)); }

    // Binary-search for the two shape-point indices that bracket `secs`.
    // .unwrap_or_else replaces the equivalent match { Ok(i) => i, Err(i) => i.saturating_sub(1) }:
    //   Ok(exact)  — `secs` coincides exactly with a known shape-point time → use that index
    //   Err(next)  — `secs` falls between two points → step back one to get the left endpoint
    let seg = times
        .binary_search(&secs)
        .unwrap_or_else(|next| next.saturating_sub(1));
    if seg >= last { let p = &points[last]; return Some((p.lat, p.lon)); }

    let t0 = times[seg];   let t1 = times[seg + 1];
    let p0 = &points[seg]; let p1 = &points[seg + 1];

    let dt   = (t1 - t0) as f64;
    let frac = if dt > 0.0 { ((secs - t0) as f64 / dt).clamp(0.0, 1.0) } else { 0.0 };

    Some((
        p0.lat + (p1.lat - p0.lat) * frac,
        p0.lon + (p1.lon - p0.lon) * frac,
    ))
}

// ── Geometry helpers ──────────────────────────────────────────────────────────

/// Haversine distance in metres between two lat/lon points.
#[inline]
fn haversine_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6_371_000.0;
    const D: f64 = std::f64::consts::PI / 180.0;
    let dlat = (lat2 - lat1) * D;
    let dlon = (lon2 - lon1) * D;
    let a = (dlat * 0.5).sin().powi(2)
        + (lat1 * D).cos() * (lat2 * D).cos() * (dlon * 0.5).sin().powi(2);
    R * 2.0 * a.sqrt().asin()
}

/// Find the shape point nearest to (stop_lat, stop_lon), searching only at or
/// after `start_from`. Forward-only prevents a later stop from snapping to a
/// shape point behind an earlier stop (important on loops and complex routes).
///
/// Uses squared equirectangular distance — fast, and accurate enough for the
/// tens-of-metres precision needed to snap a stop to its polyline.
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

/// Minimum size of the End-of-Central-Directory record (no comment).
const EOCD_MIN_SIZE: usize = 22;
/// EOCD signature
const EOCD_SIG: u32 = 0x06054b50;
/// Central Directory File Header signature
const CDFH_SIG: u32 = 0x02014b50;

/// Parse the End-of-Central-Directory block and populate `cd_entries`.
/// `data` is the raw tail bytes fetched by Swift (up to 256 KB).
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

    let cd_size   = u32::from_le_bytes(eocd[12..16].try_into().unwrap()) as u64;
    let cd_offset = u32::from_le_bytes(eocd[16..20].try_into().unwrap()) as u64;

    let tail_start_in_file = {
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

    // All files we need to request.
    // stops.txt and shapes.txt are new additions for shape interpolation.
    let targets = [
        "trips.txt",
        "routes.txt",
        "stop_times.txt",
        "stops.txt",   // needed for stop lat/lon → shape timeline anchors
        "shapes.txt",  // needed for shape polylines
    ];
    let mut ranges = Vec::new();

    for target in &targets {
        if let Some(entry) = entries.get(*target) {
            // Local file header: 30 bytes fixed + variable fname + variable extra.
            // The local extra length is INDEPENDENT of the CD extra length — this is
            // a well-known ZIP quirk. Amtrak's ZIP uses local extras of up to ~36 bytes
            // (e.g. for NTFS timestamps). We add 256 bytes of slop to cover any local
            // fname + extra combination, then the full compressed payload.
            let fetch_start = entry.local_header_offset;
            let fetch_len   = 30 + 256 + entry.compressed_size;
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
    if data.len() < 30 {
        return Err("Local header too short".into());
    }
    let sig = u32::from_le_bytes(data[0..4].try_into().unwrap());
    if sig != 0x04034b50 {
        return Err(format!("Bad local file header signature: {:08x}", sig));
    }
    let fname_len  = u16::from_le_bytes(data[26..28].try_into().unwrap()) as usize;
    let extra_len  = u16::from_le_bytes(data[28..30].try_into().unwrap()) as usize;
    let data_start = 30 + fname_len + extra_len;
    let data_end   = data_start + entry.compressed_size as usize;

    if data_end > data.len() {
        return Err(format!(
            "Data slice too short: need {} but have {}",
            data_end, data.len()
        ));
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

/// Arithmetic core for GTFS time parsing.
///
/// Converts already-validated hour/minute/second string slices to total
/// seconds from midnight. Called only after the outer validator has confirmed
/// that `min` and `sec` are exactly two digits and no extra colon exists.
/// Mirrors the `parse_time_impl` split used in the serde_helpers.rs reference.
#[inline]
fn parse_gtfs_time_impl(h: &str, m: &str, sec: &str) -> Option<u32> {
    let hh: u32 = h.parse().ok()?;
    let mm: u32 = m.parse().ok()?;
    let ss: u32 = sec.parse().ok()?;
    Some(hh * 3600 + mm * 60 + ss)
}

/// Parse a GTFS time string like "08:30:00" or "25:10:00" (overnight) into
/// total seconds from midnight. Values > 86 400 are valid for multi-day trips.
///
/// Validation rules (mirrors the serde_helpers.rs `parse_time` reference):
///   - Rejects strings shorter than 7 chars (minimum valid form is "H:MM:SS")
///   - Rejects a fourth colon-separated token ("1:2:3:4" → None)
///   - Rejects non-zero-padded minutes or seconds ("8:5:0" → None;
///     only "08:05:00" is accepted)
fn parse_gtfs_time(s: &str) -> Option<u32> {
    let s = s.trim();
    if s.len() < 7 { return None; }          // minimum "H:MM:SS"
    let mut parts = s.split(':');
    let h   = parts.next()?;
    let m   = parts.next()?;
    let sec = parts.next()?;
    if parts.next().is_some() { return None; }           // rejects "1:2:3:4"
    if m.len() != 2 || sec.len() != 2 { return None; }  // rejects "1:2:3"
    parse_gtfs_time_impl(h, m, sec)
}

/// Resolve the departure/arrival time for one stop_times.txt row.
///
/// Prefers `departure_time`; falls back to `arrival_time` per GTFS spec
/// (both columns are optional, but at least one must be present per feed).
///
/// Extracted to eliminate the identical 4-line dep/arr chain that previously
/// appeared in both `parse_stop_times` and `parse_stop_sequences`.
#[inline]
fn resolve_stop_time(
    rec:     &csv::StringRecord,
    dep_idx: Option<usize>,
    arr_idx: Option<usize>,
) -> Option<u32> {
    dep_idx
        .and_then(|i| rec.get(i))
        .and_then(parse_gtfs_time)
        .or_else(|| arr_idx.and_then(|i| rec.get(i)).and_then(parse_gtfs_time))
}

/// Build trip_id → (first_departure_secs, last_arrival_secs) from stop_times.txt.
/// These values are seconds-from-midnight on the service day and can exceed
/// 86400 for overnight/multi-day trains (GTFS spec allows e.g. "25:10:00").
fn parse_stop_times(data: &[u8]) -> Result<HashMap<String, (u32, u32)>, String> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(data);

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
        let record = result.map_err(|e| e.to_string())?;
        let trip_id = match record.get(trip_id_idx) {
            Some(s) if !s.trim().is_empty() => s.trim().to_string(),
            _ => continue,
        };

        // Prefer departure_time; fall back to arrival_time.
        let Some(t) = resolve_stop_time(&record, dep_idx, arr_idx) else { continue };

        map.entry(trip_id)
            .and_modify(|(first, last)| {
                if t < *first { *first = t; }
                if t > *last  { *last  = t; }
            })
            .or_insert((t, t));
    }

    Ok(map)
}

/// Parse stop_times.txt → trip_id → TripStopSequence.
///
/// Extended counterpart to `parse_stop_times`: stores the full ordered stop
/// sequence (lat, lon, departure_secs) needed to build shape interpolation
/// timelines. `stop_latlon` must already be populated (stops.txt loaded first).
fn parse_stop_sequences(
    data:        &[u8],
    stop_latlon: &HashMap<String, (f64, f64)>,
) -> Result<HashMap<String, TripStopSequence>, String> {
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(data);
    let headers = rdr.headers().map_err(|e| e.to_string())?.clone();

    let trip_id_idx = csv_col(&headers, "trip_id")?;
    let stop_id_idx = csv_col(&headers, "stop_id")?;
    let arr_idx = headers.iter().position(|h| h == "arrival_time");
    let dep_idx = headers.iter().position(|h| h == "departure_time");
    let seq_idx = headers.iter().position(|h| h == "stop_sequence");

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
        let seq: u32 = seq_idx
            .and_then(|i| rec.get(i))
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0);
        // Prefer departure_time; fall back to arrival_time.
        let Some(time) = resolve_stop_time(&rec, dep_idx, arr_idx) else { continue };
        rows.push(RawRow { trip_id, seq, stop_id, time });
    }

    // Sort defensively: spec requires (trip_id, stop_sequence) order, but we
    // sort to be safe against feeds that don't guarantee it.
    rows.sort_by(|a, b| a.trip_id.cmp(&b.trip_id).then(a.seq.cmp(&b.seq)));

    let mut map: HashMap<String, TripStopSequence> = HashMap::new();
    for row in &rows {
        let (lat, lon) = stop_latlon.get(&row.stop_id).copied().unwrap_or((0.0, 0.0));
        let entry = map.entry(row.trip_id.clone()).or_insert_with(|| TripStopSequence {
            stop_lats: Vec::new(),
            stop_lons: Vec::new(),
            dep_secs:  Vec::new(),
        });
        entry.stop_lats.push(lat);
        entry.stop_lons.push(lon);
        entry.dep_secs.push(row.time);
    }

    // Trips with < 2 stops can't form an interpolation segment.
    map.retain(|_, seq| seq.dep_secs.len() >= 2);
    Ok(map)
}

fn parse_routes(data: &[u8]) -> Result<HashMap<String, String>, String> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(data);

    let headers = rdr.headers().map_err(|e| e.to_string())?.clone();

    let route_id_idx    = headers.iter().position(|h| h == "route_id")
        .ok_or("routes.txt: missing route_id column")?;
    let route_short_idx = headers.iter().position(|h| h == "route_short_name");
    let route_long_idx  = headers.iter().position(|h| h == "route_long_name")
        .ok_or("routes.txt: missing route_long_name column")?;

    let mut map = HashMap::new();
    for result in rdr.records() {
        let record   = result.map_err(|e| e.to_string())?;
        let route_id = record.get(route_id_idx).unwrap_or("").trim().to_string();
        let short    = route_short_idx.and_then(|i| record.get(i)).unwrap_or("").trim().to_string();
        let long     = record.get(route_long_idx).unwrap_or("").trim().to_string();
        let name     = if !short.is_empty() { short } else { long };
        if !route_id.is_empty() { map.insert(route_id, name); }
    }
    Ok(map)
}

/// Parse trips.txt → trip_id → (train_number, route_id).
///
/// Also returns a separate map of trip_id → shape_id for interpolation.
/// The shape_id map is populated alongside the main trips map so we only
/// iterate the CSV once.
fn parse_trips(data: &[u8]) -> Result<(HashMap<String, (String, String)>, HashMap<String, String>), String> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(data);

    let headers = rdr.headers().map_err(|e| e.to_string())?.clone();

    let trip_id_idx    = headers.iter().position(|h| h == "trip_id")
        .ok_or("trips.txt: missing trip_id column")?;
    let route_id_idx   = headers.iter().position(|h| h == "route_id")
        .ok_or("trips.txt: missing route_id column")?;
    let short_name_idx = headers.iter().position(|h| h == "trip_short_name");
    let shape_id_idx   = headers.iter().position(|h| h == "shape_id");

    let mut trips_map:     HashMap<String, (String, String)> = HashMap::new();
    let mut shape_id_map:  HashMap<String, String>           = HashMap::new();

    for result in rdr.records() {
        let record   = result.map_err(|e| e.to_string())?;
        let trip_id  = record.get(trip_id_idx).unwrap_or("").trim().to_string();
        let route_id = record.get(route_id_idx).unwrap_or("").trim().to_string();
        let shape_id = shape_id_idx
            .and_then(|i| record.get(i))
            .unwrap_or("").trim().to_string();

        // Fallback: use trip_id itself as train number only if trip_short_name is absent.
        let train_num = short_name_idx
            .and_then(|i| record.get(i))
            .unwrap_or("").trim().to_string();
        let train_num = if train_num.is_empty() { trip_id.clone() } else { train_num };

        if !trip_id.is_empty() {
            // Primary key: trip_id — handles bare-integer RT feeds.
            trips_map.insert(trip_id.clone(), (train_num.clone(), route_id.clone()));

            // Secondary key: trip_short_name — handles "_AMTK_{short_name}" RT feeds.
            // Skip if identical to trip_id (Gold Runner: trip_id == trip_short_name).
            if !train_num.is_empty() && train_num != trip_id {
                trips_map.entry(train_num.clone()).or_insert((train_num.clone(), route_id));
            }

            // Shape ID: only stored for the primary trip_id key (not the secondary
            // train number alias) to avoid shape_id lookup ambiguity.
            if !shape_id.is_empty() {
                shape_id_map.insert(trip_id, shape_id);
            }
        }
    }
    Ok((trips_map, shape_id_map))
}

/// Parse shapes.txt → shape_id → ordered Vec<ShapePoint>.
///
/// Handles both Amtrak (shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence)
/// and GRGTFS (shape_id,shape_pt_sequence,shape_pt_lat,shape_pt_lon,…) column
/// orderings. Rows are sorted by sequence number so unordered input is correct.
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

    // Stable sort: preserves relative order within already-sorted shapes.
    raw.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    let mut map: HashMap<String, Vec<ShapePoint>> = HashMap::new();
    for (shape_id, _seq, lat, lon) in raw {
        map.entry(shape_id).or_default().push(ShapePoint { lat, lon });
    }
    Ok(map)
}

/// Parse stops.txt → stop_id → (lat, lon).
/// Used to resolve stop positions when building shape interpolation timelines.
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

/// Find a CSV column index by name; returns a descriptive Err if absent.
fn csv_col(headers: &csv::StringRecord, name: &str) -> Result<usize, String> {
    headers.iter().position(|h| h.trim() == name)
        .ok_or_else(|| format!("missing column '{}'", name))
}

// ── FFI result types ──────────────────────────────────────────────────────────

/// Returned by `gtfs_static_lookup`. Both pointers may be null if not found.
/// Caller must pass to `gtfs_static_free_result` when done.
#[repr(C)]
pub struct GTFSStaticResult {
    pub train_number: *const c_char,  // e.g. "168"               (null if not found)
    pub route_name:   *const c_char,  // e.g. "Northeast Regional" (null if not found)
}

/// One HTTP range request Swift needs to make.
#[repr(C)]
pub struct GTFSZipRange {
    pub filename:    *const c_char,
    pub byte_offset: u64,
    pub byte_length: u64,
}

/// Interpolated position returned by `gtfs_interpolate_position`.
/// Returned by value on the stack — no heap allocation, no free needed.
#[repr(C)]
pub struct InterpolatedPosition {
    pub lat:      f64,
    pub lon:      f64,
    /// 1 = valid interpolated position.
    /// 0 = shape data unavailable; caller should use the raw GPS fix.
    pub is_valid: i32,
}

// ── FFI functions ─────────────────────────────────────────────────────────────

/// Feed the tail bytes of the ZIP (last ~256 KB).
/// On success returns a heap-allocated array of `GTFSZipRange` and sets
/// `*out_count`. Pass the array to `gtfs_static_free_ranges` when done.
/// Returns null on error.
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
                        filename:    cs.into_raw(),
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

/// Feed the raw bytes for one GTFS file (slice starting at the local ZIP header).
///
/// Accepted filenames: "routes.txt", "trips.txt", "stop_times.txt",
///                     "stops.txt", "shapes.txt"
///
/// Feed order constraint: stops.txt MUST be fed before stop_times.txt.
///
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
            Ok(s)  => s.to_string(),
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
                // Use or_insert so the first feed loaded wins on route_id collisions.
                // GR route_ids (small integers: 1, 3, 6 …) share the same namespace
                // as Amtrak route_ids in this table. Amtrak is authoritative for its
                // own routes; GR routes not already in the table are inserted normally.
                for (k, v) in map {
                    s.route_names.entry(k).or_insert(v);
                }
                0
            }
            Err(e) => { eprintln!("parse_routes: {}", e); -1 }
        },

        "trips.txt" => match parse_trips(&decompressed) {
            Ok((trips_map, shape_id_map)) => {
                // Use or_insert so the first feed loaded (Amtrak) always wins when
                // the same key appears in a later feed (Gold Runner).
                //
                // parse_trips inserts TWO keys per trip:
                //   • trip_id          (primary, globally unique)
                //   • trip_short_name  (secondary, shared across service days)
                //
                // The secondary "train number" key is where collisions occur: GR's
                // trip_id space (701–6619) overlaps with Amtrak Thruway Connecting
                // Service train numbers (3602–6619). Without this guard, loading GR
                // second silently reassigns those 78 train-number keys to Gold Runner
                // routes, so every Amtrak Thruway vehicle in that range would display
                // the wrong route name ("GR Route 1" instead of "Amtrak Thruway…").
                //
                // GR keys not already in the table (e.g. GR-only trains 701–3601)
                // are still inserted correctly; only Amtrak-claimed keys are protected.
                for (k, v) in trips_map {
                    s.trips.entry(k).or_insert(v);
                }
                // Shape IDs use or_insert for the same collision-avoidance reason.
                for (k, v) in shape_id_map {
                    s.trip_shape_ids.entry(k).or_insert(v);
                }
                0
            }
            Err(e) => { eprintln!("parse_trips: {}", e); -1 }
        },

        "stops.txt" => match parse_stops_latlon(&decompressed) {
            Ok(map) => {
                s.stop_latlon.extend(map);
                0
            }
            Err(e) => { eprintln!("parse_stops_latlon: {}", e); -1 }
        },

        "stop_times.txt" => {
            // Build trip_windows (first/last departure) — existing behavior.
            match parse_stop_times(&decompressed) {
                Ok(map) => {
                    // stop_times are keyed by the static trip_id, which is unique
                    // across both feeds. No collision risk; extend is safe.
                    s.trip_windows.extend(map);
                }
                Err(e) => { eprintln!("parse_stop_times: {}", e); return -1; }
            }
            // Build full stop sequences for shape interpolation.
            // Requires stop_latlon to be populated (stops.txt must be fed first).
            match parse_stop_sequences(&decompressed, &s.stop_latlon) {
                Ok(map) => { s.trip_stop_seqs.extend(map); 0 }
                Err(e) => { eprintln!("parse_stop_sequences: {}", e); -1 }
            }
        },

        "shapes.txt" => match parse_shapes(&decompressed) {
            Ok(map) => { s.shape_points.extend(map); 0 }
            Err(e) => { eprintln!("parse_shapes: {}", e); -1 }
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
        route_name:   std::ptr::null(),
    });

    if trip_id.is_null() {
        return Box::into_raw(empty);
    }

    let tid = match unsafe { CStr::from_ptr(trip_id).to_str() } {
        Ok(s)  => s,
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
    if result.is_null() { return; }
    unsafe {
        let r = &*result;
        if !r.train_number.is_null() { let _ = CString::from_raw(r.train_number as *mut c_char); }
        if !r.route_name.is_null()   { let _ = CString::from_raw(r.route_name   as *mut c_char); }
        let _ = Box::from_raw(result);
    }
}

/// Free the range array returned by `gtfs_static_feed_eocd`.
#[no_mangle]
pub extern "C" fn gtfs_static_free_ranges(ranges: *mut GTFSZipRange, count: usize) {
    if ranges.is_null() || count == 0 { return; }
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

/// Returns 1 if routes, trips, and stop_times tables are all loaded, 0 otherwise.
#[no_mangle]
pub extern "C" fn gtfs_static_is_loaded() -> i32 {
    let s = store().lock().unwrap();
    if !s.route_names.is_empty() && !s.trips.is_empty() && !s.trip_windows.is_empty() { 1 } else { 0 }
}

/// Returns 1 if `trip_id` is scheduled to be active at `now_eastern`.
/// Returns 0 if pre-departure, completed, or not found in static data.
/// Returns 1 (pass-through) if stop_times haven't loaded yet.
///
/// `now_eastern` must be a Unix timestamp already adjusted to Eastern Time:
///   now_eastern = now_unix + TimeZone("America/New_York").secondsFromGMT(now)
#[no_mangle]
pub extern "C" fn gtfs_static_is_trip_active(
    trip_id: *const c_char,
    now_eastern: i64,
) -> i32 {
    if trip_id.is_null() { return 0; }
    let tid = match unsafe { CStr::from_ptr(trip_id).to_str() } {
        Ok(s)  => s,
        Err(_) => return 0,
    };
    let s = store().lock().unwrap();
    if s.trip_windows.is_empty() { return 1; }
    if s.is_trip_active(tid, now_eastern) { 1 } else { 0 }
}

/// Compute a smooth interpolated position for `trip_id` at `now_eastern`.
///
/// `now_eastern` is `now_unix + TimeZone("America/New_York").secondsFromGMT(now)`.
///
/// Returns `is_valid = 1` on success. Returns `is_valid = 0` if shape data is
/// not yet loaded or this trip has no associated shape (e.g. Thruway bus service,
/// ~40% of Amtrak trips). When `is_valid = 0`, use the raw GPS lat/lon from the
/// GTFS-RT feed instead.
///
/// Returned by value on the stack — no free call needed.
#[no_mangle]
pub extern "C" fn gtfs_interpolate_position(
    trip_id:     *const c_char,
    now_eastern: i64,
) -> InterpolatedPosition {
    let null_result = InterpolatedPosition { lat: 0.0, lon: 0.0, is_valid: 0 };
    if trip_id.is_null() { return null_result; }

    let tid = match unsafe { CStr::from_ptr(trip_id).to_str() } {
        Ok(s)  => s,
        Err(_) => return null_result,
    };

    let mut s = store().lock().unwrap();
    match s.interpolate_position(tid, now_eastern) {
        Some((lat, lon)) => InterpolatedPosition { lat, lon, is_valid: 1 },
        None             => null_result,
    }
}

/// Returns 1 when shape interpolation data is fully loaded and
/// `gtfs_interpolate_position` can return meaningful results.
/// Returns 0 while shapes.txt, stops.txt, or stop_times.txt are still loading.
///
/// Swift should check this before deciding whether to use interpolated positions
/// or fall back to the raw GPS lat/lon from the GTFS-RT feed.
#[no_mangle]
pub extern "C" fn gtfs_interpolation_is_ready() -> i32 {
    let s = store().lock().unwrap();
    if !s.shape_points.is_empty()
        && !s.stop_latlon.is_empty()
        && !s.trip_stop_seqs.is_empty()
    { 1 } else { 0 }
}

/// Evict all loaded data (e.g. before a refresh).
#[no_mangle]
pub extern "C" fn gtfs_static_reset() {
    let mut s = store().lock().unwrap();
    s.route_names.clear();
    s.trips.clear();
    s.trip_windows.clear();
    s.cd_entries.clear();
    s.shape_points.clear();
    s.stop_latlon.clear();
    s.trip_shape_ids.clear();
    s.trip_stop_seqs.clear();
    s.shape_timelines.clear();
}
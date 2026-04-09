/// GTFS Static Lookup + Shape Interpolation  —  Multi-store edition
///
/// # Multi-store design
///
/// Each GTFS static feed gets its own `GtfsStaticStore` identified by a
/// `u32` store_id.  Store IDs are handed out by `gtfs_static_store_new()` and
/// released by `gtfs_static_store_free()`.
///
/// # Backward compatibility
///
/// The original singleton functions (`gtfs_static_feed_eocd`,
/// `gtfs_static_feed_file`, `gtfs_static_lookup`, …) are preserved unchanged.
/// They route to a hidden **legacy store** (`LEGACY_STORE_ID = 1`).
///
/// # Per-store trip-id strategy
///
/// GTFS feeds encode the operator-visible run number in different fields
/// depending on the agency.  Each store carries a `TripIdStrategy` that
/// controls how `parse_trips` and `lookup` extract a display label:
///
/// | Strategy              | GTFS source                                      |
/// |-----------------------|--------------------------------------------------|
/// | `TripShortName`       | `trip_short_name` field (or `trip_id` if absent) |
/// | `TripIdNumericSuffix` | digits after leading alpha prefix in `trip_id`   |
/// | `RouteShortName`      | `route_short_name` field                         |
/// | `TripIdVerbatim`      | `trip_id` as-is                                  |
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
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use flate2::read::DeflateDecoder;

use crate::geo::haversine_m;

// ── Store ID for the legacy singleton ────────────────────────────────────────

const LEGACY_STORE_ID: u32 = 1;

// ── Trip-ID extraction strategy ──────────────────────────────────────────────

/// Controls how a store maps a `trips.txt` row to a human-readable run number
/// for display in the UI.
///
/// GTFS feeds encode the operator-facing run/trip number in different fields
/// depending on the agency.  This enum selects the extraction rule that best
/// matches the feed being loaded.  Set via `gtfs_static_store_set_strategy()`
/// **before** feeding any files.  Defaults to `TripShortName`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TripIdStrategy {
    /// Prefer `trip_short_name`; fall back to `trip_id` verbatim.
    ///
    /// Use this when `trip_short_name` is the operator-visible run number
    /// (e.g. "168", "704").  This is the most common case and the GTFS spec's
    /// intended purpose for that field.
    TripShortName = 0,

    /// Extract the leading numeric suffix from the first underscore-delimited
    /// token of `trip_id`.
    ///
    /// Some feeds embed the run number inside `trip_id` using the pattern
    /// `{LINE_CODE}{RUN}_{DATE}_{OTHER}`, e.g. `CYN1052_20260201_SID185189`.
    /// This strategy splits on `_`, takes the first token (`CYN1052`), strips
    /// any leading alphabetic prefix (`CYN`), and returns the remaining digits
    /// (`1052`) as the run number.  Falls back to the full first token when no
    /// digits are found.
    TripIdNumericSuffix = 1,

    /// Use `route_short_name` as the display label; `trip_id` is opaque.
    ///
    /// Use this when `trip_short_name` is absent or meaningless and the most
    /// useful label for the operator is the route's short name (e.g. "R5",
    /// "Paoli/Thorndale").
    RouteShortName = 2,

    /// Expose `trip_id` verbatim as the run number with no transformation.
    ///
    /// Use this as a catch-all when none of the above strategies produce a
    /// meaningful display value.
    TripIdVerbatim = 3,
}

impl TripIdStrategy {
    fn from_i32(v: i32) -> Self {
        match v {
            1 => TripIdStrategy::TripIdNumericSuffix,
            2 => TripIdStrategy::RouteShortName,
            3 => TripIdStrategy::TripIdVerbatim,
            _ => TripIdStrategy::TripShortName,
        }
    }
}

// ── Registry ─────────────────────────────────────────────────────────────────

/// Individual-store handle: each store has its own RwLock so that reads on
/// already-loaded stores are never blocked by a write (CSV parse) on another
/// store.  The global `REGISTRY` mutex is held only long enough to clone an
/// `Arc` — never across any CSV parsing or disk I/O.
type StoreHandle = Arc<RwLock<GtfsStaticStore>>;

struct Registry {
    stores:  HashMap<u32, StoreHandle>,
    next_id: u32,
}

impl Registry {
    fn new() -> Self {
        let mut r = Registry { stores: HashMap::new(), next_id: 2 }; // 1 = legacy
        r.stores.insert(
            LEGACY_STORE_ID,
            Arc::new(RwLock::new(GtfsStaticStore::new(TripIdStrategy::TripShortName))),
        );
        r
    }

    fn open(&mut self) -> u32 {
        // Advance past any IDs already occupied (handles wrapping collisions
        // when many stores are opened and closed over the lifetime of the app).
        let start = self.next_id;
        loop {
            if !self.stores.contains_key(&self.next_id) { break; }
            self.next_id = self.next_id.wrapping_add(1).max(2);
            if self.next_id == start {
                // All u32 IDs ≥ 2 are occupied — this should never happen in
                // practice, but panic rather than loop forever.
                panic!("gtfs_static: store ID space exhausted");
            }
        }
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(2);
        self.stores.insert(
            id,
            Arc::new(RwLock::new(GtfsStaticStore::new(TripIdStrategy::TripShortName))),
        );
        id
    }

    fn close(&mut self, id: u32) {
        if id != LEGACY_STORE_ID {
            self.stores.remove(&id);
        }
    }
}

static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();

fn registry() -> &'static Mutex<Registry> {
    REGISTRY.get_or_init(|| Mutex::new(Registry::new()))
}

/// Acquire the global registry mutex *briefly* to clone the `Arc` for `id`,
/// then release it immediately.  All per-store locking is done on the returned
/// handle, keeping the registry lock out of the hot path.
#[inline]
fn get_store(id: u32) -> Option<StoreHandle> {
    let reg = registry().lock().unwrap_or_else(|e| e.into_inner());
    reg.stores.get(&id).cloned()
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
    // ── Service-day support ───────────────────────────────────────────────────
    /// trip_id → service_id (from trips.txt)
    trip_service_ids:  HashMap<String, String>,
    /// trip_id → direction_id (0 or 1; u8::MAX = absent)
    trip_direction_ids: HashMap<String, u8>,
    /// trip_id → trip_headsign
    trip_headsigns:    HashMap<String, String>,
    /// trip_id → block_id (for through-service / in-seat transfers)
    trip_block_ids:    HashMap<String, String>,
    /// service_id → CalendarRecord (from calendar.txt)
    calendar:          HashMap<String, CalendarRecord>,
    /// (service_id, date_yyyymmdd) → exception_type (1=added, 2=removed)
    calendar_dates:    HashMap<(String, u32), u8>,
    /// (trip_id, stop_sequence) → (pickup_type, dropoff_type) for non-revenue stops
    stop_pickup_types: HashMap<(String, u32), (u8, u8)>,
}

/// One row from calendar.txt.
#[derive(Debug, Clone)]
struct CalendarRecord {
    monday:    bool,
    tuesday:   bool,
    wednesday: bool,
    thursday:  bool,
    friday:    bool,
    saturday:  bool,
    sunday:    bool,
    start_date: u32, // YYYYMMDD
    end_date:   u32, // YYYYMMDD
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
            route_names:        HashMap::new(),
            route_short_names:  HashMap::new(),
            trips:              HashMap::new(),
            trip_windows:       HashMap::new(),
            cd_entries:         HashMap::new(),
            shape_points:       HashMap::new(),
            stop_latlon:        HashMap::new(),
            trip_shape_ids:     HashMap::new(),
            trip_stop_seqs:     HashMap::new(),
            shape_timelines:    HashMap::new(),
            trip_service_ids:   HashMap::new(),
            trip_direction_ids: HashMap::new(),
            trip_headsigns:     HashMap::new(),
            trip_block_ids:     HashMap::new(),
            calendar:           HashMap::new(),
            calendar_dates:     HashMap::new(),
            stop_pickup_types:  HashMap::new(),
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

    fn is_trip_active(&self, trip_id: &str, now_local: i64) -> bool {
        // Resolve RT-mangled trip_id: some feeds prefix the static trip_id
        // with date or agency tokens separated by underscores.  If the full
        // trip_id is not in the table, try the last underscore-delimited token.
        let canonical = if self.trip_windows.contains_key(trip_id) {
            trip_id.to_string()
        } else if trip_id.contains('_') {
            trip_id.split('_').last().unwrap_or(trip_id).to_string()
        } else {
            trip_id.to_string()
        };

        // ── Step 1: time-window gate (fast pre-filter) ────────────────────────
        // Reject trips whose entire scheduled span (plus a generous delay
        // buffer) cannot possibly contain now_local.
        //
        // We only look back 1 calendar day (enough for overnight trips that
        // started before midnight) — a 3-day lookback was too broad and could
        // match trips from days ago within their scheduled window.
        const DELAY_BUFFER_SECS: u32 = 2 * 60 * 60; // 2 hours late tolerance
        const SECS_PER_DAY:      i64 = 86_400;

        let in_time_window = if let Some(&(first_dep, last_arr)) = self.trip_windows.get(canonical.as_str()) {
            let window_end = last_arr.saturating_add(DELAY_BUFFER_SECS);
            let mut found = false;
            for days_ago in 0i64..=1 {
                let midnight  = (now_local / SECS_PER_DAY - days_ago) * SECS_PER_DAY;
                let abs_start = midnight + first_dep as i64;
                let abs_end   = midnight + window_end as i64;
                if now_local >= abs_start && now_local <= abs_end {
                    found = true;
                    break;
                }
            }
            found
        } else {
            // No time window loaded → pass-through (data not yet fully loaded).
            return true;
        };

        if !in_time_window {
            return false;
        }

        // ── Step 2: service-day check via calendar.txt / calendar_dates.txt ───
        // If calendar data is not loaded yet, fall through to true so we don't
        // suppress real-time vehicles while the static feed is still loading.
        if self.calendar.is_empty() && self.calendar_dates.is_empty() {
            return true;
        }

        let service_id = match self.trip_service_ids.get(canonical.as_str())
            .or_else(|| self.trip_service_ids.get(trip_id))
        {
            Some(s) => s,
            None    => return true, // unknown trip → pass-through
        };

        // Determine the "service date" — trips running after midnight on GTFS
        // extended times still belong to the previous service date.  We use
        // the first_dep to decide: if (now_local % 86400) < first_dep the
        // trip started on the previous calendar day.
        let first_dep_secs = self.trip_windows.get(canonical.as_str()).map(|w| w.0).unwrap_or(0);
        let now_sod = now_local.rem_euclid(SECS_PER_DAY) as u32; // seconds since midnight (local)
        // If the vehicle is past midnight (now_sod < first_dep after 24h wrap),
        // the service date was yesterday.
        let days_offset: i64 = if first_dep_secs >= 86400 && now_sod < first_dep_secs % 86400 {
            -1 // still on yesterday's service date
        } else {
            0
        };
        let service_date_midnight = (now_local / SECS_PER_DAY + days_offset) * SECS_PER_DAY;
        let service_date_yyyymmdd = local_midnight_to_yyyymmdd(service_date_midnight);

        // calendar_dates.txt exceptions override calendar.txt rules.
        let key = (service_id.clone(), service_date_yyyymmdd);
        match self.calendar_dates.get(&key) {
            Some(&1) => return true,  // exception_type=1: service ADDED on this date
            Some(&2) => return false, // exception_type=2: service REMOVED on this date
            _        => {}
        }

        // calendar.txt: check day-of-week and date range.
        if let Some(cal) = self.calendar.get(service_id.as_str()) {
            if service_date_yyyymmdd < cal.start_date || service_date_yyyymmdd > cal.end_date {
                return false;
            }
            // Day-of-week check using Tomohiko Sakamoto's algorithm (no std::time needed).
            let dow = day_of_week_from_yyyymmdd(service_date_yyyymmdd);
            return match dow {
                0 => cal.sunday,
                1 => cal.monday,
                2 => cal.tuesday,
                3 => cal.wednesday,
                4 => cal.thursday,
                5 => cal.friday,
                6 => cal.saturday,
                _ => false,
            };
        }

        // service_id not found in calendar.txt — if it had no calendar_dates
        // entry either, we have no data.  Pass-through to avoid hiding trains.
        true
    }

    fn interpolate_position(&self, trip_id: &str, now_local: i64) -> Option<(f64, f64)> {
        let canonical = self.resolve_trip_id_for_shape(trip_id)?;
        // Timelines are built eagerly in build_timelines_eager; no mutation needed here.
        let timeline = self.shape_timelines.get(canonical.as_str())?;

        // GTFS stop_times allow times ≥ 24:00:00 (extended times for
        // trips that run past midnight).  The timeline's time_at_point values
        // are raw seconds-since-schedule-midnight and can exceed 86 400.
        //
        // Strategy: compute seconds-since-midnight and try the current service
        // day.  If that fails, try +86400 (the trip started before midnight on
        // the previous schedule day) and +172800 (two days — uncommon but
        // technically valid GTFS).  Stop at the first hit.
        let since_midnight = now_local.rem_euclid(86_400);
        query_position(timeline, since_midnight)
            .or_else(|| query_position(timeline, since_midnight + 86_400))
            .or_else(|| query_position(timeline, since_midnight + 172_800))
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

// ── Calendar helper functions ─────────────────────────────────────────────────

/// Convert a local-midnight POSIX timestamp to a YYYYMMDD integer.
///
/// `midnight_local` must equal `unix_ts + tz_offset_seconds` so that
/// integer division by 86 400 yields the correct local calendar date.
/// No timezone logic is performed here — this is pure Gregorian arithmetic.
fn local_midnight_to_yyyymmdd(midnight_local: i64) -> u32 {
    // Clamp to a plausible GTFS date range before dividing, preventing the
    // i64→i32 truncation from silently wrapping on adversarial timestamps.
    // MIN ≈ 1900-01-01 (-25 568 days), MAX ≈ 2270-01-01 (+109 938 days).
    const MIN_MIDNIGHT: i64 = -25_568 * 86_400;
    const MAX_MIDNIGHT: i64 =  109_938 * 86_400;
    let midnight_local = midnight_local.clamp(MIN_MIDNIGHT, MAX_MIDNIGHT);
    // Days since Unix epoch (1970-01-01).
    let days = (midnight_local / 86_400) as i32;
    // Gregorian calendar conversion (proleptic, works for 1970-2100).
    let z     = days + 719_468;
    let era   = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe   = z - era * 146_097;                     // day of era [0, 146096]
    let yoe   = (doe - doe/1460 + doe/36524 - doe/146096) / 365; // year of era [0, 399]
    let y     = yoe + era * 400;
    let doy   = doe - (365*yoe + yoe/4 - yoe/100);    // day of year [0, 365]
    let mp    = (5*doy + 2)/153;                       // month prime [0, 11]
    let d     = doy - (153*mp+2)/5 + 1;               // day [1, 31]
    let m     = if mp < 10 { mp + 3 } else { mp - 9 }; // month [1, 12]
    let y_adj = if m <= 2 { y + 1 } else { y };
    (y_adj * 10_000 + m * 100 + d) as u32
}

/// Return the day of week for a YYYYMMDD date (0=Sunday … 6=Saturday).
/// Uses Tomohiko Sakamoto's algorithm.
fn day_of_week_from_yyyymmdd(yyyymmdd: u32) -> u8 {
    let y0 = (yyyymmdd / 10_000) as i32;
    let m0 = ((yyyymmdd / 100) % 100) as i32;
    let d  = (yyyymmdd % 100) as i32;
    static T: [i32; 12] = [0,3,2,5,0,3,5,1,4,6,2,4];
    // Guard against invalid YYYYMMDD (month 0 or >12) — T[(m0-1)] would be OOB.
    // In normal operation m0 is always 1-12 (from local_midnight_to_yyyymmdd),
    // but defence-in-depth prevents UB if ever called with raw user data.
    if m0 < 1 || m0 > 12 { return 0; }
    let y = if m0 < 3 { y0 - 1 } else { y0 };
    ((y + y/4 - y/100 + y/400 + T[(m0-1) as usize] + d) % 7) as u8
}

/// Parse a YYYYMMDD string into a u32 integer.  Returns 0 on failure.
fn parse_yyyymmdd(s: &str) -> u32 {
    let s = s.trim();
    if s.len() != 8 { return 0; }
    s.parse::<u32>().unwrap_or(0)
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

/// Build shape-interpolation timelines for every trip that has both a shape
/// and a stop-time sequence in `store`.  Called eagerly after each file load
/// so `interpolate_position` never needs to mutate the store at query time.
fn build_timelines_eager(store: &mut GtfsStaticStore) {
    // Collect (trip_id, timeline) pairs under immutable borrows first,
    // then extend shape_timelines to avoid conflicting borrow with the iterator.
    let built: Vec<(String, TripShapeTimeline)> = store
        .trip_stop_seqs
        .iter()
        .filter_map(|(trip_id, seq)| {
            if store.shape_timelines.contains_key(trip_id.as_str()) { return None; }
            let shape_id = store.trip_shape_ids.get(trip_id.as_str())?;
            let shape    = store.shape_points.get(shape_id)?;
            Some((trip_id.clone(), build_timeline(shape, seq)))
        })
        .collect();
    store.shape_timelines.extend(built);
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
    let cd_offset_in_buf   = cd_offset.checked_sub(tail_start_in_file)
        .ok_or("EOCD: Central Directory offset underflow — malformed ZIP")? as usize;
    let cd_end = cd_offset_in_buf.checked_add(cd_size as usize)
        .ok_or("EOCD: Central Directory offset+size overflow — malformed ZIP")?;

    if cd_end > data.len() {
        return Err(format!(
            "Central Directory not in tail buffer: need offset {} size {} but have {}",
            cd_offset_in_buf, cd_size, data.len()
        ));
    }

    let cd_data = &data[cd_offset_in_buf..cd_end];
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
        pos = match pos.checked_add(fname_len)
            .and_then(|p| p.checked_add(extra_len))
            .and_then(|p| p.checked_add(comment_len)) {
            Some(p) => p,
            None    => break,
        };
        entries.insert(fname, CdEntry { local_header_offset, compressed_size, uncompressed_size, method });
    }

    let targets = ["trips.txt", "routes.txt", "stop_times.txt", "stops.txt", "shapes.txt",
        "calendar.txt", "calendar_dates.txt"];
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
    // GTFS allows extended times like 25:30:00 for post-midnight trips, but
    // clamp hh to defend against malformed feeds: hh > ~1_193_046 overflows
    // u32 when multiplied by 3600.  Values above 99 are already non-physical.
    if hh > 99 || mm > 59 || ss > 59 { return None; }
    Some(hh * 3600 + mm * 60 + ss)
}

#[inline]
fn resolve_stop_time(rec: &csv::StringRecord, dep_idx: Option<usize>, arr_idx: Option<usize>) -> Option<u32> {
    dep_idx.and_then(|i| rec.get(i)).and_then(parse_gtfs_time)
        .or_else(|| arr_idx.and_then(|i| rec.get(i)).and_then(parse_gtfs_time))
}

/// Parse stop_times.txt in a **single CSV pass** and produce all three derived maps:
/// - `trip_windows`: first/last departure second per trip (for `is_trip_active` pre-filter)
/// - `trip_stop_seqs`: lat/lon/time sequence per trip (for shape interpolation)
/// - `stop_pickup_types`: non-regular (non-zero) pickup/dropoff entries only
fn parse_stop_times_all(
    data:        &[u8],
    stop_latlon: &HashMap<String, (f64, f64)>,
) -> Result<(
    HashMap<String, (u32, u32)>,
    HashMap<String, TripStopSequence>,
    HashMap<(String, u32), (u8, u8)>,
), String> {
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(data);
    let headers = rdr.headers().map_err(|e| e.to_string())?.clone();

    let trip_id_idx = csv_col(&headers, "trip_id")?;
    let stop_id_idx = csv_col(&headers, "stop_id")?;
    let arr_idx     = headers.iter().position(|h| h == "arrival_time");
    let dep_idx     = headers.iter().position(|h| h == "departure_time");
    let seq_idx     = headers.iter().position(|h| h == "stop_sequence");
    let pickup_idx  = headers.iter().position(|h| h == "pickup_type");
    let dropoff_idx = headers.iter().position(|h| h == "drop_off_type");

    if arr_idx.is_none() && dep_idx.is_none() {
        return Err("stop_times.txt: missing both arrival_time and departure_time".into());
    }

    struct RawRow { trip_id: String, seq: u32, stop_id: String, time: u32, pickup: u8, dropoff: u8 }
    let mut rows: Vec<RawRow> = Vec::new();
    let mut windows: HashMap<String, (u32, u32)> = HashMap::new();

    for result in rdr.records() {
        let rec = result.map_err(|e| e.to_string())?;
        let trip_id = match rec.get(trip_id_idx) {
            Some(s) if !s.trim().is_empty() => s.trim().to_string(),
            _ => continue,
        };
        let Some(time) = resolve_stop_time(&rec, dep_idx, arr_idx) else { continue };
        let stop_id = rec.get(stop_id_idx).unwrap_or("").trim().to_string();
        let seq:    u32 = parse_col_u32(seq_idx,    &rec);
        let pickup:  u8 = parse_col_u32(pickup_idx,  &rec) as u8;
        let dropoff: u8 = parse_col_u32(dropoff_idx, &rec) as u8;

        windows.entry(trip_id.clone())
            .and_modify(|(first, last)| {
                if time < *first { *first = time; } else if time > *last { *last = time; }
            })
            .or_insert((time, time));

        rows.push(RawRow { trip_id, seq, stop_id, time, pickup, dropoff });
    }

    rows.sort_by(|a, b| a.trip_id.cmp(&b.trip_id).then(a.seq.cmp(&b.seq)));

    let mut seqs:         HashMap<String, TripStopSequence>    = HashMap::new();
    let mut pickup_types: HashMap<(String, u32), (u8, u8)>    = HashMap::new();

    for row in &rows {
        let (lat, lon) = stop_latlon.get(&row.stop_id).copied().unwrap_or_else(|| {
            eprintln!("gtfs_static: stop_id '{}' in stop_times.txt not found in stops.txt; using (0,0)", row.stop_id);
            (0.0, 0.0)
        });
        let entry = seqs.entry(row.trip_id.clone()).or_insert_with(|| TripStopSequence {
            stop_lats: Vec::new(), stop_lons: Vec::new(), dep_secs: Vec::new(),
        });
        entry.stop_lats.push(lat);
        entry.stop_lons.push(lon);
        entry.dep_secs.push(row.time);

        if row.pickup != 0 || row.dropoff != 0 {
            pickup_types.insert((row.trip_id.clone(), row.seq), (row.pickup, row.dropoff));
        }
    }
    seqs.retain(|_, s| s.dep_secs.len() >= 2);

    Ok((windows, seqs, pickup_types))
}

/// Returns (display_names, short_names).
/// Per spec, either route_short_name or route_long_name must be present —
/// both are optional individually.  We accept feeds that provide only one.
fn parse_routes(data: &[u8]) -> Result<(HashMap<String, String>, HashMap<String, String>), String> {
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(data);
    let headers = rdr.headers().map_err(|e| e.to_string())?.clone();
    let route_id_idx    = headers.iter().position(|h| h == "route_id")
        .ok_or("routes.txt: missing route_id column")?;
    let route_short_idx = headers.iter().position(|h| h == "route_short_name");
    let route_long_idx  = headers.iter().position(|h| h == "route_long_name");

    if route_short_idx.is_none() && route_long_idx.is_none() {
        return Err("routes.txt: missing both route_short_name and route_long_name".into());
    }

    let mut display_names: HashMap<String, String> = HashMap::new();
    let mut short_names:   HashMap<String, String> = HashMap::new();

    for result in rdr.records() {
        let record   = result.map_err(|e| e.to_string())?;
        let route_id = record.get(route_id_idx).unwrap_or("").trim().to_string();
        if route_id.is_empty() { continue; }
        let short = route_short_idx.and_then(|i| record.get(i)).unwrap_or("").trim().to_string();
        let long  = route_long_idx.and_then(|i| record.get(i)).unwrap_or("").trim().to_string();
        // Display name: prefer short if non-empty, fall back to long.
        // Per best practices, route_long_name should not repeat route_short_name
        // but we just pick the most useful non-empty value for display.
        let display = if !short.is_empty() { short.clone() } else { long };
        if !display.is_empty() {
            display_names.insert(route_id.clone(), display);
        }
        if !short.is_empty() { short_names.insert(route_id, short); }
    }
    Ok((display_names, short_names))
}

/// Parse trips.txt, applying the store's strategy for train-number extraction.
/// Returns (trips_map, shape_id_map, service_id_map, direction_id_map, headsign_map, block_id_map).
fn parse_trips_with_strategy(
    data:              &[u8],
    strategy:          TripIdStrategy,
    route_short_names: &HashMap<String, String>,
) -> Result<(
    HashMap<String, (String, String)>, // trip_id → (train_num, route_id)
    HashMap<String, String>,           // trip_id → shape_id
    HashMap<String, String>,           // trip_id → service_id
    HashMap<String, u8>,               // trip_id → direction_id (u8::MAX = absent)
    HashMap<String, String>,           // trip_id → trip_headsign
    HashMap<String, String>,           // trip_id → block_id
), String> {
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(data);
    let headers = rdr.headers().map_err(|e| e.to_string())?.clone();

    let trip_id_idx    = headers.iter().position(|h| h == "trip_id")
        .ok_or("trips.txt: missing trip_id column")?;
    let route_id_idx   = headers.iter().position(|h| h == "route_id")
        .ok_or("trips.txt: missing route_id column")?;
    let service_id_idx = headers.iter().position(|h| h == "service_id")
        .ok_or("trips.txt: missing service_id column")?;
    let short_name_idx  = headers.iter().position(|h| h == "trip_short_name");
    let shape_id_idx    = headers.iter().position(|h| h == "shape_id");
    let direction_id_idx= headers.iter().position(|h| h == "direction_id");
    let headsign_idx    = headers.iter().position(|h| h == "trip_headsign");
    let block_id_idx    = headers.iter().position(|h| h == "block_id");

    let mut trips_map:      HashMap<String, (String, String)> = HashMap::new();
    let mut shape_id_map:   HashMap<String, String>           = HashMap::new();
    let mut service_id_map: HashMap<String, String>           = HashMap::new();
    let mut direction_map:  HashMap<String, u8>               = HashMap::new();
    let mut headsign_map:   HashMap<String, String>           = HashMap::new();
    let mut block_id_map:   HashMap<String, String>           = HashMap::new();

    for result in rdr.records() {
        let record     = result.map_err(|e| e.to_string())?;
        let trip_id    = record.get(trip_id_idx).unwrap_or("").trim().to_string();
        let route_id   = record.get(route_id_idx).unwrap_or("").trim().to_string();
        let service_id = record.get(service_id_idx).unwrap_or("").trim().to_string();
        let shape_id   = shape_id_idx.and_then(|i| record.get(i)).unwrap_or("").trim().to_string();
        let short_name = short_name_idx.and_then(|i| record.get(i)).unwrap_or("").trim().to_string();
        let headsign   = headsign_idx.and_then(|i| record.get(i)).unwrap_or("").trim().to_string();
        let block_id   = block_id_idx.and_then(|i| record.get(i)).unwrap_or("").trim().to_string();
        let direction_id: u8 = direction_id_idx
            .and_then(|i| record.get(i))
            .and_then(|s| s.trim().parse::<u8>().ok())
            .unwrap_or(u8::MAX);

        if trip_id.is_empty() { continue; }

        let (train_num, secondary_key) =
            extract_train_number(strategy, &trip_id, &short_name, &route_id, route_short_names);

        trips_map.insert(trip_id.clone(), (train_num.clone(), route_id.clone()));
        if let Some(sec) = secondary_key {
            trips_map.entry(sec).or_insert((train_num.clone(), route_id.clone()));
        }
        if !shape_id.is_empty() {
            shape_id_map.insert(trip_id.clone(), shape_id);
        }
        if !service_id.is_empty() {
            service_id_map.insert(trip_id.clone(), service_id);
        }
        if direction_id != u8::MAX {
            direction_map.insert(trip_id.clone(), direction_id);
        }
        if !headsign.is_empty() {
            headsign_map.insert(trip_id.clone(), headsign);
        }
        if !block_id.is_empty() {
            block_id_map.insert(trip_id.clone(), block_id);
        }
    }
    Ok((trips_map, shape_id_map, service_id_map, direction_map, headsign_map, block_id_map))
}

/// Parse calendar.txt → service_id → CalendarRecord.
fn parse_calendar(data: &[u8]) -> Result<HashMap<String, CalendarRecord>, String> {
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(data);
    let headers = rdr.headers().map_err(|e| e.to_string())?.clone();
    let sid_idx   = csv_col(&headers, "service_id")?;
    let mon_idx   = csv_col(&headers, "monday")?;
    let tue_idx   = csv_col(&headers, "tuesday")?;
    let wed_idx   = csv_col(&headers, "wednesday")?;
    let thu_idx   = csv_col(&headers, "thursday")?;
    let fri_idx   = csv_col(&headers, "friday")?;
    let sat_idx   = csv_col(&headers, "saturday")?;
    let sun_idx   = csv_col(&headers, "sunday")?;
    let start_idx = csv_col(&headers, "start_date")?;
    let end_idx   = csv_col(&headers, "end_date")?;

    let parse_bool = |s: &str| s.trim() == "1";

    let mut map = HashMap::new();
    for result in rdr.records() {
        let rec = result.map_err(|e| e.to_string())?;
        let sid = rec.get(sid_idx).unwrap_or("").trim().to_string();
        if sid.is_empty() { continue; }
        let start_date = parse_yyyymmdd(rec.get(start_idx).unwrap_or(""));
        let end_date   = parse_yyyymmdd(rec.get(end_idx).unwrap_or(""));
        if start_date == 0 || end_date == 0 { continue; }
        map.insert(sid, CalendarRecord {
            monday:    parse_bool(rec.get(mon_idx).unwrap_or("0")),
            tuesday:   parse_bool(rec.get(tue_idx).unwrap_or("0")),
            wednesday: parse_bool(rec.get(wed_idx).unwrap_or("0")),
            thursday:  parse_bool(rec.get(thu_idx).unwrap_or("0")),
            friday:    parse_bool(rec.get(fri_idx).unwrap_or("0")),
            saturday:  parse_bool(rec.get(sat_idx).unwrap_or("0")),
            sunday:    parse_bool(rec.get(sun_idx).unwrap_or("0")),
            start_date,
            end_date,
        });
    }
    Ok(map)
}

/// Parse calendar_dates.txt → (service_id, date_yyyymmdd) → exception_type.
/// exception_type: 1=service added, 2=service removed.
fn parse_calendar_dates(data: &[u8]) -> Result<HashMap<(String, u32), u8>, String> {
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(data);
    let headers = rdr.headers().map_err(|e| e.to_string())?.clone();
    let sid_idx  = csv_col(&headers, "service_id")?;
    let date_idx = csv_col(&headers, "date")?;
    let type_idx = csv_col(&headers, "exception_type")?;

    let mut map = HashMap::new();
    for result in rdr.records() {
        let rec  = result.map_err(|e| e.to_string())?;
        let sid  = rec.get(sid_idx).unwrap_or("").trim().to_string();
        let date = parse_yyyymmdd(rec.get(date_idx).unwrap_or(""));
        let etype: u8 = rec.get(type_idx).unwrap_or("0").trim().parse().unwrap_or(0);
        if sid.is_empty() || date == 0 || etype == 0 { continue; }
        map.insert((sid, date), etype);
    }
    Ok(map)
}

/// Returns `(train_number, secondary_key)`.
/// `secondary_key` is an extra alias inserted into the trips table so that
/// RT trip_ids with a prefixed format (e.g. `"{DATE}_{STATIC_ID}"`) can still
/// resolve via the underscore-split fallback in `lookup()`.
fn extract_train_number(
    strategy:          TripIdStrategy,
    trip_id:           &str,
    short_name:        &str,
    route_id:          &str,
    route_short_names: &HashMap<String, String>,
) -> (String, Option<String>) {
    match strategy {
        TripIdStrategy::TripShortName => {
            // Prefer trip_short_name (the GTFS-spec field for operator run numbers).
            // Fall back to trip_id when trip_short_name is absent.
            let train_num = if !short_name.is_empty() {
                short_name.to_string()
            } else {
                trip_id.to_string()
            };
            // Insert a secondary alias keyed on the short name so RT feeds
            // that prefix trip_ids (e.g. "{DATE}_{SHORT}") still hit the table
            // via the underscore-split fallback in lookup().
            let secondary = if !short_name.is_empty() && short_name != trip_id {
                Some(short_name.to_string())
            } else {
                None
            };
            (train_num, secondary)
        }

        TripIdStrategy::TripIdNumericSuffix => {
            // Some feeds embed the run number inside trip_id as
            // "{LINE_CODE}{RUN}_{DATE}_{OTHER}".  Take the first
            // underscore-delimited token, strip any leading alphabetic
            // prefix (the line/route code), and return the remaining digits.
            let first_token = trip_id.split('_').next().unwrap_or(trip_id);
            let digit_start = first_token.find(|c: char| c.is_ascii_digit())
                .unwrap_or(first_token.len());
            let run_number  = &first_token[digit_start..];
            let train_num   = if run_number.is_empty() { first_token.to_string() } else { run_number.to_string() };
            (train_num, None)
        }

        TripIdStrategy::RouteShortName => {
            // Use route_short_name as the display label when trip_short_name
            // is absent and trip_id is opaque.
            let train_num = route_short_names
                .get(route_id)
                .cloned()
                .unwrap_or_else(|| trip_id.to_string());
            (train_num, None)
        }

        TripIdStrategy::TripIdVerbatim => {
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

/// Parse a single optional CSV column as `u32`, returning 0 when absent or unparseable.
/// Eliminates the repeated `idx.and_then(|i| rec.get(i)).and_then(|s| s.trim().parse().ok()).unwrap_or(0)` pattern.
#[inline]
fn parse_col_u32(idx: Option<usize>, rec: &csv::StringRecord) -> u32 {
    idx.and_then(|i| rec.get(i))
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
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
                // or_insert: first feed loaded wins on route_id collisions.
                for (k, v) in display { store.route_names.entry(k).or_insert(v); }
                for (k, v) in short   { store.route_short_names.entry(k).or_insert(v); }
                0
            }
            Err(e) => { eprintln!("parse_routes: {}", e); -1 }
        },

        "trips.txt" => {
            // routes.txt should be fed before trips.txt so route_short_names is
            // already populated when RouteShortName strategy needs it.
            // Capture the result first so the immutable borrow of
            // store.route_short_names ends before the mutable merge below.
            let strategy = store.strategy;
            let result   = parse_trips_with_strategy(&decompressed, strategy, &store.route_short_names);
            match result {
                Ok((trips_map, shape_id_map, service_id_map, direction_map, headsign_map, block_id_map)) => {
                    for (k, v) in trips_map     { store.trips.entry(k).or_insert(v); }
                    for (k, v) in shape_id_map  { store.trip_shape_ids.entry(k).or_insert(v); }
                    for (k, v) in service_id_map{ store.trip_service_ids.entry(k).or_insert(v); }
                    for (k, v) in direction_map { store.trip_direction_ids.entry(k).or_insert(v); }
                    for (k, v) in headsign_map  { store.trip_headsigns.entry(k).or_insert(v); }
                    for (k, v) in block_id_map  { store.trip_block_ids.entry(k).or_insert(v); }
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
            // Single-pass parse: produces trip_windows, trip_stop_seqs, and
            // stop_pickup_types all at once instead of three separate CSV scans.
            let result = parse_stop_times_all(&decompressed, &store.stop_latlon);
            match result {
                Ok((windows, seqs, pickup_types)) => {
                    store.trip_windows.extend(windows);
                    store.trip_stop_seqs.extend(seqs);
                    store.stop_pickup_types.extend(pickup_types);
                    // Eagerly build any timelines now possible (requires shapes.txt
                    // already loaded; if not, shapes.txt load will finish the job).
                    build_timelines_eager(store);
                    0
                }
                Err(e) => { eprintln!("parse_stop_times: {}", e); -1 }
            }
        },

        "shapes.txt" => match parse_shapes(&decompressed) {
            Ok(map) => {
                store.shape_points.extend(map);
                // Eagerly build any timelines now possible (requires stop_times.txt
                // already loaded; if not, stop_times.txt load will finish the job).
                build_timelines_eager(store);
                0
            }
            Err(e)  => { eprintln!("parse_shapes: {}", e); -1 }
        },

        "calendar.txt" => match parse_calendar(&decompressed) {
            Ok(map) => { store.calendar.extend(map); 0 }
            Err(e)  => { eprintln!("parse_calendar: {}", e); -1 }
        },

        "calendar_dates.txt" => match parse_calendar_dates(&decompressed) {
            Ok(map) => { store.calendar_dates.extend(map); 0 }
            Err(e)  => { eprintln!("parse_calendar_dates: {}", e); -1 }
        },

        // Any other file in the ZIP (agency.txt, fare_rules.txt, etc.) is
        // silently ignored.  Return 0, not -1, to avoid spurious Swift errors.
        _ => 0,
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
    let out: Vec<GTFSZipRange> = ranges.into_iter()
        .filter_map(|(name, offset, len)| {
            CString::new(name).ok().map(|cs| GTFSZipRange {
                filename: cs.into_raw(), byte_offset: offset, byte_length: len,
            })
        })
        .collect();
    let count = out.len();
    unsafe { *out_count = count };
    // Use into_boxed_slice so capacity == length; capacity mismatch in the
    // corresponding free function would be undefined behaviour.
    let mut boxed: Box<[GTFSZipRange]> = out.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);
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
/// The store defaults to `TripShortName` strategy.
/// Call `gtfs_static_store_set_strategy` before feeding files to change it.
#[no_mangle]
pub extern "C" fn gtfs_static_store_new() -> u32 {
    registry().lock().unwrap_or_else(|e| e.into_inner()).open()
}

/// Release the store identified by `store_id`.
/// Silently ignores unknown or zero IDs.
#[no_mangle]
pub extern "C" fn gtfs_static_store_free(store_id: u32) {
    if store_id == 0 { return; }
    registry().lock().unwrap_or_else(|e| e.into_inner()).close(store_id);
}

/// Set the trip-id extraction strategy for `store_id`.
/// **Call before feeding any files.**
///
/// `strategy` values:
///   0 = TripShortName      — use `trip_short_name` field (default)
///   1 = TripIdNumericSuffix — strip leading alpha prefix from first `_`-split token of `trip_id`
///   2 = RouteShortName     — use `route_short_name` field
///   3 = TripIdVerbatim     — use `trip_id` as-is
#[no_mangle]
pub extern "C" fn gtfs_static_store_set_strategy(store_id: u32, strategy: i32) {
    let Some(handle) = get_store(store_id) else { return };
    let mut store = handle.write().unwrap_or_else(|e| e.into_inner());
    store.strategy = TripIdStrategy::from_i32(strategy);
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

    let slice = unsafe { std::slice::from_raw_parts(data, data_len) };
    let Some(handle) = get_store(store_id) else { return std::ptr::null_mut() };
    let mut store = handle.write().unwrap_or_else(|e| e.into_inner());

    match parse_eocd_into(slice, &mut *store) {
        Ok(ranges) => {
            drop(store); // release write lock before allocating FFI output
            ranges_to_ffi(ranges, out_count)
        }
        Err(e) => { eprintln!("gtfs_static_store_feed_eocd: {}", e); std::ptr::null_mut() }
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
    if store_id == 0 || filename.is_null() || data.is_null() || data_len == 0 { return -1; }
    let fname = match unsafe { CStr::from_ptr(filename).to_str() } {
        Ok(s)  => s.to_string(),
        Err(_) => return -1,
    };
    let slice = unsafe { std::slice::from_raw_parts(data, data_len) };
    // Acquire the individual store write lock — NOT the global registry lock.
    // This allows concurrent reads on all other stores while this CSV is parsed.
    let Some(handle) = get_store(store_id) else { return -1 };
    let mut store = handle.write().unwrap_or_else(|e| e.into_inner());
    feed_file_into(&mut *store, &fname, slice)
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
    let Some(handle) = get_store(store_id) else { return make_static_result(None) };
    let store = handle.read().unwrap_or_else(|e| e.into_inner());
    let found = store.lookup(tid);
    drop(store); // release read lock before allocating FFI output
    make_static_result(found)
}

/// Returns 1 if routes, trips, and stop_times are loaded in `store_id`.
#[no_mangle]
pub extern "C" fn gtfs_static_store_is_loaded(store_id: u32) -> i32 {
    let Some(handle) = get_store(store_id) else { return 0 };
    let store = handle.read().unwrap_or_else(|e| e.into_inner());
    if store.is_loaded() { 1 } else { 0 }
}

/// Returns 1 if `trip_id` is scheduled as active at `now_local` in `store_id`.
#[no_mangle]
pub extern "C" fn gtfs_static_store_is_trip_active(
    store_id:    u32,
    trip_id:     *const c_char,
    now_local:   i64,
) -> i32 {
    if store_id == 0 || trip_id.is_null() { return 0; }
    let tid = match unsafe { CStr::from_ptr(trip_id).to_str() } {
        Ok(s)  => s,
        Err(_) => return 0,
    };
    let Some(handle) = get_store(store_id) else { return 0 };
    let store = handle.read().unwrap_or_else(|e| e.into_inner());
    if store.trip_windows.is_empty() { return 1; }
    if store.is_trip_active(tid, now_local) { 1 } else { 0 }
}

/// Compute a smooth interpolated position for `trip_id` at `now_local` in `store_id`.
/// `is_valid = 0` means shape data is unavailable — fall back to raw GPS.
#[no_mangle]
pub extern "C" fn gtfs_static_store_interpolate(
    store_id:    u32,
    trip_id:     *const c_char,
    now_local:   i64,
) -> InterpolatedPosition {
    let null = InterpolatedPosition { lat: 0.0, lon: 0.0, is_valid: 0 };
    if store_id == 0 || trip_id.is_null() { return null; }
    let tid = match unsafe { CStr::from_ptr(trip_id).to_str() } {
        Ok(s)  => s,
        Err(_) => return null,
    };
    let Some(handle) = get_store(store_id) else { return null };
    let store = handle.read().unwrap_or_else(|e| e.into_inner());
    match store.interpolate_position(tid, now_local) {
        Some((lat, lon)) => InterpolatedPosition { lat, lon, is_valid: 1 },
        None             => null,
    }
}

/// Returns 1 when shape interpolation data is fully loaded for `store_id`.
#[no_mangle]
pub extern "C" fn gtfs_static_store_interpolation_ready(store_id: u32) -> i32 {
    let Some(handle) = get_store(store_id) else { return 0 };
    let store = handle.read().unwrap_or_else(|e| e.into_inner());
    if store.interpolation_ready() { 1 } else { 0 }
}

/// Evict all data from `store_id` (preserves strategy).
#[no_mangle]
pub extern "C" fn gtfs_static_store_reset(store_id: u32) {
    let Some(handle) = get_store(store_id) else { return };
    let mut store = handle.write().unwrap_or_else(|e| e.into_inner());
    store.reset();
}

// ═══════════════════════════════════════════════════════════════════════════════
// MARK: - Legacy singleton FFI  (routes to LEGACY_STORE_ID = 1, zero Swift changes needed)
// ═══════════════════════════════════════════════════════════════════════════════

/// Feed the ZIP tail bytes into the legacy store.
#[no_mangle]
pub extern "C" fn gtfs_static_feed_eocd(
    data: *const u8, data_len: usize, out_count: *mut usize,
) -> *mut GTFSZipRange {
    gtfs_static_store_feed_eocd(LEGACY_STORE_ID, data, data_len, out_count)
}

/// Feed one GTFS file into the legacy store.
#[no_mangle]
pub extern "C" fn gtfs_static_feed_file(
    filename: *const c_char, data: *const u8, data_len: usize,
) -> i32 {
    gtfs_static_store_feed_file(LEGACY_STORE_ID, filename, data, data_len)
}

/// Look up a trip_id in the legacy store.
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
        // Reconstruct the Box<[GTFSZipRange]> produced by ranges_to_ffi.
        let boxed = Box::from_raw(std::slice::from_raw_parts_mut(ranges, count));
        for r in boxed.iter() {
            if !r.filename.is_null() { let _ = CString::from_raw(r.filename as *mut c_char); }
        }
    }
}

/// Returns 1 if the legacy store has all core tables loaded.
#[no_mangle]
pub extern "C" fn gtfs_static_is_loaded() -> i32 {
    gtfs_static_store_is_loaded(LEGACY_STORE_ID)
}

/// Returns 1 if `trip_id` is active at `now_local` in the legacy store.
#[no_mangle]
pub extern "C" fn gtfs_static_is_trip_active(trip_id: *const c_char, now_local: i64) -> i32 {
    gtfs_static_store_is_trip_active(LEGACY_STORE_ID, trip_id, now_local)
}

/// Compute an interpolated position from the legacy store.
#[no_mangle]
pub extern "C" fn gtfs_interpolate_position(trip_id: *const c_char, now_local: i64) -> InterpolatedPosition {
    gtfs_static_store_interpolate(LEGACY_STORE_ID, trip_id, now_local)
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

// ═══════════════════════════════════════════════════════════════════════════════
// MARK: - Trip metadata FFI (multi-store; legacy wrappers below)
// ═══════════════════════════════════════════════════════════════════════════════

/// Convert an `Option<&String>` into a heap-allocated `*const c_char`.
/// Returns `null` when `val` is `None`.
/// The caller must free the returned pointer with `CString::from_raw` /
/// `free_rust_string` when it is no longer needed.
///
/// Eliminates the repeated
/// `match val { Some(s) => CString::new(s.as_str()).map(…).unwrap_or(null()), None => null() }`
/// pattern that appears in every string-returning metadata FFI function.
#[inline]
fn optional_str_to_cstr(val: Option<&String>) -> *const c_char {
    match val {
        Some(s) => CString::new(s.as_str())
            .map(|c| c.into_raw() as *const c_char)
            .unwrap_or(std::ptr::null()),
        None => std::ptr::null(),
    }
}

/// Returns the direction_id (0 or 1) for `trip_id` in `store_id`.
/// Returns -1 when direction_id is absent or the trip is unknown.
#[no_mangle]
pub extern "C" fn gtfs_static_store_get_direction_id(
    store_id: u32,
    trip_id:  *const c_char,
) -> i32 {
    if store_id == 0 || trip_id.is_null() { return -1; }
    let tid = match unsafe { CStr::from_ptr(trip_id).to_str() } { Ok(s) => s, Err(_) => return -1 };
    let Some(handle) = get_store(store_id) else { return -1 };
    let store = handle.read().unwrap_or_else(|e| e.into_inner());
    // Try canonical trip_id, then last segment of mangled RT id.
    let dir = store.trip_direction_ids.get(tid)
        .or_else(|| {
            if tid.contains('_') { tid.split('_').last().and_then(|last| store.trip_direction_ids.get(last)) }
            else { None }
        });
    dir.map(|&d| if d == u8::MAX { -1 } else { d as i32 }).unwrap_or(-1)
}

/// Returns a heap-allocated C string with the trip_headsign for `trip_id`.
/// Returns NULL when not available.  Free with `free_rust_string`.
#[no_mangle]
pub extern "C" fn gtfs_static_store_get_headsign(
    store_id: u32,
    trip_id:  *const c_char,
) -> *const c_char {
    if store_id == 0 || trip_id.is_null() { return std::ptr::null(); }
    let tid = match unsafe { CStr::from_ptr(trip_id).to_str() } { Ok(s) => s, Err(_) => return std::ptr::null() };
    let Some(handle) = get_store(store_id) else { return std::ptr::null() };
    let store = handle.read().unwrap_or_else(|e| e.into_inner());
    let hs = store.trip_headsigns.get(tid)
        .or_else(|| {
            if tid.contains('_') { tid.split('_').last().and_then(|last| store.trip_headsigns.get(last)) }
            else { None }
        });
    let result = optional_str_to_cstr(hs);
    drop(store); // release read lock before returning CString pointer
    result
}

/// Returns a heap-allocated C string with the service_id for `trip_id`.
/// Returns NULL when not available.  Free with `free_rust_string`.
#[no_mangle]
pub extern "C" fn gtfs_static_store_get_service_id(
    store_id: u32,
    trip_id:  *const c_char,
) -> *const c_char {
    if store_id == 0 || trip_id.is_null() { return std::ptr::null(); }
    let tid = match unsafe { CStr::from_ptr(trip_id).to_str() } { Ok(s) => s, Err(_) => return std::ptr::null() };
    let Some(handle) = get_store(store_id) else { return std::ptr::null() };
    let store = handle.read().unwrap_or_else(|e| e.into_inner());
    let sid = store.trip_service_ids.get(tid)
        .or_else(|| {
            if tid.contains('_') { tid.split('_').last().and_then(|last| store.trip_service_ids.get(last)) }
            else { None }
        });
    let result = optional_str_to_cstr(sid);
    drop(store); // release read lock before returning CString pointer
    result
}

/// Returns 1 if calendar.txt or calendar_dates.txt data has been loaded for `store_id`.
/// Returns 0 when the store has no service-day data (is_trip_active will pass-through).
#[no_mangle]
pub extern "C" fn gtfs_static_store_calendar_loaded(store_id: u32) -> i32 {
    let Some(handle) = get_store(store_id) else { return 0 };
    let store = handle.read().unwrap_or_else(|e| e.into_inner());
    if !store.calendar.is_empty() || !store.calendar_dates.is_empty() { 1 } else { 0 }
}

/// Returns 1 if the stop at `(trip_id, stop_sequence)` accepts revenue passengers
/// (pickup_type == 0 AND dropoff_type == 0).  Returns 1 for any unknown stops
/// (default assumption: revenue stop).  Use this to filter non-revenue timing
/// points from next-stop results.
#[no_mangle]
pub extern "C" fn gtfs_static_store_is_stop_revenue(
    store_id:      u32,
    trip_id:       *const c_char,
    stop_sequence: u32,
) -> i32 {
    if store_id == 0 || trip_id.is_null() { return 1; }
    let tid = match unsafe { CStr::from_ptr(trip_id).to_str() } { Ok(s) => s, Err(_) => return 1 };
    let Some(handle) = get_store(store_id) else { return 1 };
    let store = handle.read().unwrap_or_else(|e| e.into_inner());
    match store.stop_pickup_types.get(&(tid.to_string(), stop_sequence)) {
        // pickup_type=1 AND dropoff_type=1 means no service (timing point / deadhead).
        Some(&(1, 1)) => 0,
        // Any entry exists but is not both 1 → partial service still counts as revenue.
        Some(_) => 1,
        // Not in map → default (all zeros → regular service).
        None    => 1,
    }
}

// ── Legacy wrappers for the new metadata functions ────────────────────────────

/// Returns the direction_id for `trip_id` in the legacy store.
/// Returns -1 when absent.
#[no_mangle]
pub extern "C" fn gtfs_static_get_direction_id(trip_id: *const c_char) -> i32 {
    gtfs_static_store_get_direction_id(LEGACY_STORE_ID, trip_id)
}

/// Returns the trip_headsign for `trip_id` in the legacy store.  Free with `free_rust_string`.
#[no_mangle]
pub extern "C" fn gtfs_static_get_headsign(trip_id: *const c_char) -> *const c_char {
    gtfs_static_store_get_headsign(LEGACY_STORE_ID, trip_id)
}

/// Returns 1 if calendar data is loaded in the legacy store.
#[no_mangle]
pub extern "C" fn gtfs_static_calendar_loaded() -> i32 {
    gtfs_static_store_calendar_loaded(LEGACY_STORE_ID)
}
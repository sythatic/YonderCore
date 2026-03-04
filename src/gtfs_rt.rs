use prost::Message;
use std::ffi::{CStr, CString, c_char};

// MARK: - Protobuf Message Definitions
// Matches the official GTFS-RT spec:
// https://raw.githubusercontent.com/google/transit/refs/heads/master/gtfs-realtime/proto/gtfs-realtime.proto

#[derive(Clone, PartialEq, Message)]
pub struct FeedMessage {
    #[prost(message, required, tag = "1")]
    pub header: FeedHeader,
    #[prost(message, repeated, tag = "2")]
    pub entity: Vec<FeedEntity>,
}

#[derive(Clone, PartialEq, Message)]
pub struct FeedHeader {
    #[prost(string, required, tag = "1")]
    pub gtfs_realtime_version: String,
    #[prost(enumeration = "Incrementality", optional, tag = "2")]
    pub incrementality: Option<i32>,
    #[prost(uint64, optional, tag = "3")]
    pub timestamp: Option<u64>,
    // Field 4: feed_version — useful for detecting static GTFS updates
    #[prost(string, optional, tag = "4")]
    pub feed_version: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct FeedEntity {
    #[prost(string, required, tag = "1")]
    pub id: String,
    #[prost(bool, optional, tag = "2")]
    pub is_deleted: Option<bool>,
    #[prost(message, optional, tag = "3")]
    pub trip_update: Option<TripUpdate>,
    #[prost(message, optional, tag = "4")]
    pub vehicle: Option<VehiclePosition>,
    #[prost(message, optional, tag = "5")]
    pub alert: Option<Alert>,
    // Fields 6–8 (shape, stop, trip_modifications) are experimental and
    // intentionally omitted; prost drops unknown fields gracefully.
    //
    // TripModifications (field 8) — add when detour/route-change support is needed:
    //   #[prost(message, optional, tag = "8")]
    //   pub trip_modifications: Option<TripModifications>,
    // See the GTFS-RT spec for the TripModifications message definition.
    // The GtfsRtManager::parse_feed loop will need a corresponding branch
    // to collect and expose modifications alongside trip_updates.
}

#[derive(Clone, PartialEq, Message)]
pub struct TripUpdate {
    #[prost(message, required, tag = "1")]
    pub trip: TripDescriptor,
    #[prost(message, optional, tag = "3")]
    pub vehicle: Option<VehicleDescriptor>,
    #[prost(message, repeated, tag = "2")]
    pub stop_time_update: Vec<StopTimeUpdate>,
    #[prost(uint64, optional, tag = "4")]
    pub timestamp: Option<u64>,
    // Field 5: trip-level delay in seconds. Experimental per spec; rarely
    // populated by Amtrak. Per-stop delay in StopTimeEvent is more reliable.
    #[prost(int32, optional, tag = "5")]
    pub delay: Option<i32>,
}

#[derive(Clone, PartialEq, Message)]
pub struct StopTimeUpdate {
    #[prost(uint32, optional, tag = "1")]
    pub stop_sequence: Option<u32>,
    #[prost(string, optional, tag = "4")]
    pub stop_id: Option<String>,
    #[prost(message, optional, tag = "2")]
    pub arrival: Option<StopTimeEvent>,
    #[prost(message, optional, tag = "3")]
    pub departure: Option<StopTimeEvent>,
    // Field 5: per-stop schedule relationship.
    // 0=SCHEDULED, 1=SKIPPED, 2=NO_DATA, 3=UNSCHEDULED
    // Required to filter skipped stops from the next-stop finder.
    #[prost(int32, optional, tag = "5")]
    pub schedule_relationship: Option<i32>,
}

/// Per-stop schedule relationship values for StopTimeUpdate (field 5).
pub mod stop_time_schedule_relationship {
    pub const SCHEDULED: i32 = 0;
    pub const SKIPPED: i32 = 1;
    pub const NO_DATA: i32 = 2;
    pub const UNSCHEDULED: i32 = 3;
}

#[derive(Clone, PartialEq, Message)]
pub struct StopTimeEvent {
    #[prost(int32, optional, tag = "1")]
    pub delay: Option<i32>,
    #[prost(int64, optional, tag = "2")]
    pub time: Option<i64>,
    #[prost(int32, optional, tag = "3")]
    pub uncertainty: Option<i32>,
    // Field 4: scheduled_time — for NEW/REPLACEMENT/DUPLICATED trips (experimental)
    #[prost(int64, optional, tag = "4")]
    pub scheduled_time: Option<i64>,
}

#[derive(Clone, PartialEq, Message)]
pub struct VehiclePosition {
    #[prost(message, optional, tag = "1")]
    pub trip: Option<TripDescriptor>,
    #[prost(message, optional, tag = "8")]
    pub vehicle: Option<VehicleDescriptor>,
    #[prost(message, optional, tag = "2")]
    pub position: Option<Position>,
    #[prost(uint32, optional, tag = "3")]
    pub current_stop_sequence: Option<u32>,
    #[prost(string, optional, tag = "7")]
    pub stop_id: Option<String>,
    #[prost(enumeration = "VehicleStopStatus", optional, tag = "4")]
    pub current_status: Option<i32>,
    #[prost(uint64, optional, tag = "5")]
    pub timestamp: Option<u64>,
    #[prost(enumeration = "CongestionLevel", optional, tag = "6")]
    pub congestion_level: Option<i32>,
    #[prost(enumeration = "OccupancyStatus", optional, tag = "9")]
    pub occupancy_status: Option<i32>,
    // Field 10: occupancy_percentage (experimental)
    #[prost(uint32, optional, tag = "10")]
    pub occupancy_percentage: Option<u32>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Alert {
    #[prost(message, repeated, tag = "1")]
    pub active_period: Vec<TimeRange>,
    #[prost(message, repeated, tag = "5")]
    pub informed_entity: Vec<EntitySelector>,
}

#[derive(Clone, PartialEq, Message)]
pub struct TimeRange {
    #[prost(uint64, optional, tag = "1")]
    pub start: Option<u64>,
    #[prost(uint64, optional, tag = "2")]
    pub end: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Position {
    #[prost(float, required, tag = "1")]
    pub latitude: f32,
    #[prost(float, required, tag = "2")]
    pub longitude: f32,
    #[prost(float, optional, tag = "3")]
    pub bearing: Option<f32>,
    // Field 4: odometer is `double` in the spec, not `float`.
    #[prost(double, optional, tag = "4")]
    pub odometer: Option<f64>,
    #[prost(float, optional, tag = "5")]
    pub speed: Option<f32>,
}

#[derive(Clone, PartialEq, Message)]
pub struct TripDescriptor {
    #[prost(string, optional, tag = "1")]
    pub trip_id: Option<String>,
    #[prost(string, optional, tag = "5")]
    pub route_id: Option<String>,
    #[prost(uint32, optional, tag = "6")]
    pub direction_id: Option<u32>,
    #[prost(string, optional, tag = "2")]
    pub start_time: Option<String>,
    #[prost(string, optional, tag = "3")]
    pub start_date: Option<String>,
    #[prost(enumeration = "ScheduleRelationship", optional, tag = "4")]
    pub schedule_relationship: Option<i32>,
}

#[derive(Clone, PartialEq, Message)]
pub struct VehicleDescriptor {
    #[prost(string, optional, tag = "1")]
    pub id: Option<String>,
    #[prost(string, optional, tag = "2")]
    pub label: Option<String>,
    #[prost(string, optional, tag = "3")]
    pub license_plate: Option<String>,
    // Field 4: wheelchair_accessible (experimental)
    #[prost(enumeration = "WheelchairAccessible", optional, tag = "4")]
    pub wheelchair_accessible: Option<i32>,
}

#[derive(Clone, PartialEq, Message)]
pub struct EntitySelector {
    #[prost(string, optional, tag = "1")]
    pub agency_id: Option<String>,
    #[prost(string, optional, tag = "2")]
    pub route_id: Option<String>,
    #[prost(int32, optional, tag = "3")]
    pub route_type: Option<i32>,
    #[prost(message, optional, tag = "4")]
    pub trip: Option<TripDescriptor>,
    #[prost(string, optional, tag = "5")]
    pub stop_id: Option<String>,
}

// MARK: - Enumerations

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum Incrementality {
    FullDataset = 0,
    Differential = 1,
}

/// Trip-level schedule relationship (TripDescriptor.schedule_relationship, field 4).
/// Note: ADDED=1 is deprecated in the spec but still emitted by some feeds
/// (e.g. Amtrak for charter/special trains). Treat it the same as SCHEDULED.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum ScheduleRelationship {
    Scheduled = 0,
    /// Deprecated in spec; treat the same as Scheduled for filtering purposes.
    Added = 1,
    Unscheduled = 2,
    Canceled = 3,
    // 4 is reserved in the spec
    Replacement = 5,
    Duplicated = 6,
    Deleted = 7,
    New = 8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum VehicleStopStatus {
    IncomingAt = 0,
    StoppedAt = 1,
    InTransitTo = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum CongestionLevel {
    UnknownCongestionLevel = 0,
    RunningSmooth = 1,
    StopAndGo = 2,
    Congestion = 3,
    SevereCongestion = 4,
}

/// Occupancy status values — must match the spec exactly (values 0–8).
/// Values 7 and 8 were previously missing, causing unexpected i32 values
/// to reach Swift via the FFI.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum OccupancyStatus {
    Empty = 0,
    ManySeatsAvailable = 1,
    FewSeatsAvailable = 2,
    StandingRoomOnly = 3,
    CrushedStandingRoomOnly = 4,
    Full = 5,
    NotAcceptingPassengers = 6,
    /// No occupancy data available at this time.
    NoDataAvailable = 7,
    /// Vehicle is not boardable (engine, maintenance car, etc.).
    NotBoardable = 8,
}

/// Wheelchair accessibility for VehicleDescriptor (field 4, experimental).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum WheelchairAccessible {
    /// No value — does not override the static GTFS value.
    NoValue = 0,
    /// Overrides static GTFS: no information available.
    Unknown = 1,
    /// Overrides static GTFS: trip is wheelchair accessible.
    WheelchairAccessible = 2,
    /// Overrides static GTFS: trip is not wheelchair accessible.
    WheelchairInaccessible = 3,
}

// MARK: - FFI Structs

#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFIVehicle {
    pub id: *const c_char,
    pub latitude: f64,
    pub longitude: f64,
    pub bearing: f32,
    pub speed: f32,
    pub route_id: *const c_char,
    pub trip_id: *const c_char,
    pub label: *const c_char,   // VehicleDescriptor.label — human-readable train number/name
    pub timestamp: i64,
    pub has_bearing: bool,
    pub has_speed: bool,
    pub occupancy_status: i32,
}

#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFITripUpdate {
    pub trip_id: *const c_char,
    pub route_id: *const c_char,
    pub vehicle_id: *const c_char,
    pub timestamp: i64,
    pub delay: i32,
    pub has_delay: bool,
    pub stop_time_updates_count: usize,
}

#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFIStopTimeUpdate {
    pub stop_id: *const c_char,
    pub stop_sequence: u32,
    pub arrival_delay: i32,
    pub arrival_time: i64,
    pub departure_delay: i32,
    pub departure_time: i64,
    pub has_arrival: bool,
    pub has_departure: bool,
}

// MARK: - GTFS-RT Manager

pub struct GtfsRtManager {
    vehicles: Vec<VehiclePosition>,
    trip_updates: Vec<TripUpdate>,
    alerts: Vec<Alert>,
    last_update: Option<u64>,
}

impl GtfsRtManager {
    pub fn new() -> Self {
        Self {
            vehicles: Vec::new(),
            trip_updates: Vec::new(),
            alerts: Vec::new(),
            last_update: None,
        }
    }

    /// Parse GTFS-RT protobuf data.
    pub fn parse_feed(&mut self, data: &[u8]) -> Result<(), String> {
        let feed = FeedMessage::decode(data)
            .map_err(|e| format!("Failed to decode protobuf: {}", e))?;

        self.vehicles.clear();
        self.trip_updates.clear();
        self.alerts.clear();
        self.last_update = feed.header.timestamp;

        // Deduplicate VehiclePosition by VehicleDescriptor.id only.
        //
        // Amtrak's feed can emit multiple entities for the same physical
        // consist when a locomotive works a through-service connection
        // (two trip_ids, one locomotive). When VehicleDescriptor.id is set
        // it uniquely identifies the equipment; keep only the most recent
        // timestamp for each id.
        //
        // IMPORTANT: do NOT fall back to VehicleDescriptor.label for dedup.
        // label is the human-readable train number (e.g. "171"), and Amtrak
        // can operate two genuine services with the same number on different
        // branches (or when schedule changeovers overlap). Deduping by label
        // would silently drop one of them. If id is absent, push unconditionally.
        let mut seen_vehicle_ids: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();

        for entity in feed.entity {
            if entity.is_deleted.unwrap_or(false) {
                continue;
            }
            if let Some(vehicle) = entity.vehicle {
                // Only deduplicate when VehicleDescriptor.id is explicitly set.
                let vid: Option<String> = vehicle
                    .vehicle
                    .as_ref()
                    .and_then(|v| v.id.as_deref())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string());

                if let Some(vid) = vid {
                    let new_ts = vehicle.timestamp.unwrap_or(0);
                    if let Some(&existing_idx) = seen_vehicle_ids.get(&vid) {
                        let existing_ts = self.vehicles[existing_idx].timestamp.unwrap_or(0);
                        if new_ts > existing_ts {
                            self.vehicles[existing_idx] = vehicle;
                        }
                    } else {
                        let idx = self.vehicles.len();
                        seen_vehicle_ids.insert(vid, idx);
                        self.vehicles.push(vehicle);
                    }
                } else {
                    // No id — push unconditionally; cannot safely dedup.
                    self.vehicles.push(vehicle);
                }
            }
            if let Some(trip_update) = entity.trip_update {
                self.trip_updates.push(trip_update);
            }
            if let Some(alert) = entity.alert {
                self.alerts.push(alert);
            }
        }

        Ok(())
    }

    /// Get all vehicles as FFI-compatible structs.
    pub fn get_vehicles(&self) -> Vec<FFIVehicle> {
        let header_ts = self.last_update.unwrap_or(0);
        self.vehicles
            .iter()
            .filter_map(|v| self.vehicle_to_ffi(v, header_ts))
            .collect()
    }

    /// Get vehicles within a bounding box.
    pub fn get_vehicles_in_region(
        &self,
        min_lat: f64,
        max_lat: f64,
        min_lon: f64,
        max_lon: f64,
    ) -> Vec<FFIVehicle> {
        let header_ts = self.last_update.unwrap_or(0);
        self.vehicles
            .iter()
            .filter_map(|v| {
                let pos = v.position.as_ref()?;
                let lat = pos.latitude as f64;
                let lon = pos.longitude as f64;
                // Coordinate validity is enforced inside vehicle_to_ffi (check 0),
                // but guard here too so the bounding-box test never runs on garbage values.
                if !lat.is_finite() || !lon.is_finite() || (lat == 0.0 && lon == 0.0) {
                    return None;
                }
                if lat >= min_lat && lat <= max_lat && lon >= min_lon && lon <= max_lon {
                    self.vehicle_to_ffi(v, header_ts)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_trip_updates(&self) -> &[TripUpdate] {
        &self.trip_updates
    }

    pub fn find_trip_update(&self, trip_id: &str) -> Option<&TripUpdate> {
        self.trip_updates.iter().find(|tu| {
            tu.trip
                .trip_id
                .as_ref()
                .map(|id| id.as_str() == trip_id)
                .unwrap_or(false)
        })
    }

    /// Internal trip-update extraction used by `GtfsRtCore::get_active_enriched_vehicles`.
    /// Does not acquire any lock (caller already holds the manager lock).
    /// Returns `(delay_seconds, has_delay, next_stop_id_ptr, next_arrival_time, has_next_stop)`.
    /// The returned `next_stop_id` pointer is a CString allocated via `into_raw()`; the
    /// caller must free it with `CString::from_raw` when it is no longer needed.
    pub(crate) fn get_trip_update_inner(
        &self,
        trip_id: &str,
    ) -> Option<(i32, bool, *const c_char, i64, bool)> {
        let tu = self.find_trip_update(trip_id)?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let mut delay_seconds: Option<i32> = tu.delay;

        if delay_seconds.is_none() {
            delay_seconds = tu
                .stop_time_update
                .iter()
                .filter(|stu| {
                    let rel = stu.schedule_relationship.unwrap_or(
                        stop_time_schedule_relationship::SCHEDULED,
                    );
                    if rel == stop_time_schedule_relationship::SKIPPED
                        || rel == stop_time_schedule_relationship::NO_DATA
                    {
                        return false;
                    }
                    let t = stu
                        .departure
                        .as_ref()
                        .and_then(|e| e.time)
                        .or_else(|| stu.arrival.as_ref().and_then(|e| e.time))
                        .unwrap_or(i64::MAX);
                    t <= now
                })
                .filter_map(|stu| {
                    stu.departure
                        .as_ref()
                        .and_then(|e| e.delay)
                        .or_else(|| stu.arrival.as_ref().and_then(|e| e.delay))
                })
                .last();
        }

        if delay_seconds.is_none() {
            delay_seconds = tu
                .stop_time_update
                .iter()
                .filter(|stu| {
                    let rel = stu.schedule_relationship.unwrap_or(
                        stop_time_schedule_relationship::SCHEDULED,
                    );
                    if rel == stop_time_schedule_relationship::SKIPPED
                        || rel == stop_time_schedule_relationship::NO_DATA
                    {
                        return false;
                    }
                    let t = stu
                        .arrival
                        .as_ref()
                        .and_then(|e| e.time)
                        .or_else(|| stu.departure.as_ref().and_then(|e| e.time))
                        .unwrap_or(0);
                    t > now
                })
                .find_map(|stu| {
                    stu.arrival
                        .as_ref()
                        .and_then(|e| e.delay)
                        .or_else(|| stu.departure.as_ref().and_then(|e| e.delay))
                });
        }

        const MAX_PLAUSIBLE_DELAY_SECS: i32 = 6 * 3600;
        if let Some(d) = delay_seconds {
            if d.abs() > MAX_PLAUSIBLE_DELAY_SECS {
                delay_seconds = None;
            }
        }

        if let Some(trip_delay) = tu.delay {
            match delay_seconds {
                Some(stop_delay) if (stop_delay - trip_delay).abs() > 3600 => {
                    delay_seconds = Some(trip_delay);
                }
                None => {
                    delay_seconds = Some(trip_delay);
                }
                _ => {}
            }
        }

        let has_delay = delay_seconds.is_some();

        let next = tu.stop_time_update.iter().find(|stu| {
            let rel = stu
                .schedule_relationship
                .unwrap_or(stop_time_schedule_relationship::SCHEDULED);
            if rel == stop_time_schedule_relationship::SKIPPED
                || rel == stop_time_schedule_relationship::NO_DATA
            {
                return false;
            }
            let t = stu
                .departure
                .as_ref()
                .and_then(|e| e.time)
                .or_else(|| stu.arrival.as_ref().and_then(|e| e.time));
            t.map(|ts| ts > now).unwrap_or(false)
        });

        let next_stop_id: *const c_char = next
            .and_then(|s| s.stop_id.as_ref())
            .and_then(|s| CString::new(s.as_str()).ok())
            .map_or(std::ptr::null(), |s| s.into_raw());

        let next_arrival_time = next
            .and_then(|s| s.arrival.as_ref().and_then(|e| e.time))
            .unwrap_or(0);

        Some((delay_seconds.unwrap_or(0), has_delay, next_stop_id, next_arrival_time, next.is_some()))
    }

    pub fn vehicle_count(&self) -> usize {
        self.vehicles.len()
    }

    pub fn trip_update_count(&self) -> usize {
        self.trip_updates.len()
    }

    /// Convert VehiclePosition to FFI struct.
    /// Filters out stale, canceled, and deleted vehicles before they reach Swift.
    ///
    /// `feed_header_ts` is the FeedHeader.Timestamp from the last parsed feed.
    /// It is used as a fallback when VehiclePosition.Timestamp is absent — the
    /// GTFS-RT spec says: "If this field is not present, use the timestamp from
    /// the feed header." When we rely on the header timestamp we skip the
    /// per-vehicle staleness check, since the header being recent tells us the
    /// feed is fresh; we just don't know when this specific GPS fix was taken.
    fn vehicle_to_ffi(&self, vehicle: &VehiclePosition, feed_header_ts: u64) -> Option<FFIVehicle> {
        let pos = vehicle.position.as_ref()?;

        // 0. Reject invalid GPS coordinates before any other processing.
        //
        //    Feeds occasionally emit error-state positions of exactly (0.0, 0.0)
        //    ("null island") when a GPS fix is unavailable, or values that are
        //    out of the WGS-84 valid range. Both cause "jumping" train icons on
        //    the map and must be filtered at the source.
        //
        //    Valid WGS-84: latitude ∈ [-90, 90], longitude ∈ [-180, 180].
        //    We also reject NaN and ±Inf (IEEE 754 edge cases prost may produce
        //    from malformed float wire values) and the exact (0.0, 0.0) pair
        //    which is Amtrak's sentinel for "no GPS fix".
        let lat = pos.latitude;
        let lon = pos.longitude;
        if !lat.is_finite()
            || !lon.is_finite()
            || lat < -90.0
            || lat > 90.0
            || lon < -180.0
            || lon > 180.0
            || (lat == 0.0 && lon == 0.0)
        {
            return None;
        }

        // 1. Staleness check.
        //
        //    VehiclePosition.timestamp is optional per the GTFS-RT spec. When
        //    absent Amtrak's feed leaves the field unset (prost decodes this as
        //    None). Applying unwrap_or(0) and then checking age > 0 would
        //    silently drop every vehicle without a timestamp — the likely cause
        //    of the 62 vs 109 train count discrepancy.
        //
        //    Spec guidance: "If not present, use the timestamp from the feed
        //    header." We do exactly that, but we only apply the 4-hour staleness
        //    guard when the timestamp is vehicle-specific. When we fall back to
        //    the header timestamp we know the *feed* is fresh; we just can't say
        //    how old this particular GPS fix is, so we let it through.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let (ts, apply_staleness_check) = match vehicle.timestamp {
            Some(t) if t > 0 => (t, true),
            _ => (feed_header_ts, false),
        };

        // If we have no timestamp at all (even the header is missing/zero),
        // the position is unverifiable — drop it.
        if ts == 0 {
            return None;
        }

        if apply_staleness_check && now.saturating_sub(ts) > 4 * 60 * 60 {
            return None;
        }

        // 2. Reject explicitly removed trips.
        //    CANCELED=3, DELETED=7 per spec.
        //    ADDED=1 (deprecated) is treated the same as SCHEDULED — do NOT filter it.
        //    REPLACEMENT=5, DUPLICATED=6, NEW=8 are active trips — do NOT filter.
        let schedule_rel = vehicle
            .trip
            .as_ref()
            .and_then(|t| t.schedule_relationship)
            .unwrap_or(0);
        if schedule_rel == ScheduleRelationship::Canceled as i32
            || schedule_rel == ScheduleRelationship::Deleted as i32
        {
            return None;
        }

        // vehicle.vehicle.id is the machine ID; fall back to label if absent
        let id_str = vehicle
            .vehicle
            .as_ref()
            .and_then(|v| v.id.as_deref().or(v.label.as_deref()))
            .unwrap_or("unknown");
        let id = CString::new(id_str).ok()?;

        // VehicleDescriptor.label — Amtrak uses this for the human-readable train number/name
        let label = vehicle
            .vehicle
            .as_ref()
            .and_then(|v| v.label.as_ref())
            .and_then(|s| CString::new(s.as_str()).ok());

        let route_id = vehicle
            .trip
            .as_ref()
            .and_then(|t| t.route_id.as_ref())
            .and_then(|s| CString::new(s.as_str()).ok());

        let trip_id = vehicle
            .trip
            .as_ref()
            .and_then(|t| t.trip_id.as_ref())
            .and_then(|s| CString::new(s.as_str()).ok());

        Some(FFIVehicle {
            id: id.into_raw(),
            latitude: pos.latitude as f64,
            longitude: pos.longitude as f64,
            bearing: pos.bearing.unwrap_or(0.0),
            speed: pos.speed.unwrap_or(0.0),
            route_id: route_id.map_or(std::ptr::null(), |s| s.into_raw()),
            trip_id: trip_id.map_or(std::ptr::null(), |s| s.into_raw()),
            label: label.map_or(std::ptr::null(), |s| s.into_raw()),
            timestamp: vehicle.timestamp.unwrap_or(0) as i64,
            has_bearing: pos.bearing.is_some(),
            has_speed: pos.speed.is_some(),
            occupancy_status: vehicle.occupancy_status.unwrap_or(0),
        })
    }
}

// MARK: - Thread-safe wrapper

pub struct GtfsRtCore {
    manager: std::sync::Mutex<GtfsRtManager>,
}

impl GtfsRtCore {
    pub fn new() -> Self {
        Self {
            manager: std::sync::Mutex::new(GtfsRtManager::new()),
        }
    }

    pub fn parse(&self, data: &[u8]) -> Result<(), String> {
        self.manager
            .lock()
            .map_err(|_| "Failed to acquire lock".to_string())?
            .parse_feed(data)
    }

    pub fn get_vehicles(&self) -> Result<Vec<FFIVehicle>, String> {
        Ok(self
            .manager
            .lock()
            .map_err(|_| "Failed to acquire lock".to_string())?
            .get_vehicles())
    }

    pub fn vehicle_count(&self) -> Result<usize, String> {
        Ok(self
            .manager
            .lock()
            .map_err(|_| "Failed to acquire lock".to_string())?
            .vehicle_count())
    }

    /// Return active vehicles, already enriched with TripUpdate data.
    ///
    /// `now_eastern` is the current Unix timestamp shifted by the Eastern Time
    /// UTC offset (DST-aware): `now_unix + TimeZone("America/New_York").secondsFromGMT(now)`.
    /// This value is forwarded to `gtfs_static_is_trip_active` which works in
    /// Eastern "seconds from midnight".
    ///
    /// Only vehicles whose trip passes the schedule-window gate are included.
    /// The gate returns 1 (pass) while stop_times are still loading, so no
    /// vehicles are incorrectly hidden during startup.
    ///
    /// The result is a single lock acquisition + a single heap allocation,
    /// replacing the previous Swift pattern of N×`gtfs_static_is_trip_active`
    /// calls followed by N×`gtfs_rt_get_trip_update` calls.
    pub fn get_active_enriched_vehicles(&self, now_eastern: i64) -> Result<Vec<FFIEnrichedVehicle>, String> {
        use crate::gtfs_static_is_trip_active;

        let mgr = self
            .manager
            .lock()
            .map_err(|_| "Failed to acquire lock".to_string())?;

        let header_ts = mgr.last_update.unwrap_or(0);

        let mut result = Vec::with_capacity(mgr.vehicles.len());

        for vehicle in &mgr.vehicles {
            // Step 1 — convert to FFI struct (staleness / coordinate / schedule filters)
            let ffi = match mgr.vehicle_to_ffi(vehicle, header_ts) {
                Some(v) => v,
                None => continue,
            };

            // Step 2 — schedule-window filter via existing Rust function.
            // Safety: ffi.trip_id is either null or a valid CString we just
            // created inside vehicle_to_ffi; we consume it below regardless.
            let trip_id_cstr: Option<&CStr> = if ffi.trip_id.is_null() {
                None
            } else {
                // SAFETY: pointer was just produced by CString::into_raw() in vehicle_to_ffi.
                Some(unsafe { CStr::from_ptr(ffi.trip_id) })
            };

            let is_active = if trip_id_cstr.is_some() {
                // gtfs_static_is_trip_active is a safe Rust pub extern "C" fn.
                gtfs_static_is_trip_active(ffi.trip_id, now_eastern) == 1
            } else {
                true // No trip_id → cannot filter; let it through
            };

            if !is_active {
                // Free strings allocated by vehicle_to_ffi before skipping.
                unsafe {
                    if !ffi.id.is_null() { let _ = CString::from_raw(ffi.id as *mut _); }
                    if !ffi.route_id.is_null() { let _ = CString::from_raw(ffi.route_id as *mut _); }
                    if !ffi.trip_id.is_null() { let _ = CString::from_raw(ffi.trip_id as *mut _); }
                    if !ffi.label.is_null() { let _ = CString::from_raw(ffi.label as *mut _); }
                }
                continue;
            }

            // Step 3 — enrich with TripUpdate data (reuse the existing logic).
            let (delay_seconds, has_delay, next_stop_id, next_arrival_time, has_next_stop) =
                if let Some(tid) = trip_id_cstr {
                    let tid_str = tid.to_str().unwrap_or("");
                    mgr.get_trip_update_inner(tid_str)
                        .unwrap_or((0, false, std::ptr::null(), 0, false))
                } else {
                    (0, false, std::ptr::null(), 0, false)
                };

            result.push(FFIEnrichedVehicle {
                id: ffi.id,
                latitude: ffi.latitude,
                longitude: ffi.longitude,
                bearing: ffi.bearing,
                speed: ffi.speed,
                route_id: ffi.route_id,
                trip_id: ffi.trip_id,
                label: ffi.label,
                timestamp: ffi.timestamp,
                has_bearing: ffi.has_bearing,
                has_speed: ffi.has_speed,
                occupancy_status: ffi.occupancy_status,
                delay_seconds,
                has_delay,
                next_stop_id,
                next_arrival_time,
                has_next_stop,
            });
        }

        Ok(result)
    }

    /// Look up trip update data for a specific trip_id.
    ///
    /// Delay extraction priority:
    ///   1. TripUpdate.delay (field 5) — trip-level propagated delay. Highest
    ///      authority but rarely populated by Amtrak.
    ///   2. Last past StopTimeUpdate with an explicit delay value — reflects
    ///      the train's current running delay based on stops already served.
    ///      This is the primary source for Amtrak.
    ///   3. First future StopTimeUpdate's delay — forward-looking estimate
    ///      for trains not yet departed or when past stops lack delay data.
    ///
    /// The "next stop" finder skips SKIPPED stops (schedule_relationship = 1)
    /// so passengers aren't directed to a stop the train won't serve.
    pub fn get_trip_update(&self, trip_id: &str) -> Result<Option<TripUpdateSummary>, String> {
        let mgr = self
            .manager
            .lock()
            .map_err(|_| "Failed to acquire lock".to_string())?;

        let Some((delay_seconds, has_delay, next_stop_id, next_arrival_time, has_next_stop)) =
            mgr.get_trip_update_inner(trip_id)
        else {
            return Ok(None);
        };

        Ok(Some(TripUpdateSummary {
            delay_seconds,
            has_delay,
            next_stop_id,
            next_arrival_time,
            has_next_stop,
        }))
    }
}

/// Combined vehicle + enrichment returned by `gtfs_rt_get_active_enriched_vehicles`.
///
/// Merges `FFIVehicle` fields with the `TripUpdateSummary` fields so Swift needs
/// only a single FFI call per update cycle instead of N×2 calls (one
/// `gtfs_static_is_trip_active` + one `gtfs_rt_get_trip_update` per vehicle).
///
/// Free with `gtfs_rt_free_enriched_vehicles`.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFIEnrichedVehicle {
    // ── Core vehicle fields (mirrors FFIVehicle) ─────────────────────────────
    pub id: *const c_char,
    pub latitude: f64,
    pub longitude: f64,
    pub bearing: f32,
    pub speed: f32,
    pub route_id: *const c_char,
    pub trip_id: *const c_char,
    pub label: *const c_char,
    pub timestamp: i64,
    pub has_bearing: bool,
    pub has_speed: bool,
    pub occupancy_status: i32,
    // ── TripUpdate enrichment fields (mirrors TripUpdateSummary) ─────────────
    pub delay_seconds: i32,
    pub has_delay: bool,
    pub next_stop_id: *const c_char,
    pub next_arrival_time: i64,
    pub has_next_stop: bool,
}

/// Flat summary of a TripUpdate for FFI — avoids exposing arrays across the boundary.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct TripUpdateSummary {
    /// Overall trip delay in seconds (positive = late, negative = early).
    pub delay_seconds: i32,
    /// True if `delay_seconds` is meaningful (i.e., the feed provided delay data).
    pub has_delay: bool,
    /// C string of the next upcoming stop_id, or null.
    pub next_stop_id: *const c_char,
    /// Unix timestamp of the predicted arrival at `next_stop_id` (0 = unknown).
    pub next_arrival_time: i64,
    /// True if `next_stop_id` and `next_arrival_time` are populated.
    pub has_next_stop: bool,
}
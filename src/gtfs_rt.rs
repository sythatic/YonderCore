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
    // Field 5: trip-level delay in seconds. Experimental per the GTFS-RT spec;
    // many feeds leave it unset. Per-stop delay in StopTimeEvent is generally
    // more precise. IMPORTANT: per spec, delay in StopTimeUpdate takes
    // precedence over this field — trip-level delay only propagates until the
    // next stop that has
    // its own StopTimeUpdate delay value.
    #[prost(int32, optional, tag = "5")]
    pub delay: Option<i32>,
    /// TripProperties (field 6, experimental): required for DUPLICATED trips.
    /// Contains the new trip_id, start_date, start_time, shape_id for
    /// duplicated or replacement service.
    #[prost(message, optional, tag = "6")]
    pub trip_properties: Option<TripProperties>,
}

/// Updated properties for a trip (experimental).
/// Required for DUPLICATED trips to carry the new trip_id, start_date,
/// start_time. Also carries an optional shape_id and headsign.
#[derive(Clone, PartialEq, Message)]
pub struct TripProperties {
    /// New trip_id for a DUPLICATED trip (must differ from the static GTFS trip_id).
    #[prost(string, optional, tag = "1")]
    pub trip_id: Option<String>,
    /// Service date in YYYYMMDD format.  Required for DUPLICATED.
    #[prost(string, optional, tag = "2")]
    pub start_date: Option<String>,
    /// Departure start time (HH:MM:SS).  Required for DUPLICATED.
    #[prost(string, optional, tag = "3")]
    pub start_time: Option<String>,
    /// Optional override shape_id for this trip instance.
    #[prost(string, optional, tag = "4")]
    pub shape_id: Option<String>,
    /// Optional headsign override for this trip instance.
    #[prost(string, optional, tag = "5")]
    pub trip_headsign: Option<String>,
    /// Optional short name override for this trip instance.
    #[prost(string, optional, tag = "6")]
    pub trip_short_name: Option<String>,
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
    /// Per-stop schedule relationship (tag 5).
    /// 0=SCHEDULED, 1=SKIPPED, 2=NO_DATA, 3=UNSCHEDULED
    ///
    /// NOTE: For NO_DATA stops, arrival/departure must NOT be supplied.
    /// For UNSCHEDULED (frequency-based), `time` must be used in
    /// StopTimeEvent — `delay` must not be populated.
    #[prost(int32, optional, tag = "5")]
    pub schedule_relationship: Option<i32>,
    /// Predicted occupancy immediately after departure (experimental, tag 6).
    /// If provided, stop_sequence must also be provided.
    #[prost(enumeration = "OccupancyStatus", optional, tag = "6")]
    pub departure_occupancy_status: Option<i32>,
    /// Realtime updates to stop_times.txt properties (experimental, tag 7).
    #[prost(message, optional, tag = "7")]
    pub stop_time_properties: Option<StopTimeProperties>,
}

/// Realtime overrides for stop_times.txt properties (experimental).
#[derive(Clone, PartialEq, Message)]
pub struct StopTimeProperties {
    /// Assigns the vehicle to a different stop at this sequence.
    /// Must reference a stop_id in stops.txt with location_type=0.
    /// When set, stop_sequence must also be set on the StopTimeUpdate.
    #[prost(string, optional, tag = "1")]
    pub assigned_stop_id: Option<String>,
    /// Headsign shown on the vehicle at this stop.
    #[prost(string, optional, tag = "2")]
    pub stop_headsign: Option<String>,
    /// Updated drop-off type (0=regular, 1=none, 2=phone agency, 3=coordinate with driver).
    #[prost(int32, optional, tag = "3")]
    pub drop_off_type: Option<i32>,
    /// Updated pickup type.
    #[prost(int32, optional, tag = "4")]
    pub pickup_type: Option<i32>,
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
    /// Percentage occupancy (experimental, tag 10).
    #[prost(uint32, optional, tag = "10")]
    pub occupancy_percentage: Option<u32>,
    /// Per-carriage details for multi-carriage vehicles (experimental, tag 11).
    /// First element = first carriage in current direction of travel.
    #[prost(message, repeated, tag = "11")]
    pub multi_carriage_details: Vec<CarriageDetails>,
}

/// Per-carriage occupancy and identification details (experimental).
#[derive(Clone, PartialEq, Message)]
pub struct CarriageDetails {
    /// Internal carriage identifier.  Should be unique per vehicle.
    #[prost(string, optional, tag = "1")]
    pub id: Option<String>,
    /// Passenger-visible label (e.g. car number printed on the side).
    #[prost(string, optional, tag = "2")]
    pub label: Option<String>,
    /// Occupancy status for this carriage.  Defaults to NO_DATA_AVAILABLE.
    #[prost(enumeration = "OccupancyStatus", optional, tag = "3")]
    pub occupancy_status: Option<i32>,
    /// Occupancy percentage for this carriage (-1 = no data).
    #[prost(int32, optional, tag = "4")]
    pub occupancy_percentage: Option<i32>,
    /// 1-based position in the vehicle in the direction of travel (Required).
    #[prost(uint32, optional, tag = "5")]
    pub carriage_sequence: Option<u32>,
}

/// A service alert.  `header_text` and `description_text` are Required per spec.
/// `cause` and `effect` are Conditionally Required (required when cause_detail /
/// effect_detail are present).
#[derive(Clone, PartialEq, Message)]
pub struct Alert {
    /// Time ranges during which the alert is active.  If absent the alert is
    /// shown as long as it appears in the feed.
    #[prost(message, repeated, tag = "1")]
    pub active_period: Vec<TimeRange>,
    /// Entities affected by the alert.  At least one must be provided (Required).
    #[prost(message, repeated, tag = "5")]
    pub informed_entity: Vec<EntitySelector>,
    /// Machine-readable cause of the alert.
    #[prost(enumeration = "Cause", optional, tag = "6")]
    pub cause: Option<i32>,
    /// Machine-readable effect on service.
    #[prost(enumeration = "Effect", optional, tag = "7")]
    pub effect: Option<i32>,
    /// URL with additional information (TranslatedString).
    #[prost(message, optional, tag = "8")]
    pub url: Option<TranslatedString>,
    /// Short bolded header shown to riders (Required per spec).
    #[prost(message, optional, tag = "10")]
    pub header_text: Option<TranslatedString>,
    /// Full alert description (Required per spec).
    #[prost(message, optional, tag = "11")]
    pub description_text: Option<TranslatedString>,
    /// TTS version of header_text (optional).
    #[prost(message, optional, tag = "12")]
    pub tts_header_text: Option<TranslatedString>,
    /// TTS version of description_text (optional).
    #[prost(message, optional, tag = "13")]
    pub tts_description_text: Option<TranslatedString>,
    /// Severity of the alert (experimental).
    #[prost(enumeration = "SeverityLevel", optional, tag = "14")]
    pub severity_level: Option<i32>,
    /// cause_detail: agency-specific cause description (experimental).
    /// If populated, `cause` must also be provided.
    #[prost(message, optional, tag = "15")]
    pub cause_detail: Option<TranslatedString>,
    /// effect_detail: agency-specific effect description (experimental).
    /// If populated, `effect` must also be provided.
    #[prost(message, optional, tag = "16")]
    pub effect_detail: Option<TranslatedString>,
}

/// An internationalized string — one translation per language.
/// Used for Alert.header_text, description_text, url, etc.
#[derive(Clone, PartialEq, Message)]
pub struct TranslatedString {
    /// At least one translation must be provided.
    #[prost(message, repeated, tag = "1")]
    pub translation: Vec<Translation>,
}

impl TranslatedString {
    /// Return the best available string: prefer the given `lang` BCP-47 code,
    /// fall back to an untagged entry, then the first available translation.
    pub fn best(&self, lang: Option<&str>) -> Option<&str> {
        if self.translation.is_empty() { return None; }
        // 1. Exact language match
        if let Some(l) = lang {
            if let Some(t) = self.translation.iter().find(|t| t.language.as_deref() == Some(l)) {
                return Some(&t.text);
            }
        }
        // 2. Untagged / language-neutral entry
        if let Some(t) = self.translation.iter().find(|t| t.language.is_none() || t.language.as_deref() == Some("")) {
            return Some(&t.text);
        }
        // 3. First available
        Some(&self.translation[0].text)
    }

    /// Convenience: return the best English or untagged translation.
    pub fn best_en(&self) -> Option<&str> {
        self.best(Some("en"))
    }
}

/// A single localized string mapped to a BCP-47 language code.
#[derive(Clone, PartialEq, Message)]
pub struct Translation {
    /// UTF-8 text content (Required).
    #[prost(string, required, tag = "1")]
    pub text: String,
    /// BCP-47 language code (e.g. "en", "fr").  May be absent if the feed
    /// is monolingual and does not tag its translations.
    #[prost(string, optional, tag = "2")]
    pub language: Option<String>,
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
    /// direction_id from trips.txt (tag 6, experimental).
    /// If provided, route_id must also be provided.
    #[prost(uint32, optional, tag = "6")]
    pub direction_id: Option<u32>,
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
/// for extra/charter/special trips. Treat it the same as SCHEDULED.
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

/// Machine-readable cause of a service alert.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum Cause {
    UnknownCause      = 0,
    OtherCause        = 1,
    TechnicalProblem  = 2,
    Strike            = 3,
    Demonstration     = 4,
    Accident          = 5,
    Holiday           = 6,
    Weather           = 7,
    Maintenance       = 8,
    Construction      = 9,
    PoliceActivity    = 10,
    MedicalEmergency  = 11,
}

/// Effect of a service alert on the affected entity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum Effect {
    NoService          = 1,
    ReducedService     = 2,
    SignificantDelays  = 3,
    Detour             = 4,
    AdditionalService  = 5,
    ModifiedService    = 6,
    OtherEffect        = 7,
    UnknownEffect      = 8,
    StopMoved          = 9,
    NoEffect           = 10,
    AccessibilityIssue = 11,
}

/// Severity of a service alert (experimental).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum SeverityLevel {
    UnknownSeverity = 1,
    Info            = 2,
    Warning         = 3,
    Severe          = 4,
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
    /// VehicleStopStatus: 0=INCOMING_AT, 1=STOPPED_AT, 2=IN_TRANSIT_TO.
    /// -1 when the feed did not provide current_status (IN_TRANSIT_TO assumed per spec).
    pub current_status: i32,
    /// TripDescriptor.direction_id (0 or 1). -1 when absent.
    pub direction_id: i32,
    /// TripDescriptor.start_date as YYYYMMDD string, or NULL.
    pub start_date: *const c_char,
    /// TripDescriptor.start_time as HH:MM:SS string, or NULL.
    /// Required for frequency-based trips (frequencies.txt exact_times=0).
    pub start_time: *const c_char,
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
        // Some feeds emit multiple entities for the same physical vehicle
        // when it works a through-service connection (two trip_ids, one
        // vehicle). When VehicleDescriptor.id is set it uniquely identifies
        // the equipment; keep only the most recent timestamp for each id.
        //
        // IMPORTANT: do NOT fall back to VehicleDescriptor.label for dedup.
        // label is the human-readable run number, and an operator can run two
        // genuine services with the same number on different branches (or when
        // schedule changeovers overlap). Deduping by label would silently drop
        // one of them. If id is absent, push unconditionally.
        let mut seen_vehicle_ids: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();

        // Deduplicate TripUpdates by trip_id.
        // Spec: "there can be at most one TripUpdate entity for each actual
        // trip instance."  Malformed feeds sometimes duplicate them. Keep the
        // one with the highest timestamp (most recently updated prediction).
        let mut seen_trip_ids: std::collections::HashMap<String, usize> =
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
            if let Some(mut trip_update) = entity.trip_update {
                // Defensively sort stop_time_updates by stop_sequence so that
                // the "next stop" finder always scans in the correct order,
                // even when a non-compliant feed emits them out of order.
                // Per spec, stop_time_updates must be ordered by stop_sequence.
                // Entries that lack stop_sequence (None) are placed last.
                trip_update.stop_time_update.sort_by_key(|stu| stu.stop_sequence.unwrap_or(u32::MAX));

                let tid: Option<String> = trip_update.trip.trip_id.clone()
                    .filter(|s| !s.is_empty());

                if let Some(tid) = tid {
                    let new_ts = trip_update.timestamp.unwrap_or(0);
                    if let Some(&existing_idx) = seen_trip_ids.get(&tid) {
                        let existing_ts = self.trip_updates[existing_idx].timestamp.unwrap_or(0);
                        if new_ts > existing_ts {
                            self.trip_updates[existing_idx] = trip_update;
                        }
                    } else {
                        let idx = self.trip_updates.len();
                        seen_trip_ids.insert(tid, idx);
                        self.trip_updates.push(trip_update);
                    }
                } else {
                    // No trip_id — push unconditionally; cannot safely dedup.
                    self.trip_updates.push(trip_update);
                }
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
                if !lat.is_finite() || !lon.is_finite() {
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

        // GTFS-RT best practice: for UNSCHEDULED (frequency-based, exact_times=0)
        // trips, StopTimeEvent.delay must not be populated — only `time` is used
        // because there is no fixed schedule to be relative to.  Skip delay
        // extraction entirely for UNSCHEDULED trips.
        let trip_schedule_rel = tu.trip.schedule_relationship.unwrap_or(0);
        let is_unscheduled = trip_schedule_rel == ScheduleRelationship::Unscheduled as i32;

        let mut delay_seconds: Option<i32> = if is_unscheduled { None } else { tu.delay };

        if !is_unscheduled && delay_seconds.is_none() {
            // Priority 2: delay from the last past stop (most current running deviation).
            // Use arrival time (when vehicle reaches the stop) to determine "past".
            delay_seconds = tu
                .stop_time_update
                .iter()
                .filter(|stu| {
                    let rel = stu.schedule_relationship.unwrap_or(
                        stop_time_schedule_relationship::SCHEDULED,
                    );
                    if rel == stop_time_schedule_relationship::SKIPPED
                        || rel == stop_time_schedule_relationship::NO_DATA
                        || rel == stop_time_schedule_relationship::UNSCHEDULED
                    {
                        return false;
                    }
                    // Use arrival time preferentially for "has the vehicle reached this stop?"
                    let t = stu
                        .arrival
                        .as_ref()
                        .and_then(|e| e.time)
                        .or_else(|| stu.departure.as_ref().and_then(|e| e.time))
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

        if !is_unscheduled && delay_seconds.is_none() {
            // Priority 3: first future stop's delay — forward-looking estimate
            // for trains not yet departed or when past stops lack delay data.
            delay_seconds = tu
                .stop_time_update
                .iter()
                .filter(|stu| {
                    let rel = stu.schedule_relationship.unwrap_or(
                        stop_time_schedule_relationship::SCHEDULED,
                    );
                    if rel == stop_time_schedule_relationship::SKIPPED
                        || rel == stop_time_schedule_relationship::NO_DATA
                        || rel == stop_time_schedule_relationship::UNSCHEDULED
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

        // If TripUpdate.delay (trip-level) is set and drastically differs from
        // the per-stop delays (>1 hour gap), prefer the trip-level value as it
        // may reflect a correction the stop-level data hasn't propagated yet.
        // Per spec: trip-level delay is only propagated until the next stop
        // with its own StopTimeUpdate delay, so per-stop normally takes precedence.
        if !is_unscheduled {
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
        }

        let has_delay = delay_seconds.is_some();

        // Next stop: the first stop whose arrival (or departure if arrival is
        // absent) is in the future.  Skip SKIPPED and NO_DATA stops.
        //
        // Prefer arrival time over departure time for the "is this in the future?"
        // test — riders care about when they need to be at the platform, not
        // when the train leaves.  Also check StopTimeProperties.assigned_stop_id
        // so we return the correct platform stop_id when a stop has been reassigned.
        let next = tu.stop_time_update.iter().find(|stu| {
            let rel = stu
                .schedule_relationship
                .unwrap_or(stop_time_schedule_relationship::SCHEDULED);
            if rel == stop_time_schedule_relationship::SKIPPED
                || rel == stop_time_schedule_relationship::NO_DATA
            {
                return false;
            }
            // Arrival-first for "is this stop still upcoming?"
            let t = stu
                .arrival
                .as_ref()
                .and_then(|e| e.time)
                .or_else(|| stu.departure.as_ref().and_then(|e| e.time));
            t.map(|ts| ts > now).unwrap_or(false)
        });

        // Prefer assigned_stop_id (platform reassignment) over the nominal stop_id.
        let next_stop_id_str: Option<&str> = next.and_then(|s| {
            s.stop_time_properties
                .as_ref()
                .and_then(|p| p.assigned_stop_id.as_deref())
                .filter(|id| !id.is_empty())
                .or_else(|| s.stop_id.as_deref())
        });

        let next_stop_id: *const c_char = next_stop_id_str
            .and_then(|s| CString::new(s).ok())
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

        // 0. Coordinate sanity check.
        //
        //    Reject IEEE 754 non-finite values (NaN / ±Inf) and coordinates
        //    outside the WGS-84 valid range — these are malformed protobuf
        //    values that would cause undefined map rendering.
        //
        //    Do NOT reject (0.0, 0.0) here.  Some feeds use this as a
        //    "no GPS fix" sentinel for a vehicle that is still active.
        //    Callers should detect lat == 0 && lon == 0 and fall back to
        //    shape interpolation or hide the icon gracefully.  Dropping the
        //    vehicle here causes it to vanish from the map whenever it passes
        //    through a dead-zone with no GPS fix.
        let lat = pos.latitude;
        let lon = pos.longitude;
        if !lat.is_finite()
            || !lon.is_finite()
            || lat < -90.0
            || lat > 90.0
            || lon < -180.0
            || lon > 180.0
        {
            return None;
        }

        // 1. Staleness check.
        //
        //    VehiclePosition.timestamp is optional per the GTFS-RT spec. When
        //    absent the field is unset (prost decodes this as None). Applying
        //    unwrap_or(0) and then checking age > 0 would silently drop every
        //    vehicle without a timestamp.
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

        // VehicleDescriptor.label — human-readable run number or name per the GTFS-RT spec
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

        // TripDescriptor.direction_id: 0 or 1; -1 when absent.
        let direction_id: i32 = vehicle
            .trip
            .as_ref()
            .and_then(|t| t.direction_id.map(|d| d as i32))
            .unwrap_or(-1);

        // TripDescriptor.start_date (YYYYMMDD) — needed to disambiguate
        // overnight trips and required for frequency-based trips.
        let start_date = vehicle
            .trip
            .as_ref()
            .and_then(|t| t.start_date.as_ref())
            .filter(|s| !s.is_empty())
            .and_then(|s| CString::new(s.as_str()).ok());

        // TripDescriptor.start_time (HH:MM:SS) — required for
        // frequency-based trips (frequencies.txt exact_times=0).
        let start_time = vehicle
            .trip
            .as_ref()
            .and_then(|t| t.start_time.as_ref())
            .filter(|s| !s.is_empty())
            .and_then(|s| CString::new(s.as_str()).ok());

        // VehicleStopStatus: map to i32 sentinel -1 when absent.
        // Per spec, when current_status is absent IN_TRANSIT_TO is assumed.
        // We still pass -1 so Swift can distinguish "not provided" from
        // "explicitly IN_TRANSIT_TO=2".
        let current_status: i32 = vehicle.current_status.unwrap_or(-1);

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
            occupancy_status: vehicle.occupancy_status.unwrap_or(OccupancyStatus::NoDataAvailable as i32),
            current_status,
            direction_id,
            start_date: start_date.map_or(std::ptr::null(), |s| s.into_raw()),
            start_time: start_time.map_or(std::ptr::null(), |s| s.into_raw()),
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

    /// Return all vehicles from the RT VehiclePositions feed, enriched with
    /// TripUpdate (delay / next-stop) data where available.
    ///
    /// `now_local` is retained in the signature for API compatibility but is
    /// no longer used to gate vehicles.  Every vehicle present in the RT feed
    /// is included — the RT feed is the authoritative source of truth for what
    /// is currently moving.  Static schedule windows are intentionally NOT used
    /// here because they cause live trains to disappear due to timezone edge
    /// cases, unusual service days, and trip_id prefix mismatches between the
    /// RT and static feeds.
    ///
    /// Vehicles with (lat == 0, lon == 0) are passed through with those
    /// coordinates intact; Swift should detect this sentinel and fall back to
    /// gtfs_interpolate_position or hide the icon.
    pub fn get_active_enriched_vehicles(&self, _now_local: i64) -> Result<Vec<FFIEnrichedVehicle>, String> {
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

            // Step 2 — extract trip_id for TripUpdate enrichment.
            // Note: we intentionally do NOT call gtfs_static_is_trip_active here.
            // If a vehicle is present in the RT VehiclePositions feed it is by
            // definition active. Suppressing it based on static schedule windows
            // causes live trains to disappear due to timezone edge cases, unusual
            // service days, or trip_id prefix mismatches between the RT and static
            // feeds.  The static gate belongs only in search/timetable UIs that
            // need to show scheduled-but-not-yet-departed trains, not on the map.
            let trip_id_cstr: Option<&CStr> = if ffi.trip_id.is_null() {
                None
            } else {
                // SAFETY: pointer was just produced by CString::into_raw() in vehicle_to_ffi.
                Some(unsafe { CStr::from_ptr(ffi.trip_id) })
            };

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
                current_status: ffi.current_status,
                direction_id: ffi.direction_id,
                start_date: ffi.start_date,
                start_time: ffi.start_time,
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
    ///      authority but not always present in the feed.
    ///   2. Last past StopTimeUpdate with an explicit delay value — reflects
    ///      the vehicle's current running delay based on stops already served.
    ///      This is the most reliable source when trip-level delay is absent.
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
    /// VehicleStopStatus: 0=INCOMING_AT, 1=STOPPED_AT, 2=IN_TRANSIT_TO.
    /// -1 when the feed did not provide current_status (IN_TRANSIT_TO assumed per spec).
    pub current_status: i32,
    /// TripDescriptor.direction_id (0 or 1).  -1 when absent.
    pub direction_id: i32,
    /// TripDescriptor.start_date as YYYYMMDD string, or NULL.
    pub start_date: *const c_char,
    /// TripDescriptor.start_time as HH:MM:SS string, or NULL.
    pub start_time: *const c_char,
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

// ── FFI ───────────────────────────────────────────────────────────────────────

/// Create a new GTFS-RT manager.
#[no_mangle]
pub extern "C" fn gtfs_rt_new() -> *mut GtfsRtCore {
    Box::into_raw(Box::new(GtfsRtCore::new()))
}

/// Parse a GTFS-RT protobuf blob into `core`, replacing any previously parsed
/// feed data.  Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn gtfs_rt_parse(
    core:     *mut GtfsRtCore,
    data:     *const u8,
    data_len: usize,
) -> i32 {
    if core.is_null() || data.is_null() { return -1; }
    unsafe {
        let data_slice = std::slice::from_raw_parts(data, data_len);
        match (&*core).parse(data_slice) { Ok(_) => 0, Err(_) => -1 }
    }
}

/// Get all vehicles from the most recently parsed feed.
/// Sets `*out_count`.  Free with `gtfs_rt_free_vehicles`.
#[no_mangle]
pub extern "C" fn gtfs_rt_get_vehicles(
    core:      *const GtfsRtCore,
    out_count: *mut usize,
) -> *mut FFIVehicle {
    if core.is_null() || out_count.is_null() { return std::ptr::null_mut(); }
    unsafe {
        let vehicles = match (&*core).get_vehicles() {
            Ok(v)  => v,
            Err(_) => { *out_count = 0; return std::ptr::null_mut(); }
        };
        *out_count = vehicles.len();
        if vehicles.is_empty() { return std::ptr::null_mut(); }
        Box::into_raw(vehicles.into_boxed_slice()) as *mut FFIVehicle
    }
}

/// Number of vehicles in the most recently parsed feed.
#[no_mangle]
pub extern "C" fn gtfs_rt_vehicle_count(core: *const GtfsRtCore) -> usize {
    if core.is_null() { return 0; }
    unsafe { (&*core).vehicle_count().unwrap_or(0) }
}

/// Free a vehicle array returned by `gtfs_rt_get_vehicles`.
#[no_mangle]
pub extern "C" fn gtfs_rt_free_vehicles(vehicles: *mut FFIVehicle, count: usize) {
    if vehicles.is_null() || count == 0 { return; }
    unsafe {
        let slice = std::slice::from_raw_parts_mut(vehicles, count);
        for v in slice.iter() {
            if !v.id.is_null()         { let _ = CString::from_raw(v.id as *mut c_char); }
            if !v.route_id.is_null()   { let _ = CString::from_raw(v.route_id as *mut c_char); }
            if !v.trip_id.is_null()    { let _ = CString::from_raw(v.trip_id as *mut c_char); }
            if !v.label.is_null()      { let _ = CString::from_raw(v.label as *mut c_char); }
            if !v.start_date.is_null() { let _ = CString::from_raw(v.start_date as *mut c_char); }
            if !v.start_time.is_null() { let _ = CString::from_raw(v.start_time as *mut c_char); }
        }
        drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(vehicles, count)));
    }
}

/// Free a `GtfsRtCore` allocated by `gtfs_rt_new`.
#[no_mangle]
pub extern "C" fn gtfs_rt_free(core: *mut GtfsRtCore) {
    if !core.is_null() { unsafe { let _ = Box::from_raw(core); } }
}

/// Look up trip-update data for `trip_id`.
/// Returns a heap-allocated `TripUpdateSummary`, or NULL if not found.
/// Free with `gtfs_rt_free_trip_update`.
#[no_mangle]
pub extern "C" fn gtfs_rt_get_trip_update(
    core:    *const GtfsRtCore,
    trip_id: *const c_char,
) -> *mut TripUpdateSummary {
    if core.is_null() || trip_id.is_null() { return std::ptr::null_mut(); }
    unsafe {
        let trip_id_str = match CStr::from_ptr(trip_id).to_str() {
            Ok(s)  => s,
            Err(_) => return std::ptr::null_mut(),
        };
        match (&*core).get_trip_update(trip_id_str) {
            Ok(Some(summary)) => Box::into_raw(Box::new(summary)),
            _                 => std::ptr::null_mut(),
        }
    }
}

/// Free a `TripUpdateSummary` returned by `gtfs_rt_get_trip_update`.
/// Also frees the heap-allocated `next_stop_id` C string inside the struct.
#[no_mangle]
pub extern "C" fn gtfs_rt_free_trip_update(summary: *mut TripUpdateSummary) {
    if summary.is_null() { return; }
    unsafe {
        let s = &*summary;
        if !s.next_stop_id.is_null() {
            let _ = CString::from_raw(s.next_stop_id as *mut c_char);
        }
        let _ = Box::from_raw(summary);
    }
}

/// Return all vehicles enriched with TripUpdate data in a single FFI call,
/// replacing the previous N×`gtfs_static_is_trip_active` + N×`gtfs_rt_get_trip_update`
/// pattern.
///
/// `now_local` is retained for API compatibility but is no longer used to
/// gate vehicles — the RT feed is the authoritative source of what is moving.
///
/// Free with `gtfs_rt_free_enriched_vehicles`.
#[no_mangle]
pub extern "C" fn gtfs_rt_get_active_enriched_vehicles(
    core:        *const GtfsRtCore,
    now_local: i64,
    out_count:   *mut usize,
) -> *mut FFIEnrichedVehicle {
    if core.is_null() || out_count.is_null() { return std::ptr::null_mut(); }
    unsafe {
        let vehicles = match (&*core).get_active_enriched_vehicles(now_local) {
            Ok(v)  => v,
            Err(_) => { *out_count = 0; return std::ptr::null_mut(); }
        };
        *out_count = vehicles.len();
        if vehicles.is_empty() { return std::ptr::null_mut(); }
        Box::into_raw(vehicles.into_boxed_slice()) as *mut FFIEnrichedVehicle
    }
}

/// Free an array returned by `gtfs_rt_get_active_enriched_vehicles`.
/// Frees every heap-allocated C string field before dropping the slice.
#[no_mangle]
pub extern "C" fn gtfs_rt_free_enriched_vehicles(
    vehicles: *mut FFIEnrichedVehicle,
    count:    usize,
) {
    if vehicles.is_null() || count == 0 { return; }
    unsafe {
        let slice = std::slice::from_raw_parts(vehicles, count);
        for v in slice.iter() {
            if !v.id.is_null()          { let _ = CString::from_raw(v.id as *mut c_char); }
            if !v.route_id.is_null()    { let _ = CString::from_raw(v.route_id as *mut c_char); }
            if !v.trip_id.is_null()     { let _ = CString::from_raw(v.trip_id as *mut c_char); }
            if !v.label.is_null()       { let _ = CString::from_raw(v.label as *mut c_char); }
            if !v.start_date.is_null()  { let _ = CString::from_raw(v.start_date as *mut c_char); }
            if !v.start_time.is_null()  { let _ = CString::from_raw(v.start_time as *mut c_char); }
            if !v.next_stop_id.is_null(){ let _ = CString::from_raw(v.next_stop_id as *mut c_char); }
        }
        drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(vehicles, count)));
    }
}

// ── Alerts FFI ────────────────────────────────────────────────────────────────

/// A service alert as returned by `gtfs_rt_get_alerts`.
///
/// All string pointers may be NULL.  `active_period_start` and
/// `active_period_end` are the first active-period's bounds (0 = unbounded).
/// `cause` and `effect` are the raw enum int32 values from the spec (0 = unknown).
///
/// Free the entire array with `gtfs_rt_free_alerts`.
#[repr(C)]
#[derive(Debug)]
pub struct FFIAlert {
    /// Short bolded header (Required per spec).  NULL if missing in feed.
    pub header_text:       *const c_char,
    /// Full alert body (Required per spec).  NULL if missing in feed.
    pub description_text:  *const c_char,
    /// Optional URL for more info.  NULL if not provided.
    pub url:               *const c_char,
    /// Cause enum value (Cause::* constants).  0 = UNKNOWN_CAUSE.
    pub cause:             i32,
    /// Effect enum value (Effect::* constants).  0 = UNKNOWN_EFFECT.
    pub effect:            i32,
    /// SeverityLevel enum value.  0 = not provided.
    pub severity_level:    i32,
    /// First active-period start (POSIX seconds).  0 = unbounded / not set.
    pub active_period_start: i64,
    /// First active-period end (POSIX seconds).  0 = unbounded / not set.
    pub active_period_end:   i64,
    /// Pipe-separated list of affected route_ids extracted from informed_entity.
    /// NULL if none.  e.g. "Acela|NEC"
    pub affected_route_ids: *const c_char,
    /// Pipe-separated list of affected stop_ids.  NULL if none.
    pub affected_stop_ids:  *const c_char,
}

/// Rust-level helper: build an `FFIAlert` from an `Alert`.
fn alert_to_ffi(alert: &Alert) -> FFIAlert {
    let lang = Some("en");

    let header_text = alert.header_text.as_ref()
        .and_then(|t| t.best(lang))
        .and_then(|s| CString::new(s).ok())
        .map_or(std::ptr::null(), |s| s.into_raw());

    let description_text = alert.description_text.as_ref()
        .and_then(|t| t.best(lang))
        .and_then(|s| CString::new(s).ok())
        .map_or(std::ptr::null(), |s| s.into_raw());

    let url = alert.url.as_ref()
        .and_then(|t| t.best(lang))
        .and_then(|s| CString::new(s).ok())
        .map_or(std::ptr::null(), |s| s.into_raw());

    let cause          = alert.cause.unwrap_or(0);
    let effect         = alert.effect.unwrap_or(0);
    let severity_level = alert.severity_level.unwrap_or(0);

    let (active_period_start, active_period_end) = alert.active_period.first()
        .map(|ap| (ap.start.unwrap_or(0) as i64, ap.end.unwrap_or(0) as i64))
        .unwrap_or((0, 0));

    let affected_route_ids = pipe_join_unique(
        alert.informed_entity.iter().filter_map(|e| e.route_id.as_deref()),
    );
    let affected_stop_ids = pipe_join_unique(
        alert.informed_entity.iter().filter_map(|e| e.stop_id.as_deref()),
    );

    FFIAlert {
        header_text,
        description_text,
        url,
        cause,
        effect,
        severity_level,
        active_period_start,
        active_period_end,
        affected_route_ids,
        affected_stop_ids,
    }
}

/// Collect non-empty strings from `iter`, deduplicate them, join with `"|"`,
/// and return a heap-allocated C string.  Returns `null` when the result
/// would be empty.  The caller must free the pointer with `CString::from_raw`.
fn pipe_join_unique<'a>(iter: impl Iterator<Item = &'a str>) -> *const c_char {
    let mut ids: Vec<&str> = iter.filter(|s| !s.is_empty()).collect();
    if ids.is_empty() { return std::ptr::null(); }
    ids.sort_unstable();
    ids.dedup();
    CString::new(ids.join("|")).ok()
        .map_or(std::ptr::null(), |s| s.into_raw())
}

/// Return all service alerts from the most recently parsed feed.
/// Sets `*out_count`.  Returns NULL when there are no alerts.
/// Free with `gtfs_rt_free_alerts`.
#[no_mangle]
pub extern "C" fn gtfs_rt_get_alerts(
    core:      *const GtfsRtCore,
    out_count: *mut usize,
) -> *mut FFIAlert {
    if core.is_null() || out_count.is_null() { return std::ptr::null_mut(); }
    unsafe {
        let mgr = match (&*core).manager.lock() {
            Ok(g)  => g,
            Err(_) => { *out_count = 0; return std::ptr::null_mut(); }
        };
        let ffi_alerts: Vec<FFIAlert> = mgr.alerts.iter().map(alert_to_ffi).collect();
        *out_count = ffi_alerts.len();
        if ffi_alerts.is_empty() { return std::ptr::null_mut(); }
        Box::into_raw(ffi_alerts.into_boxed_slice()) as *mut FFIAlert
    }
}

/// Number of service alerts in the most recently parsed feed.
#[no_mangle]
pub extern "C" fn gtfs_rt_alert_count(core: *const GtfsRtCore) -> usize {
    if core.is_null() { return 0; }
    unsafe {
        (&*core).manager.lock().map(|m| m.alerts.len()).unwrap_or(0)
    }
}

/// Free an array returned by `gtfs_rt_get_alerts`.
#[no_mangle]
pub extern "C" fn gtfs_rt_free_alerts(alerts: *mut FFIAlert, count: usize) {
    if alerts.is_null() || count == 0 { return; }
    unsafe {
        let slice = std::slice::from_raw_parts(alerts, count);
        for a in slice.iter() {
            if !a.header_text.is_null()       { let _ = CString::from_raw(a.header_text as *mut c_char); }
            if !a.description_text.is_null()  { let _ = CString::from_raw(a.description_text as *mut c_char); }
            if !a.url.is_null()               { let _ = CString::from_raw(a.url as *mut c_char); }
            if !a.affected_route_ids.is_null(){ let _ = CString::from_raw(a.affected_route_ids as *mut c_char); }
            if !a.affected_stop_ids.is_null() { let _ = CString::from_raw(a.affected_stop_ids as *mut c_char); }
        }
        drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(alerts, count)));
    }
}
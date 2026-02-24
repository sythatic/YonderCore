use prost::Message;
use std::ffi::{CString, c_char};

// MARK: - Protobuf Message Definitions
// These match the official GTFS-RT specification

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
}

#[derive(Clone, PartialEq, Message)]
pub struct StopTimeEvent {
    #[prost(int32, optional, tag = "1")]
    pub delay: Option<i32>,
    #[prost(int64, optional, tag = "2")]
    pub time: Option<i64>,
    #[prost(int32, optional, tag = "3")]
    pub uncertainty: Option<i32>,
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
}

#[derive(Clone, PartialEq, Message)]
pub struct Alert {
    #[prost(message, repeated, tag = "1")]
    pub active_period: Vec<TimeRange>,
    #[prost(message, repeated, tag = "5")]
    pub informed_entity: Vec<EntitySelector>,
    #[prost(enumeration = "AlertCause", optional, tag = "6")]
    pub cause: Option<i32>,
    #[prost(enumeration = "AlertEffect", optional, tag = "7")]
    pub effect: Option<i32>,
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
    #[prost(float, optional, tag = "4")]
    pub odometer: Option<f32>,
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

// Enumerations
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum Incrementality {
    FullDataset = 0,
    Differential = 1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum ScheduleRelationship {
    Scheduled = 0,
    Added = 1,
    Unscheduled = 2,
    Canceled = 3,
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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum AlertCause {
    UnknownCause = 0,
    OtherCause = 1,
    TechnicalProblem = 2,
    Strike = 3,
    Demonstration = 4,
    Accident = 5,
    Holiday = 6,
    Weather = 7,
    Maintenance = 8,
    Construction = 9,
    PoliceActivity = 10,
    MedicalEmergency = 11,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum AlertEffect {
    NoService = 1,
    ReducedService = 2,
    SignificantDelays = 3,
    Detour = 4,
    AdditionalService = 5,
    ModifiedService = 6,
    OtherEffect = 7,
    UnknownEffect = 8,
    StopMoved = 9,
}

// MARK: - Simplified structs for FFI
//
// FFIVehicle is a safe Rust-side struct. It is converted to CVehicle only at
// the FFI boundary (in lib.rs), ensuring Rust owns the strings until freed.

#[derive(Debug, Clone)]
pub struct FFIVehicle {
    pub id: String,
    pub latitude: f64,
    pub longitude: f64,
    pub bearing: f32,
    pub speed: f32,
    pub route_id: Option<String>,
    pub trip_id: Option<String>,
    pub timestamp: i64,
    pub has_bearing: bool,
    pub has_speed: bool,
    pub occupancy_status: i32,
}

#[derive(Debug, Clone)]
pub struct FFITripUpdate {
    pub trip_id: Option<String>,
    pub route_id: Option<String>,
    pub vehicle_id: Option<String>,
    pub timestamp: i64,
    pub delay: i32,
    pub has_delay: bool,
    pub stop_time_updates_count: usize,
}

#[derive(Debug, Clone)]
pub struct FFIStopTimeUpdate {
    pub stop_id: Option<String>,
    pub stop_sequence: u32,
    pub arrival_delay: i32,
    pub arrival_time: i64,
    pub departure_delay: i32,
    pub departure_time: i64,
    pub has_arrival: bool,
    pub has_departure: bool,
}

/// C-compatible vehicle struct with owned string pointers — only constructed at the FFI boundary.
///
/// Field types match YonderCore.h exactly:
///   - id/route_id/trip_id are `*mut c_char` (owned by Rust, freed via gtfs_rt_free_vehicles)
///   - numeric fields match their C counterparts
///
/// The caller MUST free the whole array with gtfs_rt_free_vehicles(ptr, count).
/// Individual fields must NEVER be freed by the C/Swift side directly.
#[repr(C)]
pub struct CVehicle {
    /// Owned UTF-8 C string. Never null.
    pub id: *mut c_char,
    pub latitude: f64,
    pub longitude: f64,
    pub bearing: f32,
    pub speed: f32,
    /// Owned UTF-8 C string, or NULL if not present.
    pub route_id: *mut c_char,
    /// Owned UTF-8 C string, or NULL if not present.
    pub trip_id: *mut c_char,
    pub timestamp: i64,
    pub has_bearing: bool,
    pub has_speed: bool,
    pub occupancy_status: i32,
}

impl CVehicle {
    /// Convert a safe FFIVehicle into a C-compatible struct.
    /// The returned struct owns its strings via CString::into_raw().
    /// Must be consumed with CVehicle::free_owned() to avoid leaks.
    pub fn from_ffi(v: FFIVehicle) -> Self {
        let id = CString::new(v.id).unwrap_or_default().into_raw();
        let route_id = v.route_id
            .and_then(|s| CString::new(s).ok())
            .map(|s| s.into_raw())
            .unwrap_or(std::ptr::null_mut());
        let trip_id = v.trip_id
            .and_then(|s| CString::new(s).ok())
            .map(|s| s.into_raw())
            .unwrap_or(std::ptr::null_mut());

        CVehicle {
            id,
            latitude: v.latitude,
            longitude: v.longitude,
            bearing: v.bearing,
            speed: v.speed,
            route_id,
            trip_id,
            timestamp: v.timestamp,
            has_bearing: v.has_bearing,
            has_speed: v.has_speed,
            occupancy_status: v.occupancy_status,
        }
    }

    /// Consume this struct and free all owned string pointers.
    ///
    /// Takes `self` by value (not `&self`) so it is impossible to call twice —
    /// the compiler enforces single-use, preventing double-free.
    ///
    /// # Safety
    /// All non-null pointer fields must have been produced by CString::into_raw().
    pub unsafe fn free_owned(self) {
        if !self.id.is_null() {
            drop(CString::from_raw(self.id));
        }
        if !self.route_id.is_null() {
            drop(CString::from_raw(self.route_id));
        }
        if !self.trip_id.is_null() {
            drop(CString::from_raw(self.trip_id));
        }
        // The struct itself is dropped at the end of this scope.
    }
}

// MARK: - GTFS-RT Manager

pub struct GtfsRtManager {
    vehicles: Vec<VehiclePosition>,
    /// Entity-level IDs parallel to `vehicles` — used as fallback when the
    /// nested VehicleDescriptor has no id field.
    vehicle_entity_ids: Vec<String>,
    trip_updates: Vec<TripUpdate>,
    alerts: Vec<Alert>,
    last_update: Option<u64>,
}

impl GtfsRtManager {
    pub fn new() -> Self {
        Self {
            vehicles: Vec::new(),
            vehicle_entity_ids: Vec::new(),
            trip_updates: Vec::new(),
            alerts: Vec::new(),
            last_update: None,
        }
    }

    /// Parse GTFS-RT protobuf data
    pub fn parse_feed(&mut self, data: &[u8]) -> Result<(), String> {
        let feed = FeedMessage::decode(data)
            .map_err(|e| format!("Failed to decode protobuf: {}", e))?;

        // Clear existing data
        self.vehicles.clear();
        self.vehicle_entity_ids.clear();
        self.trip_updates.clear();
        self.alerts.clear();

        // Store feed timestamp
        self.last_update = feed.header.timestamp;

        // Extract entities
        for entity in feed.entity {
            if let Some(vehicle) = entity.vehicle {
                self.vehicle_entity_ids.push(entity.id.clone());
                self.vehicles.push(vehicle);
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

    /// Get all vehicles as FFI-compatible structs
    pub fn get_vehicles(&self) -> Vec<FFIVehicle> {
        self.vehicles
            .iter()
            .zip(self.vehicle_entity_ids.iter())
            .filter_map(|(v, entity_id)| self.vehicle_to_ffi(v, entity_id))
            .collect()
    }

    /// Get vehicles within a bounding box
    pub fn get_vehicles_in_region(
        &self,
        min_lat: f64,
        max_lat: f64,
        min_lon: f64,
        max_lon: f64,
    ) -> Vec<FFIVehicle> {
        self.vehicles
            .iter()
            .zip(self.vehicle_entity_ids.iter())
            .filter_map(|(v, entity_id)| {
                let pos = v.position.as_ref()?;
                let lat = pos.latitude as f64;
                let lon = pos.longitude as f64;

                if lat >= min_lat && lat <= max_lat && lon >= min_lon && lon <= max_lon {
                    self.vehicle_to_ffi(v, entity_id)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get trip updates
    pub fn get_trip_updates(&self) -> &[TripUpdate] {
        &self.trip_updates
    }

    /// Find trip update by trip ID
    pub fn find_trip_update(&self, trip_id: &str) -> Option<&TripUpdate> {
        self.trip_updates.iter().find(|tu| {
            tu.trip
                .trip_id
                .as_ref()
                .map(|id| id.as_str() == trip_id)
                .unwrap_or(false)
        })
    }

    /// Count vehicles
    pub fn vehicle_count(&self) -> usize {
        self.vehicles.len()
    }

    /// Count trip updates
    pub fn trip_update_count(&self) -> usize {
        self.trip_updates.len()
    }

    /// Convert VehiclePosition to safe FFI struct.
    /// entity_id is used as fallback if no nested vehicle descriptor id.
    fn vehicle_to_ffi(&self, vehicle: &VehiclePosition, entity_id: &str) -> Option<FFIVehicle> {
        let pos = vehicle.position.as_ref()?;

        // Prefer vehicle.vehicle.id, fall back to the feed entity id
        let id = vehicle
            .vehicle
            .as_ref()
            .and_then(|v| v.id.as_deref())
            .filter(|s| !s.is_empty())
            .unwrap_or(entity_id)
            .to_string();

        if id.is_empty() {
            return None;
        }

        let route_id = vehicle
            .trip
            .as_ref()
            .and_then(|t| t.route_id.clone());

        let trip_id = vehicle
            .trip
            .as_ref()
            .and_then(|t| t.trip_id.clone());

        Some(FFIVehicle {
            id,
            latitude: pos.latitude as f64,
            longitude: pos.longitude as f64,
            bearing: pos.bearing.unwrap_or(0.0),
            speed: pos.speed.unwrap_or(0.0),
            route_id,
            trip_id,
            timestamp: vehicle.timestamp.unwrap_or(0) as i64,
            has_bearing: pos.bearing.is_some(),
            has_speed: pos.speed.is_some(),
            occupancy_status: vehicle.occupancy_status.unwrap_or(0),
        })
    }
}

// MARK: - Thread-safe wrapper
//
// Uses parking_lot::RwLock instead of std::sync::RwLock:
//   - No lock poisoning: a panic in one thread doesn't permanently break the lock
//   - Faster: smaller overhead on the uncontended path
//   - GTFS-RT is read-heavy (many callers querying vehicles between parses),
//     so allowing concurrent readers improves throughput.

pub struct GtfsRtCore {
    manager: parking_lot::RwLock<GtfsRtManager>,
}

impl GtfsRtCore {
    pub fn new() -> Self {
        Self {
            manager: parking_lot::RwLock::new(GtfsRtManager::new()),
        }
    }

    pub fn parse(&self, data: &[u8]) -> Result<(), String> {
        self.manager.write().parse_feed(data)
    }

    pub fn get_vehicles(&self) -> Result<Vec<FFIVehicle>, String> {
        Ok(self.manager.read().get_vehicles())
    }

    pub fn vehicle_count(&self) -> Result<usize, String> {
        Ok(self.manager.read().vehicle_count())
    }
}
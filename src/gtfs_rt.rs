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

// MARK: - Simplified structs for FFI

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
    pub label: *const c_char,   // VehicleDescriptor.label — human-readable name
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

    /// Parse GTFS-RT protobuf data
    pub fn parse_feed(&mut self, data: &[u8]) -> Result<(), String> {
        let feed = FeedMessage::decode(data)
            .map_err(|e| format!("Failed to decode protobuf: {}", e))?;

        // Clear existing data
        self.vehicles.clear();
        self.trip_updates.clear();
        self.alerts.clear();

        // Store feed timestamp
        self.last_update = feed.header.timestamp;

        // Extract entities
        for entity in feed.entity {
            if let Some(vehicle) = entity.vehicle {
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
            .filter_map(|v| self.vehicle_to_ffi(v))
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
            .filter_map(|v| {
                let pos = v.position.as_ref()?;
                let lat = pos.latitude as f64;
                let lon = pos.longitude as f64;

                if lat >= min_lat && lat <= max_lat && lon >= min_lon && lon <= max_lon {
                    self.vehicle_to_ffi(v)
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

    /// Convert VehiclePosition to FFI struct
    fn vehicle_to_ffi(&self, vehicle: &VehiclePosition) -> Option<FFIVehicle> {
        let pos = vehicle.position.as_ref()?;

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
}
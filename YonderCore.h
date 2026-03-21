#ifndef YonderCore_h
#define YonderCore_h

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

// ═══════════════════════════════════════════════════════════════════════════════
// MARK: - Opaque types
// ═══════════════════════════════════════════════════════════════════════════════

typedef struct StopsDatabase StopsDatabase;
typedef struct TileCacheCore TileCacheCore;
typedef struct GtfsRtCore    GtfsRtCore;


// ═══════════════════════════════════════════════════════════════════════════════
// MARK: - Shared value types
// ═══════════════════════════════════════════════════════════════════════════════

/// Smooth interpolated vehicle position computed from shapes.txt.
/// Returned by value — no heap allocation, no free call required.
typedef struct {
    double  lat;
    double  lon;
    int32_t is_valid; ///< 1 = valid interpolated position; 0 = use raw GPS
} InterpolatedPosition;

/// One HTTP byte-range request Swift must issue against the GTFS ZIP.
typedef struct {
    const char* filename;
    uint64_t    byte_offset;
    uint64_t    byte_length;
} GTFSZipRange;

/// Result of a static trip lookup.  Either or both pointers may be NULL.
/// Free with gtfs_static_free_result().
typedef struct {
    const char* train_number;
    const char* route_name;
} GTFSStaticResult;


// ═══════════════════════════════════════════════════════════════════════════════
// MARK: - Library probe
// ═══════════════════════════════════════════════════════════════════════════════

const char* hello_from_rust(void);
void        free_rust_string(char* ptr);


// ═══════════════════════════════════════════════════════════════════════════════
// MARK: - StopsDatabase
//
// Pipe-delimited stop format (9 fields):
//   id|name|lat|lon|url|providers|stop_code|location_type|timezone
//
// location_type: 0=stop/platform  1=station  2=entrance  3=node  4=boarding_area
// Only location_type=0 stops are routable boarding points (use find_near_boardable).
// ═══════════════════════════════════════════════════════════════════════════════

StopsDatabase* stops_db_new(void);
int32_t        stops_db_load_csv(StopsDatabase* db, const char* path);

char** stops_db_find_near(
    const StopsDatabase* db, double lat, double lon,
    double radius_meters, size_t* out_count);

/// Like stops_db_find_near but only returns location_type==0 (boardable) stops.
char** stops_db_find_near_boardable(
    const StopsDatabase* db, double lat, double lon,
    double radius_meters, size_t* out_count);

char** stops_db_get_all(const StopsDatabase* db, size_t* out_count);

char** stops_db_find_nearest(
    const StopsDatabase* db, double lat, double lon, size_t* out_count);

char** stops_db_find_by_id(
    const StopsDatabase* db, const char* stop_id, size_t* out_count);

/// Find by public-facing stop_code (shown on signs) rather than internal stop_id.
char** stops_db_find_by_code(
    const StopsDatabase* db, const char* stop_code, size_t* out_count);

char** stops_db_find_by_provider(
    const StopsDatabase* db, const char* provider, size_t* out_count);

void   stops_db_free_results(char** results, size_t count);
size_t stops_db_count(const StopsDatabase* db);
void   stops_db_free(StopsDatabase* db);


// ═══════════════════════════════════════════════════════════════════════════════
// MARK: - TileCacheCore
// ═══════════════════════════════════════════════════════════════════════════════

TileCacheCore* tile_cache_new(const char* path);

int32_t tile_cache_save(
    TileCacheCore* cache, const char* cache_key,
    const uint8_t* data, size_t data_len,
    int64_t expiration_secs, int32_t is_negative);

uint8_t* tile_cache_load(
    const TileCacheCore* cache, const char* cache_key,
    size_t* out_data_len, uint16_t* out_http_status, int32_t* out_is_negative);

void    tile_cache_free_data(uint8_t* data, size_t len);
int32_t tile_cache_remove(TileCacheCore* cache, const char* cache_key);
int32_t tile_cache_is_valid(const TileCacheCore* cache, const char* cache_key);
size_t  tile_cache_cleanup_expired(TileCacheCore* cache);
uint64_t tile_cache_size(const TileCacheCore* cache);
size_t  tile_cache_count(const TileCacheCore* cache);
int32_t tile_cache_clear_all(TileCacheCore* cache);

void tile_cache_record_memory_hit(const TileCacheCore* cache);
void tile_cache_record_network_fetch(const TileCacheCore* cache);
void tile_cache_record_cache_miss(const TileCacheCore* cache);

void tile_cache_get_stats(
    const TileCacheCore* cache,
    uint64_t* out_memory_hits, uint64_t* out_disk_hits,
    uint64_t* out_network_fetches, uint64_t* out_cache_misses,
    uint64_t* out_expired_tiles);

void    tile_cache_reset_stats(TileCacheCore* cache);
void    tile_cache_free(TileCacheCore* cache);

int32_t tile_cache_save_negative(
    TileCacheCore* cache, const char* cache_key, int64_t ttl_seconds);
int32_t tile_cache_is_negative(const TileCacheCore* cache, const char* cache_key);


// ═══════════════════════════════════════════════════════════════════════════════
// MARK: - GtfsRtCore
// ═══════════════════════════════════════════════════════════════════════════════

// ── VehicleStopStatus values (current_status field) ──────────────────────────
#define GTFS_RT_VEHICLE_STATUS_INCOMING_AT   0
#define GTFS_RT_VEHICLE_STATUS_STOPPED_AT    1
#define GTFS_RT_VEHICLE_STATUS_IN_TRANSIT_TO 2
#define GTFS_RT_VEHICLE_STATUS_NOT_PROVIDED  (-1)

// ── OccupancyStatus values (occupancy_status field) ──────────────────────────
#define GTFS_RT_OCCUPANCY_EMPTY                    0
#define GTFS_RT_OCCUPANCY_MANY_SEATS_AVAILABLE     1
#define GTFS_RT_OCCUPANCY_FEW_SEATS_AVAILABLE      2
#define GTFS_RT_OCCUPANCY_STANDING_ROOM_ONLY       3
#define GTFS_RT_OCCUPANCY_CRUSHED_STANDING_ROOM    4
#define GTFS_RT_OCCUPANCY_FULL                     5
#define GTFS_RT_OCCUPANCY_NOT_ACCEPTING_PASSENGERS 6
#define GTFS_RT_OCCUPANCY_NO_DATA_AVAILABLE        7
#define GTFS_RT_OCCUPANCY_NOT_BOARDABLE            8

// ── Alert Cause values ────────────────────────────────────────────────────────
#define GTFS_RT_CAUSE_UNKNOWN_CAUSE      0
#define GTFS_RT_CAUSE_OTHER_CAUSE        1
#define GTFS_RT_CAUSE_TECHNICAL_PROBLEM  2
#define GTFS_RT_CAUSE_STRIKE             3
#define GTFS_RT_CAUSE_DEMONSTRATION      4
#define GTFS_RT_CAUSE_ACCIDENT           5
#define GTFS_RT_CAUSE_HOLIDAY            6
#define GTFS_RT_CAUSE_WEATHER            7
#define GTFS_RT_CAUSE_MAINTENANCE        8
#define GTFS_RT_CAUSE_CONSTRUCTION       9
#define GTFS_RT_CAUSE_POLICE_ACTIVITY    10
#define GTFS_RT_CAUSE_MEDICAL_EMERGENCY  11

// ── Alert Effect values ───────────────────────────────────────────────────────
#define GTFS_RT_EFFECT_NO_SERVICE          1
#define GTFS_RT_EFFECT_REDUCED_SERVICE     2
#define GTFS_RT_EFFECT_SIGNIFICANT_DELAYS  3
#define GTFS_RT_EFFECT_DETOUR              4
#define GTFS_RT_EFFECT_ADDITIONAL_SERVICE  5
#define GTFS_RT_EFFECT_MODIFIED_SERVICE    6
#define GTFS_RT_EFFECT_OTHER_EFFECT        7
#define GTFS_RT_EFFECT_UNKNOWN_EFFECT      8
#define GTFS_RT_EFFECT_STOP_MOVED          9
#define GTFS_RT_EFFECT_NO_EFFECT           10
#define GTFS_RT_EFFECT_ACCESSIBILITY_ISSUE 11

// ── Alert SeverityLevel values (experimental) ─────────────────────────────────
#define GTFS_RT_SEVERITY_NOT_PROVIDED    0
#define GTFS_RT_SEVERITY_UNKNOWN         1
#define GTFS_RT_SEVERITY_INFO            2
#define GTFS_RT_SEVERITY_WARNING         3
#define GTFS_RT_SEVERITY_SEVERE          4

/// One vehicle position from the GTFS-RT VehiclePositions feed.
///
/// All string fields (id, route_id, trip_id, label, start_date, start_time)
/// are heap-allocated and freed as a group by gtfs_rt_free_vehicles().
///
/// start_date (YYYYMMDD) and start_time (HH:MM:SS) are required to uniquely
/// identify frequency-based trip instances and to disambiguate overnight trips.
typedef struct {
    const char* id;
    double      latitude;
    double      longitude;
    float       bearing;
    float       speed;
    const char* route_id;
    const char* trip_id;
    const char* label;          ///< VehicleDescriptor.label (human-readable train name)
    int64_t     timestamp;      ///< POSIX seconds; 0 = not provided
    bool        has_bearing;
    bool        has_speed;
    int32_t     occupancy_status; ///< GTFS_RT_OCCUPANCY_* constants; 7=NO_DATA when absent
    int32_t     current_status;   ///< GTFS_RT_VEHICLE_STATUS_*; -1=not provided
    int32_t     direction_id;     ///< 0 or 1; -1 when absent
    const char* start_date;       ///< YYYYMMDD string or NULL
    const char* start_time;       ///< HH:MM:SS string or NULL
} FFIVehicle;

/// Combined vehicle + TripUpdate enrichment.
/// Merges FFIVehicle fields with delay/next-stop from a matching TripUpdate
/// so Swift needs only a single FFI call per update cycle.
/// Free with gtfs_rt_free_enriched_vehicles().
typedef struct {
    // Core vehicle (mirrors FFIVehicle)
    const char* id;
    double      latitude;
    double      longitude;
    float       bearing;
    float       speed;
    const char* route_id;
    const char* trip_id;
    const char* label;
    int64_t     timestamp;
    bool        has_bearing;
    bool        has_speed;
    int32_t     occupancy_status;
    int32_t     current_status;
    int32_t     direction_id;
    const char* start_date;
    const char* start_time;
    // TripUpdate enrichment
    int32_t     delay_seconds;      ///< positive=late, negative=early
    bool        has_delay;          ///< false when no delay data in feed
    const char* next_stop_id;       ///< next upcoming stop_id, or NULL.
                                    ///< Reflects assigned_stop_id when stop reassigned.
    int64_t     next_arrival_time;  ///< POSIX predicted arrival at next stop (0=unknown)
    bool        has_next_stop;
} FFIEnrichedVehicle;

/// Flat per-trip delay/next-stop summary returned by gtfs_rt_get_trip_update().
/// Free with gtfs_rt_free_trip_update().
typedef struct {
    int32_t     delay_seconds;
    bool        has_delay;
    const char* next_stop_id;
    int64_t     next_arrival_time;
    bool        has_next_stop;
} TripUpdateSummary;

/// A service alert from the GTFS-RT Alerts feed.
/// header_text and description_text are Required by the GTFS-RT spec.
/// cause / effect use GTFS_RT_CAUSE_* / GTFS_RT_EFFECT_* constants.
/// active_period_{start,end}: POSIX seconds; 0 = unbounded.
/// affected_route_ids / affected_stop_ids: pipe-separated "|" lists or NULL.
/// Free the entire array with gtfs_rt_free_alerts().
typedef struct {
    const char* header_text;
    const char* description_text;
    const char* url;
    int32_t     cause;
    int32_t     effect;
    int32_t     severity_level;
    int64_t     active_period_start;
    int64_t     active_period_end;
    const char* affected_route_ids;
    const char* affected_stop_ids;
} FFIAlert;

GtfsRtCore* gtfs_rt_new(void);
int32_t     gtfs_rt_parse(GtfsRtCore* core, const uint8_t* data, size_t data_len);

FFIVehicle* gtfs_rt_get_vehicles(const GtfsRtCore* core, size_t* out_count);
size_t      gtfs_rt_vehicle_count(const GtfsRtCore* core);
void        gtfs_rt_free_vehicles(FFIVehicle* vehicles, size_t count);

FFIEnrichedVehicle* gtfs_rt_get_active_enriched_vehicles(
    const GtfsRtCore* core, int64_t now_local, size_t* out_count);
void gtfs_rt_free_enriched_vehicles(FFIEnrichedVehicle* vehicles, size_t count);

TripUpdateSummary* gtfs_rt_get_trip_update(const GtfsRtCore* core, const char* trip_id);
void               gtfs_rt_free_trip_update(TripUpdateSummary* summary);

FFIAlert* gtfs_rt_get_alerts(const GtfsRtCore* core, size_t* out_count);
size_t    gtfs_rt_alert_count(const GtfsRtCore* core);
void      gtfs_rt_free_alerts(FFIAlert* alerts, size_t count);

void gtfs_rt_free(GtfsRtCore* core);


// ═══════════════════════════════════════════════════════════════════════════════
// MARK: - GTFS Static
//
// ZIP loading protocol (7 files, in dependency order):
//   routes.txt          — any order
//   trips.txt           — after routes.txt when using RouteShortName strategy
//   stops.txt           — MUST precede stop_times.txt
//   stop_times.txt      — requires stops.txt already loaded
//   shapes.txt          — any order
//   calendar.txt        — any order; enables service-day checking in is_trip_active
//   calendar_dates.txt  — any order; service-day exceptions override calendar.txt
//
// TripIdStrategy values:
//   0 = TripShortName       (use trip_short_name field — default)
//   1 = TripIdNumericSuffix (digits after leading alpha prefix in trip_id)
//   2 = RouteShortName      (use route_short_name field)
//   3 = TripIdVerbatim      (trip_id as-is)
//
// Service-day gating:
//   When calendar.txt / calendar_dates.txt are loaded, is_trip_active performs
//   a full day-of-week + date-range + exception check.  Call
//   gtfs_static_store_calendar_loaded() to confirm the data is present.
//   Without calendar data the function falls through to the time-window check.
// ═══════════════════════════════════════════════════════════════════════════════

void gtfs_static_free_result(GTFSStaticResult* result);
void gtfs_static_free_ranges(GTFSZipRange* ranges, size_t count);

// ── Legacy single-feed API ───────────────────────────────────────────────────

GTFSZipRange* gtfs_static_feed_eocd(
    const uint8_t* data, size_t data_len, size_t* out_count);

int32_t gtfs_static_feed_file(
    const char* filename, const uint8_t* data, size_t data_len);

GTFSStaticResult* gtfs_static_lookup(const char* trip_id);
int32_t           gtfs_static_is_loaded(void);
int32_t           gtfs_static_is_trip_active(const char* trip_id, int64_t now_local);

InterpolatedPosition gtfs_interpolate_position(const char* trip_id, int64_t now_local);
int32_t              gtfs_interpolation_is_ready(void);
void                 gtfs_static_reset(void);

int32_t     gtfs_static_get_direction_id(const char* trip_id);
const char* gtfs_static_get_headsign(const char* trip_id);   ///< free with free_rust_string
int32_t     gtfs_static_calendar_loaded(void);

// ── Multi-feed per-store API ──────────────────────────────────────────────────

uint32_t gtfs_static_store_new(void);
void     gtfs_static_store_free(uint32_t store_id);
void     gtfs_static_store_set_strategy(uint32_t store_id, int32_t strategy);

GTFSZipRange* gtfs_static_store_feed_eocd(
    uint32_t store_id, const uint8_t* data, size_t data_len, size_t* out_count);

int32_t gtfs_static_store_feed_file(
    uint32_t store_id, const char* filename, const uint8_t* data, size_t data_len);

GTFSStaticResult* gtfs_static_store_lookup(uint32_t store_id, const char* trip_id);
int32_t           gtfs_static_store_is_loaded(uint32_t store_id);

int32_t gtfs_static_store_is_trip_active(
    uint32_t store_id, const char* trip_id, int64_t now_local);

InterpolatedPosition gtfs_static_store_interpolate(
    uint32_t store_id, const char* trip_id, int64_t now_local);

int32_t gtfs_static_store_interpolation_ready(uint32_t store_id);
void    gtfs_static_store_reset(uint32_t store_id);

/// Returns direction_id (0 or 1) for trip_id; -1 when absent or unknown.
int32_t     gtfs_static_store_get_direction_id(uint32_t store_id, const char* trip_id);

/// Returns trip_headsign for trip_id; NULL when absent.  Free with free_rust_string().
const char* gtfs_static_store_get_headsign(uint32_t store_id, const char* trip_id);

/// Returns service_id for trip_id; NULL when absent.  Free with free_rust_string().
const char* gtfs_static_store_get_service_id(uint32_t store_id, const char* trip_id);

/// Returns 1 if calendar.txt or calendar_dates.txt data has been loaded.
/// When 0, is_trip_active falls back to the time-window check only.
int32_t gtfs_static_store_calendar_loaded(uint32_t store_id);

/// Returns 1 if (trip_id, stop_sequence) is a revenue stop (pickup_type==0 and
/// drop_off_type==0).  Returns 1 (pass-through) for unknown stops.
/// Use to filter non-revenue timing points from next-stop results.
int32_t gtfs_static_store_is_stop_revenue(
    uint32_t store_id, const char* trip_id, uint32_t stop_sequence);


// ═══════════════════════════════════════════════════════════════════════════════
// MARK: - Shapes Editor
// ═══════════════════════════════════════════════════════════════════════════════

typedef struct {
    const char* shape_id;
    uint32_t    sequence;
    double      lat;
    double      lon;
} FFIShapePoint;

uint32_t shapes_editor_open(void);
void     shapes_editor_close(uint32_t store_id);

int32_t  shapes_editor_load(uint32_t store_id, const char* path, size_t* out_count);
int32_t  shapes_editor_save(uint32_t store_id, const char* path);

FFIShapePoint* shapes_editor_get_all(uint32_t store_id, size_t* out_count);
char*          shapes_editor_get_shape_ids(uint32_t store_id);
void           shapes_editor_free_string(char* ptr);

FFIShapePoint* shapes_editor_get_shape(
    uint32_t store_id, const char* shape_id, size_t* out_count);

size_t  shapes_editor_point_count(uint32_t store_id);

int32_t shapes_editor_update_point(
    uint32_t store_id, const char* shape_id, uint32_t sequence,
    double new_lat, double new_lon);

int32_t shapes_editor_delete_point(
    uint32_t store_id, const char* shape_id, uint32_t sequence);

int32_t shapes_editor_insert_point(
    uint32_t store_id, const char* shape_id, uint32_t after_sequence,
    double lat, double lon);

int32_t shapes_editor_delete_shape(uint32_t store_id, const char* shape_id);
int32_t shapes_editor_add_shape(uint32_t store_id, const char* shape_id);
void    shapes_editor_reset(uint32_t store_id);
void    shapes_editor_free_points(FFIShapePoint* ptr, size_t count);


#endif /* YonderCore_h */
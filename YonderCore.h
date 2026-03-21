#ifndef YonderCore_h
#define YonderCore_h

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

// ═══════════════════════════════════════════════════════════════════════════════
// MARK: - Opaque types
// ═══════════════════════════════════════════════════════════════════════════════

/// Instance-based stops database with R-tree spatial index.
/// Allocate with stops_db_new(); free with stops_db_free().
typedef struct StopsDatabase StopsDatabase;

/// On-disk tile cache with LRU eviction and statistics.
/// Allocate with tile_cache_new(); free with tile_cache_free().
typedef struct TileCacheCore TileCacheCore;

/// GTFS-RT feed parser and vehicle store.
/// Allocate with gtfs_rt_new(); free with gtfs_rt_free().
typedef struct GtfsRtCore GtfsRtCore;


// ═══════════════════════════════════════════════════════════════════════════════
// MARK: - Shared value types
//
// Declared before any function that uses them to avoid forward-declaration
// ordering bugs.
// ═══════════════════════════════════════════════════════════════════════════════

/// Smooth interpolated vehicle position computed from shapes.txt.
/// Returned by value — no heap allocation, no free call required.
///
/// When is_valid == 0 shape data is unavailable for this trip (no published
/// polyline, or static data has not finished loading).  Fall back to the raw
/// GPS lat/lon from the GTFS-RT feed.
typedef struct {
    double  lat;
    double  lon;
    int32_t is_valid; ///< 1 = valid interpolated position; 0 = use raw GPS
} InterpolatedPosition;

/// One HTTP byte-range request Swift must issue against the GTFS ZIP.
/// Free the array with gtfs_static_free_ranges().
typedef struct {
    const char* filename;    ///< null-terminated filename, e.g. "trips.txt"
    uint64_t    byte_offset; ///< start byte in the ZIP file
    uint64_t    byte_length; ///< number of bytes to fetch (includes local-header slop)
} GTFSZipRange;

/// Result of a static trip lookup.  Either or both pointers may be NULL.
/// Free with gtfs_static_free_result().
typedef struct {
    const char* train_number; ///< e.g. "168" or "1052" — NULL if unavailable
    const char* route_name;   ///< e.g. "Northeast Regional" — NULL if unavailable
} GTFSStaticResult;


// ═══════════════════════════════════════════════════════════════════════════════
// MARK: - Library probe
// ═══════════════════════════════════════════════════════════════════════════════

/// Returns a static greeting string confirming the library loaded correctly.
/// Free with free_rust_string().
const char* hello_from_rust(void);

/// Free a C string that was allocated by Rust via CString::into_raw().
void free_rust_string(char* ptr);


// ═══════════════════════════════════════════════════════════════════════════════
// MARK: - StopsDatabase
//
// Fully instance-based — allocate one or more databases, load stops from one or
// more CSV files (each call appends), then query spatially or by id/provider.
//
// All stops_db_find_* functions return pipe-delimited strings:
//   "stop_id|stop_name|lat|lon|url|provider1,provider2"
// Free every char** result with stops_db_free_results().
// ═══════════════════════════════════════════════════════════════════════════════

/// Allocate a new empty stops database.  Free with stops_db_free().
StopsDatabase* stops_db_new(void);

/// Load stops from a GTFS stops.txt-compatible CSV at path, appending to any
/// already-loaded stops.  Returns the number of stops loaded, or -1 on error.
int32_t stops_db_load_csv(StopsDatabase* db, const char* path);

/// Find all stops within radius_meters of (lat, lon).
/// Sets *out_count.  Free with stops_db_free_results().
char** stops_db_find_near(
    const StopsDatabase* db,
    double lat, double lon,
    double radius_meters,
    size_t* out_count
);

/// Return all stops in the database.
/// Sets *out_count.  Free with stops_db_free_results().
char** stops_db_get_all(const StopsDatabase* db, size_t* out_count);

/// Find the single nearest stop to (lat, lon) using the R-tree (O(log n)).
/// Returns a 1-element char** or NULL.  Free with stops_db_free_results().
char** stops_db_find_nearest(
    const StopsDatabase* db,
    double lat, double lon,
    size_t* out_count
);

/// Find a single stop by stop_id using the O(1) HashMap index.
/// Returns a 1-element char** or NULL.  Free with stops_db_free_results().
char** stops_db_find_by_id(
    const StopsDatabase* db,
    const char* stop_id,
    size_t* out_count
);

/// Find all stops whose providers list contains provider.
/// Returns a char** array or NULL.  Free with stops_db_free_results().
char** stops_db_find_by_provider(
    const StopsDatabase* db,
    const char* provider,
    size_t* out_count
);

/// Free a char** result array returned by any stops_db_find_* function.
void stops_db_free_results(char** results, size_t count);

/// Total number of stops currently in db.
size_t stops_db_count(const StopsDatabase* db);

/// Free a StopsDatabase allocated by stops_db_new().
void stops_db_free(StopsDatabase* db);


// ═══════════════════════════════════════════════════════════════════════════════
// MARK: - TileCacheCore
//
// On-disk tile cache with LRU eviction, negative-cache support, and statistics.
// Fully instance-based — allocate one per cache directory.
// ═══════════════════════════════════════════════════════════════════════════════

/// Allocate a new tile cache backed by the directory at path.
/// Returns NULL on error.  Free with tile_cache_free().
TileCacheCore* tile_cache_new(const char* path);

/// Write tile data to disk.  is_negative != 0 writes a negative-cache sentinel.
/// Returns 1 on success, 0 on error.
int32_t tile_cache_save(
    TileCacheCore* cache,
    const char*    cache_key,
    const uint8_t* data,
    size_t         data_len,
    int64_t        expiration_secs,
    int32_t        is_negative
);

/// Read tile data from disk.
/// On success sets *out_data_len, *out_http_status, *out_is_negative (any may
/// be NULL) and returns a heap-allocated byte buffer.
/// Free with tile_cache_free_data().  Returns NULL on miss or error.
uint8_t* tile_cache_load(
    const TileCacheCore* cache,
    const char*          cache_key,
    size_t*              out_data_len,
    uint16_t*            out_http_status,
    int32_t*             out_is_negative
);

/// Free a byte buffer returned by tile_cache_load().
void tile_cache_free_data(uint8_t* data, size_t len);

/// Remove a single tile by key.  Returns 1 on success, 0 on error.
int32_t tile_cache_remove(TileCacheCore* cache, const char* cache_key);

/// Returns 1 if the tile exists and has not expired.
int32_t tile_cache_is_valid(const TileCacheCore* cache, const char* cache_key);

/// Delete all expired tiles.  Returns the number of tiles removed.
size_t tile_cache_cleanup_expired(TileCacheCore* cache);

/// Total on-disk cache size in bytes.
uint64_t tile_cache_size(const TileCacheCore* cache);

/// Number of tiles currently in the cache.
size_t tile_cache_count(const TileCacheCore* cache);

/// Remove all tiles.  Returns 1 on success, 0 on error.
int32_t tile_cache_clear_all(TileCacheCore* cache);

void tile_cache_record_memory_hit(const TileCacheCore* cache);
void tile_cache_record_network_fetch(const TileCacheCore* cache);
void tile_cache_record_cache_miss(const TileCacheCore* cache);

/// Read out statistics counters.  Any output pointer may be NULL.
void tile_cache_get_stats(
    const TileCacheCore* cache,
    uint64_t* out_memory_hits,
    uint64_t* out_disk_hits,
    uint64_t* out_network_fetches,
    uint64_t* out_cache_misses,
    uint64_t* out_expired_tiles
);

/// Reset all statistics counters to zero.
void tile_cache_reset_stats(TileCacheCore* cache);

/// Free a TileCacheCore allocated by tile_cache_new().
void tile_cache_free(TileCacheCore* cache);

/// Write a negative-cache sentinel for cache_key with ttl_seconds TTL.
/// Returns 1 on success, 0 on error.
int32_t tile_cache_save_negative(
    TileCacheCore* cache,
    const char*    cache_key,
    int64_t        ttl_seconds
);

/// Returns 1 if cache_key is a valid, unexpired negative-cache entry.
int32_t tile_cache_is_negative(const TileCacheCore* cache, const char* cache_key);


// ═══════════════════════════════════════════════════════════════════════════════
// MARK: - GtfsRtCore
//
// GTFS-RT protobuf feed parser.  Fully instance-based — allocate one per feed
// URL (e.g. one for Amtrak, one for SEPTA).  Parse a fresh blob every poll
// cycle with gtfs_rt_parse(); query the parsed state with the other functions.
// ═══════════════════════════════════════════════════════════════════════════════

/// One vehicle position as returned by the GTFS-RT feed.
typedef struct {
    const char* id;
    double      latitude;
    double      longitude;
    float       bearing;
    float       speed;
    const char* route_id;
    const char* trip_id;
    const char* label;         ///< VehicleDescriptor.label (human-readable train name)
    int64_t     timestamp;
    bool        has_bearing;
    bool        has_speed;
    /// OccupancyStatus per GTFS-RT spec (0-8).
    /// 0=EMPTY  1=MANY_SEATS  2=FEW_SEATS  3=STANDING_ROOM  4=CRUSHED_STANDING
    /// 5=FULL   6=NOT_ACCEPTING  7=NO_DATA_AVAILABLE  8=NOT_BOARDABLE
    int32_t     occupancy_status;
} FFIVehicle;

/// Combined vehicle + TripUpdate enrichment.
/// Merges FFIVehicle fields with delay/next-stop data so Swift needs only a
/// single FFI call per update cycle instead of N*2 per-vehicle calls.
/// Free with gtfs_rt_free_enriched_vehicles().
typedef struct {
    // Core vehicle fields (mirrors FFIVehicle)
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
    // TripUpdate enrichment
    int32_t     delay_seconds;      ///< positive = late, negative = early
    bool        has_delay;          ///< false when delay data is genuinely absent
    const char* next_stop_id;       ///< stop_id of next upcoming stop, or NULL
    int64_t     next_arrival_time;  ///< unix timestamp of predicted arrival (0 = unknown)
    bool        has_next_stop;      ///< true when next_stop_id/next_arrival_time are set
} FFIEnrichedVehicle;

/// Flat per-trip delay/next-stop summary.
/// Returned by gtfs_rt_get_trip_update(); free with gtfs_rt_free_trip_update().
///
/// delay_seconds extraction priority:
///   1. TripUpdate.delay (trip-level; rarely set by Amtrak)
///   2. Last past StopTimeUpdate delay (primary Amtrak source)
///   3. First future StopTimeUpdate delay (pre-departure estimate)
/// has_delay is false when none of the three sources provided data.
typedef struct {
    int32_t     delay_seconds;
    bool        has_delay;
    const char* next_stop_id;       ///< or NULL
    int64_t     next_arrival_time;  ///< 0 = unknown
    bool        has_next_stop;
} TripUpdateSummary;

/// Allocate a new GTFS-RT manager.  Free with gtfs_rt_free().
GtfsRtCore* gtfs_rt_new(void);

/// Parse a GTFS-RT protobuf blob, replacing any previously parsed feed state.
/// Returns 0 on success, -1 on error.
int32_t gtfs_rt_parse(GtfsRtCore* core, const uint8_t* data, size_t data_len);

/// Get all vehicles from the most recently parsed feed.
/// Sets *out_count.  Free with gtfs_rt_free_vehicles().
FFIVehicle* gtfs_rt_get_vehicles(const GtfsRtCore* core, size_t* out_count);

/// Number of vehicles in the most recently parsed feed.
size_t gtfs_rt_vehicle_count(const GtfsRtCore* core);

/// Free a vehicle array returned by gtfs_rt_get_vehicles().
void gtfs_rt_free_vehicles(FFIVehicle* vehicles, size_t count);

/// Return all vehicles enriched with TripUpdate data in a single FFI call.
/// now_eastern is retained for API compatibility but is no longer used to gate
/// vehicles — the RT feed is authoritative for what is currently moving.
/// Free with gtfs_rt_free_enriched_vehicles().
FFIEnrichedVehicle* gtfs_rt_get_active_enriched_vehicles(
    const GtfsRtCore* core,
    int64_t           now_eastern,
    size_t*           out_count
);

/// Free an array returned by gtfs_rt_get_active_enriched_vehicles().
void gtfs_rt_free_enriched_vehicles(FFIEnrichedVehicle* vehicles, size_t count);

/// Look up trip-update data for trip_id.
/// Returns a heap-allocated TripUpdateSummary, or NULL if not found.
/// Free with gtfs_rt_free_trip_update().
TripUpdateSummary* gtfs_rt_get_trip_update(const GtfsRtCore* core, const char* trip_id);

/// Free a TripUpdateSummary returned by gtfs_rt_get_trip_update().
void gtfs_rt_free_trip_update(TripUpdateSummary* summary);

/// Free a GtfsRtCore allocated by gtfs_rt_new().
void gtfs_rt_free(GtfsRtCore* core);


// ═══════════════════════════════════════════════════════════════════════════════
// MARK: - GTFS Static
//
// Parses a GTFS ZIP via HTTP Range requests and provides:
//   * Trip lookup     — train number + route name from trip_id
//   * Active-window   — hide pre-departure / completed trains
//   * Interpolation   — smooth lat/lon between GPS pings
//
// TWO APIS — choose based on your use case:
//
//   Legacy (single-feed, Amtrak-compatible)
//   ----------------------------------------
//   gtfs_static_feed_eocd / gtfs_static_feed_file / gtfs_static_lookup / ...
//   These route to a hidden default store with ShortName strategy.
//   Existing Amtrak Swift code requires zero changes.
//
//   Multi-feed (one store per agency)
//   -----------------------------------
//   gtfs_static_store_new → gtfs_static_store_set_strategy →
//   gtfs_static_store_feed_eocd → gtfs_static_store_feed_file (x5) →
//   gtfs_static_store_lookup / gtfs_static_store_is_trip_active /
//   gtfs_static_store_interpolate → gtfs_static_store_free
//
// ZIP loading protocol (same for both APIs):
//   1. Fetch last ~256 KB of ZIP → feed_eocd() → array of GTFSZipRange
//   2. Issue HTTP Range requests; call feed_file() for each:
//        "routes.txt"     — any order
//        "trips.txt"      — any order (after routes.txt for RouteShortName strategy)
//        "stops.txt"      — MUST precede stop_times.txt
//        "stop_times.txt" — requires stops.txt already loaded
//        "shapes.txt"     — any order
//   3. Query with lookup / is_trip_active / interpolate
//
// TripIdStrategy values (multi-feed API, passed to gtfs_static_store_set_strategy):
//   0 = ShortName      — trip_short_name is the train number (Amtrak / SJJPA, default)
//   1 = SeptaTripId    — digits after leading alpha prefix in trip_id
//                        e.g. "CYN1052_20260201_SID185189" -> trainNumber "1052"
//   2 = RouteShortName — route_short_name as the line label (NJT, MBTA, etc.)
//   3 = Opaque         — trip_id verbatim as train_number
// ═══════════════════════════════════════════════════════════════════════════════

// Shared free functions (used by both legacy and multi-feed APIs)

/// Free a GTFSStaticResult returned by any lookup function.
void gtfs_static_free_result(GTFSStaticResult* result);

/// Free a GTFSZipRange array returned by any feed_eocd function.
void gtfs_static_free_ranges(GTFSZipRange* ranges, size_t count);

// Legacy single-feed API

/// Feed the last ~256 KB of the GTFS ZIP.
/// Returns an array of up to five GTFSZipRange entries.
/// Sets *out_count.  Free with gtfs_static_free_ranges().
GTFSZipRange* gtfs_static_feed_eocd(
    const uint8_t* data,
    size_t         data_len,
    size_t*        out_count
);

/// Feed raw bytes for one file entry (starting at the local ZIP header).
/// Returns 0 on success, -1 on error.
int32_t gtfs_static_feed_file(
    const char*    filename,
    const uint8_t* data,
    size_t         data_len
);

/// Look up a realtime trip_id in the legacy store.
/// Always returns non-null; free with gtfs_static_free_result().
GTFSStaticResult* gtfs_static_lookup(const char* trip_id);

/// Returns 1 if routes, trips, and stop_times are all loaded.
int32_t gtfs_static_is_loaded(void);

/// Returns 1 if trip_id is scheduled as active at now_eastern.
/// Returns 1 (pass-through) if stop_times have not yet loaded.
/// now_eastern = now_unix + TimeZone("America/New_York").secondsFromGMT(now)
int32_t gtfs_static_is_trip_active(const char* trip_id, int64_t now_eastern);

/// Compute a smooth interpolated position for trip_id at now_eastern.
/// is_valid = 0 when shape data is unavailable — fall back to raw GPS.
InterpolatedPosition gtfs_interpolate_position(const char* trip_id, int64_t now_eastern);

/// Returns 1 when shapes.txt, stops.txt, and stop_times.txt are all loaded.
int32_t gtfs_interpolation_is_ready(void);

/// Evict all data from the legacy store (e.g. before a GTFS refresh).
void gtfs_static_reset(void);

// Multi-feed per-store API

/// Allocate a new GTFS static store.  Returns a non-zero store_id.
/// Starts with ShortName strategy.  Free with gtfs_static_store_free().
uint32_t gtfs_static_store_new(void);

/// Release the store and free all its memory.  Ignores unknown or zero IDs.
void gtfs_static_store_free(uint32_t store_id);

/// Set the trip-id extraction strategy for store_id.
/// Must be called before feeding any files.  See TripIdStrategy values above.
void gtfs_static_store_set_strategy(uint32_t store_id, int32_t strategy);

/// Feed the ZIP tail bytes for store_id (step 1).
/// Returns NULL on error.  Free with gtfs_static_free_ranges().
GTFSZipRange* gtfs_static_store_feed_eocd(
    uint32_t       store_id,
    const uint8_t* data,
    size_t         data_len,
    size_t*        out_count
);

/// Feed one GTFS file into store_id (step 2).
/// Returns 0 on success, -1 on error.
int32_t gtfs_static_store_feed_file(
    uint32_t       store_id,
    const char*    filename,
    const uint8_t* data,
    size_t         data_len
);

/// Look up a realtime trip_id in store_id.
/// Always returns non-null; free with gtfs_static_free_result().
GTFSStaticResult* gtfs_static_store_lookup(uint32_t store_id, const char* trip_id);

/// Returns 1 if routes, trips, and stop_times are loaded in store_id.
int32_t gtfs_static_store_is_loaded(uint32_t store_id);

/// Returns 1 if trip_id is scheduled as active at now_eastern in store_id.
/// Returns 1 (pass-through) if stop_times have not yet loaded.
int32_t gtfs_static_store_is_trip_active(
    uint32_t    store_id,
    const char* trip_id,
    int64_t     now_eastern
);

/// Compute a smooth interpolated position for trip_id at now_eastern in store_id.
/// is_valid = 0 means shape data is unavailable — fall back to raw GPS.
InterpolatedPosition gtfs_static_store_interpolate(
    uint32_t    store_id,
    const char* trip_id,
    int64_t     now_eastern
);

/// Returns 1 when shape interpolation data is fully loaded for store_id.
int32_t gtfs_static_store_interpolation_ready(uint32_t store_id);

/// Evict all data from store_id (preserves the strategy setting).
void gtfs_static_store_reset(uint32_t store_id);


// ═══════════════════════════════════════════════════════════════════════════════
// MARK: - Shapes Editor
//
// Standalone shapes.txt editor backend for PolyPlot.  Completely isolated from
// the GTFS static pipeline — owns its own Registry and data model.
//
// Each open file gets its own independent store identified by a uint32_t
// store_id.  Stores are never merged; all functions require store_id.
// ═══════════════════════════════════════════════════════════════════════════════

/// One shape point as returned by the shapes editor FFI.
/// shape_id is a heap-allocated C string.  Free the whole array with
/// shapes_editor_free_points(ptr, count) — no other free function is correct.
typedef struct {
    const char* shape_id;  ///< null-terminated shape identifier
    uint32_t    sequence;  ///< shape_pt_sequence from shapes.txt
    double      lat;       ///< shape_pt_lat
    double      lon;       ///< shape_pt_lon
} FFIShapePoint;

/// Allocate a new independent editor store.
/// Returns a non-zero store_id on success, or 0 on failure.
/// Release with shapes_editor_close() when the file is closed.
uint32_t shapes_editor_open(void);

/// Release the store identified by store_id and free all its memory.
/// Passing an unknown ID is a no-op.
void shapes_editor_close(uint32_t store_id);

/// Parse a shapes.txt file at path into store_id, replacing any previously
/// loaded data.  Returns 0 on success, -1 on error.
/// *out_count is set to the total point count.
int32_t shapes_editor_load(uint32_t store_id, const char* path, size_t* out_count);

/// Serialise store_id back to path (overwrites the file).
/// Returns 0 on success, -1 on error.
int32_t shapes_editor_save(uint32_t store_id, const char* path);

/// Return all current points as a flat, (shape_id, sequence)-sorted array.
/// Sets *out_count.  Returns NULL if the store is empty or unknown.
/// Free with shapes_editor_free_points().
FFIShapePoint* shapes_editor_get_all(uint32_t store_id, size_t* out_count);

/// Return a newline-delimited "shape_id,count\n..." C string for store_id.
/// Returns NULL if the store is empty or unknown.
/// Free with shapes_editor_free_string().
char* shapes_editor_get_shape_ids(uint32_t store_id);

/// Free a C string returned by shapes_editor_get_shape_ids().
void shapes_editor_free_string(char* ptr);

/// Return all points for a single shape.
/// Sets *out_count.  Returns NULL if the shape does not exist.
/// Free with shapes_editor_free_points().
FFIShapePoint* shapes_editor_get_shape(
    uint32_t    store_id,
    const char* shape_id,
    size_t*     out_count
);

/// Total number of points currently held in store_id.
size_t shapes_editor_point_count(uint32_t store_id);

/// Move an existing point (shape_id, sequence) to new_lat/new_lon.
/// Returns 1 if found and updated, 0 if not found.
int32_t shapes_editor_update_point(
    uint32_t    store_id,
    const char* shape_id,
    uint32_t    sequence,
    double      new_lat,
    double      new_lon
);

/// Delete the point at (shape_id, sequence).
/// Removes the parent shape entry when it becomes empty.
/// Returns 1 if deleted, 0 if not found.
int32_t shapes_editor_delete_point(
    uint32_t    store_id,
    const char* shape_id,
    uint32_t    sequence
);

/// Insert a new point into shape_id immediately after after_sequence.
/// All points with sequence >= (after_sequence + 1) are renumbered +1.
/// Returns 1 on success, 0 on invalid arguments.
int32_t shapes_editor_insert_point(
    uint32_t    store_id,
    const char* shape_id,
    uint32_t    after_sequence,
    double      lat,
    double      lon
);

/// Delete an entire shape and all its points.
/// Returns 1 if the shape existed and was deleted, 0 if not found.
int32_t shapes_editor_delete_shape(uint32_t store_id, const char* shape_id);

/// Register a new empty shape in store_id.
/// Returns 1 if created, 0 if a shape with that ID already exists.
int32_t shapes_editor_add_shape(uint32_t store_id, const char* shape_id);

/// Clear all loaded data from store_id (does not close or free the store).
void shapes_editor_reset(uint32_t store_id);

/// Free an FFIShapePoint array returned by any shapes_editor_* function.
void shapes_editor_free_points(FFIShapePoint* ptr, size_t count);


#endif /* YonderCore_h */
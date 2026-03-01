#ifndef YonderCore_h
#define YonderCore_h

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

// MARK: - Opaque Pointers

typedef struct StopsDatabase StopsDatabase;
typedef struct TileCacheCore TileCacheCore;
typedef struct GtfsRtCore GtfsRtCore;

// MARK: - GTFS-RT Structures

typedef struct {
	const char* id;
	double latitude;
	double longitude;
	float bearing;
	float speed;
	const char* route_id;
	const char* trip_id;
	const char* label;      // ← this line
	int64_t timestamp;
	bool has_bearing;
	bool has_speed;
	int32_t occupancy_status;
} FFIVehicle;

// MARK: - Test Functions

const char* hello_from_rust(void);
void free_rust_string(char* ptr);

// MARK: - StopsDatabase Functions

StopsDatabase* stops_db_new(void);
int32_t stops_db_load_csv(StopsDatabase* db, const char* path);
char** stops_db_find_near(const StopsDatabase* db, double lat, double lon, double radius_meters, size_t* out_count);
char** stops_db_get_all(const StopsDatabase* db, size_t* out_count);
void stops_db_free_results(char** results, size_t count);
size_t stops_db_count(const StopsDatabase* db);
void stops_db_free(StopsDatabase* db);

// MARK: - TileCacheCore Functions

TileCacheCore* tile_cache_new(const char* path);
int32_t tile_cache_save(TileCacheCore* cache, const char* cache_key, const uint8_t* data, size_t data_len, int64_t expiration_secs, int32_t is_negative);
uint8_t* tile_cache_load(const TileCacheCore* cache, const char* cache_key, size_t* out_data_len, uint16_t* out_http_status, int32_t* out_is_negative);
void tile_cache_free_data(uint8_t* data, size_t len);
int32_t tile_cache_remove(TileCacheCore* cache, const char* cache_key);
int32_t tile_cache_is_valid(const TileCacheCore* cache, const char* cache_key);
size_t tile_cache_cleanup_expired(TileCacheCore* cache);
uint64_t tile_cache_size(const TileCacheCore* cache);
size_t tile_cache_count(const TileCacheCore* cache);
int32_t tile_cache_clear_all(TileCacheCore* cache);
void tile_cache_record_memory_hit(const TileCacheCore* cache);
void tile_cache_record_network_fetch(const TileCacheCore* cache);
void tile_cache_record_cache_miss(const TileCacheCore* cache);
void tile_cache_get_stats(const TileCacheCore* cache, uint64_t* out_memory_hits, uint64_t* out_disk_hits, uint64_t* out_network_fetches, uint64_t* out_cache_misses, uint64_t* out_expired_tiles);
void tile_cache_reset_stats(TileCacheCore* cache);
void tile_cache_free(TileCacheCore* cache);
int32_t tile_cache_save_negative(TileCacheCore* cache, const char* cache_key, int64_t ttl_seconds);
int32_t tile_cache_is_negative(const TileCacheCore* cache, const char* cache_key);

// MARK: - GTFS-RT Functions

/// Create a new GTFS-RT manager
GtfsRtCore* gtfs_rt_new(void);

/// Parse GTFS-RT protobuf data
/// Returns 0 on success, -1 on error
int32_t gtfs_rt_parse(GtfsRtCore* core, const uint8_t* data, size_t data_len);

/// Get all vehicles
/// Returns pointer to array of FFIVehicle structs
/// out_count is set to the number of vehicles
FFIVehicle* gtfs_rt_get_vehicles(const GtfsRtCore* core, size_t* out_count);

/// Get count of vehicles
size_t gtfs_rt_vehicle_count(const GtfsRtCore* core);

/// Free vehicle array returned by gtfs_rt_get_vehicles
void gtfs_rt_free_vehicles(FFIVehicle* vehicles, size_t count);

/// Free GTFS-RT manager
void gtfs_rt_free(GtfsRtCore* core);

// MARK: - GTFS-RT Trip Update Lookup

/// Flat summary of a TripUpdate for a single trip.
/// Returned by gtfs_rt_get_trip_update(); free with gtfs_rt_free_trip_update().
typedef struct {
    int32_t  delay_seconds;      // overall trip delay (positive = late, negative = early)
    bool     has_delay;          // true if delay_seconds is meaningful
    const char* next_stop_id;    // stop_id of the next upcoming stop, or NULL
    int64_t  next_arrival_time;  // unix timestamp of predicted arrival at next_stop_id (0 = unknown)
    bool     has_next_stop;      // true if next_stop_id and next_arrival_time are populated
} TripUpdateSummary;

/// Look up trip update data for a given trip_id.
/// Returns a heap-allocated TripUpdateSummary, or NULL if no matching TripUpdate exists.
/// Caller must free the result with gtfs_rt_free_trip_update().
TripUpdateSummary* gtfs_rt_get_trip_update(const GtfsRtCore* core, const char* trip_id);

/// Free a TripUpdateSummary returned by gtfs_rt_get_trip_update.
void gtfs_rt_free_trip_update(TripUpdateSummary* summary);


// MARK: - GTFS Static Lookup

/// One HTTP byte-range Swift must fetch from the ZIP.
typedef struct {
    const char* filename;   // "trips.txt" or "routes.txt"
    uint64_t    byte_offset;
    uint64_t    byte_length;
} GTFSZipRange;

/// Result of a static lookup. Both pointers may be NULL.
typedef struct {
    const char* train_number;  // e.g. "168"
    const char* route_name;    // e.g. "Northeast Regional"
} GTFSStaticResult;

/// Feed the last ~65 KB of the ZIP to parse the central directory.
/// Returns an array of ranges to fetch; sets *out_count.
/// Free with gtfs_static_free_ranges().
GTFSZipRange* gtfs_static_feed_eocd(const uint8_t* data, size_t data_len, size_t* out_count);

/// Feed decompressed bytes for "trips.txt" or "routes.txt" (starting at local header).
/// Returns 0 on success, -1 on error.
int32_t gtfs_static_feed_file(const char* filename, const uint8_t* data, size_t data_len);

/// Look up a realtime trip_id. Always returns non-null; free with gtfs_static_free_result().
GTFSStaticResult* gtfs_static_lookup(const char* trip_id);

/// Free result from gtfs_static_lookup.
void gtfs_static_free_result(GTFSStaticResult* result);

/// Free range array from gtfs_static_feed_eocd.
void gtfs_static_free_ranges(GTFSZipRange* ranges, size_t count);

/// Returns 1 if static data is fully loaded.
int32_t gtfs_static_is_loaded(void);

/// Clear all loaded static data.
void gtfs_static_reset(void);

#endif /* YonderCore_h */
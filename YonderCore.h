#ifndef YonderCore_h
#define YonderCore_h

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

// MARK: - Compiler hint: warn when a return value is silently discarded
//
// Applied to every function whose return value carries meaningful status.
// Clang and GCC both support __attribute__((warn_unused_result)).
// Swift automatically surfaces this as a compiler warning too.
#if defined(__GNUC__) || defined(__clang__)
#  define YONDER_NODISCARD __attribute__((warn_unused_result))
#else
#  define YONDER_NODISCARD
#endif

// MARK: - Opaque Pointers

typedef struct StopsDatabase  StopsDatabase;
typedef struct TileCacheCore  TileCacheCore;
typedef struct GtfsRtCore     GtfsRtCore;

// MARK: - Stop Structure
//
// Returned by stops_db_find_near, stops_db_get_all, and stops_db_find_by_id.
// All pointer fields are OWNED BY RUST. Free the entire array with
// stops_db_free_stop_array(ptr, count). Never free individual fields.

typedef struct {
    char*  id;          ///< Stop ID. Never NULL.
    char*  name;        ///< Stop name. Never NULL.
    double lat;
    double lon;
    char*  url;         ///< Stop URL, or NULL if not present.
    char*  providers;   ///< Comma-joined provider/agency IDs, or NULL if none.
} CStop;

// MARK: - Vehicle Structure
//
// Returned by gtfs_rt_get_vehicles.
// All pointer fields are OWNED BY RUST. Free the entire array with
// gtfs_rt_free_vehicles(ptr, count). Never free individual fields directly.

typedef struct {
    char*    id;               ///< Vehicle ID. Never NULL.
    double   latitude;
    double   longitude;
    float    bearing;
    float    speed;
    char*    route_id;         ///< Route ID, or NULL if not present.
    char*    trip_id;          ///< Trip ID, or NULL if not present.
    int64_t  timestamp;
    bool     has_bearing;
    bool     has_speed;
    int32_t  occupancy_status;
} CVehicle;

// MARK: - Error Handling
//
// On failure, FFI functions return a sentinel value (NULL, -1, or 0) AND store
// a diagnostic string in a thread-local slot. Retrieve it with yonder_last_error().
// The string is valid until the next YonderCore call on the same thread.
// Do NOT free the returned pointer.

/// Returns a pointer to the last error string for the calling thread, or NULL.
const char* yonder_last_error(void);

/// Clear the last error for the calling thread.
void yonder_clear_error(void);

// MARK: - Test / Lifecycle

/// Returns a static diagnostic string. Valid for process lifetime. Do NOT free.
const char* hello_from_rust(void);

/// Free a Rust-allocated string returned by a function that explicitly says to call this.
void free_rust_string(char* ptr);

// MARK: - StopsDatabase

YONDER_NODISCARD StopsDatabase* stops_db_new(void);

/// Load stops from a GTFS stops.txt CSV.
/// Returns the number of stops loaded (max INT32_MAX), or -1 on error.
YONDER_NODISCARD int32_t stops_db_load_csv(StopsDatabase* db, const char* path);

/// Find stops within radius_meters of (lat, lon).
/// Returns a CStop array; free with stops_db_free_stop_array.
YONDER_NODISCARD CStop* stops_db_find_near(const StopsDatabase* db,
                                            double lat, double lon, double radius_meters,
                                            size_t* out_count);

/// Get all stops.
/// Returns a CStop array; free with stops_db_free_stop_array.
YONDER_NODISCARD CStop* stops_db_get_all(const StopsDatabase* db, size_t* out_count);

/// Find a stop by its stop_id. Returns a single-element CStop array (out_count=1)
/// if found, or NULL (out_count=0) if not found.
/// Free with stops_db_free_stop_array.
YONDER_NODISCARD CStop* stops_db_find_by_id(const StopsDatabase* db,
                                              const char* stop_id,
                                              size_t* out_count);

/// Free a CStop array returned by any stops_db function.
/// `count` must match the value written to out_count.
void stops_db_free_stop_array(CStop* stops, size_t count);

size_t stops_db_count(const StopsDatabase* db);
void   stops_db_free(StopsDatabase* db);

// MARK: - TileCacheCore

YONDER_NODISCARD TileCacheCore* tile_cache_new(const char* path);

/// Save tile data. For negative cache entries, use tile_cache_save_negative instead.
/// Returns 1 on success, 0 on failure (check yonder_last_error).
YONDER_NODISCARD int32_t tile_cache_save(TileCacheCore* cache,
                                          const char*    cache_key,
                                          const uint8_t* data,
                                          size_t         data_len,
                                          int64_t        expiration_secs);

/// Save a negative cache entry (e.g. for a 404 response).
/// Returns 1 on success, 0 on failure.
YONDER_NODISCARD int32_t tile_cache_save_negative(TileCacheCore* cache,
                                                    const char*    cache_key,
                                                    int64_t        ttl_seconds);

/// Load a tile. Returns a pointer to tile data (length in *out_data_len).
/// Free with tile_cache_free_data(ptr, *out_data_len). Returns NULL if not cached.
YONDER_NODISCARD uint8_t* tile_cache_load(const TileCacheCore* cache,
                                           const char*          cache_key,
                                           size_t*              out_data_len,
                                           uint16_t*            out_http_status,
                                           int32_t*             out_is_negative);

/// Free data returned by tile_cache_load. `len` must match *out_data_len exactly.
void tile_cache_free_data(uint8_t* data, size_t len);

YONDER_NODISCARD int32_t  tile_cache_remove(TileCacheCore* cache, const char* cache_key);
int32_t  tile_cache_is_valid(const TileCacheCore* cache, const char* cache_key);
int32_t  tile_cache_is_negative(const TileCacheCore* cache, const char* cache_key);

/// Remove expired tiles. Returns the number of distinct tiles removed.
/// Return type is uint64_t — use a UInt64 on the Swift side.
uint64_t tile_cache_cleanup_expired(TileCacheCore* cache);

uint64_t tile_cache_size(const TileCacheCore* cache);
size_t   tile_cache_count(const TileCacheCore* cache);
YONDER_NODISCARD int32_t  tile_cache_clear_all(TileCacheCore* cache);

/// Read cache statistics. All out_ pointers are optional (pass NULL to skip).
/// Statistics are managed entirely by Rust; do not attempt to increment them
/// from Swift — doing so will double-count events.
void tile_cache_get_stats(const TileCacheCore* cache,
                          uint64_t* out_memory_hits,
                          uint64_t* out_disk_hits,
                          uint64_t* out_network_fetches,
                          uint64_t* out_cache_misses,
                          uint64_t* out_expired_tiles);

void tile_cache_reset_stats(TileCacheCore* cache);
void tile_cache_free(TileCacheCore* cache);

// MARK: - GTFS-RT

YONDER_NODISCARD GtfsRtCore* gtfs_rt_new(void);

/// Parse GTFS-RT protobuf data. Returns 0 on success, -1 on error.
/// On error, call yonder_last_error() for the prost decode message.
YONDER_NODISCARD int32_t gtfs_rt_parse(GtfsRtCore* core, const uint8_t* data, size_t data_len);

/// Get all vehicles as a CVehicle array.
/// Free with gtfs_rt_free_vehicles(ptr, *out_count).
/// Returns NULL if no vehicles are present.
YONDER_NODISCARD CVehicle* gtfs_rt_get_vehicles(const GtfsRtCore* core, size_t* out_count);

size_t gtfs_rt_vehicle_count(const GtfsRtCore* core);

/// Free the vehicle array returned by gtfs_rt_get_vehicles.
/// `count` must match the value written to out_count.
void gtfs_rt_free_vehicles(CVehicle* vehicles, size_t count);

void gtfs_rt_free(GtfsRtCore* core);

#endif /* YonderCore_h */

// MARK: - Stop Structure
//
// Returned by stops_db_find_near, stops_db_get_all, and stops_db_find_by_id.
// All pointer fields are OWNED BY RUST. Free the entire array with
// stops_db_free_stop_array(ptr, count). Never free individual fields.

typedef struct {
    char*  id;          ///< Stop ID. Never NULL.
    char*  name;        ///< Stop name. Never NULL.
    double lat;
    double lon;
    char*  url;         ///< Stop URL, or NULL if not present.
    char*  providers;   ///< Comma-joined provider/agency IDs, or NULL if none.
} CStop;

// MARK: - Vehicle Structure
//
// Returned by gtfs_rt_get_vehicles.
// All pointer fields are OWNED BY RUST. Free the entire array with
// gtfs_rt_free_vehicles(ptr, count). Never free individual fields directly.

typedef struct {
    char*    id;               ///< Vehicle ID. Never NULL.
    double   latitude;
    double   longitude;
    float    bearing;
    float    speed;
    char*    route_id;         ///< Route ID, or NULL if not present.
    char*    trip_id;          ///< Trip ID, or NULL if not present.
    int64_t  timestamp;
    bool     has_bearing;
    bool     has_speed;
    int32_t  occupancy_status;
} CVehicle;

// MARK: - Error Handling
//
// On failure, FFI functions return a sentinel value (NULL, -1, or 0) AND store
// a diagnostic string in a thread-local slot. Retrieve it with yonder_last_error().
// The string is valid until the next YonderCore call on the same thread.
// Do NOT free the returned pointer.

/// Returns a pointer to the last error string for the calling thread, or NULL.
const char* yonder_last_error(void);

/// Clear the last error for the calling thread.
void yonder_clear_error(void);

// MARK: - Test / Lifecycle

/// Returns a static diagnostic string. Valid for process lifetime. Do NOT free.
const char* hello_from_rust(void);

/// Free a Rust-allocated string returned by a function that explicitly says to call this.
void free_rust_string(char* ptr);

// MARK: - StopsDatabase

StopsDatabase* stops_db_new(void);

/// Load stops from a GTFS stops.txt CSV.
/// Returns the number of stops loaded (max INT32_MAX), or -1 on error.
int32_t stops_db_load_csv(StopsDatabase* db, const char* path);

/// Find stops within radius_meters of (lat, lon).
/// Returns a CStop array; free with stops_db_free_stop_array.
CStop* stops_db_find_near(const StopsDatabase* db,
                           double lat, double lon, double radius_meters,
                           size_t* out_count);

/// Get all stops.
/// Returns a CStop array; free with stops_db_free_stop_array.
CStop* stops_db_get_all(const StopsDatabase* db, size_t* out_count);

/// Find a stop by its stop_id. Returns a single-element CStop array (out_count=1)
/// if found, or NULL (out_count=0) if not found.
/// Free with stops_db_free_stop_array.
CStop* stops_db_find_by_id(const StopsDatabase* db,
                            const char* stop_id,
                            size_t* out_count);

/// Free a CStop array returned by any stops_db function.
/// `count` must match the value written to out_count.
void stops_db_free_stop_array(CStop* stops, size_t count);

size_t stops_db_count(const StopsDatabase* db);
void   stops_db_free(StopsDatabase* db);

// MARK: - TileCacheCore

TileCacheCore* tile_cache_new(const char* path);

/// Save tile data. For negative cache entries, use tile_cache_save_negative instead.
/// Returns 1 on success, 0 on failure (check yonder_last_error).
int32_t tile_cache_save(TileCacheCore* cache,
                        const char*    cache_key,
                        const uint8_t* data,
                        size_t         data_len,
                        int64_t        expiration_secs);

/// Save a negative cache entry (e.g. for a 404 response).
/// Returns 1 on success, 0 on failure.
int32_t tile_cache_save_negative(TileCacheCore* cache,
                                  const char*    cache_key,
                                  int64_t        ttl_seconds);

/// Load a tile. Returns a pointer to tile data (length in *out_data_len).
/// Free with tile_cache_free_data(ptr, *out_data_len). Returns NULL if not cached.
uint8_t* tile_cache_load(const TileCacheCore* cache,
                         const char*          cache_key,
                         size_t*              out_data_len,
                         uint16_t*            out_http_status,
                         int32_t*             out_is_negative);

/// Free data returned by tile_cache_load. `len` must match *out_data_len exactly.
void tile_cache_free_data(uint8_t* data, size_t len);

int32_t  tile_cache_remove(TileCacheCore* cache, const char* cache_key);
int32_t  tile_cache_is_valid(const TileCacheCore* cache, const char* cache_key);
int32_t  tile_cache_is_negative(const TileCacheCore* cache, const char* cache_key);

/// Remove expired tiles. Returns the number of distinct tiles removed.
/// Return type is uint64_t — use a UInt64 on the Swift side.
uint64_t tile_cache_cleanup_expired(TileCacheCore* cache);

uint64_t tile_cache_size(const TileCacheCore* cache);
size_t   tile_cache_count(const TileCacheCore* cache);
int32_t  tile_cache_clear_all(TileCacheCore* cache);

/// Read cache statistics. All out_ pointers are optional (pass NULL to skip).
/// Statistics are managed entirely by Rust; do not attempt to increment them
/// from Swift — doing so will double-count events.
void tile_cache_get_stats(const TileCacheCore* cache,
                          uint64_t* out_memory_hits,
                          uint64_t* out_disk_hits,
                          uint64_t* out_network_fetches,
                          uint64_t* out_cache_misses,
                          uint64_t* out_expired_tiles);

void tile_cache_reset_stats(TileCacheCore* cache);
void tile_cache_free(TileCacheCore* cache);

// MARK: - GTFS-RT

GtfsRtCore* gtfs_rt_new(void);

/// Parse GTFS-RT protobuf data. Returns 0 on success, -1 on error.
/// On error, call yonder_last_error() for the prost decode message.
int32_t gtfs_rt_parse(GtfsRtCore* core, const uint8_t* data, size_t data_len);

/// Get all vehicles as a CVehicle array.
/// Free with gtfs_rt_free_vehicles(ptr, *out_count).
/// Returns NULL if no vehicles are present.
CVehicle* gtfs_rt_get_vehicles(const GtfsRtCore* core, size_t* out_count);

size_t gtfs_rt_vehicle_count(const GtfsRtCore* core);

/// Free the vehicle array returned by gtfs_rt_get_vehicles.
/// `count` must match the value written to out_count.
void gtfs_rt_free_vehicles(CVehicle* vehicles, size_t count);

void gtfs_rt_free(GtfsRtCore* core);

#endif /* YonderCore_h */
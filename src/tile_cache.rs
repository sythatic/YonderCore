use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::fs;
use std::io::{self, BufWriter, Write};
use serde::{Serialize, Deserialize};
use bincode::{Encode, Decode};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// MARK: - Core Data Structures

/// Metadata for a cached tile - optimized with smaller types where possible
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encode, Decode)]
pub struct TileMeta {
    /// Unix timestamp (seconds since epoch) when tile expires
    pub expiration: i64,
    /// Unix timestamp when tile was downloaded
    pub download_time: i64,
    /// Size of the tile data in bytes
    pub data_size: u32,
    /// HTTP status code from the response
    pub http_status: u16,
    /// Whether this is a negative cache entry (404)
    pub is_negative: bool,
}

impl TileMeta {
    /// Create new metadata for a successful tile
    #[inline]
    pub fn new_success(expiration_secs: i64, data_size: u32) -> Self {
        let now = current_timestamp();
        Self {
            expiration: now + expiration_secs,
            download_time: now,
            data_size,
            http_status: 200,
            is_negative: false,
        }
    }

    /// Create new metadata for a negative cache entry (404)
    #[inline]
    pub fn new_negative(expiration_secs: i64) -> Self {
        let now = current_timestamp();
        Self {
            expiration: now + expiration_secs,
            download_time: now,
            data_size: 0,
            http_status: 404,
            is_negative: true,
        }
    }

    /// Check if this tile has expired
    #[inline(always)]
    pub fn is_expired(&self) -> bool {
        self.expiration < current_timestamp()
    }
}

/// A cached tile with its data and metadata
#[derive(Debug, Clone)]
pub struct CachedTile {
    pub data: Vec<u8>,
    pub meta: TileMeta,
}

/// Cache statistics tracked with atomic operations for thread-safe, lock-free updates
#[derive(Debug)]
pub struct CacheStatistics {
    memory_hits: AtomicU64,
    disk_hits: AtomicU64,
    network_fetches: AtomicU64,
    cache_misses: AtomicU64,
    expired_tiles: AtomicU64,
}

impl Default for CacheStatistics {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheStatistics {
    pub fn new() -> Self {
        Self {
            memory_hits: AtomicU64::new(0),
            disk_hits: AtomicU64::new(0),
            network_fetches: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            expired_tiles: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    pub fn record_memory_hit(&self) {
        self.memory_hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_disk_hit(&self) {
        self.disk_hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_network_fetch(&self) {
        self.network_fetches.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_expired(&self) {
        self.expired_tiles.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> CacheStatsSnapshot {
        CacheStatsSnapshot {
            memory_hits: self.memory_hits.load(Ordering::Relaxed),
            disk_hits: self.disk_hits.load(Ordering::Relaxed),
            network_fetches: self.network_fetches.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            expired_tiles: self.expired_tiles.load(Ordering::Relaxed),
        }
    }

    pub fn reset(&self) {
        self.memory_hits.store(0, Ordering::Relaxed);
        self.disk_hits.store(0, Ordering::Relaxed);
        self.network_fetches.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.expired_tiles.store(0, Ordering::Relaxed);
    }
}

/// Immutable snapshot of cache statistics
#[derive(Debug, Clone, Copy)]
pub struct CacheStatsSnapshot {
    pub memory_hits: u64,
    pub disk_hits: u64,
    pub network_fetches: u64,
    pub cache_misses: u64,
    pub expired_tiles: u64,
}

impl CacheStatsSnapshot {
    #[inline]
    pub fn total_requests(&self) -> u64 {
        self.memory_hits + self.disk_hits + self.network_fetches + self.cache_misses
    }

    pub fn hit_rate(&self) -> f64 {
        let hits = self.memory_hits + self.disk_hits;
        let total = self.total_requests();
        if total > 0 {
            hits as f64 / total as f64
        } else {
            0.0
        }
    }
}

// MARK: - Optimized Tile Cache Core

/// Memory cache entry
#[derive(Clone)]
struct MemoryCacheEntry {
    data: Arc<Vec<u8>>,  // Arc for cheap clones
    meta: TileMeta,
    last_access: i64,
}

/// Core tile cache manager - optimized with memory cache and better I/O
pub struct TileCacheCore {
    cache_path: PathBuf,
    statistics: Arc<CacheStatistics>,
    memory_cache: Arc<RwLock<HashMap<String, MemoryCacheEntry>>>,
    max_memory_entries: usize,
    bincode_config: bincode::config::Configuration,
    // Optional: Store original keys mapped to hashed filenames for debugging
    key_mapping: Arc<RwLock<HashMap<String, String>>>,  // original -> hashed
}

impl TileCacheCore {
    /// Create a new tile cache core with memory cache
    pub fn new(cache_path: PathBuf) -> io::Result<Self> {
        // Ensure cache directory exists
        if !cache_path.exists() {
            fs::create_dir_all(&cache_path)?;
        }

        Ok(Self {
            cache_path,
            statistics: Arc::new(CacheStatistics::new()),
            memory_cache: Arc::new(RwLock::new(HashMap::with_capacity(1000))),
            max_memory_entries: 1000,  // Configurable memory cache size
            bincode_config: bincode::config::standard(),
            key_mapping: Arc::new(RwLock::new(HashMap::with_capacity(1000))),
        })
    }

    /// Save a tile to disk with metadata and update memory cache
    pub fn save_tile(&self, cache_key: &str, data: &[u8], meta: &TileMeta) -> io::Result<()> {
        let hashed_key = self.hash_cache_key(cache_key);
        let tile_path = self.tile_path(&hashed_key);
        let meta_path = self.meta_path(&hashed_key);

        // Store the mapping for debugging
        if let Ok(mut mapping) = self.key_mapping.write() {
            mapping.insert(cache_key.to_string(), hashed_key.clone());
        }

        // Write tile data with buffered writer for better performance
        {
            let file = fs::File::create(&tile_path)?;
            let mut writer = BufWriter::with_capacity(64 * 1024, file);
            writer.write_all(data)?;
            writer.flush()?;
        }

        // Write metadata
        let meta_bytes = bincode::encode_to_vec(meta, self.bincode_config)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{}", e)))?;

        fs::write(&meta_path, meta_bytes)?;

        // Update memory cache
        if let Ok(mut cache) = self.memory_cache.write() {
            // Evict old entries if at capacity
            if cache.len() >= self.max_memory_entries {
                self.evict_lru_from_memory(&mut cache);
            }

            cache.insert(
                cache_key.to_string(),
                MemoryCacheEntry {
                    data: Arc::new(data.to_vec()),
                    meta: *meta,
                    last_access: current_timestamp(),
                },
            );
        }

        Ok(())
    }

    /// Load a tile - first check memory cache, then disk
    pub fn load_tile(&self, cache_key: &str) -> Option<CachedTile> {
        // Check memory cache first
        if let Ok(mut cache) = self.memory_cache.write() {
            if let Some(entry) = cache.get_mut(cache_key) {
                // Check if expired
                if entry.meta.is_expired() {
                    self.statistics.record_expired();
                    cache.remove(cache_key);
                    return None;
                }

                // Update access time and record hit
                entry.last_access = current_timestamp();
                self.statistics.record_memory_hit();

                return Some(CachedTile {
                    data: (*entry.data).clone(),
                    meta: entry.meta,
                });
            }
        }

        // Not in memory, try disk
        let hashed_key = self.hash_cache_key(cache_key);
        let tile_path = self.tile_path(&hashed_key);
        let meta_path = self.meta_path(&hashed_key);

        // Check if files exist
        if !tile_path.exists() || !meta_path.exists() {
            self.statistics.record_cache_miss();
            return None;
        }

        // Load metadata and check expiration
        let meta = self.load_metadata_internal(&meta_path)?;

        if meta.is_expired() {
            self.statistics.record_expired();
            // Clean up expired files
            let _ = fs::remove_file(&tile_path);
            let _ = fs::remove_file(&meta_path);
            return None;
        }

        // Load tile data
        let data = fs::read(&tile_path).ok()?;

        self.statistics.record_disk_hit();

        // Update memory cache with the loaded tile
        if let Ok(mut cache) = self.memory_cache.write() {
            if cache.len() >= self.max_memory_entries {
                self.evict_lru_from_memory(&mut cache);
            }

            cache.insert(
                cache_key.to_string(),
                MemoryCacheEntry {
                    data: Arc::new(data.clone()),
                    meta,
                    last_access: current_timestamp(),
                },
            );
        }

        Some(CachedTile { data, meta })
    }

    /// Remove a tile from cache (both memory and disk)
    pub fn remove_tile(&self, cache_key: &str) -> io::Result<()> {
        // Remove from memory cache
        if let Ok(mut cache) = self.memory_cache.write() {
            cache.remove(cache_key);
        }

        // Remove from disk
        let hashed_key = self.hash_cache_key(cache_key);
        let tile_path = self.tile_path(&hashed_key);
        let meta_path = self.meta_path(&hashed_key);

        // Attempt to remove both files (ignore errors if files don't exist)
        let _ = fs::remove_file(&tile_path);
        let _ = fs::remove_file(&meta_path);

        // Remove from key mapping
        if let Ok(mut mapping) = self.key_mapping.write() {
            mapping.retain(|_, v| v != &hashed_key);
        }

        Ok(())
    }

    /// Check if a tile exists and is not expired
    /// NOTE: This now excludes negative cache entries for clearer semantics
    pub fn is_valid(&self, cache_key: &str) -> bool {
        // Check memory cache first
        if let Ok(cache) = self.memory_cache.read() {
            if let Some(entry) = cache.get(cache_key) {
                // Valid if not expired AND not a negative cache entry
                return !entry.meta.is_expired() && !entry.meta.is_negative;
            }
        }

        // Check disk
        let hashed_key = self.hash_cache_key(cache_key);
        let meta_path = self.meta_path(&hashed_key);

        if let Some(meta) = self.load_metadata_internal(&meta_path) {
            // Valid if not expired AND not a negative cache entry
            return !meta.is_expired() && !meta.is_negative;
        }

        false
    }

    /// Clean up expired tiles from disk and memory
    pub fn cleanup_expired(&self) -> io::Result<usize> {
        let mut removed_count = 0;

        // Clean memory cache
        if let Ok(mut cache) = self.memory_cache.write() {
            let now = current_timestamp();
            cache.retain(|_, entry| {
                if entry.meta.expiration < now {
                    removed_count += 1;
                    false
                } else {
                    true
                }
            });
        }

        // Clean disk cache
        for entry in fs::read_dir(&self.cache_path)? {
            let entry = entry?;
            let path = entry.path();

            if let Some(ext) = path.extension() {
                if ext == "meta" {
                    // Load metadata and check expiration
                    if let Some(meta) = self.load_metadata_internal(&path) {
                        if meta.is_expired() {
                            // Remove both meta and tile files
                            let _ = fs::remove_file(&path);

                            let mut tile_path = path.clone();
                            tile_path.set_extension("tile");
                            let _ = fs::remove_file(&tile_path);

                            removed_count += 1;
                        }
                    }
                }
            }
        }

        Ok(removed_count)
    }

    /// Get total cache size in bytes (disk only)
    pub fn cache_size(&self) -> io::Result<u64> {
        let mut total_size = 0u64;

        for entry in fs::read_dir(&self.cache_path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;

            if metadata.is_file() {
                total_size += metadata.len();
            }
        }

        Ok(total_size)
    }

    /// Get number of tiles in cache (memory + disk, deduplicated)
    pub fn tile_count(&self) -> io::Result<usize> {
        let mut tile_files = std::collections::HashSet::new();

        // Count disk tiles
        for entry in fs::read_dir(&self.cache_path)? {
            let entry = entry?;
            let path = entry.path();

            if let Some(ext) = path.extension() {
                if ext == "tile" {
                    if let Some(stem) = path.file_stem() {
                        tile_files.insert(stem.to_string_lossy().to_string());
                    }
                }
            }
        }

        Ok(tile_files.len())
    }

    /// Clear all tiles from cache
    pub fn clear_all(&self) -> io::Result<()> {
        // Clear memory cache
        if let Ok(mut cache) = self.memory_cache.write() {
            cache.clear();
        }

        // Clear key mapping
        if let Ok(mut mapping) = self.key_mapping.write() {
            mapping.clear();
        }

        // Clear disk cache
        for entry in fs::read_dir(&self.cache_path)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                fs::remove_file(path)?;
            }
        }

        Ok(())
    }

    /// Save a negative cache entry (404, no data)
    pub fn save_negative_cache(&self, cache_key: &str, ttl_seconds: i64) -> io::Result<()> {
        let meta = TileMeta::new_negative(ttl_seconds);
        self.save_tile(cache_key, &[], &meta)
    }

    /// Check if an entry is a negative cache entry
    pub fn is_negative_cache(&self, cache_key: &str) -> bool {
        // Check memory cache first
        if let Ok(cache) = self.memory_cache.read() {
            if let Some(entry) = cache.get(cache_key) {
                return entry.meta.is_negative && !entry.meta.is_expired();
            }
        }

        // Check disk
        let hashed_key = self.hash_cache_key(cache_key);
        let meta_path = self.meta_path(&hashed_key);

        if let Some(meta) = self.load_metadata_internal(&meta_path) {
            return meta.is_negative && !meta.is_expired();
        }

        false
    }

    // MARK: - Statistics Methods

    pub fn record_memory_hit(&self) {
        self.statistics.record_memory_hit();
    }

    pub fn record_network_fetch(&self) {
        self.statistics.record_network_fetch();
    }

    pub fn record_cache_miss(&self) {
        self.statistics.record_cache_miss();
    }

    pub fn statistics(&self) -> CacheStatsSnapshot {
        self.statistics.snapshot()
    }

    pub fn reset_statistics(&self) {
        self.statistics.reset();
    }

    // MARK: - LRU Eviction

    fn evict_lru_from_memory(&self, cache: &mut HashMap<String, MemoryCacheEntry>) {
        if cache.is_empty() {
            return;
        }

        // Find the least recently accessed entry
        let mut oldest_key = None;
        let mut oldest_time = i64::MAX;

        for (key, entry) in cache.iter() {
            if entry.last_access < oldest_time {
                oldest_time = entry.last_access;
                oldest_key = Some(key.clone());
            }
        }

        if let Some(key) = oldest_key {
            cache.remove(&key);
        }
    }

    // MARK: - Private Helpers

    /// Hash cache key for filesystem safety and collision resistance
    /// Uses first 16 hex chars of hash for reasonable uniqueness
    fn hash_cache_key(&self, key: &str) -> String {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        // Convert to hex string (16 chars = 64 bits of entropy)
        format!("{:016x}", hash)
    }

    #[inline]
    fn tile_path(&self, hashed_key: &str) -> PathBuf {
        let mut path = self.cache_path.clone();
        path.push(format!("{}.tile", hashed_key));
        path
    }

    #[inline]
    fn meta_path(&self, hashed_key: &str) -> PathBuf {
        let mut path = self.cache_path.clone();
        path.push(format!("{}.meta", hashed_key));
        path
    }

    fn load_metadata_internal(&self, meta_path: &Path) -> Option<TileMeta> {
        let data = fs::read(meta_path).ok()?;
        let (meta, _): (TileMeta, usize) =
            bincode::decode_from_slice(&data, self.bincode_config).ok()?;
        Some(meta)
    }

    /// Get memory cache stats for monitoring
    pub fn memory_cache_info(&self) -> (usize, usize) {
        if let Ok(cache) = self.memory_cache.read() {
            let count = cache.len();
            let total_size: usize = cache
                .values()
                .map(|entry| entry.data.len())
                .sum();
            (count, total_size)
        } else {
            (0, 0)
        }
    }

    /// Set max memory cache entries
    pub fn set_max_memory_entries(&mut self, max_entries: usize) {
        self.max_memory_entries = max_entries;
    }

    /// Debug helper: Get original key from hashed filename
    pub fn get_original_key(&self, hashed_key: &str) -> Option<String> {
        if let Ok(mapping) = self.key_mapping.read() {
            mapping.iter()
                .find(|(_, v)| v.as_str() == hashed_key)
                .map(|(k, _)| k.clone())
        } else {
            None
        }
    }
}

// MARK: - Utility Functions

/// Get current Unix timestamp in seconds (cached for better performance)
#[inline(always)]
fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

// MARK: - Tests

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::thread;

    #[test]
    fn test_tile_meta_expiration() {
        let meta = TileMeta::new_success(60, 1024);
        assert!(!meta.is_expired());

        let expired_meta = TileMeta {
            expiration: current_timestamp() - 10,
            download_time: current_timestamp() - 70,
            data_size: 1024,
            http_status: 200,
            is_negative: false,
        };
        assert!(expired_meta.is_expired());
    }

    #[test]
    fn test_statistics_thread_safety() {
        let stats = Arc::new(CacheStatistics::new());
        let mut handles = vec![];

        // Spawn multiple threads incrementing counters
        for _ in 0..10 {
            let stats_clone = Arc::clone(&stats);
            let handle = thread::spawn(move || {
                for _ in 0..1000 {
                    stats_clone.record_memory_hit();
                    stats_clone.record_disk_hit();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.memory_hits, 10000);
        assert_eq!(snapshot.disk_hits, 10000);
    }

    #[test]
    fn test_memory_cache() {
        let temp_dir = std::env::temp_dir().join("test_memory_cache");
        let _ = fs::remove_dir_all(&temp_dir);

        let cache = TileCacheCore::new(temp_dir.clone()).unwrap();

        // Save a tile
        let data = b"test tile data";
        let meta = TileMeta::new_success(60, data.len() as u32);
        cache.save_tile("test_tile", data, &meta).unwrap();

        // First load should be from memory
        let loaded = cache.load_tile("test_tile").unwrap();
        assert_eq!(loaded.data, data);

        let stats = cache.statistics();
        assert_eq!(stats.memory_hits, 1);
        assert_eq!(stats.disk_hits, 0);

        // Clear memory cache
        if let Ok(mut mem_cache) = cache.memory_cache.write() {
            mem_cache.clear();
        }

        // Second load should be from disk
        let loaded = cache.load_tile("test_tile").unwrap();
        assert_eq!(loaded.data, data);

        let stats = cache.statistics();
        assert_eq!(stats.memory_hits, 1);
        assert_eq!(stats.disk_hits, 1);

        // Cleanup
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_concurrent_access() {
        let temp_dir = std::env::temp_dir().join("test_concurrent");
        let _ = fs::remove_dir_all(&temp_dir);

        let cache = Arc::new(TileCacheCore::new(temp_dir.clone()).unwrap());
        let mut handles = vec![];

        // Multiple threads writing and reading
        for i in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                let data = format!("data_{}", i).into_bytes();
                let meta = TileMeta::new_success(60, data.len() as u32);
                cache_clone.save_tile(&format!("tile_{}", i), &data, &meta).unwrap();

                // Read it back
                let loaded = cache_clone.load_tile(&format!("tile_{}", i)).unwrap();
                assert_eq!(loaded.data, data);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all tiles are present
        assert_eq!(cache.tile_count().unwrap(), 10);

        // Cleanup
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_hash_cache_key() {
        let temp_dir = std::env::temp_dir().join("test_hash");
        let _ = fs::remove_dir_all(&temp_dir);

        let cache = TileCacheCore::new(temp_dir.clone()).unwrap();

        // Test that different keys produce different hashes
        let hash1 = cache.hash_cache_key("test/key/1");
        let hash2 = cache.hash_cache_key("test/key/2");
        assert_ne!(hash1, hash2);

        // Test that same key produces same hash
        let hash3 = cache.hash_cache_key("test/key/1");
        assert_eq!(hash1, hash3);

        // Test that hash is exactly 16 hex chars
        assert_eq!(hash1.len(), 16);
        assert!(hash1.chars().all(|c| c.is_ascii_hexdigit()));

        // Cleanup
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_is_valid_semantics() {
        let temp_dir = std::env::temp_dir().join("test_valid_semantics");
        let _ = fs::remove_dir_all(&temp_dir);

        let cache = TileCacheCore::new(temp_dir.clone()).unwrap();

        // Save a valid tile
        let data = b"valid data";
        let meta = TileMeta::new_success(60, data.len() as u32);
        cache.save_tile("valid_tile", data, &meta).unwrap();

        // Save a negative cache entry
        cache.save_negative_cache("negative_tile", 60).unwrap();

        // Test is_valid semantics: valid tile should be valid
        assert!(cache.is_valid("valid_tile"));

        // Test is_valid semantics: negative cache should NOT be valid
        assert!(!cache.is_valid("negative_tile"));

        // But negative cache should still be recognized as negative
        assert!(cache.is_negative_cache("negative_tile"));

        // Cleanup
        let _ = fs::remove_dir_all(&temp_dir);
    }
}
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::fs;
use std::io::{self, BufWriter, Write};
use serde::{Serialize, Deserialize};
use rkyv::{Archive, Serialize as RkyvSerialize, Deserialize as RkyvDeserialize};
// NOTE: DefaultHasher is explicitly NOT stable across Rust versions/builds.
// Using a simple inline FNV-1a instead to guarantee disk cache keys are
// consistent across app updates.

// MARK: - Core Data Structures

/// Metadata for a cached tile - optimized with smaller types where possible
/// Using rkyv for zero-copy deserialization (much faster than bincode)
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
#[rkyv(derive(Debug))]
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
///
/// `last_access` uses `AtomicI64` so that `load_tile` can update the LRU
/// timestamp while holding only a *read* lock on the outer HashMap, rather
/// than upgrading to a write lock on every cache hit.
#[derive(Clone)]
struct MemoryCacheEntry {
    data: Arc<Vec<u8>>,  // Arc for cheap clones
    meta: TileMeta,
    last_access: Arc<AtomicI64>,
}

/// Core tile cache manager - optimized with memory cache and rkyv for zero-copy I/O
pub struct TileCacheCore {
    cache_path: PathBuf,
    statistics: Arc<CacheStatistics>,
    memory_cache: Arc<RwLock<HashMap<String, MemoryCacheEntry>>>,
    max_memory_entries: usize,
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

        // Prepend a version byte so future TileMeta layout changes can be
        // detected on read rather than silently producing garbage data.
        const META_VERSION: u8 = 1;
        let meta_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(meta)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("rkyv serialization error: {}", e)))?;
        let mut versioned = Vec::with_capacity(1 + meta_bytes.len());
        versioned.push(META_VERSION);
        versioned.extend_from_slice(&meta_bytes);

        fs::write(&meta_path, &versioned)?;

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
                    last_access: Arc::new(AtomicI64::new(current_timestamp())),
                },
            );
        }

        Ok(())
    }

    /// Load a tile - first check memory cache, then disk
    pub fn load_tile(&self, cache_key: &str) -> Option<CachedTile> {
        // Check memory cache first with a READ lock.
        // `last_access` is AtomicI64, so we can update the LRU timestamp
        // without upgrading to a write lock on every cache hit.
        if let Ok(cache) = self.memory_cache.read() {
            if let Some(entry) = cache.get(cache_key) {
                // Check if expired
                if entry.meta.is_expired() {
                    self.statistics.record_expired();
                    // Can't remove here (read lock) — drop the read lock first,
                    // then acquire write lock to evict. The tile will be re-checked
                    // below and evicted on the next write-lock opportunity.
                    drop(cache);
                    if let Ok(mut write_cache) = self.memory_cache.write() {
                        // Re-check under write lock to avoid TOCTOU
                        if write_cache.get(cache_key).map_or(false, |e| e.meta.is_expired()) {
                            write_cache.remove(cache_key);
                        }
                    }
                    return None;
                }

                // Touch LRU timestamp atomically — no write lock needed
                entry.last_access.store(current_timestamp(), Ordering::Relaxed);
                self.statistics.record_memory_hit();

                return Some(CachedTile {
                    data: (*entry.data).clone(),
                    meta: entry.meta,
                });
            }
        }

        // Not in memory, check disk
        let hashed_key = self.hash_cache_key(cache_key);
        let tile_path = self.tile_path(&hashed_key);
        let meta_path = self.meta_path(&hashed_key);

        // Load metadata first to check expiration before reading tile data
        let meta = self.load_metadata_internal(&meta_path)?;

        if meta.is_expired() {
            self.statistics.record_expired();
            // Delete expired files
            let _ = fs::remove_file(tile_path);
            let _ = fs::remove_file(meta_path);
            return None;
        }

        // Load tile data
        let data = fs::read(tile_path).ok()?;

        self.statistics.record_disk_hit();

        // Add to memory cache for faster future access
        if let Ok(mut cache) = self.memory_cache.write() {
            if cache.len() >= self.max_memory_entries {
                self.evict_lru_from_memory(&mut cache);
            }

            cache.insert(
                cache_key.to_string(),
                MemoryCacheEntry {
                    data: Arc::new(data.clone()),
                    meta,
                    last_access: Arc::new(AtomicI64::new(current_timestamp())),
                },
            );
        }

        Some(CachedTile { data, meta })
    }

    /// Check if a tile exists and is valid (not expired, not negative cache)
    pub fn is_valid(&self, cache_key: &str) -> bool {
        // Check memory cache first
        if let Ok(cache) = self.memory_cache.read() {
            if let Some(entry) = cache.get(cache_key) {
                return !entry.meta.is_expired() && !entry.meta.is_negative;
            }
        }

        // Check disk
        let hashed_key = self.hash_cache_key(cache_key);
        let meta_path = self.meta_path(&hashed_key);

        if let Some(meta) = self.load_metadata_internal(&meta_path) {
            !meta.is_expired() && !meta.is_negative
        } else {
            false
        }
    }

    /// Check if a tile is a negative cache entry (404)
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
            meta.is_negative && !meta.is_expired()
        } else {
            false
        }
    }

    /// Save a negative cache entry (e.g., for 404 responses)
    pub fn save_negative_cache(&self, cache_key: &str, expiration_secs: i64) -> io::Result<()> {
        let meta = TileMeta::new_negative(expiration_secs);
        let hashed_key = self.hash_cache_key(cache_key);
        let meta_path = self.meta_path(&hashed_key);

        // Store the mapping for debugging
        if let Ok(mut mapping) = self.key_mapping.write() {
            mapping.insert(cache_key.to_string(), hashed_key);
        }

        // Serialize metadata with rkyv (versioned — see save_tile)
        const META_VERSION: u8 = 1;
        let meta_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&meta)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("rkyv serialization error: {}", e)))?;
        let mut versioned = Vec::with_capacity(1 + meta_bytes.len());
        versioned.push(META_VERSION);
        versioned.extend_from_slice(&meta_bytes);

        fs::write(&meta_path, &versioned)?;

        // Update memory cache with empty data
        if let Ok(mut cache) = self.memory_cache.write() {
            if cache.len() >= self.max_memory_entries {
                self.evict_lru_from_memory(&mut cache);
            }

            cache.insert(
                cache_key.to_string(),
                MemoryCacheEntry {
                    data: Arc::new(Vec::new()),
                    meta,
                    last_access: Arc::new(AtomicI64::new(current_timestamp())),
                },
            );
        }

        Ok(())
    }

    /// Delete a specific tile from cache
    pub fn delete_tile(&self, cache_key: &str) -> io::Result<()> {
        // Remove from memory cache
        if let Ok(mut cache) = self.memory_cache.write() {
            cache.remove(cache_key);
        }

        // Remove from disk
        let hashed_key = self.hash_cache_key(cache_key);
        let tile_path = self.tile_path(&hashed_key);
        let meta_path = self.meta_path(&hashed_key);

        // Try to remove both files (ignore errors if they don't exist)
        let _ = fs::remove_file(tile_path);
        let _ = fs::remove_file(meta_path);

        // Remove from key mapping
        if let Ok(mut mapping) = self.key_mapping.write() {
            mapping.remove(cache_key);
        }

        Ok(())
    }

    /// Clear all cached tiles
    pub fn clear_all(&self) -> io::Result<()> {
        // Clear memory cache
        if let Ok(mut cache) = self.memory_cache.write() {
            cache.clear();
        }

        // Clear key mapping
        if let Ok(mut mapping) = self.key_mapping.write() {
            mapping.clear();
        }

        // Delete all files in cache directory
        if self.cache_path.exists() {
            for entry in fs::read_dir(&self.cache_path)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() {
                    fs::remove_file(path)?;
                }
            }
        }

        Ok(())
    }

    /// Clear expired tiles from disk and memory
    pub fn clear_expired(&self) -> io::Result<u64> {
        let mut cleared_count = 0u64;

        // Clear from memory cache
        if let Ok(mut cache) = self.memory_cache.write() {
            let now = current_timestamp();
            cache.retain(|_, entry| {
                let expired = entry.meta.expiration < now;
                if expired {
                    cleared_count += 1;
                }
                !expired
            });
        }

        // Clear from disk
        if self.cache_path.exists() {
            for entry in fs::read_dir(&self.cache_path)? {
                let entry = entry?;
                let path = entry.path();

                // Only process .meta files
                if path.extension().and_then(|s| s.to_str()) == Some("meta") {
                    if let Some(meta) = self.load_metadata_internal(&path) {
                        if meta.is_expired() {
                            // Delete both .meta and .tile files
                            let stem = path.file_stem().unwrap();
                            let tile_path = self.cache_path.join(format!("{}.tile", stem.to_string_lossy()));

                            let _ = fs::remove_file(&path);
                            let _ = fs::remove_file(tile_path);
                            cleared_count += 1;
                        }
                    }
                }
            }
        }

        Ok(cleared_count)
    }

    /// Get total number of cached tiles on disk
    pub fn tile_count(&self) -> io::Result<usize> {
        let mut count = 0;
        if self.cache_path.exists() {
            for entry in fs::read_dir(&self.cache_path)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("tile") {
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    /// Get total cache size on disk in bytes
    pub fn cache_size(&self) -> io::Result<u64> {
        let mut total_size = 0u64;
        if self.cache_path.exists() {
            for entry in fs::read_dir(&self.cache_path)? {
                let entry = entry?;
                let metadata = entry.metadata()?;
                if metadata.is_file() {
                    total_size += metadata.len();
                }
            }
        }
        Ok(total_size)
    }

    /// Get cache statistics
    pub fn statistics(&self) -> CacheStatsSnapshot {
        self.statistics.snapshot()
    }

    /// Reset statistics counters
    pub fn reset_statistics(&self) {
        self.statistics.reset();
    }

    /// Evict least recently used entry from memory cache.
    ///
    /// Called while the write lock on `memory_cache` is already held.
    /// Also purges the evicted key from `key_mapping` to prevent unbounded growth.
    fn evict_lru_from_memory(&self, cache: &mut HashMap<String, MemoryCacheEntry>) {
        if cache.is_empty() {
            return;
        }

        // Find the entry with the oldest access time.
        // O(n) scan is acceptable at our 1,000-entry cap.
        let mut oldest_time = i64::MAX;
        let mut oldest_key: Option<String> = None;

        for (key, entry) in cache.iter() {
            let t = entry.last_access.load(Ordering::Relaxed);
            if t < oldest_time {
                oldest_time = t;
                oldest_key = Some(key.clone());
            }
        }

        if let Some(key) = oldest_key {
            cache.remove(&key);
            // Purge from key_mapping to prevent unbounded memory growth.
            if let Ok(mut mapping) = self.key_mapping.write() {
                mapping.remove(&key);
            }
        }
    }

    // MARK: - Private Helpers

    /// Hash cache key for filesystem safety and collision resistance.
    ///
    /// Uses FNV-1a rather than `DefaultHasher` because `DefaultHasher` is
    /// explicitly not guaranteed to be stable across Rust versions or builds.
    /// An unstable hasher would silently orphan the entire disk cache after
    /// any app update that bumps the Rust toolchain.
    fn hash_cache_key(&self, key: &str) -> String {
        // FNV-1a 64-bit inline — no extra dependency required
        const FNV_OFFSET: u64 = 14695981039346656037;
        const FNV_PRIME: u64 = 1099511628211;
        let mut hash = FNV_OFFSET;
        for byte in key.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
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

        // Version byte was prepended at write time.
        // Unknown versions are rejected rather than producing garbage data.
        const META_VERSION: u8 = 1;
        let (&version, payload) = data.split_first()?;
        if version != META_VERSION {
            // Stale file from an incompatible build — treat as a cache miss.
            let _ = fs::remove_file(meta_path);
            return None;
        }

        // Zero-copy deserialization with rkyv using the correct archived type
        let archived = rkyv::access::<ArchivedTileMeta, rkyv::rancor::Error>(payload).ok()?;

        // Copy fields from archived data (very cheap - just copying primitive types)
        Some(TileMeta {
            expiration: archived.expiration.into(),
            download_time: archived.download_time.into(),
            data_size: archived.data_size.into(),
            http_status: archived.http_status.into(),
            is_negative: archived.is_negative.into(),
        })
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

    // MARK: - Statistics Recording Methods (for API compatibility)

    /// Record a memory cache hit
    #[inline(always)]
    pub fn record_memory_hit(&self) {
        self.statistics.record_memory_hit();
    }

    /// Record a disk cache hit
    #[inline(always)]
    pub fn record_disk_hit(&self) {
        self.statistics.record_disk_hit();
    }

    /// Record a network fetch
    #[inline(always)]
    pub fn record_network_fetch(&self) {
        self.statistics.record_network_fetch();
    }

    /// Record a cache miss
    #[inline(always)]
    pub fn record_cache_miss(&self) {
        self.statistics.record_cache_miss();
    }

    /// Record an expired tile
    #[inline(always)]
    pub fn record_expired(&self) {
        self.statistics.record_expired();
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

    #[test]
    fn test_rkyv_serialization() {
        let temp_dir = std::env::temp_dir().join("test_rkyv");
        let _ = fs::remove_dir_all(&temp_dir);

        let cache = TileCacheCore::new(temp_dir.clone()).unwrap();

        // Create metadata
        let meta = TileMeta::new_success(3600, 4096);

        // Save some data with metadata
        let data = b"rkyv test data";
        cache.save_tile("rkyv_test", data, &meta).unwrap();

        // Load it back
        let loaded = cache.load_tile("rkyv_test").unwrap();

        // Verify all fields match
        assert_eq!(loaded.meta.data_size, meta.data_size);
        assert_eq!(loaded.meta.http_status, meta.http_status);
        assert_eq!(loaded.meta.is_negative, meta.is_negative);
        assert_eq!(loaded.data, data);

        // Cleanup
        let _ = fs::remove_dir_all(&temp_dir);
    }
}
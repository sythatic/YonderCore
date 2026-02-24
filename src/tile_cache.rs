use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicI64, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;  // No lock-poisoning on panic; faster than std RwLock
use std::collections::HashMap;
use std::fs;
use std::io::{self, BufWriter, Write};
use serde::{Serialize, Deserialize};
use rkyv::{Archive, Serialize as RkyvSerialize, Deserialize as RkyvDeserialize};
use fnv::FnvHasher;
use std::hash::{Hash, Hasher};

// MARK: - Core Data Structures

/// Metadata for a cached tile — optimized with smaller types where possible.
/// Uses rkyv for zero-copy deserialization (much faster than bincode/serde_json).
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

/// Cache statistics — tracked with atomics for lock-free, thread-safe updates.
///
/// Statistics are managed exclusively by TileCacheCore internally. The C/Swift
/// layer reads them via tile_cache_get_stats but never writes them directly.
/// This avoids double-counting that would occur if both sides incremented.
#[derive(Debug)]
pub struct CacheStatistics {
    pub(crate) memory_hits: AtomicU64,
    pub(crate) disk_hits: AtomicU64,
    pub(crate) network_fetches: AtomicU64,
    pub(crate) cache_misses: AtomicU64,
    pub(crate) expired_tiles: AtomicU64,
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
    pub(crate) fn record_memory_hit(&self) {
        self.memory_hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub(crate) fn record_disk_hit(&self) {
        self.disk_hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub(crate) fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub(crate) fn record_expired(&self) {
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
    data: Arc<Vec<u8>>,
    meta: TileMeta,
    /// Updated lock-free so load_tile can hold a read lock while touching this.
    last_access: Arc<AtomicI64>,
}

/// Core tile cache manager with memory + disk layers and rkyv metadata serialization.
pub struct TileCacheCore {
    cache_path: PathBuf,
    statistics: Arc<CacheStatistics>,
    memory_cache: Arc<RwLock<HashMap<String, MemoryCacheEntry>>>,
    max_memory_entries: usize,
    /// Debug aid: maps original cache keys → hashed filenames
    key_mapping: Arc<RwLock<HashMap<String, String>>>,
}

impl TileCacheCore {
    pub fn new(cache_path: PathBuf) -> io::Result<Self> {
        if !cache_path.exists() {
            fs::create_dir_all(&cache_path)?;
        }

        Ok(Self {
            cache_path,
            statistics: Arc::new(CacheStatistics::new()),
            memory_cache: Arc::new(RwLock::new(HashMap::with_capacity(1000))),
            max_memory_entries: 1000,
            key_mapping: Arc::new(RwLock::new(HashMap::with_capacity(1000))),
        })
    }

    /// Save a tile to disk with metadata and update the memory cache.
    ///
    /// Uses an atomic write strategy: data is written to a `.tmp` file first,
    /// then renamed over the final path. `rename` is atomic on all major
    /// filesystems (APFS, ext4, HFS+), so a crash or out-of-storage condition
    /// mid-write can never leave a corrupt tile file visible to readers.
    pub fn save_tile(&self, cache_key: &str, data: &[u8], meta: &TileMeta) -> io::Result<()> {
        let hashed_key = self.hash_cache_key(cache_key);
        let tile_path = self.tile_path(&hashed_key);
        let meta_path = self.meta_path(&hashed_key);
        // Temporary paths used during atomic write.
        let tile_tmp = tile_path.with_extension("tile.tmp");
        let meta_tmp = meta_path.with_extension("meta.tmp");

        self.key_mapping.write().insert(cache_key.to_string(), hashed_key.clone());

        // Write tile data to a temp file, then atomically rename.
        {
            let file = fs::File::create(&tile_tmp)?;
            let mut writer = BufWriter::with_capacity(64 * 1024, file);
            writer.write_all(data)?;
            writer.flush()?;
        }
        fs::rename(&tile_tmp, &tile_path)?;

        // Write metadata to a temp file, then atomically rename.
        let meta_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(meta)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData,
                                        format!("rkyv serialization error: {}", e)))?;
        fs::write(&meta_tmp, &meta_bytes)?;
        fs::rename(&meta_tmp, &meta_path)?;

        // parking_lot::RwLock::write() returns the guard directly — no Result to unwrap.
        let mut cache = self.memory_cache.write();
        if cache.len() >= self.max_memory_entries {
            self.evict_lru_from_memory(&mut cache);
        }
        cache.insert(cache_key.to_string(), MemoryCacheEntry {
            data: Arc::new(data.to_vec()),
            meta: *meta,
            last_access: Arc::new(AtomicI64::new(current_timestamp())),
        });

        Ok(())
    }

    /// Load a tile — checks memory cache first, then disk.
    ///
    /// Statistics are recorded internally; callers do not need to increment them.
    pub fn load_tile(&self, cache_key: &str) -> Option<CachedTile> {
        // --- Memory pass (read lock) ---
        {
            let cache = self.memory_cache.read();
            if let Some(entry) = cache.get(cache_key) {
                if entry.meta.is_expired() {
                    // Drop read lock before acquiring write lock.
                    drop(cache);
                    self.statistics.record_expired();
                    let mut wcache = self.memory_cache.write();
                    // Re-check under write lock (another thread may have refreshed it).
                    if wcache.get(cache_key).map(|e| e.meta.is_expired()).unwrap_or(false) {
                        wcache.remove(cache_key);
                    }
                    // Fall through to disk path.
                } else {
                    entry.last_access.store(current_timestamp(), Ordering::Relaxed);
                    self.statistics.record_memory_hit();
                    return Some(CachedTile {
                        data: (*entry.data).clone(),
                        meta: entry.meta,
                    });
                }
            }
        }

        // --- Disk pass ---
        let hashed_key = self.hash_cache_key(cache_key);
        let tile_path = self.tile_path(&hashed_key);
        let meta_path = self.meta_path(&hashed_key);

        let meta = self.load_metadata_internal(&meta_path)?;

        if meta.is_expired() {
            self.statistics.record_expired();
            let _ = fs::remove_file(&tile_path);
            let _ = fs::remove_file(&meta_path);
            self.statistics.record_cache_miss();
            return None;
        }

        let data = match fs::read(&tile_path) {
            Ok(d) => d,
            Err(_) => {
                self.statistics.record_cache_miss();
                return None;
            }
        };

        self.statistics.record_disk_hit();

        // Promote to memory cache for future reads.
        {
            let mut cache = self.memory_cache.write();
            if cache.len() >= self.max_memory_entries {
                self.evict_lru_from_memory(&mut cache);
            }
            cache.insert(cache_key.to_string(), MemoryCacheEntry {
                data: Arc::new(data.clone()),
                meta,
                last_access: Arc::new(AtomicI64::new(current_timestamp())),
            });
        }

        Some(CachedTile { data, meta })
    }

    /// Check if a tile exists, is not expired, and is not a negative cache entry.
    pub fn is_valid(&self, cache_key: &str) -> bool {
        {
            let cache = self.memory_cache.read();
            if let Some(entry) = cache.get(cache_key) {
                return !entry.meta.is_expired() && !entry.meta.is_negative;
            }
        }

        let hashed_key = self.hash_cache_key(cache_key);
        let meta_path = self.meta_path(&hashed_key);
        if let Some(meta) = self.load_metadata_internal(&meta_path) {
            !meta.is_expired() && !meta.is_negative
        } else {
            false
        }
    }

    /// Check if a tile is a valid (non-expired) negative cache entry.
    pub fn is_negative_cache(&self, cache_key: &str) -> bool {
        {
            let cache = self.memory_cache.read();
            if let Some(entry) = cache.get(cache_key) {
                return entry.meta.is_negative && !entry.meta.is_expired();
            }
        }

        let hashed_key = self.hash_cache_key(cache_key);
        let meta_path = self.meta_path(&hashed_key);
        if let Some(meta) = self.load_metadata_internal(&meta_path) {
            meta.is_negative && !meta.is_expired()
        } else {
            false
        }
    }

    /// Save a negative cache entry (e.g. for 404 responses).
    /// No tile data is written — only a metadata file.
    pub fn save_negative_cache(&self, cache_key: &str, expiration_secs: i64) -> io::Result<()> {
        let meta = TileMeta::new_negative(expiration_secs);
        let hashed_key = self.hash_cache_key(cache_key);
        let meta_path = self.meta_path(&hashed_key);

        self.key_mapping.write().insert(cache_key.to_string(), hashed_key);

        let meta_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&meta)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData,
                                        format!("rkyv serialization error: {}", e)))?;

        fs::write(&meta_path, &meta_bytes)?;

        {
            let mut cache = self.memory_cache.write();
            if cache.len() >= self.max_memory_entries {
                self.evict_lru_from_memory(&mut cache);
            }
            cache.insert(cache_key.to_string(), MemoryCacheEntry {
                data: Arc::new(Vec::new()),
                meta,
                last_access: Arc::new(AtomicI64::new(current_timestamp())),
            });
        }

        Ok(())
    }

    /// Delete a specific tile from both memory and disk.
    pub fn delete_tile(&self, cache_key: &str) -> io::Result<()> {
        self.memory_cache.write().remove(cache_key);

        let hashed_key = self.hash_cache_key(cache_key);
        let _ = fs::remove_file(self.tile_path(&hashed_key));
        let _ = fs::remove_file(self.meta_path(&hashed_key));

        self.key_mapping.write().remove(cache_key);

        Ok(())
    }

    /// Clear all cached tiles from memory and disk.
    pub fn clear_all(&self) -> io::Result<()> {
        self.memory_cache.write().clear();
        self.key_mapping.write().clear();
        if self.cache_path.exists() {
            for entry in fs::read_dir(&self.cache_path)? {
                let entry = entry?;
                if entry.path().is_file() {
                    fs::remove_file(entry.path())?;
                }
            }
        }
        Ok(())
    }

    /// Clear expired tiles from both memory and disk.
    ///
    /// Returns the number of *distinct logical tiles* removed. A tile is counted
    /// once regardless of whether it was in both memory and disk, only memory, or
    /// only disk.
    ///
    /// ## Counting strategy
    /// The disk pass is authoritative: every expired `.meta` file corresponds to
    /// one logical tile. The memory pass evicts entries silently; those that have
    /// no disk backing (e.g. negative-cache entries that were never flushed) are
    /// caught by checking whether a corresponding `.meta` file exists — if it
    /// doesn't, we count that memory-only entry. This prevents double-counting
    /// tiles that exist in both layers.
    pub fn clear_expired(&self) -> io::Result<u64> {
        let mut count = 0u64;

        // --- Memory pass ---
        {
            let mut cache = self.memory_cache.write();
            let now = current_timestamp();
            let cache_path = &self.cache_path;
            let mut to_remove = Vec::new();

            for (key, entry) in cache.iter() {
                if entry.meta.expiration < now {
                    let hashed = {
                        let mut hasher = FnvHasher::default();
                        key.hash(&mut hasher);
                        format!("{:016x}", hasher.finish())
                    };
                    let meta_path = cache_path.join(format!("{}.meta", hashed));
                    if !meta_path.exists() {
                        count += 1;
                    }
                    to_remove.push(key.clone());
                }
            }
            for key in to_remove {
                cache.remove(&key);
            }
        }

        // --- Disk pass ---
        // Every expired .meta file represents one logical tile.
        if self.cache_path.exists() {
            for entry in fs::read_dir(&self.cache_path)? {
                let entry = entry?;
                let path = entry.path();

                if path.extension().and_then(|s| s.to_str()) == Some("meta") {
                    if let Some(meta) = self.load_metadata_internal(&path) {
                        if meta.is_expired() {
                            let stem = path.file_stem().unwrap();
                            let tile_path = self.cache_path
                                .join(format!("{}.tile", stem.to_string_lossy()));
                            let _ = fs::remove_file(&path);
                            let _ = fs::remove_file(tile_path);
                            count += 1;
                        }
                    }
                }
            }
        }

        Ok(count)
    }

    /// Get total number of cached tiles on disk (counts .tile files).
    pub fn tile_count(&self) -> io::Result<usize> {
        let mut count = 0;
        if self.cache_path.exists() {
            for entry in fs::read_dir(&self.cache_path)? {
                let entry = entry?;
                if entry.path().extension().and_then(|s| s.to_str()) == Some("tile") {
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    /// Get total cache size on disk in bytes (includes both .tile and .meta files).
    pub fn cache_size(&self) -> io::Result<u64> {
        let mut total = 0u64;
        if self.cache_path.exists() {
            for entry in fs::read_dir(&self.cache_path)? {
                let entry = entry?;
                let metadata = entry.metadata()?;
                if metadata.is_file() {
                    total += metadata.len();
                }
            }
        }
        Ok(total)
    }

    /// Get a snapshot of cache statistics.
    pub fn statistics(&self) -> CacheStatsSnapshot {
        self.statistics.snapshot()
    }

    /// Reset all statistics counters to zero.
    pub fn reset_statistics(&self) {
        self.statistics.reset();
    }

    /// Get memory cache entry count and total data size in bytes.
    pub fn memory_cache_info(&self) -> (usize, usize) {
        let cache = self.memory_cache.read();
        let count = cache.len();
        let size: usize = cache.values().map(|e| e.data.len()).sum();
        (count, size)
    }

    /// Set the maximum number of entries held in the memory cache.
    pub fn set_max_memory_entries(&mut self, max_entries: usize) {
        self.max_memory_entries = max_entries;
    }

    /// Debug helper: look up the original key that hashed to `hashed_key`.
    pub fn get_original_key(&self, hashed_key: &str) -> Option<String> {
        self.key_mapping.read()
            .iter()
            .find(|(_, v)| v.as_str() == hashed_key)
            .map(|(k, _)| k.clone())
    }

    // MARK: - Private Helpers

    /// Evict the least-recently-used entry from the memory cache.
    fn evict_lru_from_memory(&self, cache: &mut HashMap<String, MemoryCacheEntry>) {
        if cache.is_empty() {
            return;
        }
        let mut oldest_time = i64::MAX;
        let mut oldest_key: Option<String> = None;
        for (key, entry) in cache.iter() {
            let access = entry.last_access.load(Ordering::Relaxed);
            if access < oldest_time {
                oldest_time = access;
                oldest_key = Some(key.clone());
            }
        }
        if let Some(key) = oldest_key {
            cache.remove(&key);
        }
    }

    /// Hash a cache key to a stable 16-hex-char filename.
    ///
    /// Uses FNV-1a (via the `fnv` crate), which is guaranteed stable across Rust
    /// versions and process restarts — unlike `DefaultHasher`. This ensures
    /// disk-cached tiles are always discoverable after recompile.
    fn hash_cache_key(&self, key: &str) -> String {
        let mut hasher = FnvHasher::default();
        key.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }

    #[inline]
    fn tile_path(&self, hashed_key: &str) -> PathBuf {
        self.cache_path.join(format!("{}.tile", hashed_key))
    }

    #[inline]
    fn meta_path(&self, hashed_key: &str) -> PathBuf {
        self.cache_path.join(format!("{}.meta", hashed_key))
    }

    fn load_metadata_internal(&self, meta_path: &Path) -> Option<TileMeta> {
        let data = fs::read(meta_path).ok()?;
        let archived = rkyv::access::<ArchivedTileMeta, rkyv::rancor::Error>(&data).ok()?;
        Some(TileMeta {
            expiration: archived.expiration.into(),
            download_time: archived.download_time.into(),
            data_size: archived.data_size.into(),
            http_status: archived.http_status.into(),
            is_negative: archived.is_negative.into(),
        })
    }
}

// MARK: - Utility Functions

/// Get current Unix timestamp in seconds.
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

        let data = b"test tile data";
        let meta = TileMeta::new_success(60, data.len() as u32);
        cache.save_tile("test_tile", data, &meta).unwrap();

        // First load should come from memory.
        let loaded = cache.load_tile("test_tile").unwrap();
        assert_eq!(loaded.data, data);

        let stats = cache.statistics();
        assert_eq!(stats.memory_hits, 1);
        assert_eq!(stats.disk_hits, 0);

        // Evict from memory cache.
        cache.memory_cache.write().clear();

        // Second load should come from disk.
        let loaded = cache.load_tile("test_tile").unwrap();
        assert_eq!(loaded.data, data);

        let stats = cache.statistics();
        assert_eq!(stats.memory_hits, 1);
        assert_eq!(stats.disk_hits, 1);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_concurrent_access() {
        let temp_dir = std::env::temp_dir().join("test_concurrent");
        let _ = fs::remove_dir_all(&temp_dir);

        let cache = Arc::new(TileCacheCore::new(temp_dir.clone()).unwrap());
        let mut handles = vec![];

        for i in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                let data = format!("data_{}", i).into_bytes();
                let meta = TileMeta::new_success(60, data.len() as u32);
                cache_clone.save_tile(&format!("tile_{}", i), &data, &meta).unwrap();

                let loaded = cache_clone.load_tile(&format!("tile_{}", i)).unwrap();
                assert_eq!(loaded.data, data);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(cache.tile_count().unwrap(), 10);
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_hash_cache_key() {
        let temp_dir = std::env::temp_dir().join("test_hash");
        let _ = fs::remove_dir_all(&temp_dir);

        let cache = TileCacheCore::new(temp_dir.clone()).unwrap();

        let hash1 = cache.hash_cache_key("test/key/1");
        let hash2 = cache.hash_cache_key("test/key/2");
        assert_ne!(hash1, hash2);

        let hash3 = cache.hash_cache_key("test/key/1");
        assert_eq!(hash1, hash3);

        assert_eq!(hash1.len(), 16);
        assert!(hash1.chars().all(|c| c.is_ascii_hexdigit()));

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_is_valid_semantics() {
        let temp_dir = std::env::temp_dir().join("test_valid_semantics");
        let _ = fs::remove_dir_all(&temp_dir);

        let cache = TileCacheCore::new(temp_dir.clone()).unwrap();

        let data = b"valid data";
        let meta = TileMeta::new_success(60, data.len() as u32);
        cache.save_tile("valid_tile", data, &meta).unwrap();
        cache.save_negative_cache("negative_tile", 60).unwrap();

        assert!(cache.is_valid("valid_tile"));
        assert!(!cache.is_valid("negative_tile"));
        assert!(cache.is_negative_cache("negative_tile"));

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_clear_expired_no_double_count() {
        let temp_dir = std::env::temp_dir().join("test_expired_count");
        let _ = fs::remove_dir_all(&temp_dir);

        let cache = TileCacheCore::new(temp_dir.clone()).unwrap();

        // Save two tiles that are already expired.
        let expired_meta = TileMeta {
            expiration: current_timestamp() - 10,
            download_time: current_timestamp() - 70,
            data_size: 4,
            http_status: 200,
            is_negative: false,
        };
        cache.save_tile("expired_a", b"aaaa", &expired_meta).unwrap();
        cache.save_tile("expired_b", b"bbbb", &expired_meta).unwrap();

        // Both tiles are in memory AND on disk. clear_expired must count each once.
        let removed = cache.clear_expired().unwrap();
        assert_eq!(removed, 2, "Each expired tile should be counted exactly once");

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_rkyv_serialization() {
        let temp_dir = std::env::temp_dir().join("test_rkyv");
        let _ = fs::remove_dir_all(&temp_dir);

        let cache = TileCacheCore::new(temp_dir.clone()).unwrap();
        let meta = TileMeta::new_success(3600, 4096);
        let data = b"rkyv test data";
        cache.save_tile("rkyv_test", data, &meta).unwrap();

        let loaded = cache.load_tile("rkyv_test").unwrap();
        assert_eq!(loaded.meta.data_size, meta.data_size);
        assert_eq!(loaded.meta.http_status, meta.http_status);
        assert_eq!(loaded.meta.is_negative, meta.is_negative);
        assert_eq!(loaded.data, data);

        let _ = fs::remove_dir_all(&temp_dir);
    }
}
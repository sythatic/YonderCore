# YonderCore
YonderCore is a high-performance Rust-based library designed for processing transit data, specifically optimized for integration into iOS and macOS applications. It provides a robust backend for handling GTFS (General Transit Feed Specification) data, including static lookups, real-time updates, and smooth vehicle position interpolation.

# Features
- GTFS Static Lookup: Efficiently parses static GTFS files (routes, trips, stops, etc.) using HTTP range requests to minimize data transfer.
- Shape Interpolation: Computes smooth, interpolated vehicle positions between GPS pings based on shapes.txt and stop_times.txt.
- Real-time Enrichment: Integrates GTFS-RT (Real-time) feeds to provide live vehicle positions, trip updates, and occupancy status.
- Spatial Indexing: Uses an R-tree for fast O(log n) spatial queries to find nearby stops.
- Zero-Copy Tile Caching: Implements a high-performance tile cache using rkyv for zero-copy deserialization.
- Thread-Safe Architecture: Designed with thread-safe data structures (Arc, RwLock, Mutex) to handle concurrent access in mobile environments.

# Project Structure
- `lib.rs` The main entry point and FFI (Foreign Function Interface) layer.
- `gtfs_static.rs` Handles parsing and lookup of static GTFS data and shape interpolation logic.
- `gtfs_rt.rs` Manages GTFS-RT Protobuf message definitions and feed parsing.
- `tile_cache.rs` A disk and memory-based caching system for map tiles and metadata.

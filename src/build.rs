// build.rs - Generates Rust code from .proto files at build time
fn main() {
    // We'll manually include the GTFS-RT proto definitions
    // You can download gtfs-realtime.proto from:
    // https://github.com/google/transit/blob/master/gtfs-realtime/proto/gtfs-realtime.proto

    // For now, we'll use manual struct definitions in gtfs_rt.rs
    // If you want auto-generation, place gtfs-realtime.proto in project root and uncomment:

    // prost_build::compile_protos(&["gtfs-realtime.proto"], &["."]).unwrap();

    println!("cargo:rerun-if-changed=build.rs");
}
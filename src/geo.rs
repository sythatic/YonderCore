//! geo.rs — shared geodesic geometry helpers
//!
//! Centralises the Haversine distance formula used by both `stops_db`
//! (radius queries) and `gtfs_static` (shape-timeline construction).
//! All functions are `pub(crate)` — they are not part of the FFI surface.

/// Haversine distance in metres between two WGS-84 lat/lon points.
///
/// Uses the standard two-argument arcsin form.  Accurate to within ~0.3 %
/// for distances up to ~20 000 km, which is more than sufficient for
/// transit stop proximity and shape interpolation.
#[inline(always)]
pub(crate) fn haversine_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6_371_000.0;
    const D: f64 = std::f64::consts::PI / 180.0;
    let dlat = (lat2 - lat1) * D;
    let dlon = (lon2 - lon1) * D;
    let a = (dlat * 0.5).sin().powi(2)
        + (lat1 * D).cos() * (lat2 * D).cos() * (dlon * 0.5).sin().powi(2);
    R * 2.0 * a.sqrt().asin()
}

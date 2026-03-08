//! shapes_editor.rs — Standalone shapes.txt editor backend for PolyPlot
//!
//! This module is deliberately isolated from `gtfs_static.rs`.  It owns its
//! own singleton and data model so the GTFS interpolation pipeline is never
//! disturbed by editor operations, and PolyPlot edits don't accidentally
//! pollute the live shapes store.
//!
//! # FFI surface (declared in YonderCore.h)
//!
//! ```c
//! FFIShapePoint* shapes_editor_load(const char* path, size_t* out_count);
//! int32_t        shapes_editor_save(const char* path);
//! FFIShapePoint* shapes_editor_get_all(size_t* out_count);
//! size_t         shapes_editor_point_count(void);
//! int32_t        shapes_editor_update_point(const char* shape_id,
//!                                            uint32_t sequence,
//!                                            double new_lat, double new_lon);
//! int32_t        shapes_editor_delete_point(const char* shape_id, uint32_t sequence);
//! int32_t        shapes_editor_insert_point(const char* shape_id,
//!                                            uint32_t after_sequence,
//!                                            double lat, double lon);
//! int32_t        shapes_editor_delete_shape(const char* shape_id);
//! int32_t        shapes_editor_add_shape(const char* shape_id);
//! void           shapes_editor_reset(void);
//! void           shapes_editor_free_points(FFIShapePoint* ptr, size_t count);
//! ```
//!
//! # Memory contract
//!
//! Every `FFIShapePoint*` returned by this module is a heap-allocated array
//! of structs where `shape_id` is a `CString::into_raw()` pointer.  The caller
//! **must** pass the pointer and count back to `shapes_editor_free_points`; no
//! other free function is correct.

use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::sync::{Mutex, OnceLock};

// ── Singleton ─────────────────────────────────────────────────────────────────

static EDITOR: OnceLock<Mutex<ShapesEditorStore>> = OnceLock::new();

fn editor() -> &'static Mutex<ShapesEditorStore> {
    EDITOR.get_or_init(|| Mutex::new(ShapesEditorStore::new()))
}

// ── Data model ────────────────────────────────────────────────────────────────

/// Internal per-point representation.
/// Uses BTreeMap<shape_id, BTreeMap<sequence, (lat, lon)>> so iteration always
/// yields shape_id-sorted, sequence-sorted output — matching what shapes.txt
/// requires — without an explicit sort on every save.
struct ShapesEditorStore {
    /// shape_id → (sequence → (lat, lon))
    shapes: BTreeMap<String, BTreeMap<u32, (f64, f64)>>,
}

impl ShapesEditorStore {
    fn new() -> Self {
        Self { shapes: BTreeMap::new() }
    }

    // ── Parsing ──────────────────────────────────────────────────────────────

    /// Parse a shapes.txt CSV (with or without a header row) and replace
    /// the current in-memory store.  Returns the total number of points loaded.
    fn load(&mut self, csv: &str) -> Result<usize, String> {
        let mut new_shapes: BTreeMap<String, BTreeMap<u32, (f64, f64)>> = BTreeMap::new();
        let mut total = 0usize;

        let mut lines = csv.lines().peekable();

        // Detect and skip header row — GTFS requires this exact column order
        // but real-world files sometimes omit it, so we check explicitly.
        if let Some(&first) = lines.peek() {
            let low = first.to_ascii_lowercase();
            if low.contains("shape_id") || low.contains("shape_pt_lat") {
                lines.next();
            }
        }

        for (line_no, line) in lines.enumerate() {
            let line = line.trim();
            if line.is_empty() { continue; }

            // Fast split — GTFS shapes.txt has exactly 4 columns:
            //   shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence
            // (optional shape_dist_traveled at index 4 is silently ignored)
            let mut cols = line.splitn(5, ',');
            let shape_id = match cols.next() {
                Some(s) => s.trim(),
                None    => return Err(format!("line {}: missing shape_id", line_no + 2)),
            };
            let lat_str = match cols.next() {
                Some(s) => s.trim(),
                None    => return Err(format!("line {}: missing shape_pt_lat", line_no + 2)),
            };
            let lon_str = match cols.next() {
                Some(s) => s.trim(),
                None    => return Err(format!("line {}: missing shape_pt_lon", line_no + 2)),
            };
            let seq_str = match cols.next() {
                Some(s) => s.trim(),
                None    => return Err(format!("line {}: missing shape_pt_sequence", line_no + 2)),
            };

            let lat: f64 = lat_str.parse()
                .map_err(|_| format!("line {}: invalid lat '{}'", line_no + 2, lat_str))?;
            let lon: f64 = lon_str.parse()
                .map_err(|_| format!("line {}: invalid lon '{}'", line_no + 2, lon_str))?;
            let seq: u32 = seq_str.parse()
                .map_err(|_| format!("line {}: invalid seq '{}'", line_no + 2, seq_str))?;

            new_shapes
                .entry(shape_id.to_string())
                .or_default()
                .insert(seq, (lat, lon));
            total += 1;
        }

        self.shapes = new_shapes;
        Ok(total)
    }

    // ── Serialisation ────────────────────────────────────────────────────────

    /// Serialize to GTFS shapes.txt CSV bytes (UTF-8, LF line endings).
    fn to_csv_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.point_count() * 48 + 64);
        out.extend_from_slice(b"shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n");

        for (shape_id, pts) in &self.shapes {
            // BTreeMap iteration order is ascending by key (sequence number)
            for (&seq, &(lat, lon)) in pts {
                // Use enough decimal places for sub-metre precision (6 dp ≈ 11 cm)
                let line = format!("{},{:.6},{:.6},{}\n", shape_id, lat, lon, seq);
                out.extend_from_slice(line.as_bytes());
            }
        }
        out
    }

    // ── Point access ─────────────────────────────────────────────────────────

    fn point_count(&self) -> usize {
        self.shapes.values().map(|pts| pts.len()).sum()
    }

    /// Collect all points into a flat Vec ordered by (shape_id, sequence).
    fn all_points(&self) -> Vec<EditorPoint> {
        let mut out = Vec::with_capacity(self.point_count());
        for (shape_id, pts) in &self.shapes {
            for (&seq, &(lat, lon)) in pts {
                out.push(EditorPoint {
                    shape_id: shape_id.clone(),
                    sequence: seq,
                    lat,
                    lon,
                });
            }
        }
        out
    }

    // ── Mutations ────────────────────────────────────────────────────────────

    /// Move an existing point to a new lat/lon.
    fn update_point(&mut self, shape_id: &str, sequence: u32,
                    new_lat: f64, new_lon: f64) -> bool {
        if let Some(pts) = self.shapes.get_mut(shape_id) {
            if let Some(entry) = pts.get_mut(&sequence) {
                *entry = (new_lat, new_lon);
                return true;
            }
        }
        false
    }

    /// Remove a single point.  Removes the parent shape entry if it becomes empty.
    fn delete_point(&mut self, shape_id: &str, sequence: u32) -> bool {
        if let Some(pts) = self.shapes.get_mut(shape_id) {
            if pts.remove(&sequence).is_some() {
                if pts.is_empty() { self.shapes.remove(shape_id); }
                return true;
            }
        }
        false
    }

    /// Insert a new point with `sequence = after_sequence + 1`.
    /// All existing points in the same shape with sequence ≥ the new sequence
    /// are renumbered +1 so the polyline order stays contiguous.
    fn insert_point(&mut self, shape_id: &str, after_sequence: u32,
                    lat: f64, lon: f64) -> bool {
        let new_seq = after_sequence + 1;

        let pts = self.shapes.entry(shape_id.to_string()).or_default();

        // Collect keys that need to be bumped (in descending order so we don't
        // clobber a key before we've moved it).
        let to_bump: Vec<u32> = pts.keys()
            .filter(|&&s| s >= new_seq)
            .copied()
            .collect::<Vec<_>>();

        // Move highest-sequence first to avoid key collisions during rename.
        for seq in to_bump.iter().rev() {
            if let Some(val) = pts.remove(seq) {
                pts.insert(seq + 1, val);
            }
        }

        pts.insert(new_seq, (lat, lon));
        true
    }

    /// Remove an entire shape and all its points.
    fn delete_shape(&mut self, shape_id: &str) -> bool {
        self.shapes.remove(shape_id).is_some()
    }

    /// Register a new empty shape (so it appears in the editor list immediately).
    /// Returns false if the shape_id already exists.
    fn add_shape(&mut self, shape_id: &str) -> bool {
        if self.shapes.contains_key(shape_id) { return false; }
        self.shapes.insert(shape_id.to_string(), BTreeMap::new());
        true
    }
}

// ── Internal point type used by Rust only ────────────────────────────────────

struct EditorPoint {
    shape_id: String,
    sequence: u32,
    lat:      f64,
    lon:      f64,
}

// ── FFI structs ───────────────────────────────────────────────────────────────

/// One shape point as returned over the C FFI boundary.
/// `shape_id` is a null-terminated C string allocated by Rust.
/// **Free the whole array with `shapes_editor_free_points(ptr, count)`.**
#[repr(C)]
pub struct FFIShapePoint {
    pub shape_id: *const c_char, // heap-allocated CString; freed by shapes_editor_free_points
    pub sequence: u32,
    pub lat:      f64,
    pub lon:      f64,
}

// ── Helper: Vec<EditorPoint> → heap FFIShapePoint array ──────────────────────

fn points_to_ffi(points: Vec<EditorPoint>) -> (*mut FFIShapePoint, usize) {
    let count = points.len();
    if count == 0 { return (std::ptr::null_mut(), 0); }

    let ffi_points: Vec<FFIShapePoint> = points
        .into_iter()
        .map(|p| FFIShapePoint {
            shape_id: CString::new(p.shape_id)
                .map(|s| s.into_raw() as *const c_char)
                .unwrap_or(std::ptr::null()),
            sequence: p.sequence,
            lat:      p.lat,
            lon:      p.lon,
        })
        .collect();

    let boxed = ffi_points.into_boxed_slice();
    let ptr   = Box::into_raw(boxed) as *mut FFIShapePoint;
    (ptr, count)
}

// ── FFI functions ─────────────────────────────────────────────────────────────

/// Load and parse `path` (a null-terminated UTF-8 path to a shapes.txt file).
///
/// Replaces any previously loaded data.  Returns a heap-allocated array of
/// `FFIShapePoint` with `*out_count` elements, or NULL on error.
/// The caller must free the result with `shapes_editor_free_points`.
#[no_mangle]
pub extern "C" fn shapes_editor_load(
    path:      *const c_char,
    out_count: *mut usize,
) -> *mut FFIShapePoint {
    if path.is_null() || out_count.is_null() {
        return std::ptr::null_mut();
    }

    let path_str = match unsafe { CStr::from_ptr(path).to_str() } {
        Ok(s)  => s,
        Err(_) => { unsafe { *out_count = 0; } return std::ptr::null_mut(); }
    };

    let csv = match std::fs::read_to_string(path_str) {
        Ok(s)  => s,
        Err(e) => {
            eprintln!("shapes_editor_load: cannot read '{}': {}", path_str, e);
            unsafe { *out_count = 0; }
            return std::ptr::null_mut();
        }
    };

    let mut ed = editor().lock().unwrap();
    match ed.load(&csv) {
        Ok(_) => {
            let (ptr, count) = points_to_ffi(ed.all_points());
            unsafe { *out_count = count; }
            ptr
        }
        Err(e) => {
            eprintln!("shapes_editor_load: parse error: {}", e);
            unsafe { *out_count = 0; }
            std::ptr::null_mut()
        }
    }
}

/// Serialize the current in-memory shapes to `path`.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn shapes_editor_save(path: *const c_char) -> i32 {
    if path.is_null() { return -1; }

    let path_str = match unsafe { CStr::from_ptr(path).to_str() } {
        Ok(s)  => s,
        Err(_) => return -1,
    };

    let ed  = editor().lock().unwrap();
    let csv = ed.to_csv_bytes();
    drop(ed); // release lock before I/O

    match std::fs::write(path_str, &csv) {
        Ok(_)  => 0,
        Err(e) => { eprintln!("shapes_editor_save: {}", e); -1 }
    }
}

/// Return all current in-memory points as a flat array ordered by
/// (shape_id, sequence).  Sets `*out_count`; returns NULL if empty.
/// Free with `shapes_editor_free_points`.
#[no_mangle]
pub extern "C" fn shapes_editor_get_all(out_count: *mut usize) -> *mut FFIShapePoint {
    if out_count.is_null() { return std::ptr::null_mut(); }

    let ed  = editor().lock().unwrap();
    let pts = ed.all_points();
    drop(ed);

    let (ptr, count) = points_to_ffi(pts);
    unsafe { *out_count = count; }
    ptr
}

/// Return the total number of points currently in the editor store.
#[no_mangle]
pub extern "C" fn shapes_editor_point_count() -> usize {
    editor().lock().unwrap().point_count()
}

/// Move an existing point to new coordinates.
///
/// `shape_id` and `sequence` identify the point (same semantics as shapes.txt).
/// Returns 1 if the point was found and updated, 0 if not found.
#[no_mangle]
pub extern "C" fn shapes_editor_update_point(
    shape_id: *const c_char,
    sequence: u32,
    new_lat:  f64,
    new_lon:  f64,
) -> i32 {
    if shape_id.is_null() { return 0; }
    let sid = match unsafe { CStr::from_ptr(shape_id).to_str() } {
        Ok(s)  => s,
        Err(_) => return 0,
    };
    let mut ed = editor().lock().unwrap();
    if ed.update_point(sid, sequence, new_lat, new_lon) { 1 } else { 0 }
}

/// Delete a single point identified by (shape_id, sequence).
/// Returns 1 if deleted, 0 if not found.
/// If the shape becomes empty it is also removed.
#[no_mangle]
pub extern "C" fn shapes_editor_delete_point(
    shape_id: *const c_char,
    sequence: u32,
) -> i32 {
    if shape_id.is_null() { return 0; }
    let sid = match unsafe { CStr::from_ptr(shape_id).to_str() } {
        Ok(s)  => s,
        Err(_) => return 0,
    };
    let mut ed = editor().lock().unwrap();
    if ed.delete_point(sid, sequence) { 1 } else { 0 }
}

/// Insert a new point into `shape_id` immediately after `after_sequence`.
///
/// All points in the same shape with sequence ≥ (after_sequence + 1) are
/// renumbered +1 to keep sequences contiguous.
///
/// Returns 1 on success, 0 on invalid arguments.
#[no_mangle]
pub extern "C" fn shapes_editor_insert_point(
    shape_id:       *const c_char,
    after_sequence: u32,
    lat:            f64,
    lon:            f64,
) -> i32 {
    if shape_id.is_null() { return 0; }
    let sid = match unsafe { CStr::from_ptr(shape_id).to_str() } {
        Ok(s)  => s,
        Err(_) => return 0,
    };
    let mut ed = editor().lock().unwrap();
    if ed.insert_point(sid, after_sequence, lat, lon) { 1 } else { 0 }
}

/// Delete an entire shape (all points with the given shape_id).
/// Returns 1 if the shape existed and was deleted, 0 if not found.
#[no_mangle]
pub extern "C" fn shapes_editor_delete_shape(shape_id: *const c_char) -> i32 {
    if shape_id.is_null() { return 0; }
    let sid = match unsafe { CStr::from_ptr(shape_id).to_str() } {
        Ok(s)  => s,
        Err(_) => return 0,
    };
    let mut ed = editor().lock().unwrap();
    if ed.delete_shape(sid) { 1 } else { 0 }
}

/// Register a new empty shape.
/// Returns 1 if the shape was created, 0 if a shape with that ID already exists.
#[no_mangle]
pub extern "C" fn shapes_editor_add_shape(shape_id: *const c_char) -> i32 {
    if shape_id.is_null() { return 0; }
    let sid = match unsafe { CStr::from_ptr(shape_id).to_str() } {
        Ok(s)  => s,
        Err(_) => return 0,
    };
    let mut ed = editor().lock().unwrap();
    if ed.add_shape(sid) { 1 } else { 0 }
}

/// Clear all loaded data from the editor store.
#[no_mangle]
pub extern "C" fn shapes_editor_reset() {
    editor().lock().unwrap().shapes.clear();
}

/// Free an `FFIShapePoint` array returned by any `shapes_editor_*` function.
///
/// **This is the only correct way to free these arrays.**
/// Passing the pointer to any other free function will cause undefined behavior.
#[no_mangle]
pub extern "C" fn shapes_editor_free_points(ptr: *mut FFIShapePoint, count: usize) {
    if ptr.is_null() || count == 0 { return; }
    unsafe {
        let slice = std::slice::from_raw_parts(ptr, count);
        for pt in slice {
            if !pt.shape_id.is_null() {
                let _ = CString::from_raw(pt.shape_id as *mut c_char);
            }
        }
        let _ = Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, count));
    }
}

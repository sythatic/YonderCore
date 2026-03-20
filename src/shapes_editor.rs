//! shapes_editor.rs — Standalone shapes.txt editor backend for PolyPlot
//!
//! This module is deliberately isolated from `gtfs_static.rs`.  It owns its
//! own store registry and data model so the GTFS interpolation pipeline is
//! never disturbed by editor operations.
//!
//! # Multi-store design
//!
//! Each open file gets its own `ShapesEditorStore` identified by a `u32`
//! store_id.  Store IDs are handed out by `shapes_editor_open()` and released
//! by `shapes_editor_close()`.  Every mutation/query function takes a
//! `store_id` as its first argument.  There is no global singleton; stores
//! are completely independent and are never merged.
//!
//! # FFI surface (declared in YonderCore.h)
//!
//! ```c
//! uint32_t       shapes_editor_open(void);
//! void           shapes_editor_close(uint32_t store_id);
//! int32_t        shapes_editor_load(uint32_t store_id, const char* path, size_t* out_count);
//! int32_t        shapes_editor_save(uint32_t store_id, const char* path);
//! FFIShapePoint* shapes_editor_get_all(uint32_t store_id, size_t* out_count);
//! char*          shapes_editor_get_shape_ids(uint32_t store_id);
//! void           shapes_editor_free_string(char* ptr);
//! FFIShapePoint* shapes_editor_get_shape(uint32_t store_id, const char* shape_id, size_t* out_count);
//! size_t         shapes_editor_point_count(uint32_t store_id);
//! int32_t        shapes_editor_update_point(uint32_t store_id, const char* shape_id, uint32_t sequence, double new_lat, double new_lon);
//! int32_t        shapes_editor_delete_point(uint32_t store_id, const char* shape_id, uint32_t sequence);
//! int32_t        shapes_editor_insert_point(uint32_t store_id, const char* shape_id, uint32_t after_sequence, double lat, double lon);
//! int32_t        shapes_editor_delete_shape(uint32_t store_id, const char* shape_id);
//! int32_t        shapes_editor_add_shape(uint32_t store_id, const char* shape_id);
//! void           shapes_editor_reset(uint32_t store_id);
//! void           shapes_editor_free_points(FFIShapePoint* ptr, size_t count);
//! ```
//!
//! # Memory contract
//!
//! Every `FFIShapePoint*` returned by this module is a heap-allocated array
//! of structs where `shape_id` is a `CString::into_raw()` pointer.  The caller
//! **must** pass the pointer and count back to `shapes_editor_free_points`; no
//! other free function is correct.

use std::collections::{BTreeMap, HashMap};
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::sync::Mutex;

// ── Store registry ─────────────────────────────────────────────────────────────

struct Registry {
    stores:  HashMap<u32, ShapesEditorStore>,
    next_id: u32,
}

impl Registry {
    fn new() -> Self {
        Self { stores: HashMap::new(), next_id: 1 }
    }

    fn open(&mut self) -> u32 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1); // skip 0
        self.stores.insert(id, ShapesEditorStore::new());
        id
    }

    fn close(&mut self, id: u32) {
        self.stores.remove(&id);
    }

    fn get(&self, id: u32) -> Option<&ShapesEditorStore> {
        self.stores.get(&id)
    }

    fn get_mut(&mut self, id: u32) -> Option<&mut ShapesEditorStore> {
        self.stores.get_mut(&id)
    }
}

static REGISTRY: std::sync::OnceLock<Mutex<Registry>> = std::sync::OnceLock::new();

fn registry() -> &'static Mutex<Registry> {
    REGISTRY.get_or_init(|| Mutex::new(Registry::new()))
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

    fn load<R: std::io::BufRead>(&mut self, reader: R) -> Result<usize, String> {
        let mut new_shapes: BTreeMap<String, BTreeMap<u32, (f64, f64)>> = BTreeMap::new();
        let mut total      = 0usize;
        let mut line_no    = 0usize;
        let mut buf        = String::new();
        let mut saw_header = false;
        let mut reader     = reader;

        let mut col_id  = 0usize;
        let mut col_lat = 1usize;
        let mut col_lon = 2usize;
        let mut col_seq = 3usize;

        loop {
            buf.clear();
            let n = reader.read_line(&mut buf)
                .map_err(|e| format!("line {}: I/O error: {}", line_no + 1, e))?;
            if n == 0 { break; }

            line_no += 1;
            let line = buf.trim();
            if line.is_empty() { continue; }

            if !saw_header {
                saw_header = true;
                let low = line.to_ascii_lowercase();
                if low.contains("shape_id") || low.contains("shape_pt_lat") {
                    for (i, h) in line.split(',').map(|h| h.trim()).enumerate() {
                        match h.to_ascii_lowercase().as_str() {
                            "shape_id"          => col_id  = i,
                            "shape_pt_lat"      => col_lat = i,
                            "shape_pt_lon"      => col_lon = i,
                            "shape_pt_sequence" => col_seq = i,
                            _ => {}
                        }
                    }
                    continue;
                }
            }

            let cols: Vec<&str> = line.split(',').collect();
            let max_idx = col_id.max(col_lat).max(col_lon).max(col_seq);
            if cols.len() <= max_idx {
                return Err(format!("line {}: only {} columns, need index {}",
                                   line_no, cols.len(), max_idx));
            }

            let shape_id = cols[col_id].trim();
            let lat_str  = cols[col_lat].trim();
            let lon_str  = cols[col_lon].trim();
            let seq_str  = cols[col_seq].trim();

            let lat: f64 = lat_str.parse()
                .map_err(|_| format!("line {}: invalid lat '{}'", line_no, lat_str))?;
            let lon: f64 = lon_str.parse()
                .map_err(|_| format!("line {}: invalid lon '{}'", line_no, lon_str))?;
            let seq: u32 = seq_str.parse()
                .map_err(|_| format!("line {}: invalid seq '{}'", line_no, seq_str))?;

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

    fn to_csv_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.point_count() * 48 + 64);
        out.extend_from_slice(b"shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n");

        for (shape_id, pts) in &self.shapes {
            for (&seq, &(lat, lon)) in pts {
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

    fn delete_point(&mut self, shape_id: &str, sequence: u32) -> bool {
        if let Some(pts) = self.shapes.get_mut(shape_id) {
            if pts.remove(&sequence).is_some() {
                if pts.is_empty() { self.shapes.remove(shape_id); }
                return true;
            }
        }
        false
    }

    fn insert_point(&mut self, shape_id: &str, after_sequence: u32,
                    lat: f64, lon: f64) -> bool {
        let new_seq = match after_sequence.checked_add(1) {
            Some(s) => s,
            None    => return false,
        };

        let pts = self.shapes.entry(shape_id.to_string()).or_default();

        let to_bump: Vec<u32> = pts.keys()
            .filter(|&&s| s >= new_seq)
            .copied()
            .collect::<Vec<_>>();

        if let Some(&max_seq) = to_bump.last() {
            if max_seq == u32::MAX {
                return false;
            }
        }

        for seq in to_bump.iter().rev() {
            if let Some(val) = pts.remove(seq) {
                pts.insert(seq + 1, val);
            }
        }

        pts.insert(new_seq, (lat, lon));
        true
    }

    fn delete_shape(&mut self, shape_id: &str) -> bool {
        self.shapes.remove(shape_id).is_some()
    }

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
    pub shape_id: *const c_char,
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

// ── FFI panic guard ───────────────────────────────────────────────────────────

macro_rules! ffi_catch {
    ($default:expr, $body:block) => {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| $body)) {
            Ok(v)  => v,
            Err(_) => {
                eprintln!("shapes_editor: caught panic at FFI boundary");
                $default
            }
        }
    };
}

// ── FFI functions ─────────────────────────────────────────────────────────────

/// Allocate a new independent editor store and return its ID.
/// The ID is non-zero. Free the store with `shapes_editor_close()` when done.
#[no_mangle]
pub extern "C" fn shapes_editor_open() -> u32 {
    ffi_catch!(0, {
        registry().lock().unwrap().open()
    })
}

/// Release the store identified by `store_id` and free all its memory.
/// Passing an unknown ID is a no-op.
#[no_mangle]
pub extern "C" fn shapes_editor_close(store_id: u32) {
    ffi_catch!((), {
        registry().lock().unwrap().close(store_id);
    });
}

/// Load and parse `path` into the store identified by `store_id`.
/// Replaces any previously loaded data in that store.
/// Returns 0 on success, -1 on error.  `*out_count` is set to the point count.
#[no_mangle]
pub extern "C" fn shapes_editor_load(
    store_id:  u32,
    path:      *const c_char,
    out_count: *mut usize,
) -> i32 {
    if path.is_null() || out_count.is_null() { return -1; }

    ffi_catch!(-1, {
        let path_str = match unsafe { CStr::from_ptr(path).to_str() } {
            Ok(s)  => s,
            Err(_) => { unsafe { *out_count = 0; } return -1; }
        };

        let file = match std::fs::File::open(path_str) {
            Ok(f)  => f,
            Err(e) => {
                eprintln!("shapes_editor_load: cannot open '{}': {}", path_str, e);
                unsafe { *out_count = 0; }
                return -1;
            }
        };
        let reader = std::io::BufReader::new(file);

        let mut reg = registry().lock().unwrap();
        match reg.get_mut(store_id) {
            None => { unsafe { *out_count = 0; } -1 }
            Some(ed) => match ed.load(reader) {
                Ok(total) => { unsafe { *out_count = total; } 0 }
                Err(e)    => {
                    eprintln!("shapes_editor_load: parse error: {}", e);
                    unsafe { *out_count = 0; }
                    -1
                }
            }
        }
    })
}

/// Serialize the store identified by `store_id` to `path`.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn shapes_editor_save(store_id: u32, path: *const c_char) -> i32 {
    if path.is_null() { return -1; }

    ffi_catch!(-1, {
        let path_str = match unsafe { CStr::from_ptr(path).to_str() } {
            Ok(s)  => s,
            Err(_) => return -1,
        };

        let reg = registry().lock().unwrap();
        match reg.get(store_id) {
            None     => -1,
            Some(ed) => {
                let csv = ed.to_csv_bytes();
                drop(reg);
                match std::fs::write(path_str, &csv) {
                    Ok(_)  => 0,
                    Err(e) => { eprintln!("shapes_editor_save: {}", e); -1 }
                }
            }
        }
    })
}

/// Return a newline-delimited "shape_id,count" C string for `store_id`.
/// Free with `shapes_editor_free_string`. Returns NULL if empty or unknown.
#[no_mangle]
pub extern "C" fn shapes_editor_get_shape_ids(store_id: u32) -> *mut c_char {
    ffi_catch!(std::ptr::null_mut(), {
        let reg = registry().lock().unwrap();
        match reg.get(store_id) {
            None     => std::ptr::null_mut(),
            Some(ed) => {
                if ed.shapes.is_empty() { return std::ptr::null_mut(); }
                let mut out = String::new();
                for (shape_id, pts) in &ed.shapes {
                    out.push_str(shape_id);
                    out.push(',');
                    out.push_str(&pts.len().to_string());
                    out.push('\n');
                }
                drop(reg);
                match CString::new(out) {
                    Ok(s)  => s.into_raw(),
                    Err(_) => std::ptr::null_mut(),
                }
            }
        }
    })
}

/// Free a C string returned by `shapes_editor_get_shape_ids`.
#[no_mangle]
pub extern "C" fn shapes_editor_free_string(ptr: *mut c_char) {
    if ptr.is_null() { return; }
    unsafe { let _ = CString::from_raw(ptr); }
}

/// Return all points for a single shape in `store_id`.
/// Sets `*out_count`; returns NULL if not found. Free with `shapes_editor_free_points`.
#[no_mangle]
pub extern "C" fn shapes_editor_get_shape(
    store_id:  u32,
    shape_id:  *const c_char,
    out_count: *mut usize,
) -> *mut FFIShapePoint {
    if shape_id.is_null() || out_count.is_null() { return std::ptr::null_mut(); }

    ffi_catch!(std::ptr::null_mut(), {
        let sid = match unsafe { CStr::from_ptr(shape_id).to_str() } {
            Ok(s)  => s,
            Err(_) => { unsafe { *out_count = 0; } return std::ptr::null_mut(); }
        };

        let reg = registry().lock().unwrap();
        match reg.get(store_id) {
            None     => { unsafe { *out_count = 0; } std::ptr::null_mut() }
            Some(ed) => {
                let pts = match ed.shapes.get(sid) {
                    Some(m) => m.iter()
                        .map(|(&seq, &(lat, lon))| EditorPoint {
                            shape_id: sid.to_string(),
                            sequence: seq,
                            lat,
                            lon,
                        })
                        .collect::<Vec<_>>(),
                    None => { unsafe { *out_count = 0; } return std::ptr::null_mut(); }
                };
                drop(reg);
                let (ptr, count) = points_to_ffi(pts);
                unsafe { *out_count = count; }
                ptr
            }
        }
    })
}

/// Return all points in `store_id` as a flat array ordered by (shape_id, sequence).
/// Sets `*out_count`; returns NULL if empty. Free with `shapes_editor_free_points`.
#[no_mangle]
pub extern "C" fn shapes_editor_get_all(
    store_id:  u32,
    out_count: *mut usize,
) -> *mut FFIShapePoint {
    if out_count.is_null() { return std::ptr::null_mut(); }

    ffi_catch!(std::ptr::null_mut(), {
        let reg = registry().lock().unwrap();
        match reg.get(store_id) {
            None     => { unsafe { *out_count = 0; } std::ptr::null_mut() }
            Some(ed) => {
                let pts = ed.all_points();
                drop(reg);
                let (ptr, count) = points_to_ffi(pts);
                unsafe { *out_count = count; }
                ptr
            }
        }
    })
}

/// Return the total number of points in `store_id`.
#[no_mangle]
pub extern "C" fn shapes_editor_point_count(store_id: u32) -> usize {
    ffi_catch!(0, {
        registry().lock().unwrap()
            .get(store_id)
            .map(|ed| ed.point_count())
            .unwrap_or(0)
    })
}

/// Move an existing point to new coordinates. Returns 1 if updated, 0 if not found.
#[no_mangle]
pub extern "C" fn shapes_editor_update_point(
    store_id: u32,
    shape_id: *const c_char,
    sequence: u32,
    new_lat:  f64,
    new_lon:  f64,
) -> i32 {
    if shape_id.is_null() { return 0; }
    ffi_catch!(0, {
        let sid = match unsafe { CStr::from_ptr(shape_id).to_str() } {
            Ok(s)  => s,
            Err(_) => return 0,
        };
        let mut reg = registry().lock().unwrap();
        match reg.get_mut(store_id) {
            None     => 0,
            Some(ed) => if ed.update_point(sid, sequence, new_lat, new_lon) { 1 } else { 0 },
        }
    })
}

/// Delete a single point. Returns 1 if deleted, 0 if not found.
#[no_mangle]
pub extern "C" fn shapes_editor_delete_point(
    store_id: u32,
    shape_id: *const c_char,
    sequence: u32,
) -> i32 {
    if shape_id.is_null() { return 0; }
    ffi_catch!(0, {
        let sid = match unsafe { CStr::from_ptr(shape_id).to_str() } {
            Ok(s)  => s,
            Err(_) => return 0,
        };
        let mut reg = registry().lock().unwrap();
        match reg.get_mut(store_id) {
            None     => 0,
            Some(ed) => if ed.delete_point(sid, sequence) { 1 } else { 0 },
        }
    })
}

/// Insert a new point after `after_sequence`. Returns 1 on success, 0 on error.
#[no_mangle]
pub extern "C" fn shapes_editor_insert_point(
    store_id:       u32,
    shape_id:       *const c_char,
    after_sequence: u32,
    lat:            f64,
    lon:            f64,
) -> i32 {
    if shape_id.is_null() { return 0; }
    ffi_catch!(0, {
        let sid = match unsafe { CStr::from_ptr(shape_id).to_str() } {
            Ok(s)  => s,
            Err(_) => return 0,
        };
        let mut reg = registry().lock().unwrap();
        match reg.get_mut(store_id) {
            None     => 0,
            Some(ed) => if ed.insert_point(sid, after_sequence, lat, lon) { 1 } else { 0 },
        }
    })
}

/// Delete an entire shape. Returns 1 if deleted, 0 if not found.
#[no_mangle]
pub extern "C" fn shapes_editor_delete_shape(
    store_id: u32,
    shape_id: *const c_char,
) -> i32 {
    if shape_id.is_null() { return 0; }
    ffi_catch!(0, {
        let sid = match unsafe { CStr::from_ptr(shape_id).to_str() } {
            Ok(s)  => s,
            Err(_) => return 0,
        };
        let mut reg = registry().lock().unwrap();
        match reg.get_mut(store_id) {
            None     => 0,
            Some(ed) => if ed.delete_shape(sid) { 1 } else { 0 },
        }
    })
}

/// Register a new empty shape. Returns 1 if created, 0 if ID already exists.
#[no_mangle]
pub extern "C" fn shapes_editor_add_shape(
    store_id: u32,
    shape_id: *const c_char,
) -> i32 {
    if shape_id.is_null() { return 0; }
    ffi_catch!(0, {
        let sid = match unsafe { CStr::from_ptr(shape_id).to_str() } {
            Ok(s)  => s,
            Err(_) => return 0,
        };
        let mut reg = registry().lock().unwrap();
        match reg.get_mut(store_id) {
            None     => 0,
            Some(ed) => if ed.add_shape(sid) { 1 } else { 0 },
        }
    })
}

/// Clear all loaded data from `store_id` (does not close or free the store).
#[no_mangle]
pub extern "C" fn shapes_editor_reset(store_id: u32) {
    ffi_catch!((), {
        let mut reg = registry().lock().unwrap();
        if let Some(ed) = reg.get_mut(store_id) {
            ed.shapes.clear();
        }
    });
}

/// Free an `FFIShapePoint` array returned by any `shapes_editor_*` function.
///
/// **This is the only correct way to free these arrays.**
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
//! Trickle Engine — Background flush from RAM to data files
//!
//! The trickle engine runs on a background thread and periodically flushes
//! dirty entries from the RAM hash table to on-disk data files. This serves
//! two purposes:
//!
//! 1. Keeps WAL size bounded (flushed entries can be removed from WAL)
//! 2. Provides a durable on-disk copy independent of WAL replay
//!
//! The trickle engine does NOT delete entries from RAM — it copies them
//! to data files and marks them as "clean" in the dirty bitmap. RAM remains
//! the primary read surface.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use hashbrown::HashMap;
use parking_lot::{Mutex, RwLock};

use crate::config::Config;
use crate::datafile::DataFileWriter;
use crate::error::{ClawError, ClawResult};

/// Tracks which keys are dirty (modified in RAM but not yet flushed to data files).
pub struct DirtyTracker {
    /// Set of keys that have been modified since last flush
    dirty_keys: Mutex<HashSet<Vec<u8>>>,
    /// Total number of entries flushed since engine start
    total_flushed: AtomicU64,
    /// Total number of trickle cycles completed
    total_cycles: AtomicU64,
}

impl DirtyTracker {
    /// Create a new dirty tracker.
    pub fn new() -> Self {
        Self {
            dirty_keys: Mutex::new(HashSet::new()),
            total_flushed: AtomicU64::new(0),
            total_cycles: AtomicU64::new(0),
        }
    }

    /// Mark a key as dirty (called after RAM update in engine.put/delete).
    pub fn mark_dirty(&self, key: &[u8]) {
        let mut dirty = self.dirty_keys.lock();
        dirty.insert(key.to_vec());
    }

    /// Take all dirty keys, leaving the set empty.
    /// Returns the set of keys to flush in this trickle cycle.
    pub fn take_dirty(&self) -> HashSet<Vec<u8>> {
        let mut dirty = self.dirty_keys.lock();
        std::mem::take(&mut *dirty)
    }

    /// Number of keys currently dirty.
    pub fn dirty_count(&self) -> usize {
        let dirty = self.dirty_keys.lock();
        dirty.len()
    }

    /// Total entries flushed since engine start.
    pub fn total_flushed(&self) -> u64 {
        self.total_flushed.load(Ordering::Relaxed)
    }

    /// Total trickle cycles completed.
    pub fn total_cycles(&self) -> u64 {
        self.total_cycles.load(Ordering::Relaxed)
    }

    /// Record that a flush cycle completed.
    fn record_cycle(&self, flushed_count: u64) {
        self.total_flushed.fetch_add(flushed_count, Ordering::Relaxed);
        self.total_cycles.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for DirtyTracker {
    fn default() -> Self { Self::new() }
}

/// Handle to a running trickle engine background thread.
/// Dropping this handle signals the thread to stop.
pub struct TrickleHandle {
    /// Signal the background thread to stop
    shutdown: Arc<AtomicBool>,
    /// Background thread join handle
    thread: Option<thread::JoinHandle<()>>,
}

impl TrickleHandle {
    /// Request graceful shutdown and wait for the background thread to finish.
    pub fn shutdown(mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }

    /// Check if the trickle engine is still running.
    pub fn is_running(&self) -> bool {
        self.thread.as_ref().map_or(false, |h| !h.is_finished())
    }
}

impl Drop for TrickleHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

/// Start the trickle engine background thread.
///
/// The trickle engine periodically:
/// 1. Takes the set of dirty keys from the tracker
/// 2. For each dirty key, reads current value from RAM
/// 3. Writes the key-value pair to a data file with CRC32C + durable_sync
/// 4. Records flush statistics
///
/// # Arguments
/// * `data_dir` - Directory for data files (engine_path/data/)
/// * `data` - Shared reference to the RAM hash table
/// * `tracker` - Shared dirty key tracker
/// * `config` - Engine configuration (trickle_cadence controls flush interval)
pub fn start_trickle(
    data_dir: PathBuf,
    data: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
    tracker: Arc<DirtyTracker>,
    config: Config,
) -> ClawResult<TrickleHandle> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = Arc::clone(&shutdown);
    let cadence = config.trickle_cadence;

    // Create initial data file writer
    let data_dir_clone = data_dir.clone();

    let thread = thread::Builder::new()
        .name("clawstore-trickle".to_string())
        .spawn(move || {
            trickle_loop(data_dir_clone, data, tracker, cadence, shutdown_clone);
        })
        .map_err(|e| ClawError::Io {
            path: Some(data_dir),
            kind: std::io::ErrorKind::Other,
            message: format!("Failed to spawn trickle thread: {}", e),
        })?;

    Ok(TrickleHandle {
        shutdown,
        thread: Some(thread),
    })
}

/// Main trickle loop — runs on the background thread.
fn trickle_loop(
    data_dir: PathBuf,
    data: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
    tracker: Arc<DirtyTracker>,
    cadence: Duration,
    shutdown: Arc<AtomicBool>,
) {
    // Create data file writer — if this fails, log and exit
    let mut writer = match DataFileWriter::new(&data_dir) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("[TRICKLE] Failed to create data file writer: {}", e);
            return;
        }
    };

    loop {
        // Sleep for the configured cadence, checking shutdown periodically
        let wake_time = Instant::now() + cadence;
        while Instant::now() < wake_time {
            if shutdown.load(Ordering::Acquire) {
                // Final flush before shutdown
                flush_dirty(&data, &tracker, &mut writer);
                return;
            }
            thread::sleep(Duration::from_millis(100));
        }

        if shutdown.load(Ordering::Acquire) {
            flush_dirty(&data, &tracker, &mut writer);
            return;
        }

        // Execute one trickle cycle
        flush_dirty(&data, &tracker, &mut writer);
    }
}

/// Execute one flush cycle: take dirty keys, write to data files.
fn flush_dirty(
    data: &RwLock<HashMap<Vec<u8>, Vec<u8>>>,
    tracker: &DirtyTracker,
    writer: &mut DataFileWriter,
) {
    let dirty_keys = tracker.take_dirty();
    if dirty_keys.is_empty() {
        tracker.record_cycle(0);
        return;
    }

    let mut flushed = 0u64;

    // Read lock on HashMap — snapshot the values for dirty keys
    // We hold the read lock briefly to copy values, then release it
    let to_flush: Vec<(Vec<u8>, Option<Vec<u8>>)> = {
        let data = data.read();
        dirty_keys.into_iter()
            .map(|key| {
                let value = data.get(&key).cloned();
                (key, value)
            })
            .collect()
    };

    // Write to data files (no lock held — this is the slow I/O part)
    for (key, value) in to_flush {
        let result = match value {
            Some(val) => writer.write_entry(&key, &val),
            None => writer.write_tombstone(&key), // key was deleted
        };

        match result {
            Ok(_) => { flushed += 1; }
            Err(e) => {
                eprintln!("[TRICKLE] Failed to flush key ({} bytes): {}", key.len(), e);
                // Re-mark as dirty so it gets retried next cycle
                tracker.mark_dirty(&key);
            }
        }
    }

    tracker.record_cycle(flushed);

    if flushed > 0 {
        eprintln!("[TRICKLE] Flushed {} entries to data files", flushed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_dirty_tracker_basics() {
        let tracker = DirtyTracker::new();
        assert_eq!(tracker.dirty_count(), 0);

        tracker.mark_dirty(b"key1");
        tracker.mark_dirty(b"key2");
        assert_eq!(tracker.dirty_count(), 2);

        // Same key twice doesn't increase count
        tracker.mark_dirty(b"key1");
        assert_eq!(tracker.dirty_count(), 2);

        let taken = tracker.take_dirty();
        assert_eq!(taken.len(), 2);
        assert_eq!(tracker.dirty_count(), 0); // cleared after take
    }

    #[test]
    fn test_dirty_tracker_stats() {
        let tracker = DirtyTracker::new();
        assert_eq!(tracker.total_flushed(), 0);
        assert_eq!(tracker.total_cycles(), 0);

        tracker.record_cycle(5);
        assert_eq!(tracker.total_flushed(), 5);
        assert_eq!(tracker.total_cycles(), 1);

        tracker.record_cycle(3);
        assert_eq!(tracker.total_flushed(), 8);
        assert_eq!(tracker.total_cycles(), 2);
    }

    #[test]
    fn test_flush_dirty_writes_to_datafile() {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().join("data");

        let data = Arc::new(RwLock::new(HashMap::new()));
        let tracker = DirtyTracker::new();

        // Simulate engine.put: insert into RAM and mark dirty
        {
            let mut map = data.write();
            map.insert(b"k1".to_vec(), b"v1".to_vec());
            map.insert(b"k2".to_vec(), b"v2".to_vec());
        }
        tracker.mark_dirty(b"k1");
        tracker.mark_dirty(b"k2");

        // Flush
        let mut writer = DataFileWriter::new(&data_dir).unwrap();
        flush_dirty(&data, &tracker, &mut writer);

        assert_eq!(tracker.total_flushed(), 2);
        assert_eq!(tracker.total_cycles(), 1);
        assert_eq!(tracker.dirty_count(), 0);

        // Verify data files have the entries
        let data_file = std::fs::read_dir(&data_dir).unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_str().map_or(false, |n| n.starts_with("data-")))
            .map(|e| e.path())
            .expect("No data file");

        let entries = crate::datafile::DataFileReader::scan_all(&data_file).unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_flush_dirty_handles_deleted_keys() {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().join("data");

        let data = Arc::new(RwLock::new(HashMap::new()));
        let tracker = DirtyTracker::new();

        // Mark a key dirty that doesn't exist in RAM (was deleted)
        tracker.mark_dirty(b"deleted_key");

        let mut writer = DataFileWriter::new(&data_dir).unwrap();
        flush_dirty(&data, &tracker, &mut writer);

        assert_eq!(tracker.total_flushed(), 1); // tombstone written
        assert_eq!(tracker.total_cycles(), 1);
    }

    #[test]
    fn test_trickle_start_shutdown() {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let data = Arc::new(RwLock::new(HashMap::new()));
        let tracker = Arc::new(DirtyTracker::new());

        // Use a fast cadence for testing
        let mut config = Config::default();
        config.trickle_cadence = Duration::from_millis(50);

        // Put some data and mark dirty
        {
            let mut map = data.write();
            map.insert(b"trickle_key".to_vec(), b"trickle_val".to_vec());
        }
        tracker.mark_dirty(b"trickle_key");

        let handle = start_trickle(
            data_dir.clone(),
            Arc::clone(&data),
            Arc::clone(&tracker),
            config,
        ).unwrap();

        assert!(handle.is_running());

        // Wait for at least one trickle cycle
        thread::sleep(Duration::from_millis(300));

        // Should have flushed
        assert!(tracker.total_cycles() >= 1);
        assert!(tracker.total_flushed() >= 1);

        // Shutdown gracefully
        handle.shutdown();
    }

    #[test]
    fn test_trickle_no_dirty_noop() {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().join("data");

        let data = Arc::new(RwLock::new(HashMap::new()));
        let tracker = DirtyTracker::new();

        // Flush with nothing dirty
        let mut writer = DataFileWriter::new(&data_dir).unwrap();
        flush_dirty(&data, &tracker, &mut writer);

        assert_eq!(tracker.total_flushed(), 0);
        assert_eq!(tracker.total_cycles(), 1); // cycle counted even if nothing flushed
    }
}

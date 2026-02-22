//! Core storage engine — the heart of ClawStore.
//!
//! ClawStoreEngine combines a RAM hash table with a crash-safe WAL
//! and a background trickle engine that flushes dirty entries to data files.
//!
//! **Read path**: RAM-first (sub-microsecond via RwLock)
//! **Write path**: WAL-first, then RAM, then mark dirty for trickle
//! **Background**: Trickle thread flushes dirty entries to data files on cadence

use std::path::{Path, PathBuf};
use std::sync::Arc;

use hashbrown::HashMap;
use parking_lot::{RwLock, Mutex};

use crate::config::Config;
use crate::error::{ClawError, ClawResult};
use crate::format::Operation;
use crate::trickle::{DirtyTracker, TrickleHandle, start_trickle};
use crate::wal::{WalWriter, WalReader};

/// Core storage engine: RAM hash table + WAL + trickle flush.
///
/// All public methods take `&self` for concurrent access.
/// Multiple readers call `get()` simultaneously via RwLock.
/// Writers serialize through the WAL Mutex, then briefly hold the HashMap write lock.
/// The trickle engine runs in the background flushing dirty entries to data files.
pub struct ClawStoreEngine {
    /// RAM working set — concurrent reads via RwLock
    data: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
    /// Write-ahead log — single writer via Mutex
    wal: Mutex<WalWriter>,
    /// Dirty key tracker — shared with trickle thread
    dirty: Arc<DirtyTracker>,
    /// Background trickle engine handle (None if not started)
    trickle: Mutex<Option<TrickleHandle>>,
    /// Data directory path
    path: PathBuf,
    /// Engine configuration
    config: Config,
}

impl ClawStoreEngine {
    /// Open or create a ClawStore at the given path.
    ///
    /// Creates WAL and data directories, replays WAL for crash recovery,
    /// and optionally starts the background trickle engine.
    pub fn open<P: AsRef<Path>>(path: P, config: Config) -> ClawResult<Self> {
        let path = path.as_ref().to_path_buf();
        let wal_dir = path.join("wal");
        let data_dir = path.join("data");

        // Create directories
        std::fs::create_dir_all(&wal_dir).map_err(|e| ClawError::Io {
            path: Some(wal_dir.clone()),
            kind: e.kind(),
            message: format!("Failed to create WAL directory: {}", e),
        })?;
        std::fs::create_dir_all(&data_dir).map_err(|e| ClawError::Io {
            path: Some(data_dir.clone()),
            kind: e.kind(),
            message: format!("Failed to create data directory: {}", e),
        })?;

        // Replay WAL into RAM (crash recovery)
        let mut data = HashMap::new();
        let reader = WalReader::new(&wal_dir);
        let entries = reader.recover_entries()?;

        for entry in &entries {
            match entry.operation {
                Operation::Put => {
                    data.insert(entry.key.clone(), entry.value.clone());
                }
                Operation::Delete => {
                    data.remove(&entry.key);
                }
            }
        }

        let recovered_count = data.len();
        if recovered_count > 0 {
            eprintln!(
                "[ClawStore] Recovered {} entries from WAL at {}",
                recovered_count,
                wal_dir.display()
            );
        }

        let wal = WalWriter::new(&wal_dir)?;

        Ok(Self {
            data: Arc::new(RwLock::new(data)),
            wal: Mutex::new(wal),
            dirty: Arc::new(DirtyTracker::new()),
            trickle: Mutex::new(None),
            path,
            config,
        })
    }

    /// Start the background trickle engine.
    ///
    /// The trickle thread periodically flushes dirty entries from RAM to data files.
    /// Not starting trickle is valid — the WAL provides crash safety regardless.
    pub fn start_trickle(&self) -> ClawResult<()> {
        let data_dir = self.path.join("data");
        let handle = start_trickle(
            data_dir,
            Arc::clone(&self.data),
            Arc::clone(&self.dirty),
            self.config.clone(),
        )?;
        let mut trickle = self.trickle.lock();
        *trickle = Some(handle);
        Ok(())
    }

    /// Stop the background trickle engine gracefully.
    pub fn stop_trickle(&self) {
        let mut trickle = self.trickle.lock();
        if let Some(handle) = trickle.take() {
            handle.shutdown();
        }
    }

    /// Get value for key from RAM.
    ///
    /// Acquires a read lock — multiple concurrent readers allowed.
    /// Never touches the WAL or disk. This is the hot path.
    pub fn get(&self, key: &[u8]) -> ClawResult<Option<Vec<u8>>> {
        let data = self.data.read();
        Ok(data.get(key).cloned())
    }

    /// Put key-value pair with full durability.
    ///
    /// WRITE ORDERING (the fundamental contract):
    /// 1. WAL append with durable_sync
    /// 2. RAM insert
    /// 3. Mark dirty for trickle
    ///
    /// If WAL write fails, RAM is NEVER modified.
    pub fn put(&self, key: &[u8], value: &[u8]) -> ClawResult<()> {
        {
            let mut wal = self.wal.lock();
            wal.append_durable(key, value, Operation::Put)?;
        }
        {
            let mut data = self.data.write();
            data.insert(key.to_vec(), value.to_vec());
        }
        self.dirty.mark_dirty(key);
        Ok(())
    }

    /// Put WITHOUT durable sync (fast path). Still marks dirty.
    pub fn put_fast(&self, key: &[u8], value: &[u8]) -> ClawResult<()> {
        {
            let mut wal = self.wal.lock();
            wal.append_fast(key, value, Operation::Put)?;
        }
        {
            let mut data = self.data.write();
            data.insert(key.to_vec(), value.to_vec());
        }
        self.dirty.mark_dirty(key);
        Ok(())
    }

    /// Sync the WAL to persistent storage.
    ///
    /// Call this after a batch of `put_fast` writes to make them all
    /// durable at once. One fsync for the entire batch instead of per-write.
    /// This is the path Reth uses during block sync: buffer all writes,
    /// then commit with a single fsync.
    pub fn sync_wal(&self) -> ClawResult<()> {
        let wal = self.wal.lock();
        wal.sync()
    }

    /// Delete with full durability. Marks dirty so trickle writes tombstone.
    pub fn delete(&self, key: &[u8]) -> ClawResult<()> {
        {
            let mut wal = self.wal.lock();
            wal.append_durable(key, &[], Operation::Delete)?;
        }
        {
            let mut data = self.data.write();
            data.remove(key);
        }
        self.dirty.mark_dirty(key);
        Ok(())
    }

    /// Check if key exists in RAM.
    pub fn contains_key(&self, key: &[u8]) -> bool {
        let data = self.data.read();
        data.contains_key(key)
    }

    /// Number of key-value pairs in RAM.
    pub fn len(&self) -> usize {
        let data = self.data.read();
        data.len()
    }

    /// Returns true if the store has no entries.
    pub fn is_empty(&self) -> bool {
        let data = self.data.read();
        data.is_empty()
    }

    /// Data directory path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Keys waiting to be flushed by trickle.
    pub fn dirty_count(&self) -> usize {
        self.dirty.dirty_count()
    }

    /// Total entries flushed to data files since engine start.
    pub fn total_flushed(&self) -> u64 {
        self.dirty.total_flushed()
    }

    /// Total trickle cycles completed.
    pub fn trickle_cycles(&self) -> u64 {
        self.dirty.total_cycles()
    }

    /// Scan all key-value pairs whose key starts with `prefix`.
    ///
    /// Returns pairs with the prefix stripped from keys, sorted by key.
    /// This is the bridge between ClawStore's flat namespace and Reth's
    /// table-scoped cursor iteration.
    ///
    /// Acquires a read lock — concurrent with other readers.
    pub fn prefix_scan(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let data = self.data.read();
        let mut results: Vec<(Vec<u8>, Vec<u8>)> = data.iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k[prefix.len()..].to_vec(), v.clone()))
            .collect();
        results.sort_by(|(a, _), (b, _)| a.cmp(b));
        results
    }

    /// Count entries whose key starts with `prefix`.
    ///
    /// More efficient than `prefix_scan().len()` — no cloning or sorting.
    pub fn prefix_count(&self, prefix: &[u8]) -> usize {
        let data = self.data.read();
        data.keys().filter(|k| k.starts_with(prefix)).count()
    }
}

impl Drop for ClawStoreEngine {
    fn drop(&mut self) {
        let mut trickle = self.trickle.lock();
        if let Some(handle) = trickle.take() {
            handle.shutdown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::time::Duration;

    fn test_engine() -> (ClawStoreEngine, TempDir) {
        let dir = TempDir::new().unwrap();
        let engine = ClawStoreEngine::open(dir.path(), Config::default()).unwrap();
        (engine, dir)
    }

    #[test]
    fn test_open_empty() {
        let (engine, _dir) = test_engine();
        assert_eq!(engine.len(), 0);
        assert!(engine.is_empty());
        assert_eq!(engine.dirty_count(), 0);
    }

    #[test]
    fn test_put_get() {
        let (engine, _dir) = test_engine();
        engine.put(b"hello", b"world").unwrap();
        assert_eq!(engine.get(b"hello").unwrap(), Some(b"world".to_vec()));
        assert_eq!(engine.len(), 1);
        assert!(engine.contains_key(b"hello"));
    }

    #[test]
    fn test_put_marks_dirty() {
        let (engine, _dir) = test_engine();
        engine.put(b"k1", b"v1").unwrap();
        assert_eq!(engine.dirty_count(), 1);
        engine.put(b"k2", b"v2").unwrap();
        assert_eq!(engine.dirty_count(), 2);
        engine.put(b"k1", b"v1_new").unwrap(); // same key
        assert_eq!(engine.dirty_count(), 2);
    }

    #[test]
    fn test_delete_marks_dirty() {
        let (engine, _dir) = test_engine();
        engine.put(b"k", b"v").unwrap();
        engine.delete(b"k").unwrap();
        assert_eq!(engine.dirty_count(), 1); // same key
    }

    #[test]
    fn test_put_overwrite() {
        let (engine, _dir) = test_engine();
        engine.put(b"k", b"v1").unwrap();
        engine.put(b"k", b"v2").unwrap();
        assert_eq!(engine.get(b"k").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(engine.len(), 1);
    }

    #[test]
    fn test_delete() {
        let (engine, _dir) = test_engine();
        engine.put(b"k", b"v").unwrap();
        engine.delete(b"k").unwrap();
        assert!(!engine.contains_key(b"k"));
        assert_eq!(engine.len(), 0);
    }

    #[test]
    fn test_crash_recovery() {
        let dir = TempDir::new().unwrap();
        {
            let engine = ClawStoreEngine::open(dir.path(), Config::default()).unwrap();
            engine.put(b"survive1", b"yes").unwrap();
            engine.put(b"survive2", b"also_yes").unwrap();
            engine.put(b"doomed", b"temp").unwrap();
            engine.delete(b"doomed").unwrap();
        }
        {
            let engine = ClawStoreEngine::open(dir.path(), Config::default()).unwrap();
            assert_eq!(engine.get(b"survive1").unwrap(), Some(b"yes".to_vec()));
            assert_eq!(engine.get(b"survive2").unwrap(), Some(b"also_yes".to_vec()));
            assert_eq!(engine.get(b"doomed").unwrap(), None);
            assert_eq!(engine.len(), 2);
        }
    }

    #[test]
    fn test_trickle_integration() {
        let dir = TempDir::new().unwrap();
        let mut config = Config::default();
        config.trickle_cadence = Duration::from_millis(50);

        let engine = ClawStoreEngine::open(dir.path(), config).unwrap();
        engine.put(b"t1", b"v1").unwrap();
        engine.put(b"t2", b"v2").unwrap();
        assert_eq!(engine.dirty_count(), 2);

        engine.start_trickle().unwrap();
        std::thread::sleep(Duration::from_millis(300));

        assert_eq!(engine.dirty_count(), 0);
        assert!(engine.total_flushed() >= 2);
        assert!(engine.trickle_cycles() >= 1);

        // Data still in RAM
        assert_eq!(engine.get(b"t1").unwrap(), Some(b"v1".to_vec()));

        // Data files created
        let data_dir = dir.path().join("data");
        let has_data_files = std::fs::read_dir(&data_dir).unwrap()
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_str().map_or(false, |n| n.starts_with("data-")));
        assert!(has_data_files, "Trickle should create data files");

        engine.stop_trickle();
    }

    #[test]
    fn test_concurrent_reads() {
        let (engine, _dir) = test_engine();
        let engine = Arc::new(engine);

        for i in 0..100 {
            engine.put(format!("k{}", i).as_bytes(), format!("v{}", i).as_bytes()).unwrap();
        }

        let mut handles = vec![];
        for _ in 0..10 {
            let e = Arc::clone(&engine);
            handles.push(std::thread::spawn(move || {
                for i in 0..100 {
                    let val = e.get(format!("k{}", i).as_bytes()).unwrap().unwrap();
                    assert_eq!(val, format!("v{}", i).as_bytes());
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
    }

    #[test]
    fn test_put_get_many() {
        let (engine, _dir) = test_engine();
        for i in 0..1000 {
            engine.put(format!("key{:04}", i).as_bytes(), format!("val{:04}", i).as_bytes()).unwrap();
        }
        assert_eq!(engine.len(), 1000);
        for i in 0..1000 {
            let actual = engine.get(format!("key{:04}", i).as_bytes()).unwrap().unwrap();
            assert_eq!(actual, format!("val{:04}", i).as_bytes());
        }
    }

    #[test]
    fn test_put_fast() {
        let (engine, _dir) = test_engine();
        engine.put_fast(b"fast", b"lane").unwrap();
        assert_eq!(engine.get(b"fast").unwrap(), Some(b"lane".to_vec()));
        assert_eq!(engine.dirty_count(), 1);
    }

    #[test]
    fn test_prefix_scan() {
        let (engine, _dir) = test_engine();

        // Table 0x01 entries
        engine.put(&[0x01, b'a'], b"val_a").unwrap();
        engine.put(&[0x01, b'c'], b"val_c").unwrap();
        engine.put(&[0x01, b'b'], b"val_b").unwrap();

        // Table 0x02 entries (should not appear)
        engine.put(&[0x02, b'x'], b"val_x").unwrap();

        let results = engine.prefix_scan(&[0x01]);
        assert_eq!(results.len(), 3);
        // Should be sorted by key
        assert_eq!(results[0], (vec![b'a'], b"val_a".to_vec()));
        assert_eq!(results[1], (vec![b'b'], b"val_b".to_vec()));
        assert_eq!(results[2], (vec![b'c'], b"val_c".to_vec()));
    }

    #[test]
    fn test_prefix_count() {
        let (engine, _dir) = test_engine();

        engine.put(&[0x01, b'a'], b"v").unwrap();
        engine.put(&[0x01, b'b'], b"v").unwrap();
        engine.put(&[0x02, b'a'], b"v").unwrap();

        assert_eq!(engine.prefix_count(&[0x01]), 2);
        assert_eq!(engine.prefix_count(&[0x02]), 1);
        assert_eq!(engine.prefix_count(&[0x03]), 0);
    }
}

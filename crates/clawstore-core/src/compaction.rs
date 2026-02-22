//! Compaction — Dead space reclamation for data files
//!
//! Over time, data files accumulate tombstones and stale values (overwritten keys).
//! Compaction reads a data file, keeps only the latest live entries, writes them
//! to a new file, and atomically replaces the old file.
//!
//! Compaction uses the atomic rename pattern for crash safety:
//! 1. Write new compacted file (data-{seq}.claw.compact)
//! 2. durable_sync the new file
//! 3. Rename new file over old file (atomic on POSIX)
//! 4. durable_sync the parent directory

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::datafile::{DataFileReader, DataEntry};
use crate::error::{ClawError, ClawResult};
use crate::platform_durability::durable_sync;

/// Result of a compaction operation.
#[derive(Debug)]
pub struct CompactionResult {
    /// Path to the compacted file
    pub file_path: PathBuf,
    /// Number of entries in the original file
    pub original_entries: usize,
    /// Number of live entries after compaction
    pub live_entries: usize,
    /// Number of entries removed (tombstones + stale overwrites)
    pub removed_entries: usize,
    /// Original file size in bytes
    pub original_bytes: u64,
    /// Compacted file size in bytes
    pub compacted_bytes: u64,
}

impl CompactionResult {
    /// Ratio of dead space in the original file (0.0 = no waste, 1.0 = all waste).
    pub fn dead_space_ratio(&self) -> f64 {
        if self.original_entries == 0 {
            return 0.0;
        }
        self.removed_entries as f64 / self.original_entries as f64
    }

    /// Space saved in bytes.
    pub fn bytes_saved(&self) -> u64 {
        self.original_bytes.saturating_sub(self.compacted_bytes)
    }
}

/// Compact a single data file by removing tombstones and keeping only
/// the latest value for each key.
///
/// Uses the atomic rename pattern for crash safety:
/// 1. Scan original file, deduplicate by key (last write wins)
/// 2. Write live entries to a temp file (.compact suffix)
/// 3. durable_sync the temp file
/// 4. Rename temp file over original (atomic on POSIX)
/// 5. durable_sync the directory
///
/// If the process crashes at any point:
/// - Before rename: original file is intact, temp file is orphaned (harmless)
/// - After rename: new file is the compacted version (correct)
pub fn compact_file(file_path: &Path) -> ClawResult<CompactionResult> {
    let original_bytes = fs::metadata(file_path)
        .map_err(|e| ClawError::Io {
            path: Some(file_path.to_path_buf()),
            kind: e.kind(),
            message: format!("Failed to stat file for compaction: {}", e),
        })?
        .len();

    // Step 1: Scan all entries from the original file
    let all_entries = DataFileReader::scan_all(file_path)?;
    let original_entries = all_entries.len();

    // Deduplicate: keep only the LAST entry for each key (last-write-wins)
    // Tombstones override previous values
    let mut latest: HashMap<Vec<u8>, DataEntry> = HashMap::new();
    for entry in all_entries {
        latest.insert(entry.key.clone(), entry);
    }

    // Filter out tombstones — they served their purpose
    let live: Vec<&DataEntry> = latest.values()
        .filter(|e| !e.is_tombstone)
        .collect();
    let live_entries = live.len();
    let removed_entries = original_entries - live_entries;

    // Step 2: Write live entries to temp file
    let compact_path = file_path.with_extension("claw.compact");

    {
        let mut compact_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&compact_path)
            .map_err(|e| ClawError::Io {
                path: Some(compact_path.clone()),
                kind: e.kind(),
                message: format!("Failed to create compact file: {}", e),
            })?;

        for entry in &live {
            // Reuse the datafile format: header + key + value
            let key = &entry.key;
            let value = &entry.value;

            // Build header manually (same format as DataChunkHeader)
            let checksum = crc32c::crc32c(&[key.as_slice(), value.as_slice()].concat());
            let mut hdr = [0u8; 24];
            hdr[0..4].copy_from_slice(&crate::format::MAGIC_ARRAY);
            hdr[4..6].copy_from_slice(&(key.len() as u16).to_le_bytes());
            hdr[6..10].copy_from_slice(&(value.len() as u32).to_le_bytes());
            hdr[10..14].copy_from_slice(&checksum.to_le_bytes());
            // flags = 0 (live entry), rest is zero padding

            use std::io::Write;
            compact_file.write_all(&hdr).map_err(|e| ClawError::Io {
                path: Some(compact_path.clone()),
                kind: e.kind(),
                message: format!("Failed to write compacted entry: {}", e),
            })?;
            compact_file.write_all(key).map_err(|e| ClawError::Io {
                path: Some(compact_path.clone()),
                kind: e.kind(),
                message: format!("Failed to write compacted key: {}", e),
            })?;
            compact_file.write_all(value).map_err(|e| ClawError::Io {
                path: Some(compact_path.clone()),
                kind: e.kind(),
                message: format!("Failed to write compacted value: {}", e),
            })?;
        }

        // Step 3: durable_sync the compacted file
        durable_sync(&compact_file).map_err(|e| ClawError::Io {
            path: Some(compact_path.clone()),
            kind: e.kind(),
            message: format!("Failed to sync compacted file: {}", e),
        })?;
    }

    let compacted_bytes = fs::metadata(&compact_path)
        .map_err(|e| ClawError::Io {
            path: Some(compact_path.clone()),
            kind: e.kind(),
            message: format!("Failed to stat compacted file: {}", e),
        })?
        .len();

    // Step 4: Atomic rename — replace original with compacted
    fs::rename(&compact_path, file_path).map_err(|e| ClawError::Io {
        path: Some(file_path.to_path_buf()),
        kind: e.kind(),
        message: format!("Failed to rename compacted file: {}", e),
    })?;

    // Step 5: durable_sync the directory to ensure rename is persisted
    if let Some(parent) = file_path.parent() {
        let dir = fs::File::open(parent).map_err(|e| ClawError::Io {
            path: Some(parent.to_path_buf()),
            kind: e.kind(),
            message: format!("Failed to open directory for sync: {}", e),
        })?;
        durable_sync(&dir).map_err(|e| ClawError::Io {
            path: Some(parent.to_path_buf()),
            kind: e.kind(),
            message: format!("Failed to sync directory after compaction: {}", e),
        })?;
    }

    Ok(CompactionResult {
        file_path: file_path.to_path_buf(),
        original_entries,
        live_entries,
        removed_entries,
        original_bytes,
        compacted_bytes,
    })
}

/// Check if a data file needs compaction based on dead space ratio.
pub fn needs_compaction(file_path: &Path, threshold: f64) -> ClawResult<bool> {
    let entries = DataFileReader::scan_all(file_path)?;
    if entries.is_empty() {
        return Ok(false);
    }

    // Count unique live keys (last-write-wins dedup)
    let mut latest: HashMap<&[u8], bool> = HashMap::new();
    for entry in &entries {
        latest.insert(&entry.key, entry.is_tombstone);
    }

    let live_count = latest.values().filter(|&&is_tomb| !is_tomb).count();
    let dead_ratio = 1.0 - (live_count as f64 / entries.len() as f64);

    Ok(dead_ratio >= threshold)
}

/// Compact all data files in a directory that exceed the dead space threshold.
pub fn compact_directory(data_dir: &Path, threshold: f64) -> ClawResult<Vec<CompactionResult>> {
    let mut results = Vec::new();

    let dir_entries = fs::read_dir(data_dir).map_err(|e| ClawError::Io {
        path: Some(data_dir.to_path_buf()),
        kind: e.kind(),
        message: format!("Failed to read data directory: {}", e),
    })?;

    let mut data_files: Vec<PathBuf> = Vec::new();
    for entry in dir_entries.flatten() {
        if let Some(name) = entry.file_name().to_str() {
            if name.starts_with("data-") && name.ends_with(".claw") && !name.contains(".compact") {
                data_files.push(entry.path());
            }
        }
    }

    for file_path in data_files {
        if needs_compaction(&file_path, threshold)? {
            let result = compact_file(&file_path)?;
            eprintln!(
                "[COMPACTION] {} : {} -> {} entries ({} bytes saved)",
                file_path.display(),
                result.original_entries,
                result.live_entries,
                result.bytes_saved()
            );
            results.push(result);
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafile::DataFileWriter;
    use tempfile::TempDir;

    fn find_data_file(dir: &Path) -> PathBuf {
        fs::read_dir(dir).unwrap()
            .filter_map(|e| e.ok())
            .find(|e| {
                let name = e.file_name();
                let n = name.to_str().unwrap_or("");
                n.starts_with("data-") && n.ends_with(".claw") && !n.contains(".compact")
            })
            .map(|e| e.path())
            .expect("No data file found")
    }

    #[test]
    fn test_compact_removes_tombstones() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("data");

        let mut writer = DataFileWriter::new(&dir).unwrap();
        writer.write_entry(b"keep", b"alive").unwrap();
        writer.write_entry(b"dead", b"temporary").unwrap();
        writer.write_tombstone(b"dead").unwrap();
        drop(writer);

        let file = find_data_file(&dir);
        let result = compact_file(&file).unwrap();

        assert_eq!(result.original_entries, 3);
        assert_eq!(result.live_entries, 1);
        assert_eq!(result.removed_entries, 2);
        assert!(result.compacted_bytes < result.original_bytes);

        // Verify the compacted file only has the live entry
        let entries = DataFileReader::scan_all(&file).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, b"keep");
        assert_eq!(entries[0].value, b"alive");
    }

    #[test]
    fn test_compact_deduplicates_overwrites() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("data");

        let mut writer = DataFileWriter::new(&dir).unwrap();
        writer.write_entry(b"k", b"v1").unwrap();
        writer.write_entry(b"k", b"v2").unwrap();
        writer.write_entry(b"k", b"v3_final").unwrap();
        drop(writer);

        let file = find_data_file(&dir);
        let result = compact_file(&file).unwrap();

        assert_eq!(result.original_entries, 3);
        assert_eq!(result.live_entries, 1);

        let entries = DataFileReader::scan_all(&file).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, b"k");
        assert_eq!(entries[0].value, b"v3_final");
    }

    #[test]
    fn test_compact_empty_file_after_all_deleted() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("data");

        let mut writer = DataFileWriter::new(&dir).unwrap();
        writer.write_entry(b"a", b"1").unwrap();
        writer.write_tombstone(b"a").unwrap();
        drop(writer);

        let file = find_data_file(&dir);
        let result = compact_file(&file).unwrap();

        assert_eq!(result.live_entries, 0);
        assert_eq!(result.removed_entries, 2);
    }

    #[test]
    fn test_needs_compaction_threshold() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("data");

        let mut writer = DataFileWriter::new(&dir).unwrap();
        // 5 entries, 2 will be overwritten = 2/5 = 40% dead
        writer.write_entry(b"a", b"1").unwrap();
        writer.write_entry(b"b", b"2").unwrap();
        writer.write_entry(b"c", b"3").unwrap();
        writer.write_entry(b"a", b"new_a").unwrap(); // overwrites old a
        writer.write_entry(b"b", b"new_b").unwrap(); // overwrites old b
        drop(writer);

        let file = find_data_file(&dir);

        // 40% dead, threshold 30% -> needs compaction
        assert!(needs_compaction(&file, 0.3).unwrap());

        // 40% dead, threshold 50% -> doesn't need compaction
        assert!(!needs_compaction(&file, 0.5).unwrap());
    }

    #[test]
    fn test_compact_preserves_crash_safety() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("data");

        let mut writer = DataFileWriter::new(&dir).unwrap();
        writer.write_entry(b"important", b"data").unwrap();
        writer.write_entry(b"stale", b"old").unwrap();
        writer.write_entry(b"stale", b"new").unwrap();
        drop(writer);

        let file = find_data_file(&dir);
        let result = compact_file(&file).unwrap();

        // After compaction, data should still be readable and valid
        let entries = DataFileReader::scan_all(&result.file_path).unwrap();
        assert_eq!(entries.len(), 2);

        // Verify CRC32C checksums are valid (scan_all validates them)
        for entry in &entries {
            assert!(!entry.is_tombstone);
        }
    }

    #[test]
    fn test_dead_space_ratio() {
        let result = CompactionResult {
            file_path: PathBuf::from("/tmp/test"),
            original_entries: 100,
            live_entries: 60,
            removed_entries: 40,
            original_bytes: 10000,
            compacted_bytes: 6000,
        };

        assert!((result.dead_space_ratio() - 0.4).abs() < f64::EPSILON);
        assert_eq!(result.bytes_saved(), 4000);
    }
}

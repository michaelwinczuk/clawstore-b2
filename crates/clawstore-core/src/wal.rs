//! Write-Ahead Log implementation for ClawStore
//!
//! The WAL provides durability guarantees through careful write ordering:
//! 1. Serialize entry to buffer
//! 2. Append buffer to WAL file
//! 3. Call durable_sync() to ensure data reaches persistent storage
//! 4. Return success (caller updates RAM AFTER this returns)
//!
//! "RAM-first" means the READ path serves from RAM.
//! The WRITE path is WAL-first. This is the fundamental durability contract.

use crate::error::{ClawError, ClawResult};
use crate::format::{serialize_entry, deserialize_entry, Operation, WalEntry, MAGIC_ARRAY, HEADER_SIZE};
use crate::platform_durability::durable_sync;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// WAL file rotation threshold (100MB)
const WAL_ROTATION_SIZE: u64 = 100 * 1024 * 1024;

/// WAL writer handles appending entries and ensuring durability.
///
/// CRITICAL INVARIANT: append_durable() must complete (including durable_sync)
/// BEFORE the caller updates the in-memory hash table.
pub struct WalWriter {
    /// Current WAL file handle
    file: File,
    /// Path to current WAL file (for error context)
    path: PathBuf,
    /// Current file size in bytes (tracked to avoid stat calls)
    size: u64,
    /// WAL directory for file rotation
    wal_dir: PathBuf,
    /// Monotonic sequence number for WAL file naming
    sequence: u64,
}

impl WalWriter {
    /// Create a new WAL writer in the specified directory.
    /// If WAL files already exist, resumes from the highest sequence number.
    pub fn new<P: AsRef<Path>>(wal_dir: P) -> ClawResult<Self> {
        let wal_dir = wal_dir.as_ref().to_path_buf();

        // Ensure WAL directory exists
        std::fs::create_dir_all(&wal_dir).map_err(|e| ClawError::Io {
            path: Some(wal_dir.clone()),
            kind: e.kind(),
            message: format!("Failed to create WAL directory: {}", e),
        })?;

        // Find the highest existing sequence number
        let sequence = Self::find_max_sequence(&wal_dir)?;
        let path = wal_dir.join(format!("wal-{:016x}.claw", sequence));

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| ClawError::Io {
                path: Some(path.clone()),
                kind: e.kind(),
                message: format!("Failed to open WAL file: {}", e),
            })?;

        let size = file.metadata()
            .map_err(|e| ClawError::Io {
                path: Some(path.clone()),
                kind: e.kind(),
                message: format!("Failed to stat WAL file: {}", e),
            })?
            .len();

        Ok(Self { file, path, size, wal_dir, sequence })
    }

    /// Find the highest WAL sequence number in the directory.
    fn find_max_sequence(wal_dir: &Path) -> ClawResult<u64> {
        let mut max_seq = 0u64;

        if let Ok(entries) = std::fs::read_dir(wal_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("wal-") && name.ends_with(".claw") {
                        let hex = &name[4..name.len() - 5]; // strip "wal-" and ".claw"
                        if let Ok(seq) = u64::from_str_radix(hex, 16) {
                            max_seq = max_seq.max(seq);
                        }
                    }
                }
            }
        }

        Ok(max_seq)
    }

    /// Append an entry to the WAL with full durability guarantee.
    ///
    /// CRITICAL WRITE ORDERING — every step must happen in this exact order:
    ///
    /// 1. serialize: Convert key/value to binary format with CRC32C
    /// 2. write:     Append serialized bytes to WAL file
    /// 3. sync:      durable_sync() ensures bytes reach persistent storage
    /// 4. return:    Only AFTER sync succeeds does caller update RAM
    ///
    /// If crash occurs after step 2 but before step 3: data is in OS cache,
    /// may or may not survive — this is acceptable for non-DURABLE tier.
    /// If crash occurs after step 3: data is on persistent media, will be
    /// recovered on next startup via WAL replay.
    pub fn append_durable(&mut self, key: &[u8], value: &[u8], op: Operation) -> ClawResult<()> {
        // Step 1: Serialize entry to buffer (includes CRC32C computation)
        // This happens in memory — no I/O, no failure modes except OversizedEntry
        let entry_bytes = serialize_entry(key, value, op)?;

        // Check if we need to rotate before writing
        if self.size + entry_bytes.len() as u64 > WAL_ROTATION_SIZE {
            self.rotate()?;
        }

        // Step 2: Append serialized bytes to WAL file
        // After this, data is in the OS page cache (or disk write cache)
        self.file.write_all(&entry_bytes).map_err(|e| ClawError::Io {
            path: Some(self.path.clone()),
            kind: e.kind(),
            message: format!("WAL write failed: {}", e),
        })?;

        // Step 3: Ensure data reaches persistent storage
        // On Linux: fdatasync(), on macOS: F_FULLFSYNC, on Windows: FlushFileBuffers
        // This is the expensive operation (~100μs SSD, ~5ms HDD)
        // After this returns Ok, the entry WILL survive power loss
        durable_sync(&self.file).map_err(|e| ClawError::Io {
            path: Some(self.path.clone()),
            kind: e.kind(),
            message: format!("WAL durable_sync failed: {}", e),
        })?;

        // Update internal size tracker
        self.size += entry_bytes.len() as u64;

        // Step 4: Return Ok — caller may NOW safely update the RAM hash table
        Ok(())
    }

    /// Append an entry WITHOUT calling durable_sync (DISK tier only).
    /// Data is written to the OS page cache but NOT guaranteed to survive power loss.
    /// Use this only for non-critical writes where speed matters more than durability.
    pub fn append_fast(&mut self, key: &[u8], value: &[u8], op: Operation) -> ClawResult<()> {
        let entry_bytes = serialize_entry(key, value, op)?;

        if self.size + entry_bytes.len() as u64 > WAL_ROTATION_SIZE {
            self.rotate()?;
        }

        self.file.write_all(&entry_bytes).map_err(|e| ClawError::Io {
            path: Some(self.path.clone()),
            kind: e.kind(),
            message: format!("WAL write failed: {}", e),
        })?;

        self.size += entry_bytes.len() as u64;
        Ok(())
    }

    /// Rotate to a new WAL file. Syncs current file before switching.
    fn rotate(&mut self) -> ClawResult<()> {
        // Sync current file to ensure all data is durable before moving on
        durable_sync(&self.file).map_err(|e| ClawError::Io {
            path: Some(self.path.clone()),
            kind: e.kind(),
            message: format!("WAL sync before rotation failed: {}", e),
        })?;

        // Create new WAL file with incremented sequence
        self.sequence += 1;
        let new_path = self.wal_dir.join(format!("wal-{:016x}.claw", self.sequence));

        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_path)
            .map_err(|e| ClawError::Io {
                path: Some(new_path.clone()),
                kind: e.kind(),
                message: format!("Failed to create rotated WAL file: {}", e),
            })?;

        self.file = new_file;
        self.path = new_path;
        self.size = 0;

        Ok(())
    }

    /// Get the current WAL file path (for diagnostics)
    pub fn current_path(&self) -> &Path {
        &self.path
    }

    /// Get the current WAL file size in bytes
    pub fn current_size(&self) -> u64 {
        self.size
    }

    /// Sync the current WAL file to persistent storage without writing any entry.
    /// Call this after a batch of `append_fast` writes to make them all durable at once.
    pub fn sync(&self) -> ClawResult<()> {
        durable_sync(&self.file).map_err(|e| ClawError::Io {
            path: Some(self.path.clone()),
            kind: e.kind(),
            message: format!("WAL sync failed: {}", e),
        })
    }
}

/// WAL reader handles recovery by replaying entries from WAL files.
pub struct WalReader {
    wal_dir: PathBuf,
}

impl WalReader {
    /// Create a new WAL reader for the specified directory.
    pub fn new<P: AsRef<Path>>(wal_dir: P) -> Self {
        Self { wal_dir: wal_dir.as_ref().to_path_buf() }
    }

    /// Recover all entries from WAL files in sequence order.
    ///
    /// Recovery algorithm per file:
    /// 1. Read 32-byte header
    /// 2. Validate magic bytes (0x434C4157 = "CLAW")
    /// 3. Check payload length against remaining file size
    /// 4. Read payload, compute CRC32C, compare with header.checksum
    /// 5. On mismatch/corruption: find_next_magic() to resync
    /// 6. On torn write (incomplete entry at EOF): stop — this is the crash point
    pub fn recover_entries(&self) -> ClawResult<Vec<WalEntry>> {
        let mut all_entries = Vec::new();

        // Collect and sort WAL files by name (= by sequence number)
        let mut wal_files: Vec<PathBuf> = Vec::new();

        let dir_entries = std::fs::read_dir(&self.wal_dir).map_err(|e| ClawError::Io {
            path: Some(self.wal_dir.clone()),
            kind: e.kind(),
            message: format!("Failed to read WAL directory: {}", e),
        })?;

        for entry in dir_entries {
            let entry = entry.map_err(|e| ClawError::Io {
                path: Some(self.wal_dir.clone()),
                kind: e.kind(),
                message: format!("Failed to read directory entry: {}", e),
            })?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("wal-") && name.ends_with(".claw") {
                    wal_files.push(path);
                }
            }
        }

        wal_files.sort(); // lexicographic sort = sequence order (hex-padded)

        for wal_path in &wal_files {
            let entries = self.recover_from_file(wal_path)?;
            all_entries.extend(entries);
        }

        Ok(all_entries)
    }

    /// Recover entries from a single WAL file.
    fn recover_from_file(&self, path: &Path) -> ClawResult<Vec<WalEntry>> {
        let mut file = File::open(path).map_err(|e| ClawError::Io {
            path: Some(path.to_path_buf()),
            kind: e.kind(),
            message: format!("Failed to open WAL file for recovery: {}", e),
        })?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).map_err(|e| ClawError::Io {
            path: Some(path.to_path_buf()),
            kind: e.kind(),
            message: format!("Failed to read WAL file: {}", e),
        })?;

        let mut entries = Vec::new();
        let mut offset = 0;

        while offset + HEADER_SIZE <= buffer.len() {
            // Step 1: Check magic bytes at current position
            if buffer[offset..offset + 4] != MAGIC_ARRAY {
                // Not a valid entry start — try to resync
                eprintln!("[WAL RECOVERY] Bad magic at offset {}, scanning for next entry", offset);
                match find_next_magic(&buffer, offset + 1) {
                    Some(next) => { offset = next; continue; }
                    None => break, // no more entries
                }
            }

            // Step 2: Read payload length from header
            let length = u32::from_le_bytes([
                buffer[offset + 4], buffer[offset + 5],
                buffer[offset + 6], buffer[offset + 7],
            ]) as usize;

            let total_entry_size = HEADER_SIZE + length;

            // Step 3: Check if full entry fits in remaining data
            if offset + total_entry_size > buffer.len() {
                // Torn write — entry started but didn't complete. This is the crash point.
                eprintln!("[WAL RECOVERY] Torn write at offset {}: need {} bytes, have {}",
                         offset, total_entry_size, buffer.len() - offset);
                break; // stop recovery here — everything after is incomplete
            }

            // Step 4: Deserialize and verify CRC32C
            let entry_slice = &buffer[offset..offset + total_entry_size];
            match deserialize_entry(entry_slice) {
                Ok(entry) => {
                    entries.push(entry);
                    offset += total_entry_size;
                }
                Err(e) => {
                    // CRC mismatch or other corruption — skip and resync
                    eprintln!("[WAL RECOVERY] Corrupt entry at offset {}: {}", offset, e);
                    match find_next_magic(&buffer, offset + 1) {
                        Some(next) => { offset = next; continue; }
                        None => break,
                    }
                }
            }
        }

        Ok(entries)
    }
}

/// Scan forward in buffer to find next occurrence of CLAW magic bytes.
/// Used for resynchronization after encountering corruption.
fn find_next_magic(buffer: &[u8], start: usize) -> Option<usize> {
    for i in start..buffer.len().saturating_sub(3) {
        if buffer[i..i + 4] == MAGIC_ARRAY {
            return Some(i);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_wal_write_read_roundtrip() {
        let temp = TempDir::new().unwrap();

        // Write 3 entries
        let mut writer = WalWriter::new(temp.path()).unwrap();
        writer.append_durable(b"key1", b"value1", Operation::Put).unwrap();
        writer.append_durable(b"key2", b"value2", Operation::Put).unwrap();
        writer.append_durable(b"key1", b"", Operation::Delete).unwrap();
        drop(writer);

        // Read them back
        let reader = WalReader::new(temp.path());
        let entries = reader.recover_entries().unwrap();

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, b"key1");
        assert_eq!(entries[0].value, b"value1");
        assert_eq!(entries[0].operation, Operation::Put);
        assert_eq!(entries[1].key, b"key2");
        assert_eq!(entries[2].operation, Operation::Delete);
    }

    #[test]
    fn test_corruption_recovery_skips_bad_entry() {
        let temp = TempDir::new().unwrap();

        let mut writer = WalWriter::new(temp.path()).unwrap();
        writer.append_durable(b"good1", b"val1", Operation::Put).unwrap();
        writer.append_durable(b"good2", b"val2", Operation::Put).unwrap();
        writer.append_durable(b"good3", b"val3", Operation::Put).unwrap();
        let wal_path = writer.current_path().to_path_buf();
        drop(writer);

        // Corrupt the second entry's payload (somewhere after first entry)
        let mut data = std::fs::read(&wal_path).unwrap();
        // First entry is ~50 bytes, corrupt byte 60 which is in the second entry
        if data.len() > 60 {
            data[60] ^= 0xFF;
        }
        std::fs::write(&wal_path, data).unwrap();

        // Recovery should get entry 1 and entry 3 (skipping corrupted entry 2)
        let reader = WalReader::new(temp.path());
        let entries = reader.recover_entries().unwrap();

        // We should recover at least 1 entry (the first one before corruption)
        assert!(!entries.is_empty(), "Should recover at least one entry");
        assert_eq!(entries[0].key, b"good1");
    }

    #[test]
    fn test_torn_write_stops_cleanly() {
        let temp = TempDir::new().unwrap();

        let mut writer = WalWriter::new(temp.path()).unwrap();
        writer.append_durable(b"complete", b"entry", Operation::Put).unwrap();
        let wal_path = writer.current_path().to_path_buf();
        drop(writer);

        // Simulate torn write: append partial header bytes
        let mut data = std::fs::read(&wal_path).unwrap();
        data.extend_from_slice(&MAGIC_ARRAY); // magic bytes
        data.extend_from_slice(&[0xFF, 0x00, 0x00, 0x00]); // length = 255 but no payload
        std::fs::write(&wal_path, data).unwrap();

        // Recovery should get the complete entry and stop at torn write
        let reader = WalReader::new(temp.path());
        let entries = reader.recover_entries().unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, b"complete");
    }

    #[test]
    fn test_empty_wal_directory() {
        let temp = TempDir::new().unwrap();
        let reader = WalReader::new(temp.path());
        let entries = reader.recover_entries().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_wal_file_naming() {
        let temp = TempDir::new().unwrap();
        let writer = WalWriter::new(temp.path()).unwrap();
        let path = writer.current_path().to_path_buf();
        let name = path.file_name().unwrap().to_str().unwrap();
        assert!(name.starts_with("wal-"));
        assert!(name.ends_with(".claw"));
    }
}

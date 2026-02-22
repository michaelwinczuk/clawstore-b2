//! Data file storage layer for ClawStore B2
//!
//! Data files store key-value pairs flushed from RAM by the trickle engine.
//! Each entry has a CRC32C checksum for silent SSD corruption detection (bit rot).
//!
//! File format: DataChunkHeader (24 bytes) + key_bytes + value_bytes

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{ClawError, ClawResult};
use crate::format::{MAGIC_ARRAY, MAX_KEY_SIZE, MAX_VALUE_SIZE};
use crate::platform_durability::durable_sync;

/// Data chunk header size in bytes
const DATA_HEADER_SIZE: usize = 24;

/// Tombstone flag in the flags byte
const FLAG_TOMBSTONE: u8 = 0x01;

/// Maximum data file size before rotation (256MB)
const MAX_DATA_FILE_SIZE: u64 = 256 * 1024 * 1024;

/// Data chunk header for on-disk entries.
/// Size: 24 bytes, alignment: 4
///
/// Layout:
///   [0..4]   magic:     [u8;4] - 0x434C4157 ("CLAW")
///   [4..6]   key_len:   u16 LE
///   [6..10]  value_len: u32 LE
///   [10..14] checksum:  u32 LE - CRC32C of (key_bytes + value_bytes)
///   [14]     flags:     u8     - bit 0 = tombstone
///   [15..18] reserved:  [u8;3]
///   [18..24] padding:   [u8;6]
#[derive(Debug, Clone, Copy)]
struct DataChunkHeader {
    magic: [u8; 4],
    key_len: u16,
    value_len: u32,
    checksum: u32,
    flags: u8,
}

impl DataChunkHeader {
    fn new(key_len: u16, value_len: u32, checksum: u32, flags: u8) -> Self {
        Self { magic: MAGIC_ARRAY, key_len, value_len, checksum, flags }
    }

    fn to_bytes(&self) -> [u8; DATA_HEADER_SIZE] {
        let mut buf = [0u8; DATA_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4..6].copy_from_slice(&self.key_len.to_le_bytes());
        buf[6..10].copy_from_slice(&self.value_len.to_le_bytes());
        buf[10..14].copy_from_slice(&self.checksum.to_le_bytes());
        buf[14] = self.flags;
        // bytes 15..24 are reserved/padding, already zero
        buf
    }

    fn from_bytes(buf: &[u8; DATA_HEADER_SIZE]) -> Self {
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&buf[0..4]);
        Self {
            magic,
            key_len: u16::from_le_bytes([buf[4], buf[5]]),
            value_len: u32::from_le_bytes([buf[6], buf[7], buf[8], buf[9]]),
            checksum: u32::from_le_bytes([buf[10], buf[11], buf[12], buf[13]]),
            flags: buf[14],
        }
    }

    fn is_tombstone(&self) -> bool {
        (self.flags & FLAG_TOMBSTONE) != 0
    }
}

/// A data entry read from a data file.
#[derive(Debug, Clone)]
pub struct DataEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub offset: u64,
    pub is_tombstone: bool,
}

/// Writes entries to data files with CRC32C checksums and durable sync.
pub struct DataFileWriter {
    file: File,
    path: PathBuf,
    size: u64,
    data_dir: PathBuf,
    sequence: u64,
}

impl DataFileWriter {
    /// Create a new data file writer in the given directory.
    pub fn new(data_dir: &Path) -> ClawResult<Self> {
        std::fs::create_dir_all(data_dir).map_err(|e| ClawError::Io {
            path: Some(data_dir.to_path_buf()),
            kind: e.kind(),
            message: format!("Failed to create data directory: {}", e),
        })?;

        // Find highest existing sequence
        let mut max_seq = 0u64;
        if let Ok(entries) = std::fs::read_dir(data_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("data-") && name.ends_with(".claw") {
                        let hex = &name[5..name.len() - 5];
                        if let Ok(seq) = u64::from_str_radix(hex, 16) {
                            max_seq = max_seq.max(seq);
                        }
                    }
                }
            }
        }

        let sequence = max_seq + 1;
        let path = data_dir.join(format!("data-{:016x}.claw", sequence));

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| ClawError::Io {
                path: Some(path.clone()),
                kind: e.kind(),
                message: format!("Failed to open data file: {}", e),
            })?;

        let size = file.metadata()
            .map_err(|e| ClawError::Io {
                path: Some(path.clone()),
                kind: e.kind(),
                message: format!("Failed to stat data file: {}", e),
            })?
            .len();

        Ok(Self { file, path, size, data_dir: data_dir.to_path_buf(), sequence })
    }

    /// Write a key-value entry. Returns the byte offset where entry was written.
    pub fn write_entry(&mut self, key: &[u8], value: &[u8]) -> ClawResult<u64> {
        self.write_internal(key, value, false)
    }

    /// Write a tombstone (deletion marker). Returns byte offset.
    pub fn write_tombstone(&mut self, key: &[u8]) -> ClawResult<u64> {
        self.write_internal(key, &[], true)
    }

    /// Internal write with optional tombstone flag.
    fn write_internal(&mut self, key: &[u8], value: &[u8], tombstone: bool) -> ClawResult<u64> {
        // Validate sizes before allocation
        if key.len() > MAX_KEY_SIZE {
            return Err(ClawError::OversizedEntry {
                entry_size: key.len() as u64,
                max_size: MAX_KEY_SIZE as u64,
                component: "key".to_string(),
            });
        }
        if value.len() > MAX_VALUE_SIZE {
            return Err(ClawError::OversizedEntry {
                entry_size: value.len() as u64,
                max_size: MAX_VALUE_SIZE as u64,
                component: "value".to_string(),
            });
        }

        let actual_value: &[u8] = if tombstone { &[] } else { value };
        let entry_size = DATA_HEADER_SIZE as u64 + key.len() as u64 + actual_value.len() as u64;

        // Rotate if needed
        if self.size + entry_size > MAX_DATA_FILE_SIZE {
            self.rotate()?;
        }

        // Compute CRC32C over key + value
        let mut payload = Vec::with_capacity(key.len() + actual_value.len());
        payload.extend_from_slice(key);
        payload.extend_from_slice(actual_value);
        let checksum = crc32c::crc32c(&payload);

        let flags = if tombstone { FLAG_TOMBSTONE } else { 0 };
        let header = DataChunkHeader::new(key.len() as u16, actual_value.len() as u32, checksum, flags);

        let offset = self.size;

        // Write header + key + value
        self.file.write_all(&header.to_bytes()).map_err(|e| ClawError::Io {
            path: Some(self.path.clone()), kind: e.kind(),
            message: format!("Data file write failed: {}", e),
        })?;
        self.file.write_all(key).map_err(|e| ClawError::Io {
            path: Some(self.path.clone()), kind: e.kind(),
            message: format!("Data file write key failed: {}", e),
        })?;
        self.file.write_all(actual_value).map_err(|e| ClawError::Io {
            path: Some(self.path.clone()), kind: e.kind(),
            message: format!("Data file write value failed: {}", e),
        })?;

        // Durable sync — data must survive power loss
        durable_sync(&self.file).map_err(|e| ClawError::Io {
            path: Some(self.path.clone()), kind: e.kind(),
            message: format!("Data file durable_sync failed: {}", e),
        })?;

        self.size += entry_size;
        Ok(offset)
    }

    /// Current file size in bytes.
    pub fn current_size(&self) -> u64 {
        self.size
    }

    /// Rotate to a new data file.
    pub fn rotate(&mut self) -> ClawResult<()> {
        durable_sync(&self.file).map_err(|e| ClawError::Io {
            path: Some(self.path.clone()), kind: e.kind(),
            message: format!("Data file sync before rotation failed: {}", e),
        })?;

        self.sequence += 1;
        let new_path = self.data_dir.join(format!("data-{:016x}.claw", self.sequence));

        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_path)
            .map_err(|e| ClawError::Io {
                path: Some(new_path.clone()), kind: e.kind(),
                message: format!("Failed to create rotated data file: {}", e),
            })?;

        self.file = new_file;
        self.path = new_path;
        self.size = 0;
        Ok(())
    }
}

/// Reads entries from data files with CRC32C verification.
pub struct DataFileReader;

impl DataFileReader {
    /// Read a single entry at a given offset. Returns None for tombstones.
    pub fn read_entry(file_path: &Path, offset: u64) -> ClawResult<Option<DataEntry>> {
        let mut file = File::open(file_path).map_err(|e| ClawError::Io {
            path: Some(file_path.to_path_buf()), kind: e.kind(),
            message: format!("Failed to open data file: {}", e),
        })?;
        file.seek(SeekFrom::Start(offset))?;

        // Read header
        let mut hdr_buf = [0u8; DATA_HEADER_SIZE];
        file.read_exact(&mut hdr_buf).map_err(|e| ClawError::Io {
            path: Some(file_path.to_path_buf()), kind: e.kind(),
            message: format!("Failed to read data chunk header at offset {}: {}", offset, e),
        })?;
        let hdr = DataChunkHeader::from_bytes(&hdr_buf);

        // Validate magic
        if hdr.magic != MAGIC_ARRAY {
            return Err(ClawError::NoMagicFound {
                path: file_path.to_path_buf(),
                offset,
                found_bytes: hdr.magic,
            });
        }

        // Validate sizes
        if hdr.key_len as usize > MAX_KEY_SIZE {
            return Err(ClawError::WalCorrupted {
                path: file_path.to_path_buf(), offset,
                reason: format!("key_len {} exceeds MAX_KEY_SIZE {}", hdr.key_len, MAX_KEY_SIZE),
            });
        }
        if hdr.value_len as usize > MAX_VALUE_SIZE {
            return Err(ClawError::WalCorrupted {
                path: file_path.to_path_buf(), offset,
                reason: format!("value_len {} exceeds MAX_VALUE_SIZE {}", hdr.value_len, MAX_VALUE_SIZE),
            });
        }

        // Read key + value
        let mut key = vec![0u8; hdr.key_len as usize];
        file.read_exact(&mut key)?;
        let mut value = vec![0u8; hdr.value_len as usize];
        file.read_exact(&mut value)?;

        // Verify CRC32C
        let mut payload = Vec::with_capacity(key.len() + value.len());
        payload.extend_from_slice(&key);
        payload.extend_from_slice(&value);
        let computed = crc32c::crc32c(&payload);

        if computed != hdr.checksum {
            return Err(ClawError::ChecksumMismatch {
                path: file_path.to_path_buf(),
                expected: hdr.checksum,
                actual: computed,
                offset,
            });
        }

        if hdr.is_tombstone() {
            return Ok(None);
        }

        Ok(Some(DataEntry { key, value, offset, is_tombstone: false }))
    }

    /// Scan all entries from a data file. Used during compaction.
    pub fn scan_all(file_path: &Path) -> ClawResult<Vec<DataEntry>> {
        let mut file = File::open(file_path).map_err(|e| ClawError::Io {
            path: Some(file_path.to_path_buf()), kind: e.kind(),
            message: format!("Failed to open data file for scan: {}", e),
        })?;

        let file_len = file.metadata()?.len();
        let mut entries = Vec::new();
        let mut offset = 0u64;

        while offset + DATA_HEADER_SIZE as u64 <= file_len {
            file.seek(SeekFrom::Start(offset))?;

            // Read header
            let mut hdr_buf = [0u8; DATA_HEADER_SIZE];
            if file.read_exact(&mut hdr_buf).is_err() {
                break;
            }
            let hdr = DataChunkHeader::from_bytes(&hdr_buf);

            // Validate magic
            if hdr.magic != MAGIC_ARRAY {
                // Corruption — scan forward for next magic
                match find_next_magic(&mut file, offset + 1, file_len) {
                    Some(next) => { offset = next; continue; }
                    None => break,
                }
            }

            // Validate sizes
            if hdr.key_len as usize > MAX_KEY_SIZE || hdr.value_len as usize > MAX_VALUE_SIZE {
                match find_next_magic(&mut file, offset + 1, file_len) {
                    Some(next) => { offset = next; continue; }
                    None => break,
                }
            }

            let entry_total = DATA_HEADER_SIZE as u64 + hdr.key_len as u64 + hdr.value_len as u64;
            if offset + entry_total > file_len {
                break; // truncated entry
            }

            // Read key + value
            let mut key = vec![0u8; hdr.key_len as usize];
            let mut value = vec![0u8; hdr.value_len as usize];
            if file.read_exact(&mut key).is_err() || file.read_exact(&mut value).is_err() {
                break;
            }

            // Verify CRC32C
            let mut payload = Vec::with_capacity(key.len() + value.len());
            payload.extend_from_slice(&key);
            payload.extend_from_slice(&value);
            let computed = crc32c::crc32c(&payload);

            if computed == hdr.checksum {
                entries.push(DataEntry {
                    key,
                    value,
                    offset,
                    is_tombstone: hdr.is_tombstone(),
                });
            } else {
                eprintln!("[DATA SCAN] CRC mismatch at offset {}, skipping", offset);
            }

            offset += entry_total;
        }

        Ok(entries)
    }
}

/// Scan forward in file to find next CLAW magic bytes (corruption recovery).
fn find_next_magic(file: &mut File, start: u64, file_len: u64) -> Option<u64> {
    let mut buf = [0u8; 4096];
    let mut pos = start;

    while pos + 4 <= file_len {
        file.seek(SeekFrom::Start(pos)).ok()?;
        let n = file.read(&mut buf).ok()?;
        if n < 4 { return None; }

        for i in 0..n.saturating_sub(3) {
            if buf[i..i + 4] == MAGIC_ARRAY {
                return Some(pos + i as u64);
            }
        }
        pos += n.saturating_sub(3) as u64;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn find_data_file(dir: &Path) -> PathBuf {
        std::fs::read_dir(dir).unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_str().map_or(false, |n| n.starts_with("data-")))
            .map(|e| e.path())
            .expect("No data file found")
    }

    #[test]
    fn test_write_read_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("data");
        let mut writer = DataFileWriter::new(&dir).unwrap();

        let offset = writer.write_entry(b"mykey", b"myvalue").unwrap();
        let file = find_data_file(&dir);
        let entry = DataFileReader::read_entry(&file, offset).unwrap().unwrap();

        assert_eq!(entry.key, b"mykey");
        assert_eq!(entry.value, b"myvalue");
        assert!(!entry.is_tombstone);
    }

    #[test]
    fn test_tombstone() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("data");
        let mut writer = DataFileWriter::new(&dir).unwrap();

        let offset = writer.write_tombstone(b"gone").unwrap();
        let file = find_data_file(&dir);
        let entry = DataFileReader::read_entry(&file, offset).unwrap();

        assert!(entry.is_none(), "Tombstone should return None");
    }

    #[test]
    fn test_checksum_validation() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("data");
        let mut writer = DataFileWriter::new(&dir).unwrap();

        let offset = writer.write_entry(b"key", b"value").unwrap();
        let file = find_data_file(&dir);

        // Corrupt a payload byte
        {
            let mut f = OpenOptions::new().write(true).open(&file).unwrap();
            f.seek(SeekFrom::Start(offset + DATA_HEADER_SIZE as u64 + 3)).unwrap();
            f.write_all(&[0xFF]).unwrap();
        }

        let result = DataFileReader::read_entry(&file, offset);
        assert!(matches!(result, Err(ClawError::ChecksumMismatch { .. })));
    }

    #[test]
    fn test_scan_all() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("data");
        let mut writer = DataFileWriter::new(&dir).unwrap();

        writer.write_entry(b"k1", b"v1").unwrap();
        writer.write_entry(b"k2", b"v2").unwrap();
        writer.write_tombstone(b"k3").unwrap();
        writer.write_entry(b"k4", b"v4").unwrap();

        let file = find_data_file(&dir);
        let entries = DataFileReader::scan_all(&file).unwrap();

        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].key, b"k1");
        assert!(!entries[0].is_tombstone);
        assert_eq!(entries[2].key, b"k3");
        assert!(entries[2].is_tombstone);
        assert_eq!(entries[3].key, b"k4");
    }

    #[test]
    fn test_oversized_rejected() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("data");
        let mut writer = DataFileWriter::new(&dir).unwrap();

        let big_key = vec![0xAA; MAX_KEY_SIZE + 1];
        assert!(matches!(
            writer.write_entry(&big_key, b"v"),
            Err(ClawError::OversizedEntry { .. })
        ));

        let big_val = vec![0xBB; MAX_VALUE_SIZE + 1];
        assert!(matches!(
            writer.write_entry(b"k", &big_val),
            Err(ClawError::OversizedEntry { .. })
        ));
    }
}

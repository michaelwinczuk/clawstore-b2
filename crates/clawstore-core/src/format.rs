//! Binary format definitions for ClawStore WAL entries
//!
//! All WAL entries follow a consistent format:
//! ChunkHeader (32 bytes) + key_len(u16) + value_len(u32) + operation(u8) + padding(u8) + key_bytes + value_bytes

use crate::error::{ClawError, ClawResult};

/// Magic bytes identifying ClawStore WAL entries: "CLAW" in ASCII (little-endian)
pub const MAGIC_BYTES: u32 = 0x574C4143; // 'C','L','A','W' stored as little-endian u32

/// Magic bytes as a byte array for comparison
pub const MAGIC_ARRAY: [u8; 4] = [0x43, 0x4C, 0x41, 0x57]; // 'C','L','A','W'

/// Maximum key size in bytes
pub const MAX_KEY_SIZE: usize = 128;

/// Maximum value size in bytes (32MB)
pub const MAX_VALUE_SIZE: usize = 32 * 1024 * 1024;

/// Header size in bytes
pub const HEADER_SIZE: usize = 32;

/// WAL operation types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Operation {
    /// Insert or update a key-value pair
    Put = 1,
    /// Delete a key
    Delete = 2,
}

/// Fixed-size header for each WAL entry
/// Size: 32 bytes, alignment: 4
///
/// Layout:
///   [0..4]   magic:      u32  - 0x434C4157 ("CLAW")
///   [4..8]   length:     u32  - payload length in bytes
///   [8..12]  checksum:   u32  - CRC32C of payload bytes
///   [12]     entry_type: u8   - operation type
///   [13..16] reserved:   [u8;3]
///   [16..32] padding:    [u8;16]
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct ChunkHeader {
    /// Magic bytes for entry identification and corruption recovery
    pub magic: [u8; 4],
    /// Total length of the entry payload (excluding this header)
    pub length: u32,
    /// CRC32C checksum of the payload bytes
    pub checksum: u32,
    /// Operation type (Put=1 or Delete=2)
    pub entry_type: u8,
    /// Reserved for future use, must be zero
    pub reserved: [u8; 3],
    /// Padding to reach 32 bytes
    pub _padding: [u8; 16],
}

/// Complete WAL entry structure (deserialized)
#[derive(Debug, Clone)]
pub struct WalEntry {
    pub header: ChunkHeader,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub operation: Operation,
}

impl ChunkHeader {
    /// Create a new header with the given parameters
    pub fn new(length: u32, checksum: u32, entry_type: Operation) -> Self {
        Self {
            magic: MAGIC_ARRAY,
            length,
            checksum,
            entry_type: entry_type as u8,
            reserved: [0; 3],
            _padding: [0; 16],
        }
    }

    /// Serialize header to bytes for writing
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4..8].copy_from_slice(&self.length.to_le_bytes());
        buf[8..12].copy_from_slice(&self.checksum.to_le_bytes());
        buf[12] = self.entry_type;
        buf[13..16].copy_from_slice(&self.reserved);
        // _padding is already zeroed
        buf
    }

    /// Parse header from bytes
    pub fn from_bytes(bytes: &[u8; HEADER_SIZE]) -> Self {
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[0..4]);

        Self {
            magic,
            length: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            checksum: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            entry_type: bytes[12],
            reserved: [bytes[13], bytes[14], bytes[15]],
            _padding: {
                let mut pad = [0u8; 16];
                pad.copy_from_slice(&bytes[16..32]);
                pad
            },
        }
    }
}

/// Serialize a key-value pair into a complete WAL entry
///
/// Format: ChunkHeader(32) + key_len(u16 LE) + value_len(u32 LE) + operation(u8) + padding(u8) + key + value
pub fn serialize_entry(key: &[u8], value: &[u8], op: Operation) -> ClawResult<Vec<u8>> {
    // Validate input sizes BEFORE any allocation (prevents memory exhaustion attacks)
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

    // Payload: key_len(2) + value_len(4) + operation(1) + padding(1) + key + value
    let payload_size = 2 + 4 + 1 + 1 + key.len() + value.len();
    let total_size = HEADER_SIZE + payload_size;

    // Build payload to compute checksum
    let mut payload = Vec::with_capacity(payload_size);
    payload.extend_from_slice(&(key.len() as u16).to_le_bytes());
    payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
    payload.push(op as u8);
    payload.push(0); // padding byte
    payload.extend_from_slice(key);
    payload.extend_from_slice(value);

    // Compute CRC32C checksum over payload bytes
    let checksum = crc32c::crc32c(&payload);

    // Create header
    let header = ChunkHeader::new(payload.len() as u32, checksum, op);

    // Assemble complete entry: header + payload
    let mut buffer = Vec::with_capacity(total_size);
    buffer.extend_from_slice(&header.to_bytes());
    buffer.extend_from_slice(&payload);

    Ok(buffer)
}

/// Deserialize a WAL entry from a byte slice
pub fn deserialize_entry(data: &[u8]) -> ClawResult<WalEntry> {
    if data.len() < HEADER_SIZE {
        return Err(ClawError::WalCorrupted {
            path: std::path::PathBuf::from("<buffer>"),
            offset: 0,
            reason: format!("Entry too short: {} bytes, need at least {}", data.len(), HEADER_SIZE),
        });
    }

    // Parse header
    let header_bytes: [u8; HEADER_SIZE] = data[..HEADER_SIZE].try_into().unwrap();
    let header = ChunkHeader::from_bytes(&header_bytes);

    // Validate magic bytes
    if header.magic != MAGIC_ARRAY {
        return Err(ClawError::NoMagicFound {
            path: std::path::PathBuf::from("<buffer>"),
            offset: 0,
            found_bytes: header.magic,
        });
    }

    // Validate payload fits in data
    let payload_start = HEADER_SIZE;
    let payload_end = payload_start + header.length as usize;

    if data.len() < payload_end {
        return Err(ClawError::TornWrite {
            path: std::path::PathBuf::from("<buffer>"),
            expected_size: header.length,
            available_bytes: (data.len() - payload_start) as u64,
            offset: payload_start as u64,
        });
    }

    let payload = &data[payload_start..payload_end];

    // Verify CRC32C checksum
    let computed_checksum = crc32c::crc32c(payload);
    if computed_checksum != header.checksum {
        return Err(ClawError::ChecksumMismatch {
            path: std::path::PathBuf::from("<buffer>"),
            expected: header.checksum,
            actual: computed_checksum,
            offset: payload_start as u64,
        });
    }

    // Parse payload: key_len(2) + value_len(4) + operation(1) + padding(1) + key + value
    if payload.len() < 8 {
        return Err(ClawError::WalCorrupted {
            path: std::path::PathBuf::from("<buffer>"),
            offset: payload_start as u64,
            reason: "Payload too short for header fields".to_string(),
        });
    }

    let key_len = u16::from_le_bytes([payload[0], payload[1]]) as usize;
    let value_len = u32::from_le_bytes([payload[2], payload[3], payload[4], payload[5]]) as usize;
    let operation = match payload[6] {
        1 => Operation::Put,
        2 => Operation::Delete,
        other => return Err(ClawError::WalCorrupted {
            path: std::path::PathBuf::from("<buffer>"),
            offset: (payload_start + 6) as u64,
            reason: format!("Invalid operation type: {}", other),
        }),
    };

    let data_start = 8; // after key_len + value_len + op + padding
    let key_end = data_start + key_len;
    let value_end = key_end + value_len;

    if payload.len() < value_end {
        return Err(ClawError::WalCorrupted {
            path: std::path::PathBuf::from("<buffer>"),
            offset: payload_start as u64,
            reason: format!("Payload too short: need {} bytes for key({}) + value({})",
                          value_end, key_len, value_len),
        });
    }

    Ok(WalEntry {
        header,
        key: payload[data_start..key_end].to_vec(),
        value: payload[key_end..value_end].to_vec(),
        operation,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_size() {
        assert_eq!(std::mem::size_of::<ChunkHeader>(), HEADER_SIZE);
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let key = b"test_key";
        let value = b"test_value_data";

        let serialized = serialize_entry(key, value, Operation::Put).unwrap();
        let deserialized = deserialize_entry(&serialized).unwrap();

        assert_eq!(deserialized.key, key);
        assert_eq!(deserialized.value, value);
        assert_eq!(deserialized.operation, Operation::Put);
        assert_eq!(deserialized.header.magic, MAGIC_ARRAY);
    }

    #[test]
    fn test_delete_roundtrip() {
        let key = b"delete_me";
        let value = b"";

        let serialized = serialize_entry(key, value, Operation::Delete).unwrap();
        let deserialized = deserialize_entry(&serialized).unwrap();

        assert_eq!(deserialized.key, b"delete_me");
        assert_eq!(deserialized.value, b"");
        assert_eq!(deserialized.operation, Operation::Delete);
    }

    #[test]
    fn test_oversized_key_rejected() {
        let key = vec![0u8; MAX_KEY_SIZE + 1];
        let result = serialize_entry(&key, b"val", Operation::Put);
        assert!(matches!(result, Err(ClawError::OversizedEntry { component, .. }) if component == "key"));
    }

    #[test]
    fn test_oversized_value_rejected() {
        let value = vec![0u8; MAX_VALUE_SIZE + 1];
        let result = serialize_entry(b"k", &value, Operation::Put);
        assert!(matches!(result, Err(ClawError::OversizedEntry { component, .. }) if component == "value"));
    }

    #[test]
    fn test_corrupted_magic_detected() {
        let mut data = serialize_entry(b"key", b"value", Operation::Put).unwrap();
        data[0] = 0xFF; // corrupt magic
        assert!(matches!(deserialize_entry(&data), Err(ClawError::NoMagicFound { .. })));
    }

    #[test]
    fn test_corrupted_payload_detected() {
        let mut data = serialize_entry(b"key", b"value", Operation::Put).unwrap();
        let last = data.len() - 1;
        data[last] ^= 0xFF; // corrupt payload
        assert!(matches!(deserialize_entry(&data), Err(ClawError::ChecksumMismatch { .. })));
    }

    #[test]
    fn test_max_key_size_accepted() {
        let key = vec![0x41u8; MAX_KEY_SIZE]; // exactly at limit
        let result = serialize_entry(&key, b"v", Operation::Put);
        assert!(result.is_ok());
    }
}

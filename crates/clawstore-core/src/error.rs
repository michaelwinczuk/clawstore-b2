//! Error types for ClawStore operations
//!
//! All ClawStore errors are represented by the ClawError enum, which provides
//! detailed context for debugging and recovery.

use std::fmt;
use std::error::Error;
use std::path::PathBuf;

/// ClawStore error types with detailed context
#[derive(Debug, Clone)]
pub enum ClawError {
    /// I/O operation failed
    Io {
        /// The file path where the error occurred
        path: Option<PathBuf>,
        /// The underlying I/O error kind
        kind: std::io::ErrorKind,
        /// Human-readable description
        message: String,
    },

    /// WAL file is corrupted and cannot be recovered
    WalCorrupted {
        /// Path to the corrupted WAL file
        path: PathBuf,
        /// Byte offset where corruption was detected
        offset: u64,
        /// Description of the corruption
        reason: String,
    },

    /// Checksum verification failed
    ChecksumMismatch {
        /// File where checksum failed
        path: PathBuf,
        /// Expected checksum value
        expected: u32,
        /// Actual checksum computed
        actual: u32,
        /// Byte offset of the corrupted data
        offset: u64,
    },

    /// Torn write detected (partial write at end of file)
    TornWrite {
        /// File with torn write
        path: PathBuf,
        /// Expected entry size
        expected_size: u32,
        /// Actual bytes available
        available_bytes: u64,
        /// Offset where torn write begins
        offset: u64,
    },

    /// Snapshot memory usage would exceed configured limit
    SnapshotMemoryExceeded {
        /// Requested memory size
        requested_bytes: u64,
        /// Configured memory limit
        limit_bytes: u64,
    },

    /// Entry size exceeds maximum allowed
    OversizedEntry {
        /// Size of the oversized entry
        entry_size: u64,
        /// Maximum allowed size
        max_size: u64,
        /// Whether it's the key or value that's oversized
        component: String,
    },

    /// Magic bytes not found at expected location
    NoMagicFound {
        /// File being read
        path: PathBuf,
        /// Offset where magic was expected
        offset: u64,
        /// Bytes actually found
        found_bytes: [u8; 4],
    },
}

impl fmt::Display for ClawError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClawError::Io { path, kind, message } => {
                if let Some(path) = path {
                    write!(f, "I/O error in {}: {} ({})", path.display(), message, kind)
                } else {
                    write!(f, "I/O error: {} ({})", message, kind)
                }
            }

            ClawError::WalCorrupted { path, offset, reason } => {
                write!(f, "WAL corrupted in {} at offset {}: {}", path.display(), offset, reason)
            }

            ClawError::ChecksumMismatch { path, expected, actual, offset } => {
                write!(f, "Checksum mismatch in {} at offset {}: expected 0x{:08x}, got 0x{:08x}",
                       path.display(), offset, expected, actual)
            }

            ClawError::TornWrite { path, expected_size, available_bytes, offset } => {
                write!(f, "Torn write in {} at offset {}: expected {} bytes, only {} available",
                       path.display(), offset, expected_size, available_bytes)
            }

            ClawError::SnapshotMemoryExceeded { requested_bytes, limit_bytes } => {
                write!(f, "Snapshot memory limit exceeded: requested {} bytes, limit {} bytes",
                       requested_bytes, limit_bytes)
            }

            ClawError::OversizedEntry { entry_size, max_size, component } => {
                write!(f, "Entry {} too large: {} bytes exceeds limit of {} bytes",
                       component, entry_size, max_size)
            }

            ClawError::NoMagicFound { path, offset, found_bytes } => {
                write!(f, "Magic bytes not found in {} at offset {}: found {:02x}{:02x}{:02x}{:02x}",
                       path.display(), offset, found_bytes[0], found_bytes[1], found_bytes[2], found_bytes[3])
            }
        }
    }
}

impl Error for ClawError {}

/// Convert std::io::Error to ClawError::Io
impl From<std::io::Error> for ClawError {
    fn from(err: std::io::Error) -> Self {
        ClawError::Io {
            path: None,
            kind: err.kind(),
            message: err.to_string(),
        }
    }
}

/// Result type alias for ClawStore operations
pub type ClawResult<T> = Result<T, ClawError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = ClawError::ChecksumMismatch {
            path: PathBuf::from("/tmp/test.wal"),
            expected: 0x12345678,
            actual: 0x87654321,
            offset: 1024,
        };

        let display = format!("{}", err);
        assert!(display.contains("Checksum mismatch"));
        assert!(display.contains("0x12345678"));
        assert!(display.contains("0x87654321"));
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let claw_err: ClawError = io_err.into();

        match claw_err {
            ClawError::Io { kind, .. } => assert_eq!(kind, std::io::ErrorKind::NotFound),
            _ => panic!("Expected Io error"),
        }
    }
}

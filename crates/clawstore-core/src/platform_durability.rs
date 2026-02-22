//! Platform-specific durable sync implementations
//!
//! Each platform has different guarantees for when data is actually written to persistent storage.
//! This module provides a unified interface that maps to the strongest durability guarantee
//! available on each platform.

use std::fs::File;
use std::io;

/// Ensures data is durably written to persistent storage before returning.
///
/// Platform behaviors:
/// - Linux: fdatasync() - syncs data but not metadata (faster than fsync)
/// - macOS/iOS: fcntl(F_FULLFSYNC) - bypasses disk cache, ensures data reaches physical media
/// - Windows: FlushFileBuffers() - flushes internal buffers and requests device flush
/// - Other: file.sync_data() - Rust stdlib fallback
///
/// # Safety
/// This function makes system calls that may block for extended periods during heavy I/O.
/// The caller must not hold locks that could cause deadlocks during the sync operation.
pub fn durable_sync(file: &File) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        // Linux: fdatasync() syncs file data but not metadata (atime, mtime)
        // This is faster than fsync() and sufficient for WAL durability
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();
        // SAFETY: fdatasync is a POSIX system call that operates on a valid file descriptor.
        // We obtain the fd from a valid File reference, so it is guaranteed to be open.
        let result = unsafe { libc::fdatasync(fd) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        // macOS/iOS: F_FULLFSYNC bypasses the disk cache and ensures data reaches physical media.
        // Standard fsync() on macOS only flushes to the disk's volatile write cache,
        // which can be lost on power failure. F_FULLFSYNC is the only way to get true
        // durability on Apple platforms.
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();
        // SAFETY: fcntl with F_FULLFSYNC is a macOS system call that operates on a valid fd.
        // We obtain the fd from a valid File reference, so it is guaranteed to be open.
        let result = unsafe { libc::fcntl(fd, libc::F_FULLFSYNC) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(target_os = "windows")]
    {
        // Windows: FlushFileBuffers() flushes internal buffers and requests device flush.
        // This is the Windows equivalent of fsync().
        use std::os::windows::io::AsRawHandle;
        use winapi::um::fileapi::FlushFileBuffers;
        let handle = file.as_raw_handle();
        // SAFETY: FlushFileBuffers is a Windows API call on a valid file handle.
        // We obtain the handle from a valid File reference.
        let result = unsafe { FlushFileBuffers(handle as *mut _) };
        if result != 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "ios", target_os = "windows")))]
    {
        // Fallback: Use Rust's sync_data() for other platforms (FreeBSD, etc.)
        // This maps to the platform's best available sync primitive.
        file.sync_data()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_durable_sync_success() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"test data for durable sync").unwrap();

        // Should not panic or return error on valid file
        let result = durable_sync(file.as_file());
        assert!(result.is_ok(), "durable_sync failed: {:?}", result.err());
    }
}

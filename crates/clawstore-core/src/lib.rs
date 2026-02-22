//! ClawStore Core — Universal RAM-First Storage Engine
//!
//! A high-performance key-value storage engine where RAM is the primary
//! working surface and an SSD-backed write-ahead log provides crash safety.
//!
//! # Architecture
//!
//! - **Read path**: Serve directly from RAM hash table (sub-microsecond)
//! - **Write path**: WAL-first, then RAM update (crash-safe)
//! - **Trickle engine**: Background flush from RAM to SSD during idle
//!
//! # Zero Blockchain Dependencies
//!
//! This crate has no Ethereum types, no blockchain assumptions.
//! It can be used for any key-value workload on any computer.
//! Blockchain-specific adapters live in separate crates (e.g. clawstore-reth).

pub mod compaction;
pub mod config;
pub mod datafile;
pub mod engine;
pub mod error;
pub mod format;
pub mod platform_durability;
pub mod trickle;
pub mod wal;

// Re-export key types for convenience
pub use config::Config;
pub use datafile::{DataEntry, DataFileReader, DataFileWriter};
pub use engine::ClawStoreEngine;
pub use error::{ClawError, ClawResult};
pub use format::Operation;
pub use trickle::{DirtyTracker, TrickleHandle, start_trickle};
pub use wal::{WalWriter, WalReader};

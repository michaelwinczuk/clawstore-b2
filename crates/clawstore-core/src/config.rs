//! Configuration management for ClawStore
//!
//! Provides memory tier presets for different hardware classes
//! and a builder for custom configurations.

use std::time::Duration;

/// ClawStore configuration with memory tier presets
#[derive(Debug, Clone)]
pub struct Config {
    /// Maximum memory for active MVCC snapshots (bytes)
    pub max_snapshot_memory_bytes: u64,
    /// Maximum time-to-live for snapshots before forced expiry (seconds)
    pub max_snapshot_ttl_secs: u64,
    /// WAL file rotation threshold (bytes)
    pub wal_rotation_size_bytes: u64,
    /// Compaction trigger: compact when dead space ratio exceeds this
    pub compaction_trigger_ratio: f64,
    /// Background trickle flush cadence
    pub trickle_cadence: Duration,
    /// Maximum key size in bytes
    pub max_key_size: usize,
    /// Maximum value size in bytes
    pub max_value_size: usize,
}

impl Config {
    /// Server-class: 64GB machine, 39GB for ClawStore
    pub fn server() -> Self {
        Self {
            max_snapshot_memory_bytes: 39 * 1024 * 1024 * 1024,
            max_snapshot_ttl_secs: 3600,
            wal_rotation_size_bytes: 100 * 1024 * 1024,
            compaction_trigger_ratio: 0.3,
            trickle_cadence: Duration::from_secs(12),
            max_key_size: 128,
            max_value_size: 32 * 1024 * 1024,
        }
    }

    /// Phone-class: 16GB device, 1.5GB for ClawStore
    pub fn phone() -> Self {
        Self {
            max_snapshot_memory_bytes: 1536 * 1024 * 1024,
            max_snapshot_ttl_secs: 1800,
            wal_rotation_size_bytes: 50 * 1024 * 1024,
            compaction_trigger_ratio: 0.25,
            trickle_cadence: Duration::from_secs(15),
            max_key_size: 128,
            max_value_size: 16 * 1024 * 1024,
        }
    }

    /// Budget-class: 4GB device, 400MB for ClawStore
    pub fn budget() -> Self {
        Self {
            max_snapshot_memory_bytes: 400 * 1024 * 1024,
            max_snapshot_ttl_secs: 900,
            wal_rotation_size_bytes: 25 * 1024 * 1024,
            compaction_trigger_ratio: 0.2,
            trickle_cadence: Duration::from_secs(20),
            max_key_size: 64,
            max_value_size: 8 * 1024 * 1024,
        }
    }

    /// Validate all configuration parameters
    pub fn validate(&self) -> Result<(), String> {
        if self.max_snapshot_memory_bytes == 0 {
            return Err("max_snapshot_memory_bytes must be > 0".into());
        }
        if self.max_snapshot_ttl_secs == 0 {
            return Err("max_snapshot_ttl_secs must be > 0".into());
        }
        if self.wal_rotation_size_bytes < 1024 * 1024 {
            return Err("wal_rotation_size_bytes must be >= 1MB".into());
        }
        if self.compaction_trigger_ratio <= 0.0 || self.compaction_trigger_ratio >= 1.0 {
            return Err("compaction_trigger_ratio must be in (0.0, 1.0)".into());
        }
        if self.trickle_cadence.as_millis() == 0 {
            return Err("trickle_cadence must be > 0".into());
        }
        if self.max_key_size == 0 || self.max_key_size > 1024 {
            return Err("max_key_size must be in [1, 1024]".into());
        }
        if self.max_value_size == 0 || self.max_value_size > 128 * 1024 * 1024 {
            return Err("max_value_size must be in [1, 128MB]".into());
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self { Self::server() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_presets_valid() {
        assert!(Config::server().validate().is_ok());
        assert!(Config::phone().validate().is_ok());
        assert!(Config::budget().validate().is_ok());
    }

    #[test]
    fn test_tier_ordering() {
        let s = Config::server();
        let p = Config::phone();
        let b = Config::budget();
        assert!(s.max_snapshot_memory_bytes > p.max_snapshot_memory_bytes);
        assert!(p.max_snapshot_memory_bytes > b.max_snapshot_memory_bytes);
    }
}

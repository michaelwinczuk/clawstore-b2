//! `Database` trait implementation for ClawStore.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use reth_db_api::database::Database;
use reth_storage_errors::db::DatabaseError;

use clawstore_core::{ClawStoreEngine, Config as ClawConfig};

use crate::tx::{ClawReadTx, ClawWriteTx};

/// ClawStore database implementing Reth's `Database` trait.
///
/// Wraps a `ClawStoreEngine` and provides read/write transactions
/// that satisfy Reth's `DbTx` and `DbTxMut` interfaces.
pub struct ClawDatabase {
    engine: Arc<ClawStoreEngine>,
    path: PathBuf,
}

impl ClawDatabase {
    /// Open a ClawStore database at the given path.
    pub fn open<P: AsRef<Path>>(path: P, config: ClawConfig) -> Result<Self, DatabaseError> {
        let path = path.as_ref().to_path_buf();
        let engine = ClawStoreEngine::open(&path, config).map_err(|e| {
            DatabaseError::Other(e.to_string())
        })?;
        Ok(Self {
            engine: Arc::new(engine),
            path,
        })
    }

    /// Get a reference to the underlying engine.
    pub fn engine(&self) -> &ClawStoreEngine {
        &self.engine
    }

    /// Get the database path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Internal: get Arc to engine for transaction creation.
    pub(crate) fn engine_arc(&self) -> Arc<ClawStoreEngine> {
        Arc::clone(&self.engine)
    }
}

impl std::fmt::Debug for ClawDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClawDatabase")
            .field("path", &self.path)
            .field("entries", &self.engine.len())
            .finish()
    }
}

impl Database for ClawDatabase {
    type TX = ClawReadTx;
    type TXMut = ClawWriteTx;

    fn tx(&self) -> Result<Self::TX, DatabaseError> {
        Ok(ClawReadTx::new(self.engine_arc()))
    }

    fn tx_mut(&self) -> Result<Self::TXMut, DatabaseError> {
        Ok(ClawWriteTx::new(self.engine_arc()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_open_and_debug() {
        let dir = TempDir::new().unwrap();
        let db = ClawDatabase::open(dir.path(), ClawConfig::default()).unwrap();
        let debug_str = format!("{:?}", db);
        assert!(debug_str.contains("ClawDatabase"));
    }

    #[test]
    fn test_create_transactions() {
        let dir = TempDir::new().unwrap();
        let db = ClawDatabase::open(dir.path(), ClawConfig::default()).unwrap();
        let _tx = db.tx().unwrap();
        let _tx_mut = db.tx_mut().unwrap();
    }
}

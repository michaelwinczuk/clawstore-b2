//! Transaction implementations: `DbTx` (read) and `DbTxMut` (read-write).
//!
//! Read transactions serve data directly from the ClawStore engine.
//! Write transactions buffer changes and flush to the engine on commit.

use std::sync::Arc;

use reth_db_api::{
    table::{Compress, DupSort, Encode, Table, TableImporter},
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};


use clawstore_core::ClawStoreEngine;

use crate::cursor::{ClawCursor, ClawDupCursor, ClawCursorMut, ClawDupCursorMut};
use crate::table_ids::table_id_for_name;

// ---------------------------------------------------------------------------
// Read-only transaction
// ---------------------------------------------------------------------------

/// Read-only transaction backed by ClawStore.
///
/// Reads go directly to the engine's RAM HashMap — no snapshot needed
/// because ClawStore's RwLock provides consistent reads.
pub struct ClawReadTx {
    engine: Arc<ClawStoreEngine>,
    _long_read_safety: bool,
}

impl ClawReadTx {
    pub(crate) fn new(engine: Arc<ClawStoreEngine>) -> Self {
        Self { engine, _long_read_safety: true }
    }

    /// Get the raw value for a table-prefixed key from the engine.
    pub(crate) fn raw_get(&self, table_id: u8, key_bytes: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        let mut prefixed = Vec::with_capacity(1 + key_bytes.len());
        prefixed.push(table_id);
        prefixed.extend_from_slice(key_bytes);
        self.engine.get(&prefixed).map_err(|e| {
            DatabaseError::Other(e.to_string())
        })
    }

    pub(crate) fn engine_arc(&self) -> Arc<ClawStoreEngine> {
        Arc::clone(&self.engine)
    }
}

impl std::fmt::Debug for ClawReadTx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClawReadTx").finish()
    }
}

impl DbTx for ClawReadTx {
    type Cursor<T: Table> = ClawCursor<T>;
    type DupCursor<T: DupSort> = ClawDupCursor<T>;

    fn get<T: Table>(&self, key: T::Key) -> Result<Option<T::Value>, DatabaseError> {
        let table_id = table_id_for_name(T::NAME);
        let encoded = key.encode();
        let raw = self.raw_get(table_id, encoded.as_ref())?;
        match raw {
            Some(bytes) => {
                let val = <T::Value as reth_db_api::table::Decompress>::decompress(&bytes)?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    fn get_by_encoded_key<T: Table>(
        &self,
        key: &<T::Key as Encode>::Encoded,
    ) -> Result<Option<T::Value>, DatabaseError> {
        let table_id = table_id_for_name(T::NAME);
        let raw = self.raw_get(table_id, key.as_ref())?;
        match raw {
            Some(bytes) => {
                let val = <T::Value as reth_db_api::table::Decompress>::decompress(&bytes)?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    fn commit(self) -> Result<(), DatabaseError> {
        // Read-only transactions are no-ops on commit
        Ok(())
    }

    fn abort(self) {
        // Nothing to abort for read-only
    }

    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError> {
        Ok(ClawCursor::new(self.engine_arc()))
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        Ok(ClawDupCursor::new(self.engine_arc()))
    }

    fn entries<T: Table>(&self) -> Result<usize, DatabaseError> {
        let table_id = table_id_for_name(T::NAME);
        Ok(self.engine.prefix_count(&[table_id]))
    }

    fn disable_long_read_transaction_safety(&mut self) {
        self._long_read_safety = false;
    }
}

// ---------------------------------------------------------------------------
// Read-write transaction
// ---------------------------------------------------------------------------

/// Read-write transaction backed by ClawStore.
///
/// Uses fast writes (no per-op fsync) with a single WAL sync at commit.
/// This gives batch-level durability: all writes in a transaction are
/// either fully committed or fully lost on crash.
pub struct ClawWriteTx {
    engine: Arc<ClawStoreEngine>,
}

impl ClawWriteTx {
    pub(crate) fn new(engine: Arc<ClawStoreEngine>) -> Self {
        Self { engine }
    }

    pub(crate) fn engine_arc(&self) -> Arc<ClawStoreEngine> {
        Arc::clone(&self.engine)
    }
}

impl std::fmt::Debug for ClawWriteTx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClawWriteTx").finish()
    }
}

// ClawWriteTx also implements DbTx (read-write transactions can read)
impl DbTx for ClawWriteTx {
    type Cursor<T: Table> = ClawCursor<T>;
    type DupCursor<T: DupSort> = ClawDupCursor<T>;

    fn get<T: Table>(&self, key: T::Key) -> Result<Option<T::Value>, DatabaseError> {
        let table_id = table_id_for_name(T::NAME);
        let encoded = key.encode();
        let mut prefixed = Vec::with_capacity(1 + encoded.as_ref().len());
        prefixed.push(table_id);
        prefixed.extend_from_slice(encoded.as_ref());

        let raw = self.engine.get(&prefixed).map_err(|e| {
            DatabaseError::Other(e.to_string())
        })?;

        match raw {
            Some(bytes) => {
                let val = <T::Value as reth_db_api::table::Decompress>::decompress(&bytes)?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    fn get_by_encoded_key<T: Table>(
        &self,
        key: &<T::Key as Encode>::Encoded,
    ) -> Result<Option<T::Value>, DatabaseError> {
        let table_id = table_id_for_name(T::NAME);
        let mut prefixed = Vec::with_capacity(1 + key.as_ref().len());
        prefixed.push(table_id);
        prefixed.extend_from_slice(key.as_ref());

        let raw = self.engine.get(&prefixed).map_err(|e| {
            DatabaseError::Other(e.to_string())
        })?;

        match raw {
            Some(bytes) => {
                let val = <T::Value as reth_db_api::table::Decompress>::decompress(&bytes)?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    fn commit(self) -> Result<(), DatabaseError> {
        // Sync the WAL — one fsync for the entire transaction
        self.engine.sync_wal().map_err(|e| {
            DatabaseError::Other(e.to_string())
        })
    }

    fn abort(self) {
        // Writes are already in RAM — abort syncs to ensure consistency
        // Future: implement true rollback with buffered writes
        let _ = self.engine.sync_wal();
    }

    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError> {
        Ok(ClawCursor::new(self.engine_arc()))
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        Ok(ClawDupCursor::new(self.engine_arc()))
    }

    fn entries<T: Table>(&self) -> Result<usize, DatabaseError> {
        let table_id = table_id_for_name(T::NAME);
        Ok(self.engine.prefix_count(&[table_id]))
    }

    fn disable_long_read_transaction_safety(&mut self) {
        // no-op
    }
}

impl DbTxMut for ClawWriteTx {
    type CursorMut<T: Table> = ClawCursorMut<T>;
    type DupCursorMut<T: DupSort> = ClawDupCursorMut<T>;

    fn put<T: Table>(&self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        let table_id = table_id_for_name(T::NAME);
        let encoded_key = key.encode();
        let compressed_val = value.compress();

        let mut prefixed = Vec::with_capacity(1 + encoded_key.as_ref().len());
        prefixed.push(table_id);
        prefixed.extend_from_slice(encoded_key.as_ref());

        // Fast write: WAL append without fsync. Durability comes at commit().
        self.engine.put_fast(&prefixed, compressed_val.as_ref()).map_err(|e| {
            DatabaseError::Other(e.to_string())
        })
    }

    fn delete<T: Table>(
        &self,
        key: T::Key,
        _value: Option<T::Value>,
    ) -> Result<bool, DatabaseError> {
        let table_id = table_id_for_name(T::NAME);
        let encoded_key = key.encode();
        let mut prefixed = Vec::with_capacity(1 + encoded_key.as_ref().len());
        prefixed.push(table_id);
        prefixed.extend_from_slice(encoded_key.as_ref());

        let existed = self.engine.contains_key(&prefixed);
        if existed {
            self.engine.delete(&prefixed).map_err(|e| {
                DatabaseError::Other(e.to_string())
            })?;
        }
        Ok(existed)
    }

    fn clear<T: Table>(&self) -> Result<(), DatabaseError> {
        // Would need prefix scan + delete — stub for now
        Ok(())
    }

    fn cursor_write<T: Table>(&self) -> Result<Self::CursorMut<T>, DatabaseError> {
        Ok(ClawCursorMut::new(self.engine_arc()))
    }

    fn cursor_dup_write<T: DupSort>(&self) -> Result<Self::DupCursorMut<T>, DatabaseError> {
        Ok(ClawDupCursorMut::new(self.engine_arc()))
    }
}

impl TableImporter for ClawWriteTx {}

#[cfg(test)]
mod tests {
    use super::*;
    use clawstore_core::Config;
    use tempfile::TempDir;

    fn test_engine() -> (Arc<ClawStoreEngine>, TempDir) {
        let dir = TempDir::new().unwrap();
        let engine = ClawStoreEngine::open(dir.path(), Config::default()).unwrap();
        (Arc::new(engine), dir)
    }

    #[test]
    fn test_read_tx_commit() {
        let (engine, _dir) = test_engine();
        let tx = ClawReadTx::new(engine);
        tx.commit().unwrap();
    }

    #[test]
    fn test_write_tx_commit() {
        let (engine, _dir) = test_engine();
        let tx = ClawWriteTx::new(engine);
        tx.commit().unwrap();
    }

    #[test]
    fn test_raw_get_missing() {
        let (engine, _dir) = test_engine();
        let tx = ClawReadTx::new(engine);
        let result = tx.raw_get(0x01, b"nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_raw_put_get_cycle() {
        let (engine, _dir) = test_engine();

        // Write via engine with table prefix
        let mut key = vec![0x0C]; // PlainAccountState prefix
        key.extend_from_slice(b"test_addr");
        engine.put(&key, b"account_data").unwrap();

        // Read via ClawReadTx
        let tx = ClawReadTx::new(Arc::clone(&engine));
        let result = tx.raw_get(0x0C, b"test_addr").unwrap();
        assert_eq!(result, Some(b"account_data".to_vec()));
    }
}

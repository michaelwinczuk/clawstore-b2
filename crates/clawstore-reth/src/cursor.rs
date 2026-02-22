//! Cursor implementations for ClawStore.
//!
//! Reth cursors provide ordered iteration over table entries.
//! ClawStore's HashMap is unordered, so cursors build a sorted snapshot
//! of keys for the target table on creation, then iterate over that.
//!
//! This is the trade-off: cursor creation is O(n) where n = entries in table,
//! but individual operations (seek, next, prev) are O(log n) via BTreeMap.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use reth_db_api::{
    common::{PairResult, ValueOnlyResult},
    cursor::{
        DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW,
        Walker, ReverseWalker, RangeWalker, DupWalker,
    },
    table::{Decode, Decompress, DupSort, Encode, Compress, Table},
    DatabaseError,
};

use clawstore_core::ClawStoreEngine;
use crate::table_ids::table_id_for_name;

// ---------------------------------------------------------------------------
// Helper: snapshot table data from engine into a BTreeMap
// ---------------------------------------------------------------------------

/// Build a sorted snapshot of all entries for a given table.
///
/// Scans the engine's HashMap for keys with the table's prefix byte,
/// strips the prefix, and collects into a BTreeMap<Vec<u8>, Vec<u8>>.
fn snapshot_table(engine: &ClawStoreEngine, table_id: u8) -> BTreeMap<Vec<u8>, Vec<u8>> {
    engine.prefix_scan(&[table_id]).into_iter().collect()
}

// ---------------------------------------------------------------------------
// Read-only cursor
// ---------------------------------------------------------------------------

/// Read-only cursor over a Reth table backed by ClawStore.
///
/// Currently a stub that satisfies the trait bounds. Full cursor iteration
/// requires adding prefix_scan to ClawStoreEngine (next development phase).
pub struct ClawCursor<T: Table> {
    /// Sorted snapshot of table data (encoded key bytes -> compressed value bytes)
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    /// Current position in the sorted data (encoded key bytes)
    position: Option<Vec<u8>>,
    _phantom: PhantomData<T>,
}

impl<T: Table> ClawCursor<T> {
    pub(crate) fn new(engine: Arc<ClawStoreEngine>) -> Self {
        let table_id = table_id_for_name(T::NAME);
        let data = snapshot_table(&engine, table_id);
        Self {
            data,
            position: None,
            _phantom: PhantomData,
        }
    }

    /// Decode a key-value pair from raw bytes.
    fn decode_pair(key_bytes: &[u8], val_bytes: &[u8]) -> PairResult<T> {
        let key = <T::Key as Decode>::decode(key_bytes)?;
        let value = <T::Value as Decompress>::decompress(val_bytes)?;
        Ok(Some((key, value)))
    }
}

impl<T: Table> std::fmt::Debug for ClawCursor<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClawCursor")
            .field("table", &T::NAME)
            .field("entries", &self.data.len())
            .field("position", &self.position)
            .finish()
    }
}

impl<T: Table> DbCursorRO<T> for ClawCursor<T> {
    fn first(&mut self) -> PairResult<T> {
        match self.data.iter().next() {
            Some((k, v)) => {
                self.position = Some(k.clone());
                Self::decode_pair(k, v)
            }
            None => Ok(None),
        }
    }

    fn seek_exact(&mut self, key: T::Key) -> PairResult<T> {
        let encoded = key.encode();
        let key_bytes = encoded.as_ref().to_vec();
        match self.data.get(&key_bytes) {
            Some(v) => {
                self.position = Some(key_bytes.clone());
                Self::decode_pair(&key_bytes, v)
            }
            None => Ok(None),
        }
    }

    fn seek(&mut self, key: T::Key) -> PairResult<T> {
        let encoded = key.encode();
        let key_bytes = encoded.as_ref().to_vec();
        // Find first entry >= key
        match self.data.range(key_bytes.clone()..).next() {
            Some((k, v)) => {
                self.position = Some(k.clone());
                Self::decode_pair(k, v)
            }
            None => Ok(None),
        }
    }

    fn next(&mut self) -> PairResult<T> {
        let pos = match &self.position {
            Some(p) => p.clone(),
            None => return self.first(),
        };
        // Find next entry after current position
        match self.data.range((Bound::Excluded(pos), Bound::Unbounded)).next() {
            Some((k, v)) => {
                self.position = Some(k.clone());
                Self::decode_pair(k, v)
            }
            None => Ok(None),
        }
    }

    fn prev(&mut self) -> PairResult<T> {
        let pos = match &self.position {
            Some(p) => p.clone(),
            None => return self.last(),
        };
        match self.data.range(..pos).next_back() {
            Some((k, v)) => {
                self.position = Some(k.clone());
                Self::decode_pair(k, v)
            }
            None => Ok(None),
        }
    }

    fn last(&mut self) -> PairResult<T> {
        match self.data.iter().next_back() {
            Some((k, v)) => {
                self.position = Some(k.clone());
                Self::decode_pair(k, v)
            }
            None => Ok(None),
        }
    }

    fn current(&mut self) -> PairResult<T> {
        let pos = match &self.position {
            Some(p) => p.clone(),
            None => return Ok(None),
        };
        match self.data.get(&pos) {
            Some(v) => Self::decode_pair(&pos, v),
            None => Ok(None),
        }
    }

    fn walk(&mut self, start_key: Option<T::Key>) -> Result<Walker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match start_key {
            Some(key) => self.seek(key).transpose(),
            None => self.first().transpose(),
        };
        Ok(Walker::new(self, start))
    }

    fn walk_range(
        &mut self,
        range: impl RangeBounds<T::Key>,
    ) -> Result<RangeWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match range.start_bound() {
            Bound::Included(key) => self.seek(key.clone()).transpose(),
            Bound::Excluded(key) => {
                // Seek to key, then advance if exact match
                let _ = self.seek(key.clone());
                match self.current()? {
                    Some((k, _)) if k == *key => self.next().transpose(),
                    Some((k, v)) => Some(Ok((k, v))),
                    None => None,
                }
            }
            Bound::Unbounded => self.first().transpose(),
        };

        let end_key = match range.end_bound() {
            Bound::Included(key) => Bound::Included(key.clone()),
            Bound::Excluded(key) => Bound::Excluded(key.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        Ok(RangeWalker::new(self, start, end_key))
    }

    fn walk_back(
        &mut self,
        start_key: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match start_key {
            Some(key) => self.seek(key).transpose(),
            None => self.last().transpose(),
        };
        Ok(ReverseWalker::new(self, start))
    }
}

// ---------------------------------------------------------------------------
// DupSort read-only cursor
// ---------------------------------------------------------------------------

/// Read-only DupSort cursor. Wraps ClawCursor with dup-specific operations.
pub struct ClawDupCursor<T: DupSort> {
    inner: ClawCursor<T>,
}

impl<T: DupSort> ClawDupCursor<T> {
    pub(crate) fn new(engine: Arc<ClawStoreEngine>) -> Self {
        Self {
            inner: ClawCursor::new(engine),
        }
    }
}

impl<T: DupSort> std::fmt::Debug for ClawDupCursor<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClawDupCursor")
            .field("inner", &self.inner)
            .finish()
    }
}

// Forward DbCursorRO to inner cursor
impl<T: DupSort> DbCursorRO<T> for ClawDupCursor<T> {
    fn first(&mut self) -> PairResult<T> { self.inner.first() }
    fn seek_exact(&mut self, key: T::Key) -> PairResult<T> { self.inner.seek_exact(key) }
    fn seek(&mut self, key: T::Key) -> PairResult<T> { self.inner.seek(key) }
    fn next(&mut self) -> PairResult<T> { self.inner.next() }
    fn prev(&mut self) -> PairResult<T> { self.inner.prev() }
    fn last(&mut self) -> PairResult<T> { self.inner.last() }
    fn current(&mut self) -> PairResult<T> { self.inner.current() }

    fn walk(&mut self, start_key: Option<T::Key>) -> Result<Walker<'_, T, Self>, DatabaseError>
    where Self: Sized {
        let start = match start_key {
            Some(key) => self.seek(key).transpose(),
            None => self.first().transpose(),
        };
        Ok(Walker::new(self, start))
    }

    fn walk_range(
        &mut self,
        range: impl RangeBounds<T::Key>,
    ) -> Result<RangeWalker<'_, T, Self>, DatabaseError>
    where Self: Sized {
        let start = match range.start_bound() {
            Bound::Included(key) => self.seek(key.clone()).transpose(),
            Bound::Excluded(key) => {
                let _ = self.seek(key.clone());
                match self.current()? {
                    Some((k, _)) if k == *key => self.next().transpose(),
                    Some((k, v)) => Some(Ok((k, v))),
                    None => None,
                }
            }
            Bound::Unbounded => self.first().transpose(),
        };
        let end_key = match range.end_bound() {
            Bound::Included(key) => Bound::Included(key.clone()),
            Bound::Excluded(key) => Bound::Excluded(key.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(RangeWalker::new(self, start, end_key))
    }

    fn walk_back(
        &mut self,
        start_key: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError>
    where Self: Sized {
        let start = match start_key {
            Some(key) => self.seek(key).transpose(),
            None => self.last().transpose(),
        };
        Ok(ReverseWalker::new(self, start))
    }
}

impl<T: DupSort> DbDupCursorRO<T> for ClawDupCursor<T> {
    fn prev_dup(&mut self) -> PairResult<T> {
        // In a flat KV store, prev_dup is the same as prev
        self.inner.prev()
    }

    fn next_dup(&mut self) -> PairResult<T> {
        self.inner.next()
    }

    fn last_dup(&mut self) -> ValueOnlyResult<T> {
        // Return the value at the last position
        match self.inner.current()? {
            Some((_k, v)) => Ok(Some(v)),
            None => Ok(None),
        }
    }

    fn next_no_dup(&mut self) -> PairResult<T> {
        self.inner.next()
    }

    fn next_dup_val(&mut self) -> ValueOnlyResult<T> {
        match self.inner.next()? {
            Some((_k, v)) => Ok(Some(v)),
            None => Ok(None),
        }
    }

    fn seek_by_key_subkey(&mut self, key: T::Key, _subkey: T::SubKey) -> ValueOnlyResult<T> {
        match self.inner.seek(key)? {
            Some((_k, v)) => Ok(Some(v)),
            None => Ok(None),
        }
    }

    fn walk_dup(
        &mut self,
        key: Option<T::Key>,
        _subkey: Option<T::SubKey>,
    ) -> Result<DupWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match key {
            Some(k) => self.seek_exact(k).transpose(),
            None => self.first().transpose(),
        };
        Ok(DupWalker { cursor: self, start })
    }
}

// ---------------------------------------------------------------------------
// Read-write cursor
// ---------------------------------------------------------------------------

/// Mutable cursor. Wraps ClawCursor and delegates writes to the engine.
pub struct ClawCursorMut<T: Table> {
    inner: ClawCursor<T>,
    engine: Arc<ClawStoreEngine>,
}

impl<T: Table> ClawCursorMut<T> {
    pub(crate) fn new(engine: Arc<ClawStoreEngine>) -> Self {
        let inner = ClawCursor::new(Arc::clone(&engine));
        Self { inner, engine }
    }

    fn write_entry(&self, key_bytes: &[u8], val_bytes: &[u8]) -> Result<(), DatabaseError> {
        let table_id = table_id_for_name(T::NAME);
        let mut prefixed = Vec::with_capacity(1 + key_bytes.len());
        prefixed.push(table_id);
        prefixed.extend_from_slice(key_bytes);
        self.engine.put_fast(&prefixed, val_bytes).map_err(|e| {
            DatabaseError::Other(e.to_string())
        })
    }
}

impl<T: Table> std::fmt::Debug for ClawCursorMut<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClawCursorMut")
            .field("table", &T::NAME)
            .finish()
    }
}

impl<T: Table> DbCursorRO<T> for ClawCursorMut<T> {
    fn first(&mut self) -> PairResult<T> { self.inner.first() }
    fn seek_exact(&mut self, key: T::Key) -> PairResult<T> { self.inner.seek_exact(key) }
    fn seek(&mut self, key: T::Key) -> PairResult<T> { self.inner.seek(key) }
    fn next(&mut self) -> PairResult<T> { self.inner.next() }
    fn prev(&mut self) -> PairResult<T> { self.inner.prev() }
    fn last(&mut self) -> PairResult<T> { self.inner.last() }
    fn current(&mut self) -> PairResult<T> { self.inner.current() }

    fn walk(&mut self, start_key: Option<T::Key>) -> Result<Walker<'_, T, Self>, DatabaseError>
    where Self: Sized {
        let start = match start_key {
            Some(key) => self.seek(key).transpose(),
            None => self.first().transpose(),
        };
        Ok(Walker::new(self, start))
    }

    fn walk_range(
        &mut self,
        range: impl RangeBounds<T::Key>,
    ) -> Result<RangeWalker<'_, T, Self>, DatabaseError>
    where Self: Sized {
        let start = match range.start_bound() {
            Bound::Included(key) => self.seek(key.clone()).transpose(),
            Bound::Excluded(key) => {
                let _ = self.seek(key.clone());
                match self.current()? {
                    Some((k, _)) if k == *key => self.next().transpose(),
                    Some((k, v)) => Some(Ok((k, v))),
                    None => None,
                }
            }
            Bound::Unbounded => self.first().transpose(),
        };
        let end_key = match range.end_bound() {
            Bound::Included(key) => Bound::Included(key.clone()),
            Bound::Excluded(key) => Bound::Excluded(key.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(RangeWalker::new(self, start, end_key))
    }

    fn walk_back(
        &mut self,
        start_key: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError>
    where Self: Sized {
        let start = match start_key {
            Some(key) => self.seek(key).transpose(),
            None => self.last().transpose(),
        };
        Ok(ReverseWalker::new(self, start))
    }
}

impl<T: Table> DbCursorRW<T> for ClawCursorMut<T> {
    fn upsert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        let encoded_key = key.encode();
        let mut compressed_val = <<T::Value as Compress>::Compressed as Default>::default();
        value.compress_to_buf(&mut compressed_val);
        self.write_entry(encoded_key.as_ref(), compressed_val.as_ref())?;
        // Update snapshot
        self.inner.data.insert(
            encoded_key.as_ref().to_vec(),
            compressed_val.as_ref().to_vec(),
        );
        Ok(())
    }

    fn insert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        let encoded_key = key.encode();
        let key_bytes = encoded_key.as_ref().to_vec();
        if self.inner.data.contains_key(&key_bytes) {
            return Err(DatabaseError::Other(
                format!("Key already exists in table {}", T::NAME)
            ));
        }
        let mut compressed_val = <<T::Value as Compress>::Compressed as Default>::default();
        value.compress_to_buf(&mut compressed_val);
        self.write_entry(&key_bytes, compressed_val.as_ref())?;
        self.inner.data.insert(key_bytes, compressed_val.as_ref().to_vec());
        Ok(())
    }

    fn append(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        self.upsert(key, value)
    }

    fn delete_current(&mut self) -> Result<(), DatabaseError> {
        if let Some(pos) = self.inner.position.clone() {
            let table_id = table_id_for_name(T::NAME);
            let mut prefixed = Vec::with_capacity(1 + pos.len());
            prefixed.push(table_id);
            prefixed.extend_from_slice(&pos);
            self.engine.delete(&prefixed).map_err(|e| {
                DatabaseError::Other(e.to_string())
            })?;
            self.inner.data.remove(&pos);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// DupSort read-write cursor
// ---------------------------------------------------------------------------

/// Mutable DupSort cursor.
pub struct ClawDupCursorMut<T: DupSort> {
    inner: ClawCursorMut<T>,
}

impl<T: DupSort> ClawDupCursorMut<T> {
    pub(crate) fn new(engine: Arc<ClawStoreEngine>) -> Self {
        Self {
            inner: ClawCursorMut::new(engine),
        }
    }
}

impl<T: DupSort> std::fmt::Debug for ClawDupCursorMut<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClawDupCursorMut")
            .field("table", &T::NAME)
            .finish()
    }
}

impl<T: DupSort> DbCursorRO<T> for ClawDupCursorMut<T> {
    fn first(&mut self) -> PairResult<T> { self.inner.first() }
    fn seek_exact(&mut self, key: T::Key) -> PairResult<T> { self.inner.seek_exact(key) }
    fn seek(&mut self, key: T::Key) -> PairResult<T> { self.inner.seek(key) }
    fn next(&mut self) -> PairResult<T> { self.inner.next() }
    fn prev(&mut self) -> PairResult<T> { self.inner.prev() }
    fn last(&mut self) -> PairResult<T> { self.inner.last() }
    fn current(&mut self) -> PairResult<T> { self.inner.current() }

    fn walk(&mut self, start_key: Option<T::Key>) -> Result<Walker<'_, T, Self>, DatabaseError>
    where Self: Sized {
        let start = match start_key {
            Some(key) => self.seek(key).transpose(),
            None => self.first().transpose(),
        };
        Ok(Walker::new(self, start))
    }

    fn walk_range(
        &mut self,
        range: impl RangeBounds<T::Key>,
    ) -> Result<RangeWalker<'_, T, Self>, DatabaseError>
    where Self: Sized {
        let start = match range.start_bound() {
            Bound::Included(key) => self.seek(key.clone()).transpose(),
            Bound::Excluded(key) => {
                let _ = self.seek(key.clone());
                match self.current()? {
                    Some((k, _)) if k == *key => self.next().transpose(),
                    Some((k, v)) => Some(Ok((k, v))),
                    None => None,
                }
            }
            Bound::Unbounded => self.first().transpose(),
        };
        let end_key = match range.end_bound() {
            Bound::Included(key) => Bound::Included(key.clone()),
            Bound::Excluded(key) => Bound::Excluded(key.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(RangeWalker::new(self, start, end_key))
    }

    fn walk_back(
        &mut self,
        start_key: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError>
    where Self: Sized {
        let start = match start_key {
            Some(key) => self.seek(key).transpose(),
            None => self.last().transpose(),
        };
        Ok(ReverseWalker::new(self, start))
    }
}

impl<T: DupSort> DbCursorRW<T> for ClawDupCursorMut<T> {
    fn upsert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        self.inner.upsert(key, value)
    }
    fn insert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        self.inner.insert(key, value)
    }
    fn append(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        self.inner.append(key, value)
    }
    fn delete_current(&mut self) -> Result<(), DatabaseError> {
        self.inner.delete_current()
    }
}

impl<T: DupSort> DbDupCursorRO<T> for ClawDupCursorMut<T> {
    fn prev_dup(&mut self) -> PairResult<T> { self.inner.prev() }
    fn next_dup(&mut self) -> PairResult<T> { self.inner.next() }
    fn last_dup(&mut self) -> ValueOnlyResult<T> {
        match self.inner.current()? {
            Some((_k, v)) => Ok(Some(v)),
            None => Ok(None),
        }
    }
    fn next_no_dup(&mut self) -> PairResult<T> { self.inner.next() }
    fn next_dup_val(&mut self) -> ValueOnlyResult<T> {
        match self.inner.next()? {
            Some((_k, v)) => Ok(Some(v)),
            None => Ok(None),
        }
    }
    fn seek_by_key_subkey(&mut self, key: T::Key, _subkey: T::SubKey) -> ValueOnlyResult<T> {
        match self.inner.seek(key)? {
            Some((_k, v)) => Ok(Some(v)),
            None => Ok(None),
        }
    }
    fn walk_dup(
        &mut self,
        key: Option<T::Key>,
        _subkey: Option<T::SubKey>,
    ) -> Result<DupWalker<'_, T, Self>, DatabaseError>
    where Self: Sized {
        let start = match key {
            Some(k) => self.seek_exact(k).transpose(),
            None => self.first().transpose(),
        };
        Ok(DupWalker { cursor: self, start })
    }
}

impl<T: DupSort> DbDupCursorRW<T> for ClawDupCursorMut<T> {
    fn delete_current_duplicates(&mut self) -> Result<(), DatabaseError> {
        self.inner.delete_current()
    }

    fn append_dup(&mut self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        self.inner.upsert(key, &value)
    }
}

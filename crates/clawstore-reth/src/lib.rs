//! ClawStore adapter for Reth
//!
//! Implements Reth's `Database`, `DbTx`, `DbTxMut`, and cursor traits over
//! ClawStore B2's RAM-first key-value engine.
//!
//! # Architecture
//!
//! Reth expects ordered, table-scoped key-value access with cursor iteration.
//! ClawStore provides a flat `HashMap<Vec<u8>, Vec<u8>>` namespace.
//!
//! The bridge works as follows:
//! - Each Reth `Table` is assigned a unique prefix byte (table ID)
//! - Keys are stored as `[table_id][encoded_key]` in ClawStore
//! - Read transactions snapshot data into a `BTreeMap` for ordered cursor iteration
//! - Write transactions buffer changes and flush to ClawStore on commit

pub mod db;
pub mod tx;
pub mod cursor;
pub mod table_ids;

pub use db::ClawDatabase;

//! Integration tests: ClawStore as a Reth database backend.
//!
//! These tests exercise the full Database -> DbTx -> Cursor pipeline
//! using actual Reth table types (CanonicalHeaders, PlainAccountState, etc).

use std::time::Instant;

use alloy_primitives::{Address, B256, U256, address};
use reth_db::tables::{CanonicalHeaders, HeaderNumbers, PlainAccountState};
use reth_db_api::{
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO},
    database::Database,
    transaction::{DbTx, DbTxMut},
};
use reth_primitives_traits::Account;
use tempfile::TempDir;

use clawstore_core::Config;
use clawstore_reth::ClawDatabase;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_db() -> (ClawDatabase, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = ClawDatabase::open(dir.path(), Config::default()).unwrap();
    (db, dir)
}

// ---------------------------------------------------------------------------
// Basic Table Operations
// ---------------------------------------------------------------------------

#[test]
fn test_canonical_headers_put_get() {
    let (db, _dir) = test_db();

    // Write: block 0 -> genesis hash
    let genesis_hash = B256::repeat_byte(0xAA);
    let tx = db.tx_mut().unwrap();
    tx.put::<CanonicalHeaders>(0u64, genesis_hash).unwrap();
    tx.put::<CanonicalHeaders>(1u64, B256::repeat_byte(0xBB)).unwrap();
    tx.put::<CanonicalHeaders>(2u64, B256::repeat_byte(0xCC)).unwrap();
    tx.commit().unwrap();

    // Read
    let tx = db.tx().unwrap();
    let val = tx.get::<CanonicalHeaders>(0u64).unwrap();
    assert_eq!(val, Some(genesis_hash));

    let val = tx.get::<CanonicalHeaders>(1u64).unwrap();
    assert_eq!(val, Some(B256::repeat_byte(0xBB)));

    let val = tx.get::<CanonicalHeaders>(2u64).unwrap();
    assert_eq!(val, Some(B256::repeat_byte(0xCC)));

    // Missing key
    let val = tx.get::<CanonicalHeaders>(999u64).unwrap();
    assert_eq!(val, None);
}

#[test]
fn test_header_numbers_put_get() {
    let (db, _dir) = test_db();

    let hash = B256::repeat_byte(0xDE);
    let tx = db.tx_mut().unwrap();
    tx.put::<HeaderNumbers>(hash, 42u64).unwrap();
    tx.commit().unwrap();

    let tx = db.tx().unwrap();
    let val = tx.get::<HeaderNumbers>(hash).unwrap();
    assert_eq!(val, Some(42u64));
}

#[test]
fn test_plain_account_state() {
    let (db, _dir) = test_db();

    let addr = address!("d8dA6BF26964aF9D7eEd9e03E53415D37aA96045"); // vitalik.eth
    let account = Account {
        nonce: 42,
        balance: U256::from(1_000_000_000_000_000_000u128), // 1 ETH
        bytecode_hash: None,
    };

    let tx = db.tx_mut().unwrap();
    tx.put::<PlainAccountState>(addr, account).unwrap();
    tx.commit().unwrap();

    let tx = db.tx().unwrap();
    let retrieved = tx.get::<PlainAccountState>(addr).unwrap().unwrap();
    assert_eq!(retrieved.nonce, 42);
    assert_eq!(retrieved.balance, U256::from(1_000_000_000_000_000_000u128));
    assert_eq!(retrieved.bytecode_hash, None);
}

// ---------------------------------------------------------------------------
// Table Isolation
// ---------------------------------------------------------------------------

#[test]
fn test_table_isolation() {
    let (db, _dir) = test_db();

    // Put different data in different tables
    let tx = db.tx_mut().unwrap();
    tx.put::<CanonicalHeaders>(0u64, B256::repeat_byte(0x11)).unwrap();
    tx.put::<CanonicalHeaders>(1u64, B256::repeat_byte(0x22)).unwrap();
    tx.commit().unwrap();

    // Different table, same key type — should not collide
    let tx = db.tx_mut().unwrap();
    tx.put::<HeaderNumbers>(B256::repeat_byte(0x11), 100u64).unwrap();
    tx.commit().unwrap();

    // Verify isolation
    let tx = db.tx().unwrap();
    let entries_ch = tx.entries::<CanonicalHeaders>().unwrap();
    let entries_hn = tx.entries::<HeaderNumbers>().unwrap();
    assert_eq!(entries_ch, 2);
    assert_eq!(entries_hn, 1);
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

#[test]
fn test_delete() {
    let (db, _dir) = test_db();

    let tx = db.tx_mut().unwrap();
    tx.put::<CanonicalHeaders>(0u64, B256::ZERO).unwrap();
    tx.put::<CanonicalHeaders>(1u64, B256::ZERO).unwrap();
    tx.commit().unwrap();

    // Delete block 0
    let tx = db.tx_mut().unwrap();
    let deleted = tx.delete::<CanonicalHeaders>(0u64, None).unwrap();
    assert!(deleted);
    tx.commit().unwrap();

    // Verify gone
    let tx = db.tx().unwrap();
    assert_eq!(tx.get::<CanonicalHeaders>(0u64).unwrap(), None);
    assert_eq!(tx.get::<CanonicalHeaders>(1u64).unwrap(), Some(B256::ZERO));

    // Delete non-existent
    let tx = db.tx_mut().unwrap();
    let deleted = tx.delete::<CanonicalHeaders>(999u64, None).unwrap();
    assert!(!deleted);
}

// ---------------------------------------------------------------------------
// Cursor Operations
// ---------------------------------------------------------------------------

#[test]
fn test_cursor_walk() {
    let (db, _dir) = test_db();

    // Insert 100 canonical headers
    let tx = db.tx_mut().unwrap();
    for i in 0u64..100 {
        tx.put::<CanonicalHeaders>(i, B256::from(U256::from(i))).unwrap();
    }
    tx.commit().unwrap();

    // Walk all entries
    let tx = db.tx().unwrap();
    let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();
    let mut count = 0u64;
    let mut walker = cursor.walk(None).unwrap();
    while let Some(Ok((k, _v))) = walker.next() {
        assert_eq!(k, count, "Keys should be in sorted order");
        count += 1;
    }
    assert_eq!(count, 100);
}

#[test]
fn test_cursor_seek() {
    let (db, _dir) = test_db();

    let tx = db.tx_mut().unwrap();
    for i in (0u64..10).step_by(2) {
        // 0, 2, 4, 6, 8
        tx.put::<CanonicalHeaders>(i, B256::from(U256::from(i))).unwrap();
    }
    tx.commit().unwrap();

    let tx = db.tx().unwrap();
    let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();

    // Seek exact
    let result = cursor.seek_exact(4u64).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().0, 4u64);

    // Seek exact miss
    let result = cursor.seek_exact(3u64).unwrap();
    assert!(result.is_none());

    // Seek (>=) — seeking 3 should land on 4
    let result = cursor.seek(3u64).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().0, 4u64);

    // First / Last
    let first = cursor.first().unwrap().unwrap();
    assert_eq!(first.0, 0u64);
    let last = cursor.last().unwrap().unwrap();
    assert_eq!(last.0, 8u64);
}

#[test]
fn test_cursor_prev_next() {
    let (db, _dir) = test_db();

    let tx = db.tx_mut().unwrap();
    for i in 0u64..5 {
        tx.put::<CanonicalHeaders>(i, B256::ZERO).unwrap();
    }
    tx.commit().unwrap();

    let tx = db.tx().unwrap();
    let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();

    // Navigate forward
    let first = cursor.first().unwrap().unwrap();
    assert_eq!(first.0, 0u64);
    let second = cursor.next().unwrap().unwrap();
    assert_eq!(second.0, 1u64);
    let third = cursor.next().unwrap().unwrap();
    assert_eq!(third.0, 2u64);

    // Navigate backward
    let back = cursor.prev().unwrap().unwrap();
    assert_eq!(back.0, 1u64);
    let back = cursor.prev().unwrap().unwrap();
    assert_eq!(back.0, 0u64);

    // Prev at beginning
    let at_start = cursor.prev().unwrap();
    assert!(at_start.is_none());
}

#[test]
fn test_cursor_walk_range() {
    let (db, _dir) = test_db();

    let tx = db.tx_mut().unwrap();
    for i in 0u64..20 {
        tx.put::<CanonicalHeaders>(i, B256::from(U256::from(i))).unwrap();
    }
    tx.commit().unwrap();

    let tx = db.tx().unwrap();
    let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();

    // Walk range [5..=10]
    let mut count = 0;
    let mut walker = cursor.walk_range(5u64..=10u64).unwrap();
    while let Some(Ok((k, _v))) = walker.next() {
        assert!(k >= 5 && k <= 10, "Key {} out of range", k);
        count += 1;
    }
    assert_eq!(count, 6); // 5, 6, 7, 8, 9, 10
}

// ---------------------------------------------------------------------------
// Cursor Write Operations
// ---------------------------------------------------------------------------

#[test]
fn test_cursor_upsert() {
    let (db, _dir) = test_db();

    let tx = db.tx_mut().unwrap();
    let mut cursor = tx.cursor_write::<CanonicalHeaders>().unwrap();

    // Insert via upsert
    cursor.upsert(0u64, &B256::repeat_byte(0x01)).unwrap();
    cursor.upsert(1u64, &B256::repeat_byte(0x02)).unwrap();

    // Overwrite via upsert
    cursor.upsert(0u64, &B256::repeat_byte(0xFF)).unwrap();

    tx.commit().unwrap();

    // Verify the overwrite
    let tx = db.tx().unwrap();
    let val = tx.get::<CanonicalHeaders>(0u64).unwrap().unwrap();
    assert_eq!(val, B256::repeat_byte(0xFF));
}

// ---------------------------------------------------------------------------
// Account State Round-Trip (Complex Type)
// ---------------------------------------------------------------------------

#[test]
fn test_account_state_roundtrip_many() {
    let (db, _dir) = test_db();
    let start = Instant::now();

    let count = 1000u64;
    let tx = db.tx_mut().unwrap();
    for i in 0..count {
        // Generate deterministic addresses
        let addr_bytes = B256::from(U256::from(i));
        let addr = Address::from_slice(&addr_bytes.as_slice()[12..]);
        let account = Account {
            nonce: i,
            balance: U256::from(i * 1_000_000_000),
            bytecode_hash: if i % 10 == 0 { Some(B256::repeat_byte(i as u8)) } else { None },
        };
        tx.put::<PlainAccountState>(addr, account).unwrap();
    }
    tx.commit().unwrap();
    let write_time = start.elapsed();

    // Read all back
    let start = Instant::now();
    let tx = db.tx().unwrap();
    for i in 0..count {
        let addr_bytes = B256::from(U256::from(i));
        let addr = Address::from_slice(&addr_bytes.as_slice()[12..]);
        let account = tx.get::<PlainAccountState>(addr).unwrap().unwrap();
        assert_eq!(account.nonce, i);
    }
    let read_time = start.elapsed();

    println!("\n=== Account State Round-Trip ===");
    println!("  Write {} accounts: {:?} ({:.0} accounts/sec)",
        count, write_time, count as f64 / write_time.as_secs_f64());
    println!("  Read  {} accounts: {:?} ({:.0} accounts/sec)",
        count, read_time, count as f64 / read_time.as_secs_f64());
    println!("  Write latency: {:.2} µs/op", write_time.as_micros() as f64 / count as f64);
    println!("  Read  latency: {:.2} µs/op", read_time.as_micros() as f64 / count as f64);
}

// ---------------------------------------------------------------------------
// Bulk Load Benchmark
// ---------------------------------------------------------------------------

#[test]
fn test_bulk_canonical_headers() {
    let (db, _dir) = test_db();
    let block_count = 10_000u64;

    // Bulk write
    let start = Instant::now();
    let tx = db.tx_mut().unwrap();
    for i in 0..block_count {
        tx.put::<CanonicalHeaders>(i, B256::from(U256::from(i))).unwrap();
    }
    tx.commit().unwrap();
    let write_time = start.elapsed();

    // Bulk read
    let start = Instant::now();
    let tx = db.tx().unwrap();
    for i in 0..block_count {
        let val = tx.get::<CanonicalHeaders>(i).unwrap();
        assert!(val.is_some());
    }
    let read_time = start.elapsed();

    // Cursor walk
    let start = Instant::now();
    let tx = db.tx().unwrap();
    let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();
    let mut count = 0u64;
    let mut walker = cursor.walk(None).unwrap();
    while let Some(Ok(_)) = walker.next() {
        count += 1;
    }
    let walk_time = start.elapsed();
    assert_eq!(count, block_count);

    // Entries count
    let tx = db.tx().unwrap();
    let entries = tx.entries::<CanonicalHeaders>().unwrap();
    assert_eq!(entries, block_count as usize);

    println!("\n=== Bulk Canonical Headers ({} blocks) ===", block_count);
    println!("  Write:  {:?} ({:.0} blocks/sec, {:.2} µs/op)",
        write_time,
        block_count as f64 / write_time.as_secs_f64(),
        write_time.as_micros() as f64 / block_count as f64);
    println!("  Read:   {:?} ({:.0} blocks/sec, {:.2} µs/op)",
        read_time,
        block_count as f64 / read_time.as_secs_f64(),
        read_time.as_micros() as f64 / block_count as f64);
    println!("  Walk:   {:?} ({:.0} entries/sec, {:.2} µs/op)",
        walk_time,
        block_count as f64 / walk_time.as_secs_f64(),
        walk_time.as_micros() as f64 / block_count as f64);
    println!("  Entries: {}", entries);
}

// ---------------------------------------------------------------------------
// Overwrite / Update Pattern
// ---------------------------------------------------------------------------

#[test]
fn test_state_overwrite() {
    let (db, _dir) = test_db();
    let addr = address!("0000000000000000000000000000000000000001");

    // Initial write
    let tx = db.tx_mut().unwrap();
    tx.put::<PlainAccountState>(addr, Account {
        nonce: 0,
        balance: U256::from(100),
        bytecode_hash: None,
    }).unwrap();
    tx.commit().unwrap();

    // Overwrite (simulating a state transition)
    let tx = db.tx_mut().unwrap();
    tx.put::<PlainAccountState>(addr, Account {
        nonce: 1,
        balance: U256::from(90),
        bytecode_hash: None,
    }).unwrap();
    tx.commit().unwrap();

    // Verify latest state
    let tx = db.tx().unwrap();
    let account = tx.get::<PlainAccountState>(addr).unwrap().unwrap();
    assert_eq!(account.nonce, 1);
    assert_eq!(account.balance, U256::from(90));
}

//! Benchmarks: ClawStore as a Reth database backend.
//!
//! Measures throughput and latency for typical Reth database operations.

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, BatchSize};

use alloy_primitives::{Address, B256, U256};
use reth_db::tables::{CanonicalHeaders, PlainAccountState};
use reth_db_api::{
    cursor::DbCursorRO,
    database::Database,
    transaction::{DbTx, DbTxMut},
};
use reth_primitives_traits::Account;
use tempfile::TempDir;

use clawstore_core::Config;
use clawstore_reth::ClawDatabase;

fn test_db() -> (ClawDatabase, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = ClawDatabase::open(dir.path(), Config::default()).unwrap();
    (db, dir)
}

// ---------------------------------------------------------------------------
// Point reads / writes
// ---------------------------------------------------------------------------

fn bench_put_canonical_header(c: &mut Criterion) {
    let (db, _dir) = test_db();
    let mut i = 0u64;

    c.bench_function("put_canonical_header", |b| {
        b.iter(|| {
            let tx = db.tx_mut().unwrap();
            tx.put::<CanonicalHeaders>(i, B256::from(U256::from(i))).unwrap();
            tx.commit().unwrap();
            i += 1;
        })
    });
}

fn bench_get_canonical_header(c: &mut Criterion) {
    let (db, _dir) = test_db();
    let count = 10_000u64;

    // Pre-fill
    let tx = db.tx_mut().unwrap();
    for i in 0..count {
        tx.put::<CanonicalHeaders>(i, B256::from(U256::from(i))).unwrap();
    }
    tx.commit().unwrap();

    let mut i = 0u64;
    c.bench_function("get_canonical_header/10k", |b| {
        b.iter(|| {
            let tx = db.tx().unwrap();
            let _ = tx.get::<CanonicalHeaders>(i % count).unwrap();
            tx.commit().unwrap();
            i += 1;
        })
    });
}

fn bench_put_account_state(c: &mut Criterion) {
    let (db, _dir) = test_db();
    let mut i = 0u64;

    c.bench_function("put_account_state", |b| {
        b.iter(|| {
            let addr_bytes = B256::from(U256::from(i));
            let addr = Address::from_slice(&addr_bytes.as_slice()[12..]);
            let account = Account {
                nonce: i,
                balance: U256::from(i * 1_000_000_000),
                bytecode_hash: None,
            };
            let tx = db.tx_mut().unwrap();
            tx.put::<PlainAccountState>(addr, account).unwrap();
            tx.commit().unwrap();
            i += 1;
        })
    });
}

fn bench_get_account_state(c: &mut Criterion) {
    let (db, _dir) = test_db();
    let count = 10_000u64;

    // Pre-fill
    let tx = db.tx_mut().unwrap();
    for i in 0..count {
        let addr_bytes = B256::from(U256::from(i));
        let addr = Address::from_slice(&addr_bytes.as_slice()[12..]);
        let account = Account {
            nonce: i,
            balance: U256::from(i * 1_000_000_000),
            bytecode_hash: None,
        };
        tx.put::<PlainAccountState>(addr, account).unwrap();
    }
    tx.commit().unwrap();

    let mut i = 0u64;
    c.bench_function("get_account_state/10k", |b| {
        b.iter(|| {
            let idx = i % count;
            let addr_bytes = B256::from(U256::from(idx));
            let addr = Address::from_slice(&addr_bytes.as_slice()[12..]);
            let tx = db.tx().unwrap();
            let _ = tx.get::<PlainAccountState>(addr).unwrap();
            tx.commit().unwrap();
            i += 1;
        })
    });
}

// ---------------------------------------------------------------------------
// Batch operations
// ---------------------------------------------------------------------------

fn bench_batch_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_write");
    group.sample_size(10); // Reduce from 100 — these are slow

    for size in [100, 1_000, 10_000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    let dir = TempDir::new().unwrap();
                    let db = ClawDatabase::open(dir.path(), Config::default()).unwrap();
                    (db, dir)
                },
                |(db, _dir)| {
                    let tx = db.tx_mut().unwrap();
                    for i in 0..size as u64 {
                        tx.put::<CanonicalHeaders>(i, B256::from(U256::from(i))).unwrap();
                    }
                    tx.commit().unwrap();
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_batch_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_read");
    group.sample_size(10);

    for size in [100, 1_000, 10_000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    let dir = TempDir::new().unwrap();
                    let db = ClawDatabase::open(dir.path(), Config::default()).unwrap();
                    let tx = db.tx_mut().unwrap();
                    for i in 0..size as u64 {
                        tx.put::<CanonicalHeaders>(i, B256::from(U256::from(i))).unwrap();
                    }
                    tx.commit().unwrap();
                    (db, dir)
                },
                |(db, _dir)| {
                    let tx = db.tx().unwrap();
                    for i in 0..size as u64 {
                        let _ = tx.get::<CanonicalHeaders>(i).unwrap();
                    }
                    tx.commit().unwrap();
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Cursor operations
// ---------------------------------------------------------------------------

fn bench_cursor_walk(c: &mut Criterion) {
    let mut group = c.benchmark_group("cursor_walk");
    group.sample_size(20);

    for size in [1_000, 10_000].iter() {
        let (db, _dir) = test_db();
        let tx = db.tx_mut().unwrap();
        for i in 0..*size as u64 {
            tx.put::<CanonicalHeaders>(i, B256::from(U256::from(i))).unwrap();
        }
        tx.commit().unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &_size| {
            b.iter(|| {
                let tx = db.tx().unwrap();
                let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();
                let mut count = 0u64;
                let mut walker = cursor.walk(None).unwrap();
                while let Some(Ok(_)) = walker.next() {
                    count += 1;
                }
                count
            })
        });
    }
    group.finish();
}

fn bench_cursor_seek(c: &mut Criterion) {
    let (db, _dir) = test_db();
    let count = 10_000u64;

    let tx = db.tx_mut().unwrap();
    for i in 0..count {
        tx.put::<CanonicalHeaders>(i, B256::from(U256::from(i))).unwrap();
    }
    tx.commit().unwrap();

    let mut i = 0u64;
    c.bench_function("cursor_seek/10k", |b| {
        b.iter(|| {
            let tx = db.tx().unwrap();
            let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();
            let _ = cursor.seek(i % count).unwrap();
            tx.commit().unwrap();
            i += 1;
        })
    });
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_put_canonical_header,
    bench_get_canonical_header,
    bench_put_account_state,
    bench_get_account_state,
    bench_batch_write,
    bench_batch_read,
    bench_cursor_walk,
    bench_cursor_seek,
);
criterion_main!(benches);

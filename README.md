# ClawStore B2

**RAM-first storage engine for Ethereum execution clients.**

ClawStore is a high-performance key-value storage engine designed to replace MDBX as the database backend for [Reth](https://github.com/paradigmxyz/reth). It uses a RAM-first architecture where all reads are served directly from an in-memory hash table, achieving sub-100 nanosecond read latency with full crash safety via a write-ahead log.

## Performance

Benchmarked on Apple M-series silicon via Criterion, using real Reth table types (`CanonicalHeaders`, `PlainAccountState`) through the full encode/compress pipeline:

| Operation | Latency | Throughput |
|-----------|---------|------------|
| Point read (block header) | **59 ns** | 16.9M ops/sec |
| Point read (account state) | **81 ns** | 12.4M ops/sec |
| Batch read 10K entries | 135 ns/op | 7.4M ops/sec |
| Cursor walk 10K entries | 224 ns/entry | 4.5M entries/sec |
| Batch write 10K entries | 2.6 µs/op | 382K ops/sec |
| Single durable write | 4.1 ms | 244 ops/sec |

Write throughput scales with batch size — a single `F_FULLFSYNC` commits the entire transaction:

| Batch Size | Total Time | Per-Write Latency | Speedup vs Single |
|------------|-----------|-------------------|-------------------|
| 1 | 4.1 ms | 4.1 ms | 1x |
| 100 | 6.1 ms | 61 µs | 67x |
| 1,000 | 7.7 ms | 7.7 µs | 532x |
| 10,000 | 26 ms | 2.6 µs | 1,577x |

## Architecture

```
┌─────────────────────────────────────────┐
│              Reth Node                  │
│  ┌───────────────────────────────────┐  │
│  │        clawstore-reth             │  │
│  │  Database · DbTx · DbTxMut       │  │
│  │  Cursors · TableImporter          │  │
│  └──────────────┬────────────────────┘  │
│  ┌──────────────▼────────────────────┐  │
│  │        clawstore-core             │  │
│  │  ┌────────┐ ┌─────┐ ┌─────────┐  │  │
│  │  │  RAM   │ │ WAL │ │ Trickle │  │  │
│  │  │HashMap │ │F_FULL│ │  Flush  │  │  │
│  │  │        │ │FSYNC │ │         │  │  │
│  │  └────────┘ └─────┘ └─────────┘  │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
```

**Read path**: Direct HashMap lookup → sub-100ns, no B-tree traversal, no page cache, no mmap.

**Write path**: WAL append (fast) → RAM insert → dirty mark → single fsync at transaction commit.

**Trickle engine**: Background thread flushes dirty entries from RAM to sorted data files during idle periods. Configurable cadence.

**Compaction**: Merges data files to reclaim dead space from overwrites and deletes.

## Crate Structure

| Crate | Purpose |
|-------|---------|
| `clawstore-core` | Storage engine with zero blockchain dependencies. Usable for any key-value workload. |
| `clawstore-reth` | Reth adapter implementing `Database`, `DbTx`, `DbTxMut`, and all cursor traits against Reth v1.11.0. |

## Quick Start

```rust
use clawstore_core::{ClawStoreEngine, Config};

let engine = ClawStoreEngine::open("./my-db", Config::default())?;

// Write (WAL-first, crash-safe)
engine.put(b"key", b"value")?;

// Read (sub-100ns from RAM)
let val = engine.get(b"key")?;
assert_eq!(val, Some(b"value".to_vec()));

// Start background trickle flush
engine.start_trickle()?;
```

### As a Reth Backend

```rust
use clawstore_reth::ClawDatabase;
use reth_db::tables::CanonicalHeaders;
use reth_db_api::{database::Database, transaction::{DbTx, DbTxMut}};

let db = ClawDatabase::open("./reth-db", Config::default())?;

// Write through Reth's typed table API
let tx = db.tx_mut()?;
tx.put::<CanonicalHeaders>(0u64, block_hash)?;
tx.commit()?; // Single fsync for the entire transaction

// Read
let tx = db.tx()?;
let hash = tx.get::<CanonicalHeaders>(0u64)?;
```

## Building

```bash
# Run all tests (69 tests)
cargo test --workspace

# Run with performance output
cargo test --workspace -- --nocapture

# Run Criterion benchmarks
cargo bench -p clawstore-reth
```

Requires Rust 1.93+ (nightly features used by Reth dependencies).

## Crash Safety

ClawStore provides transaction-level durability:

- Every write is appended to the WAL before updating RAM
- `commit()` calls `F_FULLFSYNC` (macOS) / `fdatasync` (Linux) — a single hardware flush for the entire transaction
- On crash recovery, the WAL is replayed to restore the RAM working set
- CRC32C checksums on every WAL entry detect corruption

## Test Suite

- **48 tests** in `clawstore-core`: engine operations, WAL replay, crash recovery, trickle integration, compaction, prefix scan
- **8 tests** in `clawstore-reth`: adapter unit tests for Database/DbTx/cursor creation
- **13 tests** in integration: real Reth table types (CanonicalHeaders, HeaderNumbers, PlainAccountState), cursor walks, seeks, ranges, bulk operations

## Status

This is an early-stage project. Current state:

- [x] Core storage engine (RAM + WAL + trickle + compaction)
- [x] Reth Database/DbTx/DbTxMut trait implementation
- [x] All cursor traits (RO, RW, DupSort)
- [x] Integration tests with real Reth table types
- [x] Criterion benchmarks
- [ ] MVCC (multi-version concurrency control)
- [ ] WAL truncation after trickle flush
- [ ] Reth node integration test (full sync)
- [ ] Production hardening

## Origin

Built by [OpenClaw](https://github.com/OpenClaw) — a multi-agent AI development pipeline. ClawStore B2 was developed in ~8 hours of human-AI collaboration at a total API cost of $0.30.

## License

MIT OR Apache-2.0

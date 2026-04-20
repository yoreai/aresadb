//! v2 cluster benchmarks — the alpha.2 distributed stack.
//!
//! This file is the v2 counterpart of the legacy `distributed_bench.rs`
//! suite. Where that file measures v0.2-era pure data-structure helpers
//! (`BloomFilter`, `Compressor`, `ShardManager`), this one measures the
//! new distributed components that actually ship in `v2.0.0-alpha.2`:
//!
//! 1. **Single-node Raft apply loop** — [`aresadb_raft::SingleNode`]
//!    end-to-end throughput for both single-key writes and
//!    multi-key batches. One-voter, in-memory backends; the only cost
//!    is openraft's log-append / apply pipeline plus the loopback
//!    network. This is the number we quote as "best-case Raft apply
//!    latency" — add replication + fsync on top for a real multi-node
//!    cluster. The batched variant is what pulls the per-key cost down
//!    toward the storage layer's floor.
//!
//! 2. **Range backend: redb vs fjall** — side-by-side point puts,
//!    group commits, warm point gets, and short prefix scans against
//!    [`aresadb_engine_redb::RedbBackend`] and
//!    [`aresadb_engine_lsm::FjallBackend`], each opened on its own
//!    `TempDir` so the numbers reflect on-disk fsync behaviour. This is
//!    what a `RangeRuntime` pays per logical write, once the Raft
//!    layer has agreed on the value.
//!
//! The micro-bench tracks are designed so the LSM vs B-tree delta
//! surfaces where it matters: `put_batched` amortises one fsync over
//! many keys (LSM's group-commit sweet spot), and `scan_range` exercises
//! ordered iteration (where fjall's SSTables beat redb's page walks at
//! larger working sets). See `docs/publishing-audit.md` §4a for the
//! takeaways the scaffold already confirms, plus the follow-up tracks
//! (3-node gRPC apply, leader failover, range create) that require the
//! full `aresadb-cluster` harness.
//!
//! ## Running
//!
//! ```ignore
//! # Smoke test (fast, ~5s):
//! cargo bench --bench v2_cluster_bench -- --sample-size=10
//!
//! # Full run (~2–5 min, publication-grade numbers):
//! cargo bench --bench v2_cluster_bench
//! ```

use std::sync::Arc;

use aresadb_core::{StorageBackend, WriteBatch};
use aresadb_engine_lsm::FjallBackend;
use aresadb_engine_redb::RedbBackend;
use aresadb_raft::SingleNode;
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};

/// Multi-thread runtime: openraft spawns long-lived background tasks
/// (the raft core, the log replication loop) so a `current_thread`
/// runtime would serialise work that is meant to run in parallel and
/// skew the numbers low.
fn rt() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build tokio runtime")
}

// ============================================================================
// Raft apply loop — SingleNode::in_memory()
// ============================================================================

fn bench_raft_apply_single_node(c: &mut Criterion) {
    let mut group = c.benchmark_group("v2/raft/apply_single_node");
    // One 64-byte key + one 256-byte value per client_write so
    // Throughput::Bytes is meaningful — dominated by Raft apply, not
    // payload copying.
    let value = Bytes::from(vec![0xABu8; 256]);
    group.throughput(Throughput::Bytes((64 + 256) as u64));

    group.bench_function("put_one", |b| {
        let runtime = rt();
        let node = runtime
            .block_on(SingleNode::in_memory())
            .expect("spin up single-node raft");
        let mut counter: u64 = 0;

        b.to_async(&runtime).iter(|| {
            let node = &node;
            counter = counter.wrapping_add(1);
            let key = format!("bench/key/{counter:016x}");
            let value = value.clone();
            async move {
                let mut batch = WriteBatch::new();
                batch.put(key, value);
                node.write(batch).await.expect("client_write")
            }
        });

        // Wind down the raft task so the runtime can drop cleanly.
        runtime.block_on(async {
            let _ = node.raft.shutdown().await;
        });
    });

    // Batched path — one openraft `client_write` carrying N keys.
    // The point is to show the per-key cost once the log-append /
    // apply-loop overhead is amortised. The bench measures per
    // *iteration* (i.e. per batch), so throughput at batch=N is
    // `N / per-iter-time`.
    for batch_size in [16usize, 128].iter() {
        group.throughput(Throughput::Bytes((*batch_size as u64) * (64 + 256) as u64));
        group.bench_with_input(
            BenchmarkId::new("put_batched", batch_size),
            batch_size,
            |b, &batch_size| {
                let runtime = rt();
                let node = runtime
                    .block_on(SingleNode::in_memory())
                    .expect("spin up single-node raft");
                let value = value.clone();
                let mut counter: u64 = 0;

                b.to_async(&runtime).iter(|| {
                    let node = &node;
                    let base = counter;
                    counter = counter.wrapping_add(batch_size as u64);
                    let value = value.clone();
                    async move {
                        let mut batch = WriteBatch::new();
                        for i in 0..batch_size as u64 {
                            batch.put(
                                format!("bench/batch/{:016x}", base.wrapping_add(i)),
                                value.clone(),
                            );
                        }
                        node.write(batch).await.expect("client_write")
                    }
                });

                runtime.block_on(async {
                    let _ = node.raft.shutdown().await;
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Range backend: redb vs fjall (single put/get, on-disk, fsync included)
// ============================================================================

/// Shared scaffolding for a backend bench: keep the `TempDir` alive
/// for the whole benchmark so the `Database` handle it holds doesn't
/// race with the OS reclaiming the directory.
struct BackendFixture<B> {
    backend: Arc<B>,
    #[allow(dead_code)]
    tmp: TempDir,
}

fn open_redb(runtime: &Runtime) -> BackendFixture<RedbBackend> {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("bench.redb");
    let backend = runtime
        .block_on(RedbBackend::open(path))
        .expect("open redb");
    BackendFixture { backend, tmp }
}

fn open_fjall(runtime: &Runtime) -> BackendFixture<FjallBackend> {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("bench.fjall");
    let backend = runtime
        .block_on(FjallBackend::open(path))
        .expect("open fjall");
    BackendFixture { backend, tmp }
}

fn bench_engine_backends(c: &mut Criterion) {
    let mut group = c.benchmark_group("v2/engine/backend");
    let value = Bytes::from(vec![0x42u8; 256]);
    group.throughput(Throughput::Bytes((32 + 256) as u64));

    // --- put ---------------------------------------------------------
    for engine in ["redb", "fjall"].iter() {
        group.bench_with_input(BenchmarkId::new("put", engine), engine, |b, &engine| {
            let runtime = rt();
            let backend: Arc<dyn StorageBackend> = match engine {
                "redb" => open_redb(&runtime).backend,
                "fjall" => open_fjall(&runtime).backend,
                other => panic!("unexpected engine label {other}"),
            };
            let mut counter: u64 = 0;

            b.to_async(&runtime).iter(|| {
                let backend = backend.clone();
                counter = counter.wrapping_add(1);
                let key = format!("bench/put/{counter:016x}");
                let value = value.clone();
                async move {
                    let mut batch = WriteBatch::new();
                    batch.put(key, value);
                    backend.write_batch(batch).await.expect("write_batch")
                }
            });
        });
    }

    // --- put (batched, 64 keys per commit) ---------------------------
    //
    // This is the track where LSM engines typically pull ahead: one
    // journal fsync amortises across many keys, versus the redb
    // B-tree which still pays one page commit per write_batch (though
    // one fsync).
    const BATCH_SIZE: usize = 64;
    group.throughput(Throughput::Bytes((BATCH_SIZE as u64) * (32 + 256) as u64));
    for engine in ["redb", "fjall"].iter() {
        group.bench_with_input(
            BenchmarkId::new("put_batched", engine),
            engine,
            |b, &engine| {
                let runtime = rt();
                let backend: Arc<dyn StorageBackend> = match engine {
                    "redb" => open_redb(&runtime).backend,
                    "fjall" => open_fjall(&runtime).backend,
                    other => panic!("unexpected engine label {other}"),
                };
                let value = value.clone();
                let mut counter: u64 = 0;

                b.to_async(&runtime).iter(|| {
                    let backend = backend.clone();
                    let base = counter;
                    counter = counter.wrapping_add(BATCH_SIZE as u64);
                    let value = value.clone();
                    async move {
                        let mut batch = WriteBatch::new();
                        for i in 0..BATCH_SIZE as u64 {
                            batch.put(
                                format!("bench/bput/{:016x}", base.wrapping_add(i)),
                                value.clone(),
                            );
                        }
                        backend.write_batch(batch).await.expect("write_batch")
                    }
                });
            },
        );
    }

    // Restore single-key throughput labelling for the following tracks.
    group.throughput(Throughput::Bytes((32 + 256) as u64));

    // --- get (warm cache) --------------------------------------------
    //
    // 100 keys loaded once, then random gets. Fjall + redb both have
    // in-process caches for hot pages, so this captures the "steady
    // state" read path — no I/O once the working set fits in RAM.
    for engine in ["redb", "fjall"].iter() {
        group.bench_with_input(
            BenchmarkId::new("get_warm", engine),
            engine,
            |b, &engine| {
                let runtime = rt();
                let backend: Arc<dyn StorageBackend> = match engine {
                    "redb" => open_redb(&runtime).backend,
                    "fjall" => open_fjall(&runtime).backend,
                    other => panic!("unexpected engine label {other}"),
                };

                runtime.block_on(async {
                    for i in 0u64..100 {
                        let mut batch = WriteBatch::new();
                        batch.put(format!("bench/get/{i:016x}"), value.clone());
                        backend.write_batch(batch).await.expect("seed");
                    }
                });

                let mut counter: u64 = 0;
                b.to_async(&runtime).iter(|| {
                    let backend = backend.clone();
                    counter = counter.wrapping_add(1);
                    let key = format!("bench/get/{:016x}", counter % 100);
                    async move {
                        backend
                            .get(key.as_bytes())
                            .await
                            .expect("get")
                            .expect("seeded key present")
                    }
                });
            },
        );
    }

    // --- scan_range (1k-key prefix, full iteration) ------------------
    //
    // Preload 1000 keys under a common prefix, then time a full scan
    // that drains the returned stream. This is where LSM SSTable scans
    // ought to diverge from redb page walks as the working set grows;
    // at 1k keys it's in-memory for both engines, so expect roughly
    // balanced numbers on this size.
    const SCAN_KEYS: u64 = 1_000;
    let scan_value = Bytes::from(vec![0x43u8; 256]);
    group.throughput(Throughput::Elements(SCAN_KEYS));
    for engine in ["redb", "fjall"].iter() {
        group.bench_with_input(
            BenchmarkId::new("scan_range", engine),
            engine,
            |b, &engine| {
                let runtime = rt();
                let backend: Arc<dyn StorageBackend> = match engine {
                    "redb" => open_redb(&runtime).backend,
                    "fjall" => open_fjall(&runtime).backend,
                    other => panic!("unexpected engine label {other}"),
                };

                runtime.block_on(async {
                    for i in 0..SCAN_KEYS {
                        let mut batch = WriteBatch::new();
                        batch.put(format!("bench/scan/{i:016x}"), scan_value.clone());
                        backend.write_batch(batch).await.expect("seed scan");
                    }
                });

                use aresadb_core::KeyRange;
                use futures::StreamExt;

                b.to_async(&runtime).iter(|| {
                    let backend = backend.clone();
                    async move {
                        let range = KeyRange::prefix(Bytes::from_static(b"bench/scan/"));
                        let mut stream = backend.scan(range).await.expect("scan");
                        let mut n: u64 = 0;
                        while let Some(item) = stream.next().await {
                            let _ = item.expect("scan item");
                            n += 1;
                        }
                        debug_assert_eq!(n, SCAN_KEYS);
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_raft_apply_single_node, bench_engine_backends);
criterion_main!(benches);

//! Distributed System Benchmarks
//!
//! Criterion benchmarks for sharding, compression, bloom filters, and WAL.

use aresadb::distributed::{BloomFilter, Compressor, ShardManager, WalEntryType, WriteAheadLog};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tempfile::TempDir;
use tokio::runtime::Runtime;

fn create_runtime() -> Runtime {
    Runtime::new().unwrap()
}

// ============================================================================
// Bloom Filter Benchmarks
// ============================================================================

fn bench_bloom_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("distributed/bloom");

    // Insert benchmark
    for size in [1_000, 10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("insert", size), size, |b, &n| {
            let mut bloom = BloomFilter::new(n, 0.01);
            let mut i = 0u64;
            b.iter(|| {
                bloom.insert(format!("key_{}", i).as_bytes());
                i = i.wrapping_add(1);
            })
        });
    }

    // Query benchmark (existing keys)
    let mut bloom = BloomFilter::new(100_000, 0.01);
    for i in 0..100_000 {
        bloom.insert(format!("key_{}", i).as_bytes());
    }

    group.bench_function("query_existing", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key_{}", i % 100_000);
            black_box(bloom.may_contain(key.as_bytes()));
            i = i.wrapping_add(1);
        })
    });

    // Query benchmark (non-existing keys)
    group.bench_function("query_nonexisting", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("missing_{}", i);
            black_box(bloom.may_contain(key.as_bytes()));
            i = i.wrapping_add(1);
        })
    });

    // Serialization
    group.bench_function("serialize", |b| b.iter(|| black_box(bloom.to_bytes())));

    let bytes = bloom.to_bytes();
    group.bench_function("deserialize", |b| {
        b.iter(|| black_box(BloomFilter::from_bytes(&bytes).unwrap()))
    });

    group.finish();
}

// ============================================================================
// Compression Benchmarks
// ============================================================================

fn bench_compression(c: &mut Criterion) {
    let compressor = Compressor::new();

    let mut group = c.benchmark_group("distributed/compression");

    // Various data sizes
    for size in [1_000, 10_000, 100_000, 1_000_000].iter() {
        let data: Vec<u8> = (0..*size).map(|i| (i % 256) as u8).collect();

        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::new("compress", size), &data, |b, d| {
            b.iter(|| black_box(compressor.compress(d).unwrap()))
        });

        let compressed = compressor.compress(&data).unwrap();
        group.throughput(Throughput::Bytes(compressed.len() as u64));
        group.bench_with_input(BenchmarkId::new("decompress", size), &compressed, |b, c| {
            b.iter(|| black_box(compressor.decompress(c).unwrap()))
        });
    }

    // Different data patterns
    let patterns: Vec<(&str, Vec<u8>)> = vec![
        ("zeros", vec![0u8; 100_000]),
        (
            "random",
            (0..100_000).map(|i| (i * 17 % 256) as u8).collect(),
        ),
        ("text", "Hello world! ".repeat(8000).into_bytes()),
    ];

    for (name, data) in patterns {
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(BenchmarkId::new("compress_pattern", name), &data, |b, d| {
            b.iter(|| black_box(compressor.compress(d).unwrap()))
        });
    }

    group.finish();
}

// ============================================================================
// WAL Benchmarks
// ============================================================================

fn bench_wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("distributed/wal");

    // Write throughput
    for entry_size in [100, 1_000, 10_000].iter() {
        let data: Vec<u8> = vec![42u8; *entry_size];

        group.throughput(Throughput::Bytes(*entry_size as u64));
        group.bench_with_input(BenchmarkId::new("append", entry_size), &data, |b, d| {
            let temp = TempDir::new().unwrap();
            let wal = WriteAheadLog::open(temp.path().join("bench.wal")).unwrap();

            b.iter(|| wal.append(WalEntryType::InsertNode, d.clone()).unwrap())
        });
    }

    // Flush latency
    let temp = TempDir::new().unwrap();
    let wal = WriteAheadLog::open(temp.path().join("flush.wal")).unwrap();
    for _ in 0..100 {
        wal.append(WalEntryType::InsertNode, vec![1, 2, 3]).unwrap();
    }

    group.bench_function("flush", |b| b.iter(|| wal.flush().unwrap()));

    // Read throughput
    let temp = TempDir::new().unwrap();
    {
        let wal = WriteAheadLog::open(temp.path().join("read.wal")).unwrap();
        for _ in 0..1000 {
            wal.append(WalEntryType::InsertNode, vec![0u8; 100])
                .unwrap();
        }
        wal.flush().unwrap();
    }

    let wal = WriteAheadLog::open(temp.path().join("read.wal")).unwrap();
    group.bench_function("read_all", |b| {
        b.iter(|| black_box(wal.read_all().unwrap()))
    });

    group.finish();
}

// ============================================================================
// Sharding Benchmarks
// ============================================================================

fn bench_sharding(c: &mut Criterion) {
    let rt = create_runtime();

    let mut group = c.benchmark_group("distributed/sharding");

    // Hash computation
    group.bench_function("shard_key_hash", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key_{}", i);
            black_box(ShardManager::shard_for_key(key.as_bytes(), 8));
            i = i.wrapping_add(1);
        })
    });

    // Distribution uniformity test (measure time for many keys)
    for num_shards in [4, 8, 16].iter() {
        group.bench_with_input(
            BenchmarkId::new("distribution", num_shards),
            num_shards,
            |b, &n| {
                let mut i = 0u64;
                b.iter(|| {
                    let key = format!("key_{}", i);
                    let shard = ShardManager::shard_for_key(key.as_bytes(), n);
                    black_box(shard);
                    i = i.wrapping_add(1);
                })
            },
        );
    }

    // Full shard manager operations
    let temp = TempDir::new().unwrap();
    let manager = rt.block_on(async {
        use aresadb::distributed::ShardConfig;
        ShardManager::new(ShardConfig {
            num_shards: 4,
            virtual_nodes: 100,
            base_path: temp.path().to_path_buf(),
        })
        .await
        .unwrap()
    });

    group.bench_function("insert_node", |b| {
        b.to_async(&rt).iter(|| async {
            let node = aresadb::storage::Node::new(
                "item",
                aresadb::storage::Value::from_json(serde_json::json!({"test": true})).unwrap(),
            );
            black_box(manager.insert_node(&node).await.unwrap())
        })
    });

    group.finish();
}

// ============================================================================
// Combined Distributed Pipeline Benchmarks
// ============================================================================

fn bench_distributed_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("distributed/pipeline");

    // Full write path: compress + WAL + shard
    let compressor = Compressor::new();

    group.bench_function("write_pipeline", |b| {
        let temp = TempDir::new().unwrap();
        let wal = WriteAheadLog::open(temp.path().join("pipeline.wal")).unwrap();

        let data = serde_json::to_vec(&serde_json::json!({
            "id": "12345",
            "type": "user",
            "props": {"name": "Test User", "age": 30}
        }))
        .unwrap();

        b.iter(|| {
            // 1. Compress
            let compressed = compressor.compress(&data).unwrap();

            // 2. Determine shard
            let shard = ShardManager::shard_for_key(b"12345", 8);

            // 3. Write to WAL
            wal.append(WalEntryType::InsertNode, compressed).unwrap();

            black_box(shard)
        })
    });

    // Full read path: bloom check + decompress
    let mut bloom = BloomFilter::new(10_000, 0.01);
    for i in 0..10_000 {
        bloom.insert(format!("key_{}", i).as_bytes());
    }

    let original = vec![42u8; 1000];
    let compressed = compressor.compress(&original).unwrap();

    group.bench_function("read_pipeline", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key_{}", i % 10_000);

            // 1. Bloom filter check
            if bloom.may_contain(key.as_bytes()) {
                // 2. Determine shard
                let shard = ShardManager::shard_for_key(key.as_bytes(), 8);

                // 3. Decompress (simulate fetch result)
                let data = compressor.decompress(&compressed).unwrap();

                black_box((shard, data))
            }

            i = i.wrapping_add(1);
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_bloom_filter,
    bench_compression,
    bench_wal,
    bench_sharding,
    bench_distributed_pipeline,
);

criterion_main!(benches);

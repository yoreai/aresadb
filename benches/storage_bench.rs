//! Storage Engine Benchmarks
//!
//! Criterion benchmarks for the storage layer.

use std::sync::Arc;

use aresadb::storage::Database;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tempfile::TempDir;
use tokio::runtime::Runtime;

fn create_runtime() -> Runtime {
    Runtime::new().unwrap()
}

fn bench_node_insert(c: &mut Criterion) {
    let rt = create_runtime();
    let temp = TempDir::new().unwrap();
    let db = Arc::new(rt.block_on(Database::create(temp.path(), "bench")).unwrap());

    let mut group = c.benchmark_group("storage/insert");

    // Simple node
    group.throughput(Throughput::Elements(1));
    group.bench_function("simple_node", |b| {
        let db = db.clone();
        b.to_async(&rt).iter(move || {
            let db = db.clone();
            async move {
                db.insert_node("item", serde_json::json!({"key": "value"}))
                    .await
                    .unwrap()
            }
        })
    });

    // Node with many properties
    let big_props = serde_json::json!({
        "field1": "value1",
        "field2": 42,
        "field3": true,
        "field4": vec![1, 2, 3, 4, 5],
        "field5": {"nested": "object"},
        "field6": "longer string value for benchmarking purposes",
        "field7": 3.125,
        "field8": null,
        "field9": "another value",
        "field10": 999999
    });

    group.bench_function("complex_node", |b| {
        let db = db.clone();
        let big_props = big_props.clone();
        b.to_async(&rt).iter(move || {
            let db = db.clone();
            let props = big_props.clone();
            async move { db.insert_node("complex", props).await.unwrap() }
        })
    });

    group.finish();
}

fn bench_node_read(c: &mut Criterion) {
    let rt = create_runtime();
    let temp = TempDir::new().unwrap();
    let db = Arc::new(rt.block_on(async {
        let db = Database::create(temp.path(), "bench").await.unwrap();
        for i in 0..1000 {
            db.insert_node("item", serde_json::json!({"index": i}))
                .await
                .unwrap();
        }
        db
    }));

    let mut group = c.benchmark_group("storage/read");

    // Get existing node
    let node = rt.block_on(db.get_all_by_type("item", Some(1))).unwrap()[0].clone();
    let node_id = node.id.to_string();

    group.throughput(Throughput::Elements(1));
    group.bench_function("get_by_id", |b| {
        let db = db.clone();
        let node_id = node_id.clone();
        b.to_async(&rt).iter(move || {
            let db = db.clone();
            let node_id = node_id.clone();
            async move { db.get_node(&node_id).await.unwrap() }
        })
    });

    // Get by type
    for size in [10, 100, 500].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("get_by_type", size), size, |b, &n| {
            let db = db.clone();
            b.to_async(&rt).iter(move || {
                let db = db.clone();
                async move { db.get_all_by_type("item", Some(n)).await.unwrap() }
            })
        });
    }

    group.finish();
}

fn bench_edge_operations(c: &mut Criterion) {
    let rt = create_runtime();
    let temp = TempDir::new().unwrap();
    let db = Arc::new(rt.block_on(async {
        let db = Database::create(temp.path(), "bench").await.unwrap();
        for i in 0..100 {
            db.insert_node("vertex", serde_json::json!({"i": i}))
                .await
                .unwrap();
        }
        db
    }));

    let nodes = rt.block_on(db.get_all_by_type("vertex", None)).unwrap();
    let node_ids: Vec<String> = nodes.iter().map(|n| n.id.to_string()).collect();

    let mut group = c.benchmark_group("storage/edges");

    // Create edge
    let mut edge_idx = 0usize;
    group.bench_function("create_edge", |b| {
        let db = db.clone();
        let node_ids = node_ids.clone();
        b.to_async(&rt).iter(move || {
            let from_idx = edge_idx % node_ids.len();
            let to_idx = (edge_idx + 1) % node_ids.len();
            edge_idx += 1;

            let from = node_ids[from_idx].clone();
            let to = node_ids[to_idx].clone();
            let db = db.clone();

            async move { db.create_edge(&from, &to, "connects", None).await.unwrap() }
        })
    });

    // Query edges
    let first_id = node_ids[0].clone();
    group.bench_function("get_edges_from", |b| {
        let db = db.clone();
        let first_id = first_id.clone();
        b.to_async(&rt).iter(move || {
            let db = db.clone();
            let first_id = first_id.clone();
            async move { db.get_edges_from(&first_id, None).await.unwrap() }
        })
    });

    group.finish();
}

fn bench_batch_operations(c: &mut Criterion) {
    let rt = create_runtime();

    let mut group = c.benchmark_group("storage/batch");

    for batch_size in [10, 50, 100, 500].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("batch_insert", batch_size),
            batch_size,
            |b, &n| {
                b.to_async(&rt).iter(|| async {
                    let temp = TempDir::new().unwrap();
                    let db = Database::create(temp.path(), "batch").await.unwrap();

                    for i in 0..n {
                        db.insert_node("item", serde_json::json!({"i": i}))
                            .await
                            .unwrap();
                    }
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_node_insert,
    bench_node_read,
    bench_edge_operations,
    bench_batch_operations,
);

criterion_main!(benches);

//! Stress and Concurrency Tests
//!
//! Tests that push the database to its limits.

use aresadb::distributed::{BloomFilter, Compressor};
use aresadb::storage::{Database, DistanceMetric};
use aresadb::QueryEngine;
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

mod common;

/// Helper to create a temp database
async fn create_temp_db() -> (Database, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::create(temp_dir.path(), "stress_test")
        .await
        .unwrap();
    (db, temp_dir)
}

/// Test many sequential inserts
#[tokio::test]
async fn test_many_sequential_inserts() {
    let (db, _temp_dir) = create_temp_db().await;

    let start = Instant::now();
    let count = 1000;

    for i in 0..count {
        db.insert_node(
            "item",
            serde_json::json!({
                "index": i,
                "name": format!("item_{}", i)
            }),
        )
        .await
        .unwrap();
    }

    let elapsed = start.elapsed();
    println!(
        "Inserted {} nodes in {:?} ({:.2} ops/sec)",
        count,
        elapsed,
        count as f64 / elapsed.as_secs_f64()
    );

    // Verify count
    let nodes = db.get_all_by_type("item", Some(count + 100)).await.unwrap();
    assert_eq!(nodes.len(), count);
}

/// Test sequential reads
#[tokio::test]
async fn test_many_sequential_reads() {
    let (db, _temp_dir) = create_temp_db().await;

    // Insert test data
    let count = 100;
    for i in 0..count {
        db.insert_node(
            "read_test",
            serde_json::json!({
                "value": i
            }),
        )
        .await
        .unwrap();
    }

    let start = Instant::now();
    let read_count = 500;

    for _ in 0..read_count {
        let _ = db.get_all_by_type("read_test", Some(100)).await.unwrap();
    }

    let elapsed = start.elapsed();
    println!(
        "Performed {} reads in {:?} ({:.2} ops/sec)",
        read_count,
        elapsed,
        read_count as f64 / elapsed.as_secs_f64()
    );
}

/// Test edge creation performance
#[tokio::test]
async fn test_edge_creation_performance() {
    let (db, _temp_dir) = create_temp_db().await;

    // Create nodes
    let mut node_ids = Vec::new();
    for i in 0..100 {
        let node = db
            .insert_node(
                "edge_test",
                serde_json::json!({
                    "index": i
                }),
            )
            .await
            .unwrap();
        node_ids.push(node.id);
    }

    let start = Instant::now();
    let mut edge_count = 0;

    // Create edges between adjacent nodes
    for i in 0..node_ids.len() - 1 {
        db.create_edge(
            &node_ids[i].to_string(),
            &node_ids[i + 1].to_string(),
            "connects_to",
            Some(serde_json::json!({"order": i})),
        )
        .await
        .unwrap();
        edge_count += 1;
    }

    let elapsed = start.elapsed();
    println!(
        "Created {} edges in {:?} ({:.2} ops/sec)",
        edge_count,
        elapsed,
        edge_count as f64 / elapsed.as_secs_f64()
    );
}

/// Test bloom filter performance
#[test]
fn test_bloom_filter_bulk_operations() {
    let count = 100_000_usize;
    let mut filter = BloomFilter::new(count, 0.01);

    let start = Instant::now();

    // Insert many items
    for i in 0..count as u64 {
        filter.insert(&i.to_le_bytes());
    }

    let insert_time = start.elapsed();

    let start = Instant::now();

    // Check all items
    for i in 0..count as u64 {
        assert!(filter.may_contain(&i.to_le_bytes()));
    }

    let check_time = start.elapsed();

    println!(
        "Bloom filter: inserted {} items in {:?}, checked in {:?}",
        count, insert_time, check_time
    );
}

/// Test compression performance
#[test]
fn test_compression_performance() {
    let compressor = Compressor::default();

    // Create sample data
    let data: Vec<u8> = (0..100_000_u32).flat_map(|i| i.to_le_bytes()).collect();

    let start = Instant::now();
    let compressed = compressor.compress(&data).unwrap();
    let compress_time = start.elapsed();

    let start = Instant::now();
    let decompressed = compressor.decompress(&compressed).unwrap();
    let decompress_time = start.elapsed();

    println!(
        "Compression: {} bytes -> {} bytes (ratio: {:.2}x)",
        data.len(),
        compressed.len(),
        data.len() as f64 / compressed.len() as f64
    );
    println!(
        "Compress: {:?}, Decompress: {:?}",
        compress_time, decompress_time
    );

    assert_eq!(data, decompressed);
}

/// Test database status performance
#[tokio::test]
async fn test_status_performance() {
    let (db, _temp_dir) = create_temp_db().await;

    // Insert some data
    for i in 0..100 {
        db.insert_node("status_test", serde_json::json!({"i": i}))
            .await
            .unwrap();
    }

    let start = Instant::now();
    let iterations = 100;

    for _ in 0..iterations {
        let _ = db.status().await.unwrap();
    }

    let elapsed = start.elapsed();
    println!(
        "Status called {} times in {:?} ({:.2} ops/sec)",
        iterations,
        elapsed,
        iterations as f64 / elapsed.as_secs_f64()
    );
}

/// Test mixed workload
#[tokio::test]
async fn test_mixed_workload() {
    let (db, _temp_dir) = create_temp_db().await;

    let start = Instant::now();
    let iterations = 200;

    for i in 0..iterations {
        match i % 4 {
            0 => {
                // Insert
                let _ = db.insert_node("mixed", serde_json::json!({"i": i})).await;
            }
            1 | 2 => {
                // Read
                let _ = db.get_all_by_type("mixed", Some(10)).await;
            }
            _ => {
                // Status
                let _ = db.status().await;
            }
        }
    }

    let elapsed = start.elapsed();
    println!(
        "Mixed workload: {} operations in {:?} ({:.2} ops/sec)",
        iterations,
        elapsed,
        iterations as f64 / elapsed.as_secs_f64()
    );
}

/// Test large property values
#[tokio::test]
async fn test_large_properties() {
    let (db, _temp_dir) = create_temp_db().await;

    // Create large property value
    let large_text: String = (0..10_000).map(|_| 'x').collect();

    let start = Instant::now();

    let node = db
        .insert_node(
            "large",
            serde_json::json!({
                "content": large_text,
                "size": 10_000
            }),
        )
        .await
        .unwrap();

    let elapsed = start.elapsed();
    println!("Inserted node with 10KB text in {:?}", elapsed);

    // Retrieve and verify
    let retrieved = db.get_node(&node.id.to_string()).await.unwrap();
    assert!(retrieved.is_some());
}

/// Test concurrent reads from multiple tasks
#[tokio::test]
async fn test_concurrent_reads() {
    let (db, _temp_dir) = create_temp_db().await;

    // Insert data
    let mut ids = Vec::new();
    for i in 0..500 {
        let node = db
            .insert_node(
                "concurrent",
                serde_json::json!({
                    "index": i,
                    "name": format!("item_{}", i)
                }),
            )
            .await
            .unwrap();
        ids.push(node.id.to_string());
    }

    let db = Arc::new(db);
    let ids = Arc::new(ids);

    let start = Instant::now();
    let mut handles = Vec::new();

    // 10 concurrent readers
    for reader_id in 0..10 {
        let db = db.clone();
        let ids = ids.clone();
        handles.push(tokio::spawn(async move {
            let mut success = 0u64;
            for i in 0..100 {
                let idx = (reader_id * 100 + i * 7) % ids.len();
                if let Ok(Some(_)) = db.get_node(&ids[idx]).await {
                    success += 1;
                }
            }
            success
        }));
    }

    let mut total_reads = 0u64;
    for handle in handles {
        total_reads += handle.await.unwrap();
    }

    let elapsed = start.elapsed();
    println!(
        "Concurrent reads: {} reads in {:?} ({:.0} ops/sec, 10 readers)",
        total_reads,
        elapsed,
        total_reads as f64 / elapsed.as_secs_f64()
    );
    assert_eq!(total_reads, 1000);
}

/// Test concurrent reads and writes
#[tokio::test]
async fn test_concurrent_read_write() {
    let (db, _temp_dir) = create_temp_db().await;

    // Seed some data
    for i in 0..100 {
        db.insert_node("rw_test", serde_json::json!({"i": i}))
            .await
            .unwrap();
    }

    let db = Arc::new(db);
    let start = Instant::now();
    let mut handles = Vec::new();

    // 5 writer tasks
    for writer_id in 0..5 {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..50 {
                db.insert_node(
                    "rw_test",
                    serde_json::json!({
                        "writer": writer_id,
                        "seq": i
                    }),
                )
                .await
                .unwrap();
            }
        }));
    }

    // 5 reader tasks
    for _reader_id in 0..5 {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..50 {
                let _ = db.get_all_by_type("rw_test", Some(10)).await.unwrap();
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start.elapsed();
    println!("Concurrent R/W: 250 writes + 250 reads in {:?}", elapsed);

    // Verify: 100 initial + 250 inserts = 350
    let all = db.get_all_by_type("rw_test", Some(1000)).await.unwrap();
    assert_eq!(all.len(), 350);
}

/// Test batch insert at scale
#[tokio::test]
async fn test_batch_insert_scale() {
    let (db, _temp_dir) = create_temp_db().await;

    let count = 10_000;
    let batch_size = 2_000;
    let start = Instant::now();

    for batch_start in (0..count).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(count);
        let items: Vec<(&str, serde_json::Value)> = (batch_start..batch_end)
            .map(|i| {
                (
                    "scale_test",
                    serde_json::json!({"i": i, "data": format!("payload_{}", i)}),
                )
            })
            .collect();

        db.insert_nodes_batch(items).await.unwrap();
    }

    let elapsed = start.elapsed();
    let rate = count as f64 / elapsed.as_secs_f64();
    println!(
        "Batch insert: {} nodes in {:?} ({:.0} nodes/sec)",
        count, elapsed, rate
    );

    let status = db.status().await.unwrap();
    assert_eq!(status.node_count, count as u64);
}

/// Test SQL query under concurrent load
#[tokio::test]
async fn test_concurrent_sql_queries() {
    let (db, _temp_dir) = create_temp_db().await;

    // Insert test data
    for i in 0..200 {
        db.insert_node(
            "sql_test",
            serde_json::json!({
                "name": format!("user_{}", i),
                "age": 20 + (i % 50),
                "city": format!("city_{}", i % 10)
            }),
        )
        .await
        .unwrap();
    }

    let db = Arc::new(db);
    let mut handles = Vec::new();

    for query_id in 0..5 {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            let qe = QueryEngine::new((*db).clone());
            let mut success = 0u64;

            for i in 0..20 {
                let sql = match (query_id + i) % 3 {
                    0 => "SELECT * FROM sql_test WHERE age > 40 LIMIT 10".to_string(),
                    1 => "SELECT * FROM sql_test LIMIT 5".to_string(),
                    _ => "SELECT * FROM sql_test ORDER BY name LIMIT 10".to_string(),
                };

                if qe.execute_sql(&sql, None).await.is_ok() {
                    success += 1;
                }
            }
            success
        }));
    }

    let mut total = 0u64;
    for handle in handles {
        total += handle.await.unwrap();
    }

    println!("Concurrent SQL: {} queries executed successfully", total);
    assert_eq!(total, 100);
}

/// Test secondary index + full-text search under load
#[tokio::test]
async fn test_index_stress() {
    let (db, _temp_dir) = create_temp_db().await;

    // Insert documents
    let doc_count = 1000;
    let items: Vec<(&str, serde_json::Value)> = (0..doc_count)
        .map(|i| ("article", serde_json::json!({
            "title": format!("Article about topic {}", i % 50),
            "category": format!("cat_{}", i % 20),
            "body": format!("This article discusses topic {} with various details about subject {}", i % 50, i % 30)
        })))
        .collect();

    db.insert_nodes_batch(items).await.unwrap();

    // Create secondary index
    let idx_count = db.create_index("article", "category").await.unwrap();
    assert_eq!(idx_count, doc_count as u64);

    // Test index lookups
    for cat in 0..20 {
        let results = db
            .index_lookup(
                "article",
                "category",
                &aresadb::storage::Value::String(format!("cat_{}", cat)),
            )
            .await
            .unwrap();
        assert!(results.is_some());
        assert_eq!(results.unwrap().len(), 50); // 1000 / 20 = 50 per category
    }

    // Create full-text index
    let ft_count = db.create_fulltext_index("article", "body").await.unwrap();
    assert_eq!(ft_count, doc_count as u64);

    // Test full-text search
    let results = db
        .fulltext_search("article", "body", "discusses topic details", 10)
        .await
        .unwrap();
    assert!(!results.is_empty());
    assert!(results[0].1 > 0.0); // Has a positive BM25 score

    println!(
        "Index stress: {} docs, {} property indexed, {} FT indexed, FTS returned {} results",
        doc_count,
        idx_count,
        ft_count,
        results.len()
    );
}

/// Test vector search at moderate scale
#[tokio::test]
async fn test_vector_search_stress() {
    let (db, _temp_dir) = create_temp_db().await;

    let vec_count = 1000;
    let dim = 64;

    // Insert vectors
    for i in 0..vec_count {
        let embedding: Vec<f32> = (0..dim)
            .map(|d| ((i * dim + d) as f32 * 0.01).sin())
            .collect();

        db.insert_with_embedding(
            "vec_doc",
            serde_json::json!({"title": format!("doc_{}", i)}),
            "embedding",
            embedding,
        )
        .await
        .unwrap();
    }

    // Build HNSW index
    let stats = db
        .rebuild_vector_index("vec_doc", "embedding")
        .await
        .unwrap();
    assert_eq!(stats.num_vectors, vec_count);

    // Run multiple searches
    let start = Instant::now();
    let search_count = 100;

    for i in 0..search_count {
        let query: Vec<f32> = (0..dim)
            .map(|d| ((i * dim + d) as f32 * 0.02).cos())
            .collect();
        let results = db
            .similarity_search(&query, "vec_doc", "embedding", 10, DistanceMetric::Cosine)
            .await
            .unwrap();
        assert!(!results.is_empty());
    }

    let elapsed = start.elapsed();
    println!(
        "Vector search: {} searches over {} vectors in {:?} ({:.0} searches/sec)",
        search_count,
        vec_count,
        elapsed,
        search_count as f64 / elapsed.as_secs_f64()
    );
}

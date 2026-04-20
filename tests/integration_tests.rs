//! Integration Tests for AresaDB
//!
//! End-to-end tests for the complete database functionality.

mod common;

use aresadb::distributed::{BloomFilter, Compressor, ShardConfig, ShardManager};
use aresadb::query::{Operator, QueryOperation, QueryParser};
use aresadb::storage::{Database, Node, Value};
use tempfile::TempDir;

// ============================================================================
// Database Integration Tests
// ============================================================================

mod database_tests {
    use super::*;

    #[tokio::test]
    async fn test_database_lifecycle() {
        let temp = TempDir::new().unwrap();

        // Create
        let db = Database::create(temp.path(), "test_db").await.unwrap();
        assert_eq!(db.name(), "test_db");

        let status = db.status().await.unwrap();
        assert_eq!(status.node_count, 0);
        assert_eq!(status.edge_count, 0);

        drop(db);

        // Reopen
        let db = Database::open(temp.path()).await.unwrap();
        assert_eq!(db.name(), "test_db");
    }

    #[tokio::test]
    async fn test_node_crud_operations() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "test").await.unwrap();

        // Create
        let props = serde_json::json!({
            "name": "Alice",
            "email": "alice@example.com",
            "age": 30
        });
        let node = db.insert_node("user", props).await.unwrap();
        assert_eq!(node.node_type, "user");

        let node_id = node.id.to_string();

        // Read
        let retrieved = db.get_node(&node_id).await.unwrap().unwrap();
        assert_eq!(retrieved.node_type, "user");
        assert_eq!(retrieved.get("name").unwrap().as_str(), Some("Alice"));
        assert_eq!(retrieved.get("age").unwrap().as_int(), Some(30));

        // Update
        let new_props = serde_json::json!({
            "age": 31,
            "city": "New York"
        });
        let updated = db.update_node(&node_id, new_props).await.unwrap();
        assert_eq!(updated.get("age").unwrap().as_int(), Some(31));
        assert_eq!(updated.get("city").unwrap().as_str(), Some("New York"));

        // Delete
        db.delete_node(&node_id).await.unwrap();
        let deleted = db.get_node(&node_id).await.unwrap();
        assert!(deleted.is_none());
    }

    #[tokio::test]
    async fn test_edge_operations() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "test").await.unwrap();

        // Create nodes
        let alice = db
            .insert_node("user", serde_json::json!({"name": "Alice"}))
            .await
            .unwrap();
        let bob = db
            .insert_node("user", serde_json::json!({"name": "Bob"}))
            .await
            .unwrap();
        let post = db
            .insert_node("post", serde_json::json!({"title": "Hello World"}))
            .await
            .unwrap();

        // Create edges
        let follows = db
            .create_edge(&alice.id.to_string(), &bob.id.to_string(), "follows", None)
            .await
            .unwrap();
        assert_eq!(follows.edge_type, "follows");

        let wrote = db
            .create_edge(
                &alice.id.to_string(),
                &post.id.to_string(),
                "wrote",
                Some(serde_json::json!({"published": true})),
            )
            .await
            .unwrap();
        assert_eq!(wrote.edge_type, "wrote");

        // Query edges
        let alice_edges = db
            .get_edges_from(&alice.id.to_string(), None)
            .await
            .unwrap();
        assert_eq!(alice_edges.len(), 2);

        let follows_edges = db
            .get_edges_from(&alice.id.to_string(), Some("follows"))
            .await
            .unwrap();
        assert_eq!(follows_edges.len(), 1);

        let incoming = db.get_edges_to(&bob.id.to_string(), None).await.unwrap();
        assert_eq!(incoming.len(), 1);
    }

    #[tokio::test]
    async fn test_graph_view() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "test").await.unwrap();

        // Create a small social graph
        let alice = db
            .insert_node("user", serde_json::json!({"name": "Alice"}))
            .await
            .unwrap();
        let bob = db
            .insert_node("user", serde_json::json!({"name": "Bob"}))
            .await
            .unwrap();
        let charlie = db
            .insert_node("user", serde_json::json!({"name": "Charlie"}))
            .await
            .unwrap();

        db.create_edge(&alice.id.to_string(), &bob.id.to_string(), "follows", None)
            .await
            .unwrap();
        db.create_edge(
            &bob.id.to_string(),
            &charlie.id.to_string(),
            "follows",
            None,
        )
        .await
        .unwrap();

        let graph = db.get_as_graph("user", Some(10)).await.unwrap();
        assert_eq!(graph.nodes.len(), 3);
        assert_eq!(graph.edges.len(), 2);
    }

    #[tokio::test]
    async fn test_kv_view() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "test").await.unwrap();

        db.insert_node(
            "config",
            serde_json::json!({"key": "theme", "value": "dark"}),
        )
        .await
        .unwrap();
        db.insert_node(
            "config",
            serde_json::json!({"key": "language", "value": "en"}),
        )
        .await
        .unwrap();

        let kv = db.get_as_kv("config", None).await.unwrap();
        assert_eq!(kv.entries.len(), 2);
    }

    #[tokio::test]
    async fn test_batch_insert() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "test").await.unwrap();

        // Insert many nodes
        for i in 0..100 {
            let props = serde_json::json!({
                "name": format!("User {}", i),
                "index": i
            });
            db.insert_node("user", props).await.unwrap();
        }

        let users = db.get_all_by_type("user", None).await.unwrap();
        assert_eq!(users.len(), 100);

        // Test limit
        let limited = db.get_all_by_type("user", Some(10)).await.unwrap();
        assert_eq!(limited.len(), 10);
    }
}

// ============================================================================
// Query Parser Tests
// ============================================================================

mod query_parser_tests {
    use super::*;

    #[test]
    fn test_select_queries() {
        let parser = QueryParser::new();

        // Simple select
        let query = parser.parse("SELECT * FROM users").unwrap();
        assert_eq!(query.operation, QueryOperation::Select);
        assert_eq!(query.target, "users");
        assert!(query.columns.is_empty()); // * means all

        // Select with columns
        let query = parser.parse("SELECT name, email FROM users").unwrap();
        assert_eq!(query.columns, vec!["name", "email"]);

        // Select with WHERE
        let query = parser.parse("SELECT * FROM users WHERE age > 18").unwrap();
        assert_eq!(query.conditions.len(), 1);
        assert_eq!(query.conditions[0].column, "age");
        assert_eq!(query.conditions[0].operator, Operator::Gt);

        // Select with LIMIT
        let query = parser.parse("SELECT * FROM users LIMIT 10").unwrap();
        assert_eq!(query.limit, Some(10));
    }

    #[test]
    fn test_insert_queries() {
        let parser = QueryParser::new();

        let query = parser
            .parse("INSERT INTO users (name, age) VALUES ('John', 30)")
            .unwrap();

        assert_eq!(query.operation, QueryOperation::Insert);
        assert_eq!(query.target, "users");

        let data = query.data.unwrap();
        assert_eq!(data.get("name").unwrap().as_str(), Some("John"));
        assert_eq!(data.get("age").unwrap().as_int(), Some(30));
    }

    #[test]
    fn test_update_queries() {
        let parser = QueryParser::new();

        let query = parser
            .parse("UPDATE users SET age = 31 WHERE id = 1")
            .unwrap();

        assert_eq!(query.operation, QueryOperation::Update);
        assert_eq!(query.target, "users");
        assert!(query.conditions.len() >= 1);
    }

    #[test]
    fn test_delete_queries() {
        let parser = QueryParser::new();

        let query = parser
            .parse("DELETE FROM users WHERE status = 'inactive'")
            .unwrap();

        assert_eq!(query.operation, QueryOperation::Delete);
        assert_eq!(query.target, "users");
        assert_eq!(query.conditions.len(), 1);
    }

    #[test]
    fn test_natural_language_queries() {
        let parser = QueryParser::new();

        // Simple NL query
        let query = parser.parse_natural_language("get all users").unwrap();
        assert_eq!(query.operation, QueryOperation::Select);
        assert_eq!(query.target, "users");
    }

    #[test]
    fn test_query_validation() {
        let parser = QueryParser::new();

        assert!(parser.validate("SELECT * FROM users"));
        assert!(!parser.validate("SELEC * FROM users")); // Typo
        assert!(!parser.validate("")); // Empty
    }
}

// ============================================================================
// Bloom Filter Tests
// ============================================================================

mod bloom_filter_tests {
    use super::*;

    #[test]
    fn test_bloom_filter_accuracy() {
        let mut bloom = BloomFilter::new(10000, 0.01);

        // Insert 5000 items
        for i in 0..5000 {
            bloom.insert(format!("item_{}", i).as_bytes());
        }

        // All inserted items should be found
        for i in 0..5000 {
            assert!(bloom.may_contain(format!("item_{}", i).as_bytes()));
        }

        // Count false positives for items not inserted
        let mut false_positives = 0;
        for i in 5000..10000 {
            if bloom.may_contain(format!("item_{}", i).as_bytes()) {
                false_positives += 1;
            }
        }

        // False positive rate should be close to 1%
        let fpp = false_positives as f64 / 5000.0;
        assert!(fpp < 0.03, "FPP too high: {:.2}%", fpp * 100.0);
    }

    #[test]
    fn test_bloom_filter_serialization() {
        let mut bloom = BloomFilter::new(1000, 0.01);
        for i in 0..100 {
            bloom.insert(format!("key_{}", i).as_bytes());
        }

        let bytes = bloom.to_bytes();
        let restored = BloomFilter::from_bytes(&bytes).unwrap();

        // Verify all items are still found
        for i in 0..100 {
            assert!(restored.may_contain(format!("key_{}", i).as_bytes()));
        }
    }

    #[test]
    fn test_bloom_filter_clear() {
        let mut bloom = BloomFilter::new(100, 0.01);
        bloom.insert(b"test");
        assert!(bloom.may_contain(b"test"));

        bloom.clear();
        assert!(!bloom.may_contain(b"test"));
    }
}

// ============================================================================
// Compression Tests
// ============================================================================

mod compression_tests {
    use super::*;

    #[test]
    fn test_compression_roundtrip() {
        let compressor = Compressor::new();

        let test_cases: Vec<(&str, Vec<u8>)> = vec![
            ("empty", vec![]),
            ("small", b"hello".to_vec()),
            ("repetitive", vec![0u8; 1000]),
            (
                "random-ish",
                (0..1000).map(|i| (i * 17 % 256) as u8).collect(),
            ),
            (
                "text",
                b"The quick brown fox jumps over the lazy dog".repeat(20),
            ),
        ];

        for (name, data) in test_cases {
            let compressed = compressor.compress(&data).unwrap();
            let decompressed = compressor.decompress(&compressed).unwrap();
            assert_eq!(data, decompressed, "Failed for case: {}", name);
        }
    }

    #[test]
    fn test_compression_efficiency() {
        let compressor = Compressor::new();

        // Highly compressible data
        let data = vec![0u8; 10000];
        let (compressed, stats) = compressor.compress_with_stats(&data).unwrap();

        assert!(stats.ratio > 1.0);
        assert!(compressed.len() < data.len());
    }
}

// ============================================================================
// Sharding Tests
// ============================================================================

mod sharding_tests {
    use super::*;

    #[tokio::test]
    async fn test_shard_manager_distribution() {
        let temp = TempDir::new().unwrap();

        let config = ShardConfig {
            num_shards: 4,
            virtual_nodes: 100,
            base_path: temp.path().to_path_buf(),
        };

        let manager = ShardManager::new(config).await.unwrap();

        // Insert nodes
        for i in 0..100 {
            let node = Node::new(
                "item",
                Value::from_json(serde_json::json!({
                    "index": i
                }))
                .unwrap(),
            );
            manager.insert_node(&node).await.unwrap();
        }

        // Check distribution
        let stats = manager.stats().await.unwrap();
        assert_eq!(stats.total_nodes, 100);
        assert_eq!(stats.num_shards, 4);

        // Each shard should have some nodes (with good distribution)
        for shard_stat in &stats.shards {
            assert!(
                shard_stat.node_count > 5,
                "Shard {} has too few nodes: {}",
                shard_stat.id,
                shard_stat.node_count
            );
        }
    }

    #[test]
    fn test_shard_key_determinism() {
        // Same key should always map to same shard
        let shard1 = ShardManager::shard_for_key(b"test_key", 8);
        let shard2 = ShardManager::shard_for_key(b"test_key", 8);
        assert_eq!(shard1, shard2);
    }

    #[test]
    fn test_shard_distribution() {
        let mut distribution = [0usize; 8];

        for i in 0..10000 {
            let key = format!("key_{}", i);
            let shard = ShardManager::shard_for_key(key.as_bytes(), 8) as usize;
            distribution[shard] += 1;
        }

        // Each shard should have roughly 1250 keys (10000/8)
        let avg = 10000 / 8;
        for (i, &count) in distribution.iter().enumerate() {
            assert!(
                count > avg / 2 && count < avg * 2,
                "Shard {} has uneven distribution: {} (expected ~{})",
                i,
                count,
                avg
            );
        }
    }
}

// ============================================================================
// Concurrent Access Tests
// ============================================================================

mod concurrent_tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_concurrent_reads() {
        let temp = TempDir::new().unwrap();
        let db = Arc::new(Database::create(temp.path(), "test").await.unwrap());

        // Insert initial data
        let node = db
            .insert_node("user", serde_json::json!({"name": "Alice"}))
            .await
            .unwrap();
        let node_id = node.id.to_string();

        // Spawn multiple concurrent readers
        let mut handles = vec![];
        for _ in 0..10 {
            let db = Arc::clone(&db);
            let id = node_id.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..100 {
                    let result = db.get_node(&id).await;
                    assert!(result.is_ok());
                    assert!(result.unwrap().is_some());
                }
            }));
        }

        // Wait for all readers
        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_concurrent_writes() {
        let temp = TempDir::new().unwrap();
        let db = Arc::new(Database::create(temp.path(), "test").await.unwrap());

        // Spawn multiple concurrent writers
        let mut handles = vec![];
        for writer_id in 0..10 {
            let db = Arc::clone(&db);
            handles.push(tokio::spawn(async move {
                for i in 0..10 {
                    let props = serde_json::json!({
                        "writer": writer_id,
                        "seq": i
                    });
                    db.insert_node("item", props).await.unwrap();
                }
            }));
        }

        // Wait for all writers
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all nodes were created
        let items = db.get_all_by_type("item", None).await.unwrap();
        assert_eq!(items.len(), 100);
    }
}

// ============================================================================
// Error Handling Tests
// ============================================================================

mod error_handling_tests {
    use super::*;

    #[tokio::test]
    async fn test_invalid_node_id() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "test").await.unwrap();

        let result = db.get_node("not-a-valid-uuid").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_nonexistent_node() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "test").await.unwrap();

        let fake_id = uuid::Uuid::new_v4().to_string();
        let result = db
            .update_node(&fake_id, serde_json::json!({"key": "value"}))
            .await;
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_sql() {
        let parser = QueryParser::new();

        // Invalid syntax
        assert!(parser.parse("SELEC * FROM users").is_err());

        // Empty
        assert!(parser.parse("").is_err());
    }
}

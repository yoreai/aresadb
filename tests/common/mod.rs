//! Common test utilities for AresaDB tests.

#![allow(dead_code)]

pub mod cloud;

use aresadb::storage::Database;
use std::sync::Arc;
use tempfile::TempDir;

/// Test fixtures for database tests
pub struct TestDb {
    pub db: Arc<Database>,
    pub temp_dir: TempDir,
}

impl TestDb {
    /// Create a new test database with a temporary directory
    pub async fn new(name: &str) -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db = Database::create(temp_dir.path(), name)
            .await
            .expect("Failed to create database");

        Self {
            db: Arc::new(db),
            temp_dir,
        }
    }

    /// Create a database with sample data
    pub async fn with_sample_data() -> Self {
        let fixture = Self::new("sample").await;

        // Create some users
        for i in 0..10 {
            fixture
                .db
                .insert_node(
                    "user",
                    serde_json::json!({
                        "name": format!("User {}", i),
                        "email": format!("user{}@example.com", i),
                        "age": 20 + i
                    }),
                )
                .await
                .unwrap();
        }

        // Create some posts
        for i in 0..20 {
            fixture
                .db
                .insert_node(
                    "post",
                    serde_json::json!({
                        "title": format!("Post {}", i),
                        "content": format!("Content for post {}", i),
                        "likes": i * 5
                    }),
                )
                .await
                .unwrap();
        }

        fixture
    }

    /// Create a database with a graph structure
    pub async fn with_graph() -> Self {
        let fixture = Self::new("graph").await;

        // Create vertices
        let mut ids = Vec::new();
        for i in 0..10 {
            let node = fixture
                .db
                .insert_node(
                    "vertex",
                    serde_json::json!({
                        "label": format!("V{}", i)
                    }),
                )
                .await
                .unwrap();
            ids.push(node.id.to_string());
        }

        // Create edges (each vertex connects to next 2)
        for (i, from_id) in ids.iter().enumerate() {
            for j in 1..=2 {
                let to_idx = (i + j) % ids.len();
                let to_id = &ids[to_idx];

                fixture
                    .db
                    .create_edge(
                        from_id,
                        to_id,
                        "links",
                        Some(serde_json::json!({
                            "weight": j as f64
                        })),
                    )
                    .await
                    .unwrap();
            }
        }

        fixture
    }
}

/// Timing helper for benchmarks
pub struct Timer {
    name: String,
    start: std::time::Instant,
}

impl Timer {
    pub fn start(name: &str) -> Self {
        Self {
            name: name.to_string(),
            start: std::time::Instant::now(),
        }
    }

    pub fn elapsed(&self) -> std::time::Duration {
        self.start.elapsed()
    }

    pub fn report(&self, ops: usize) {
        let duration = self.elapsed();
        let ops_per_sec = ops as f64 / duration.as_secs_f64();
        println!(
            "[{}] {} ops in {:?} ({:.0} ops/sec)",
            self.name, ops, duration, ops_per_sec
        );
    }
}

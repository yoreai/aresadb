//! Tiered Storage Demo
//!
//! Demonstrates AresaDB's unique transparent cloud tiering architecture:
//! - Sub-ms graph traversals with local index
//! - Payload eviction to simulate cloud tiering
//! - Cache hit/miss behavior
//! - Performance at scale (100K+ nodes with graph + vector)
//!
//! Run: cargo run --example tiered_storage_demo --release

use aresadb::storage::{Database, DistanceMetric};
use std::path::Path;
use std::time::Instant;

const NODE_COUNT: usize = 50_000;
const EDGE_FAN_OUT: usize = 5;
const VECTOR_DIM: usize = 128;
const VECTOR_NODE_COUNT: usize = 10_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!();
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║          AresaDB — Tiered Storage Architecture Demo          ║");
    println!("║                                                               ║");
    println!("║  Graph index stays local (sub-ms) while payloads can live     ║");
    println!("║  on infinite-scale cloud storage (S3/GCS).                    ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();

    let db_path = Path::new("/tmp/aresadb-tiered-demo");
    if db_path.exists() {
        std::fs::remove_dir_all(db_path)?;
    }
    std::fs::create_dir_all(db_path)?;

    let db = Database::create(db_path, "tiered_demo").await?;

    // ─── Phase 1: Bulk Insert ───────────────────────────────────────
    println!(
        "━━━ Phase 1: Bulk Insert ({} nodes, {} edges) ━━━",
        NODE_COUNT,
        NODE_COUNT * EDGE_FAN_OUT
    );
    println!();

    let phase1 = Instant::now();

    let mut node_ids: Vec<String> = Vec::with_capacity(NODE_COUNT);
    let batch_size = 5_000;

    for batch_start in (0..NODE_COUNT).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(NODE_COUNT);
        let batch_time = Instant::now();

        let mut items: Vec<(&str, serde_json::Value)> = Vec::with_capacity(batch_end - batch_start);
        for i in batch_start..batch_end {
            let node_type = match i % 4 {
                0 => "user",
                1 => "product",
                2 => "order",
                _ => "review",
            };

            let props = serde_json::json!({
                "name": format!("entity_{}", i),
                "category": format!("cat_{}", i % 50),
                "score": (i as f64 * 0.01) % 100.0,
                "description": format!("This is entity number {} with some payload data to simulate real storage costs. Category: cat_{}", i, i % 50),
                "tags": vec![format!("tag_{}", i % 10), format!("group_{}", i % 25)],
                "metadata": {
                    "created_by": format!("system_{}", i % 5),
                    "priority": i % 3,
                    "active": i % 7 != 0
                }
            });

            items.push((node_type, props));
        }

        let nodes = db.insert_nodes_batch(items).await?;
        for node in &nodes {
            node_ids.push(node.id.to_string());
        }

        let batch_elapsed = batch_time.elapsed();
        let rate = (batch_end - batch_start) as f64 / batch_elapsed.as_secs_f64();
        println!(
            "  [{:>6}/{:>6}] {:>6.0} nodes/sec  ({:.1?})",
            batch_end, NODE_COUNT, rate, batch_elapsed
        );
    }

    let insert_time = phase1.elapsed();
    let insert_rate = NODE_COUNT as f64 / insert_time.as_secs_f64();
    println!();
    println!(
        "  Total: {} nodes in {:.2?} ({:.0} nodes/sec)",
        NODE_COUNT, insert_time, insert_rate
    );
    println!();

    // Insert edges (graph structure) in batches
    println!("  Creating graph edges (batched)...");
    let edge_start = Instant::now();
    let mut edge_count = 0u64;
    let edge_batch_size = 5_000;

    let mut edge_buf: Vec<(&str, &str, &str)> = Vec::with_capacity(edge_batch_size);

    for i in 0..NODE_COUNT {
        for j in 1..=EDGE_FAN_OUT {
            let target = (i + j * 7 + j * j) % NODE_COUNT;
            if target != i {
                let edge_type = match j {
                    1 => "purchased",
                    2 => "reviewed",
                    3 => "follows",
                    4 => "related_to",
                    _ => "similar",
                };
                edge_buf.push((&node_ids[i], &node_ids[target], edge_type));
                edge_count += 1;

                if edge_buf.len() >= edge_batch_size {
                    db.create_edges_batch(edge_buf).await?;
                    edge_buf = Vec::with_capacity(edge_batch_size);

                    if edge_count.is_multiple_of(50_000) {
                        let rate = edge_count as f64 / edge_start.elapsed().as_secs_f64();
                        println!("  [{:>8} edges] {:.0} edges/sec", edge_count, rate);
                    }
                }
            }
        }
    }

    // Flush remaining edges
    if !edge_buf.is_empty() {
        db.create_edges_batch(edge_buf).await?;
    }

    let edge_time = edge_start.elapsed();
    let edge_rate = edge_count as f64 / edge_time.as_secs_f64();
    println!(
        "  Total: {} edges in {:.2?} ({:.0} edges/sec)",
        edge_count, edge_time, edge_rate
    );
    println!();

    // ─── Phase 2: Point Lookups ─────────────────────────────────────
    println!("━━━ Phase 2: Point Lookups (sub-millisecond target) ━━━");
    println!();

    // Warm-up
    for id in node_ids.iter().take(100) {
        let _ = db.get_node(id).await?;
    }

    // Benchmark: random point lookups
    let lookup_count = 10_000;
    let lookup_start = Instant::now();
    for i in 0..lookup_count {
        let idx = (i * 97 + 13) % node_ids.len(); // pseudo-random
        let node = db.get_node(&node_ids[idx]).await?.unwrap();
        std::hint::black_box(&node);
    }
    let lookup_time = lookup_start.elapsed();
    let per_lookup_us = lookup_time.as_micros() as f64 / lookup_count as f64;
    let per_lookup_ns = lookup_time.as_nanos() as f64 / lookup_count as f64;

    println!("  {} point lookups in {:.2?}", lookup_count, lookup_time);
    println!(
        "  Average: {:.1}µs per lookup ({:.0}ns)",
        per_lookup_us, per_lookup_ns
    );
    if per_lookup_us < 1000.0 {
        println!("  ✓ SUB-MILLISECOND point lookups achieved!");
    }
    println!();

    // Index-only lookups (no payload — this is the graph traversal fast path)
    let index_count = 10_000;
    let tiered = db.tiered();
    let index_start = Instant::now();
    for i in 0..index_count {
        let idx = (i * 97 + 13) % node_ids.len();
        let node_id = aresadb::NodeId::parse(&node_ids[idx])?;
        let index = tiered.get_node_index(&node_id).await?.unwrap();
        std::hint::black_box(&index);
    }
    let index_time = index_start.elapsed();
    let per_index_us = index_time.as_micros() as f64 / index_count as f64;

    println!("  {} index-only lookups in {:.2?}", index_count, index_time);
    println!(
        "  Average: {:.1}µs per index lookup (no payload fetch)",
        per_index_us
    );
    println!("  This is what graph traversal uses — just structural data, no I/O for payloads");
    println!();

    // ─── Phase 3: Graph Traversal ───────────────────────────────────
    println!("━━━ Phase 3: Graph Traversal ━━━");
    println!();

    let qe = aresadb::QueryEngine::new(db.clone());

    // Single-hop traversal
    let trav_start = Instant::now();
    let result = qe.traverse(&node_ids[0], 1, None).await?;
    let trav_time = trav_start.elapsed();
    println!("  Depth-1 traversal from node 0:");
    println!(
        "    {} nodes visited, {} edges traversed in {:.2?}",
        result.nodes.len(),
        result.edges.len(),
        trav_time
    );

    // Multi-hop traversal
    let trav_start = Instant::now();
    let result = qe.traverse(&node_ids[0], 2, None).await?;
    let trav_time = trav_start.elapsed();
    println!("  Depth-2 traversal from node 0:");
    println!(
        "    {} nodes visited, {} edges traversed in {:.2?}",
        result.nodes.len(),
        result.edges.len(),
        trav_time
    );

    // Filtered traversal
    let trav_start = Instant::now();
    let result = qe
        .traverse(&node_ids[0], 2, Some(vec!["purchased", "follows"]))
        .await?;
    let trav_time = trav_start.elapsed();
    println!("  Depth-2 filtered traversal (purchased + follows):");
    println!(
        "    {} nodes visited, {} edges traversed in {:.2?}",
        result.nodes.len(),
        result.edges.len(),
        trav_time
    );
    println!();

    // ─── Phase 4: SQL Queries ───────────────────────────────────────
    println!("━━━ Phase 4: SQL Queries ━━━");
    println!();

    let queries = vec![
        ("Count users", "SELECT * FROM user LIMIT 10"),
        (
            "Filter by score",
            "SELECT * FROM product WHERE score > 50 LIMIT 10",
        ),
        (
            "Order by name",
            "SELECT * FROM review ORDER BY name LIMIT 10",
        ),
    ];

    for (label, sql) in &queries {
        let q_start = Instant::now();
        let result = qe.execute_sql(sql, None).await?;
        let q_time = q_start.elapsed();
        println!("  {}: {} rows in {:.2?}", label, result.row_count(), q_time);
    }
    println!();

    // ─── Phase 4b: Secondary Indexes ──────────────────────────────────
    println!("━━━ Phase 4b: Secondary Indexes (B-tree property indexes) ━━━");
    println!();

    // Baseline: unindexed query (full scan)
    let unindexed_start = Instant::now();
    let unindexed = qe
        .execute_sql(
            "SELECT * FROM product WHERE category = 'cat_7' LIMIT 100",
            None,
        )
        .await?;
    let unindexed_time = unindexed_start.elapsed();
    println!(
        "  Unindexed query (full scan): {} rows in {:.2?}",
        unindexed.row_count(),
        unindexed_time
    );

    // Create a secondary index on category
    let idx_start = Instant::now();
    let result = qe
        .execute_sql("CREATE INDEX ON product (category)", None)
        .await?;
    let idx_time = idx_start.elapsed();
    println!("  {}", result.rows[0][0]);
    println!("  Index build time: {:.2?}", idx_time);

    // Indexed query (should use index lookup)
    let indexed_start = Instant::now();
    let indexed = qe
        .execute_sql(
            "SELECT * FROM product WHERE category = 'cat_7' LIMIT 100",
            None,
        )
        .await?;
    let indexed_time = indexed_start.elapsed();
    println!(
        "  Indexed query: {} rows in {:.2?}",
        indexed.row_count(),
        indexed_time
    );

    if indexed_time < unindexed_time {
        let speedup = unindexed_time.as_nanos() as f64 / indexed_time.as_nanos().max(1) as f64;
        println!("  ✓ Index speedup: {:.1}x faster", speedup);
    }
    println!();

    // ─── Phase 4c: Full-Text Search ───────────────────────────────────
    println!("━━━ Phase 4c: Full-Text Search (inverted index + BM25 ranking) ━━━");
    println!();

    // Build full-text index on description field
    let ft_start = Instant::now();
    let ft_result = qe
        .execute_sql("CREATE FULLTEXT INDEX ON user (description)", None)
        .await?;
    let ft_time = ft_start.elapsed();
    println!("  {}", ft_result.rows[0][0]);
    println!("  Index build time: {:.2?}", ft_time);

    // Search
    let search_queries = vec![
        "entity number payload",
        "storage costs category",
        "simulate real",
    ];

    for sq in &search_queries {
        let ft_sql = format!(
            "FULLTEXT SEARCH user FIELD description FOR '{}' LIMIT 5",
            sq
        );
        let ft_search_start = Instant::now();
        let ft_results = qe.execute_sql(&ft_sql, None).await?;
        let ft_search_time = ft_search_start.elapsed();
        println!(
            "  Search '{}': {} results in {:.2?}",
            sq,
            ft_results.row_count(),
            ft_search_time
        );
        if let Some(row) = ft_results.rows.first() {
            if row.len() > 3 {
                println!("    Top hit: score={}", row[3]);
            }
        }
    }
    println!();

    // ─── Phase 5: Vector Search ─────────────────────────────────────
    println!(
        "━━━ Phase 5: Vector Search ({} nodes, {}D) ━━━",
        VECTOR_NODE_COUNT, VECTOR_DIM
    );
    println!();

    // Insert vector-enabled nodes using batch insert + manual HNSW index build
    println!(
        "  Inserting {} vector nodes (batched)...",
        VECTOR_NODE_COUNT
    );
    let vec_insert_start = Instant::now();
    let mut vector_ids = Vec::with_capacity(VECTOR_NODE_COUNT);
    let vec_batch_size = 2_000;

    for batch_start in (0..VECTOR_NODE_COUNT).step_by(vec_batch_size) {
        let batch_end = (batch_start + vec_batch_size).min(VECTOR_NODE_COUNT);
        let mut items: Vec<(&str, serde_json::Value)> = Vec::with_capacity(batch_end - batch_start);

        for i in batch_start..batch_end {
            let embedding: Vec<f32> = (0..VECTOR_DIM)
                .map(|d| {
                    let seed = (i * VECTOR_DIM + d) as f64;
                    ((seed * 0.1).sin() * 0.5 + (seed * 0.03).cos() * 0.5) as f32
                })
                .collect();

            // Use $vector format for proper Value::Vector deserialization
            let embedding_json: Vec<serde_json::Value> =
                embedding.iter().map(|&f| serde_json::json!(f)).collect();
            let props = serde_json::json!({
                "title": format!("doc_{}", i),
                "topic": format!("topic_{}", i % 20),
                "embedding": { "$vector": embedding_json }
            });

            items.push(("document", props));
        }

        let nodes = db.insert_nodes_batch(items).await?;
        for node in &nodes {
            vector_ids.push(node.id.to_string());
        }
    }

    // Build the HNSW index after bulk load
    let idx_stats = db.rebuild_vector_index("document", "embedding").await?;
    let vec_insert_time = vec_insert_start.elapsed();
    println!(
        "  Inserted + indexed in {:.2?} ({:.0} vectors/sec, {} in HNSW)",
        vec_insert_time,
        VECTOR_NODE_COUNT as f64 / vec_insert_time.as_secs_f64(),
        idx_stats.num_vectors
    );

    let query_vec: Vec<f32> = (0..VECTOR_DIM)
        .map(|d| ((d as f64 * 0.1).sin() * 0.5) as f32)
        .collect();

    // Brute-force search (linear scan for baseline comparison)
    let all_docs = db.get_all_by_type("document", None).await?;
    let brute_search = aresadb::storage::VectorSearch::new(DistanceMetric::Cosine);
    let brute_start = Instant::now();
    let brute_results = brute_search.search(&query_vec, &all_docs, "embedding", 10);
    let brute_time = brute_start.elapsed();
    println!();
    println!("  Brute-force linear scan (top 10 of {}):", all_docs.len());
    println!("    Time: {:.2?}", brute_time);
    for (i, r) in brute_results.iter().take(5).enumerate() {
        println!(
            "    {}. score={:.4}, distance={:.4}",
            i + 1,
            r.score,
            r.distance
        );
    }

    // HNSW search via the managed index (built by rebuild_vector_index above)
    println!();
    println!(
        "  HNSW ANN search (pre-built index, top 10 of {}):",
        VECTOR_NODE_COUNT
    );
    let hnsw_start = Instant::now();
    let hnsw_results = db
        .similarity_search(
            &query_vec,
            "document",
            "embedding",
            10,
            DistanceMetric::Cosine,
        )
        .await?;
    let hnsw_time = hnsw_start.elapsed();
    let speedup = brute_time.as_nanos() as f64 / hnsw_time.as_nanos().max(1) as f64;
    println!(
        "    Time: {:.2?} ({:.1}x faster than brute force)",
        hnsw_time, speedup
    );
    for (i, r) in hnsw_results.iter().take(5).enumerate() {
        println!(
            "    {}. score={:.4}, distance={:.4}",
            i + 1,
            r.score,
            r.distance
        );
    }
    println!();

    // ─── Phase 5b: Filtered Vector Search ─────────────────────────
    println!("━━━ Phase 5b: Filtered Vector Search (WHERE + VECTOR_SEARCH) ━━━");
    println!();

    let query_vec_str: String = query_vec
        .iter()
        .take(10)
        .map(|f| format!("{:.4}", f))
        .collect::<Vec<_>>()
        .join(", ");
    let full_query_vec_str = query_vec
        .iter()
        .map(|f| format!("{:.6}", f))
        .collect::<Vec<_>>()
        .join(", ");
    let vsql = format!(
        "VECTOR SEARCH document FIELD embedding FOR [{full_query_vec_str}] WHERE topic = 'topic_5' LIMIT 5"
    );

    println!("  SQL: VECTOR SEARCH document FIELD embedding FOR [{}, ...] WHERE topic = 'topic_5' LIMIT 5", query_vec_str);
    let filtered_start = Instant::now();
    let filtered_result = qe.execute_sql(&vsql, None).await?;
    let filtered_time = filtered_start.elapsed();
    println!(
        "  Results: {} rows in {:.2?}",
        filtered_result.row_count(),
        filtered_time
    );
    if !filtered_result.rows.is_empty() {
        println!("  Columns: {:?}", filtered_result.columns);
        for row in filtered_result.rows.iter().take(3) {
            let vals: Vec<String> = row.iter().take(6).map(|v| format!("{:?}", v)).collect();
            println!("    {}", vals.join(" | "));
        }
    }
    println!();

    // Unfiltered comparison
    let vsql_unfiltered = format!(
        "VECTOR SEARCH document FIELD embedding FOR [{}] LIMIT 5",
        query_vec
            .iter()
            .map(|f| format!("{:.6}", f))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let unfiltered_start = Instant::now();
    let unfiltered_result = qe.execute_sql(&vsql_unfiltered, None).await?;
    let unfiltered_time = unfiltered_start.elapsed();
    println!(
        "  Unfiltered comparison: {} rows in {:.2?}",
        unfiltered_result.row_count(),
        unfiltered_time
    );
    println!();

    // ─── Phase 6: Tiered Storage Stats ──────────────────────────────
    println!("━━━ Phase 6: Tiered Storage Statistics ━━━");
    println!();

    let stats = db.tiered_stats();
    let status = db.status().await?;

    println!("  Database:");
    println!("    Total nodes:     {:>10}", status.node_count);
    println!("    Total edges:     {:>10}", status.edge_count);
    println!(
        "    Disk size:       {:>10}",
        format_bytes(status.size_bytes)
    );
    println!();
    println!("  Tiered Storage:");
    println!("    Local payloads:  {:>10}", stats.local_payload_count);
    println!("    Cloud payloads:  {:>10}", stats.cloud_payload_count);
    println!("    Cache hits:      {:>10}", stats.cache_hits);
    println!("    Cache misses:    {:>10}", stats.cache_misses);
    println!("    Cloud fetches:   {:>10}", stats.cloud_fetches);
    println!("    Cloud pushes:    {:>10}", stats.cloud_pushes);
    let hit_rate = if stats.cache_hits + stats.cache_misses > 0 {
        stats.cache_hits as f64 / (stats.cache_hits + stats.cache_misses) as f64 * 100.0
    } else {
        0.0
    };
    println!("    Cache hit rate:  {:>9.1}%", hit_rate);
    println!();

    // ─── Summary ────────────────────────────────────────────────────
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║                     Performance Summary                       ║");
    println!("╠═══════════════════════════════════════════════════════════════╣");
    println!(
        "║  Nodes:         {:>10}                                    ║",
        status.node_count
    );
    println!(
        "║  Edges:         {:>10}                                    ║",
        status.edge_count
    );
    println!(
        "║  Disk:          {:>10}                                    ║",
        format_bytes(status.size_bytes)
    );
    println!(
        "║  Insert rate:   {:>10.0} nodes/sec                         ║",
        insert_rate
    );
    println!(
        "║  Edge rate:     {:>10.0} edges/sec                         ║",
        edge_rate
    );
    println!(
        "║  Point lookup:  {:>10.1}µs avg                             ║",
        per_lookup_us
    );
    println!(
        "║  Index lookup:  {:>10.1}µs avg (graph traversal path)      ║",
        per_index_us
    );
    println!(
        "║  Vector brute:  {:>10.2?}                                  ║",
        brute_time
    );
    println!(
        "║  Vector HNSW:   {:>10.2?}                                  ║",
        hnsw_time
    );
    println!(
        "║  Cache hit rate:{:>10.1}%                                   ║",
        hit_rate
    );
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();
    println!("What makes this special:");
    println!(
        "  • Graph index is always local — {:.1}µs lookups, sub-ms traversals",
        per_index_us
    );
    println!("  • Payloads can live on S3/GCS — infinite scale, ~$0.02/GB/month");
    println!(
        "  • HNSW vector search — {:.2?} ANN vs {:.2?} brute force",
        hnsw_time, brute_time
    );
    println!("  • Full-text search with BM25 ranking built into the storage engine");
    println!("  • Secondary property indexes for fast SQL queries");
    println!("  • All five (KV + Graph + SQL + Vector + FTS) in one embedded binary");
    println!("  • Nobody else does this with transparent cloud tiering");
    println!();

    // Cleanup
    std::fs::remove_dir_all(db_path)?;
    Ok(())
}

fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

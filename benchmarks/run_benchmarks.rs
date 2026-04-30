//! AresaDB Reproducible Benchmark Suite
//!
//! Captures all numbers cited in BENCHMARKS.md and the publication.
//! Run: cargo run --example benchmark_suite --release
//!
//! Output: structured JSON + human-readable summary suitable for
//! inclusion in papers and documentation.

use aresadb::storage::{Database, DistanceMetric};
use aresadb::QueryEngine;
use std::path::Path;
use std::time::Instant;

const WARMUP_ITERS: usize = 3;
const SAMPLE_ITERS: usize = 10;

#[derive(serde::Serialize)]
struct BenchmarkResults {
    metadata: Metadata,
    insert: InsertResults,
    point_lookup: LookupResults,
    index_lookup: LookupResults,
    graph_traversal: TraversalResults,
    sql_query: SqlResults,
    secondary_index: SecondaryIndexResults,
    fulltext_search: FullTextResults,
    vector_search: VectorResults,
}

#[derive(serde::Serialize)]
struct Metadata {
    timestamp: String,
    os: String,
    arch: String,
    node_count: usize,
    edge_count: u64,
    vector_count: usize,
    vector_dim: usize,
}

#[derive(serde::Serialize)]
struct InsertResults {
    individual_rate_per_sec: f64,
    batch_rate_per_sec: f64,
    edge_batch_rate_per_sec: f64,
    batch_speedup: f64,
}

#[derive(serde::Serialize)]
struct LookupResults {
    count: usize,
    total_us: f64,
    mean_us: f64,
    p50_us: f64,
    p99_us: f64,
}

#[derive(serde::Serialize)]
struct TraversalResults {
    depth1_nodes: usize,
    depth1_edges: usize,
    depth1_us: f64,
    depth2_nodes: usize,
    depth2_edges: usize,
    depth2_us: f64,
    depth3_nodes: usize,
    depth3_edges: usize,
    depth3_us: f64,
}

#[derive(serde::Serialize)]
struct SqlResults {
    select_limit_10_us: f64,
    filter_scan_us: f64,
    order_by_us: f64,
}

#[derive(serde::Serialize)]
struct SecondaryIndexResults {
    build_time_ms: f64,
    entries_indexed: u64,
    unindexed_query_us: f64,
    indexed_query_us: f64,
    speedup: f64,
}

#[derive(serde::Serialize)]
struct FullTextResults {
    build_time_ms: f64,
    docs_indexed: u64,
    search_us: f64,
    results_count: usize,
}

#[derive(serde::Serialize)]
struct VectorResults {
    count: usize,
    dim: usize,
    build_time_ms: f64,
    brute_force_us: f64,
    hnsw_us: f64,
    speedup: f64,
    filtered_us: f64,
}

fn median(data: &mut [f64]) -> f64 {
    data.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mid = data.len() / 2;
    if data.len().is_multiple_of(2) {
        (data[mid - 1] + data[mid]) / 2.0
    } else {
        data[mid]
    }
}

fn percentile(data: &mut [f64], p: f64) -> f64 {
    data.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((p / 100.0) * (data.len() - 1) as f64).round() as usize;
    data[idx.min(data.len() - 1)]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_path = Path::new("/tmp/aresadb-benchmark");
    if db_path.exists() {
        std::fs::remove_dir_all(db_path)?;
    }
    std::fs::create_dir_all(db_path)?;

    let node_count = 50_000usize;
    let edge_fan_out = 5usize;
    let vector_count = 10_000usize;
    let vector_dim = 128usize;

    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║          AresaDB Reproducible Benchmark Suite                ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();

    let db = Database::create(db_path, "benchmark").await?;

    // ─── Individual Insert Rate ──────────────────────────────────────
    print!("  Measuring individual insert rate...");
    let indiv_count = 1000;
    let indiv_start = Instant::now();
    for i in 0..indiv_count {
        db.insert_node("warmup", serde_json::json!({"i": i}))
            .await?;
    }
    let indiv_rate = indiv_count as f64 / indiv_start.elapsed().as_secs_f64();
    println!(" {:.0} nodes/sec", indiv_rate);

    // ─── Batch Insert ────────────────────────────────────────────────
    print!("  Batch inserting {} nodes...", node_count);
    let batch_size = 5_000;
    let mut node_ids: Vec<String> = Vec::with_capacity(node_count);
    let batch_start = Instant::now();

    for batch_i in (0..node_count).step_by(batch_size) {
        let batch_end = (batch_i + batch_size).min(node_count);
        let items: Vec<(&str, serde_json::Value)> = (batch_i..batch_end)
            .map(|i| {
                let t = match i % 4 { 0 => "user", 1 => "product", 2 => "order", _ => "review" };
                (t, serde_json::json!({
                    "name": format!("entity_{}", i),
                    "category": format!("cat_{}", i % 50),
                    "score": (i as f64 * 0.01) % 100.0,
                    "description": format!("Entity {} discussing topic {} with details about subject {}", i, i % 50, i % 30),
                }))
            })
            .collect();

        let nodes = db.insert_nodes_batch(items).await?;
        for n in &nodes {
            node_ids.push(n.id.to_string());
        }
    }
    let batch_rate = node_count as f64 / batch_start.elapsed().as_secs_f64();
    println!(" {:.0} nodes/sec", batch_rate);

    // ─── Edge Insert ─────────────────────────────────────────────────
    print!("  Batch inserting edges...");
    let edge_start = Instant::now();
    let mut edge_count = 0u64;
    let edge_batch_size = 5_000;
    let mut edge_buf: Vec<(&str, &str, &str)> = Vec::with_capacity(edge_batch_size);

    for i in 0..node_count {
        for j in 1..=edge_fan_out {
            let target = (i + j * 7 + j * j) % node_count;
            if target != i {
                let etype = match j {
                    1 => "purchased",
                    2 => "reviewed",
                    3 => "follows",
                    4 => "related_to",
                    _ => "similar",
                };
                edge_buf.push((&node_ids[i], &node_ids[target], etype));
                edge_count += 1;
                if edge_buf.len() >= edge_batch_size {
                    db.create_edges_batch(edge_buf).await?;
                    edge_buf = Vec::with_capacity(edge_batch_size);
                }
            }
        }
    }
    if !edge_buf.is_empty() {
        db.create_edges_batch(edge_buf).await?;
    }
    let edge_rate = edge_count as f64 / edge_start.elapsed().as_secs_f64();
    println!(" {} edges at {:.0} edges/sec", edge_count, edge_rate);

    // ─── Point Lookups ───────────────────────────────────────────────
    print!("  Point lookups...");
    // Warmup
    for id in node_ids.iter().take(200) {
        let _ = db.get_node(id).await?;
    }

    let lookup_count = 10_000;
    let mut latencies_us = Vec::with_capacity(lookup_count);
    for i in 0..lookup_count {
        let idx = (i * 97 + 13) % node_ids.len();
        let t = Instant::now();
        let _ = db.get_node(&node_ids[idx]).await?.unwrap();
        latencies_us.push(t.elapsed().as_micros() as f64);
    }
    let total_lookup_us = latencies_us.iter().sum::<f64>();
    let mean_lookup = total_lookup_us / lookup_count as f64;
    let p50_lookup = median(&mut latencies_us.clone());
    let p99_lookup = percentile(&mut latencies_us, 99.0);
    println!(
        " mean={:.1}µs  p50={:.1}µs  p99={:.1}µs",
        mean_lookup, p50_lookup, p99_lookup
    );

    // ─── Index-Only Lookups ──────────────────────────────────────────
    print!("  Index-only lookups...");
    let tiered = db.tiered();
    let mut idx_latencies = Vec::with_capacity(lookup_count);
    for i in 0..lookup_count {
        let idx = (i * 97 + 13) % node_ids.len();
        let node_id = aresadb::NodeId::parse(&node_ids[idx])?;
        let t = Instant::now();
        let _ = tiered.get_node_index(&node_id).await?.unwrap();
        idx_latencies.push(t.elapsed().as_micros() as f64);
    }
    let mean_idx = idx_latencies.iter().sum::<f64>() / lookup_count as f64;
    let p50_idx = median(&mut idx_latencies.clone());
    let p99_idx = percentile(&mut idx_latencies, 99.0);
    println!(
        " mean={:.1}µs  p50={:.1}µs  p99={:.1}µs",
        mean_idx, p50_idx, p99_idx
    );

    // ─── Graph Traversal ─────────────────────────────────────────────
    print!("  Graph traversal...");
    let qe = QueryEngine::new(db.clone());

    let mut d1_times = Vec::new();
    let mut d1_nodes = 0;
    let mut d1_edges = 0;
    for _ in 0..SAMPLE_ITERS {
        let t = Instant::now();
        let r = qe.traverse(&node_ids[0], 1, None).await?;
        d1_times.push(t.elapsed().as_micros() as f64);
        d1_nodes = r.nodes.len();
        d1_edges = r.edges.len();
    }

    let mut d2_times = Vec::new();
    let mut d2_nodes = 0;
    let mut d2_edges = 0;
    for _ in 0..SAMPLE_ITERS {
        let t = Instant::now();
        let r = qe.traverse(&node_ids[0], 2, None).await?;
        d2_times.push(t.elapsed().as_micros() as f64);
        d2_nodes = r.nodes.len();
        d2_edges = r.edges.len();
    }

    let mut d3_times = Vec::new();
    let mut d3_nodes = 0;
    let mut d3_edges = 0;
    for _ in 0..SAMPLE_ITERS {
        let t = Instant::now();
        let r = qe.traverse(&node_ids[0], 3, None).await?;
        d3_times.push(t.elapsed().as_micros() as f64);
        d3_nodes = r.nodes.len();
        d3_edges = r.edges.len();
    }
    println!(
        " d1={:.0}µs  d2={:.0}µs  d3={:.0}µs",
        median(&mut d1_times.clone()),
        median(&mut d2_times.clone()),
        median(&mut d3_times.clone())
    );

    // ─── SQL Queries ─────────────────────────────────────────────────
    print!("  SQL queries...");
    // Warmup
    for _ in 0..WARMUP_ITERS {
        let _ = qe.execute_sql("SELECT * FROM user LIMIT 10", None).await?;
    }

    let mut select_times = Vec::new();
    for _ in 0..SAMPLE_ITERS {
        let t = Instant::now();
        let _ = qe.execute_sql("SELECT * FROM user LIMIT 10", None).await?;
        select_times.push(t.elapsed().as_micros() as f64);
    }

    let mut filter_times = Vec::new();
    for _ in 0..SAMPLE_ITERS {
        let t = Instant::now();
        let _ = qe
            .execute_sql("SELECT * FROM product WHERE score > 50 LIMIT 10", None)
            .await?;
        filter_times.push(t.elapsed().as_micros() as f64);
    }

    let mut order_times = Vec::new();
    for _ in 0..SAMPLE_ITERS {
        let t = Instant::now();
        let _ = qe
            .execute_sql("SELECT * FROM review ORDER BY name LIMIT 10", None)
            .await?;
        order_times.push(t.elapsed().as_micros() as f64);
    }
    println!(
        " select={:.0}µs  filter={:.0}µs  order={:.0}µs",
        median(&mut select_times.clone()),
        median(&mut filter_times.clone()),
        median(&mut order_times.clone())
    );

    // ─── Secondary Index ─────────────────────────────────────────────
    print!("  Secondary index...");
    // Unindexed baseline
    let mut unindexed_times = Vec::new();
    for _ in 0..SAMPLE_ITERS {
        let t = Instant::now();
        let _ = qe
            .execute_sql(
                "SELECT * FROM product WHERE category = 'cat_7' LIMIT 100",
                None,
            )
            .await?;
        unindexed_times.push(t.elapsed().as_micros() as f64);
    }

    let idx_build_start = Instant::now();
    let idx_count = db.create_index("product", "category").await?;
    let idx_build_ms = idx_build_start.elapsed().as_millis() as f64;

    // Refresh planner in a new engine
    let qe2 = QueryEngine::new(db.clone());
    let mut indexed_times = Vec::new();
    for _ in 0..SAMPLE_ITERS {
        let t = Instant::now();
        let _ = qe2
            .execute_sql(
                "SELECT * FROM product WHERE category = 'cat_7' LIMIT 100",
                None,
            )
            .await?;
        indexed_times.push(t.elapsed().as_micros() as f64);
    }
    let unindexed_med = median(&mut unindexed_times.clone());
    let indexed_med = median(&mut indexed_times.clone());
    let idx_speedup = unindexed_med / indexed_med.max(1.0);
    println!(
        " unindexed={:.0}µs  indexed={:.0}µs  speedup={:.1}x",
        unindexed_med, indexed_med, idx_speedup
    );

    // ─── Full-Text Search ────────────────────────────────────────────
    print!("  Full-text search...");
    let ft_build_start = Instant::now();
    let ft_count = db.create_fulltext_index("user", "description").await?;
    let ft_build_ms = ft_build_start.elapsed().as_millis() as f64;

    let mut ft_times = Vec::new();
    let mut ft_result_count = 0;
    for _ in 0..SAMPLE_ITERS {
        let t = Instant::now();
        let results = db
            .fulltext_search("user", "description", "entity topic details", 10)
            .await?;
        ft_times.push(t.elapsed().as_micros() as f64);
        ft_result_count = results.len();
    }
    let ft_med = median(&mut ft_times.clone());
    println!(
        " build={:.0}ms  search={:.0}µs  results={}",
        ft_build_ms, ft_med, ft_result_count
    );

    // ─── Vector Search ───────────────────────────────────────────────
    print!("  Inserting {} vectors...", vector_count);
    let vec_batch_size = 2_000;
    for batch_start in (0..vector_count).step_by(vec_batch_size) {
        let batch_end = (batch_start + vec_batch_size).min(vector_count);
        let mut items: Vec<(&str, serde_json::Value)> = Vec::with_capacity(batch_end - batch_start);
        for i in batch_start..batch_end {
            let emb: Vec<f32> = (0..vector_dim)
                .map(|d| {
                    let s = (i * vector_dim + d) as f64;
                    ((s * 0.1).sin() * 0.5 + (s * 0.03).cos() * 0.5) as f32
                })
                .collect();
            let emb_json: Vec<serde_json::Value> =
                emb.iter().map(|&f| serde_json::json!(f)).collect();
            items.push((
                "document",
                serde_json::json!({
                    "title": format!("doc_{}", i),
                    "topic": format!("topic_{}", i % 20),
                    "embedding": { "$vector": emb_json }
                }),
            ));
        }
        db.insert_nodes_batch(items).await?;
    }

    let hnsw_build_start = Instant::now();
    let _ = db.rebuild_vector_index("document", "embedding").await?;
    let hnsw_build_ms = hnsw_build_start.elapsed().as_millis() as f64;
    println!(" done (HNSW build: {:.0}ms)", hnsw_build_ms);

    let query_vec: Vec<f32> = (0..vector_dim)
        .map(|d| ((d as f64 * 0.1).sin() * 0.5) as f32)
        .collect();

    // Brute force
    let all_docs = db.get_all_by_type("document", None).await?;
    let brute = aresadb::storage::VectorSearch::new(DistanceMetric::Cosine);
    let mut brute_times = Vec::new();
    for _ in 0..SAMPLE_ITERS {
        let t = Instant::now();
        let _ = brute.search(&query_vec, &all_docs, "embedding", 10);
        brute_times.push(t.elapsed().as_micros() as f64);
    }

    // HNSW
    let mut hnsw_times = Vec::new();
    for _ in 0..SAMPLE_ITERS {
        let t = Instant::now();
        let _ = db
            .similarity_search(
                &query_vec,
                "document",
                "embedding",
                10,
                DistanceMetric::Cosine,
            )
            .await?;
        hnsw_times.push(t.elapsed().as_micros() as f64);
    }

    // Filtered vector search
    let qe3 = QueryEngine::new(db.clone());
    let vsql = format!(
        "VECTOR SEARCH document FIELD embedding FOR [{}] WHERE topic = 'topic_5' LIMIT 5",
        query_vec
            .iter()
            .map(|f| format!("{:.6}", f))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let mut filtered_times = Vec::new();
    for _ in 0..SAMPLE_ITERS {
        let t = Instant::now();
        let _ = qe3.execute_sql(&vsql, None).await?;
        filtered_times.push(t.elapsed().as_micros() as f64);
    }

    let brute_med = median(&mut brute_times.clone());
    let hnsw_med = median(&mut hnsw_times.clone());
    let filtered_med = median(&mut filtered_times.clone());
    let vec_speedup = brute_med / hnsw_med.max(1.0);
    println!(
        "  Vector: brute={:.0}µs  HNSW={:.0}µs  speedup={:.1}x  filtered={:.0}µs",
        brute_med, hnsw_med, vec_speedup, filtered_med
    );

    // ─── Build Results ───────────────────────────────────────────────
    let results = BenchmarkResults {
        metadata: Metadata {
            timestamp: chrono::Utc::now().to_rfc3339(),
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            node_count,
            edge_count,
            vector_count,
            vector_dim,
        },
        insert: InsertResults {
            individual_rate_per_sec: indiv_rate,
            batch_rate_per_sec: batch_rate,
            edge_batch_rate_per_sec: edge_rate,
            batch_speedup: batch_rate / indiv_rate.max(1.0),
        },
        point_lookup: LookupResults {
            count: lookup_count,
            total_us: total_lookup_us,
            mean_us: mean_lookup,
            p50_us: p50_lookup,
            p99_us: p99_lookup,
        },
        index_lookup: LookupResults {
            count: lookup_count,
            total_us: idx_latencies.iter().sum(),
            mean_us: mean_idx,
            p50_us: p50_idx,
            p99_us: p99_idx,
        },
        graph_traversal: TraversalResults {
            depth1_nodes: d1_nodes,
            depth1_edges: d1_edges,
            depth1_us: median(&mut d1_times),
            depth2_nodes: d2_nodes,
            depth2_edges: d2_edges,
            depth2_us: median(&mut d2_times),
            depth3_nodes: d3_nodes,
            depth3_edges: d3_edges,
            depth3_us: median(&mut d3_times),
        },
        sql_query: SqlResults {
            select_limit_10_us: median(&mut select_times),
            filter_scan_us: median(&mut filter_times),
            order_by_us: median(&mut order_times),
        },
        secondary_index: SecondaryIndexResults {
            build_time_ms: idx_build_ms,
            entries_indexed: idx_count,
            unindexed_query_us: unindexed_med,
            indexed_query_us: indexed_med,
            speedup: idx_speedup,
        },
        fulltext_search: FullTextResults {
            build_time_ms: ft_build_ms,
            docs_indexed: ft_count,
            search_us: ft_med,
            results_count: ft_result_count,
        },
        vector_search: VectorResults {
            count: vector_count,
            dim: vector_dim,
            build_time_ms: hnsw_build_ms,
            brute_force_us: brute_med,
            hnsw_us: hnsw_med,
            speedup: vec_speedup,
            filtered_us: filtered_med,
        },
    };

    // ─── Output JSON ─────────────────────────────────────────────────
    let json = serde_json::to_string_pretty(&results)?;
    let output_path = "/tmp/aresadb-benchmark-results.json";
    std::fs::write(output_path, &json)?;

    println!();
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║                     Benchmark Results                         ║");
    println!("╠═══════════════════════════════════════════════════════════════╣");
    println!("║                                                               ║");
    println!("║  INSERT THROUGHPUT                                            ║");
    println!(
        "║    Individual:    {:>8.0} nodes/sec                          ║",
        results.insert.individual_rate_per_sec
    );
    println!(
        "║    Batch:         {:>8.0} nodes/sec ({:.0}x speedup)           ║",
        results.insert.batch_rate_per_sec, results.insert.batch_speedup
    );
    println!(
        "║    Edges:         {:>8.0} edges/sec                          ║",
        results.insert.edge_batch_rate_per_sec
    );
    println!("║                                                               ║");
    println!(
        "║  POINT LOOKUP (n={})                                      ║",
        results.point_lookup.count
    );
    println!(
        "║    Payload:  mean={:>6.1}µs  p50={:>6.1}µs  p99={:>6.1}µs     ║",
        results.point_lookup.mean_us, results.point_lookup.p50_us, results.point_lookup.p99_us
    );
    println!(
        "║    Index:    mean={:>6.1}µs  p50={:>6.1}µs  p99={:>6.1}µs     ║",
        results.index_lookup.mean_us, results.index_lookup.p50_us, results.index_lookup.p99_us
    );
    println!("║                                                               ║");
    println!("║  GRAPH TRAVERSAL                                              ║");
    println!(
        "║    Depth 1: {:>3} nodes, {:>3} edges in {:>8.0}µs              ║",
        results.graph_traversal.depth1_nodes,
        results.graph_traversal.depth1_edges,
        results.graph_traversal.depth1_us
    );
    println!(
        "║    Depth 2: {:>3} nodes, {:>3} edges in {:>8.0}µs              ║",
        results.graph_traversal.depth2_nodes,
        results.graph_traversal.depth2_edges,
        results.graph_traversal.depth2_us
    );
    println!(
        "║    Depth 3: {:>3} nodes, {:>3} edges in {:>8.0}µs              ║",
        results.graph_traversal.depth3_nodes,
        results.graph_traversal.depth3_edges,
        results.graph_traversal.depth3_us
    );
    println!("║                                                               ║");
    println!("║  SQL QUERIES (50K records)                                    ║");
    println!(
        "║    SELECT LIMIT 10:     {:>8.0}µs                            ║",
        results.sql_query.select_limit_10_us
    );
    println!(
        "║    WHERE + filter:      {:>8.0}µs                            ║",
        results.sql_query.filter_scan_us
    );
    println!(
        "║    ORDER BY + LIMIT:    {:>8.0}µs                            ║",
        results.sql_query.order_by_us
    );
    println!("║                                                               ║");
    println!("║  SECONDARY INDEX                                              ║");
    println!(
        "║    Build ({:>5} entries):  {:>6.0}ms                           ║",
        results.secondary_index.entries_indexed, results.secondary_index.build_time_ms
    );
    println!(
        "║    Unindexed query:      {:>6.0}µs                            ║",
        results.secondary_index.unindexed_query_us
    );
    println!(
        "║    Indexed query:        {:>6.0}µs                            ║",
        results.secondary_index.indexed_query_us
    );
    println!(
        "║    Speedup:              {:>6.1}x                             ║",
        results.secondary_index.speedup
    );
    println!("║                                                               ║");
    println!("║  FULL-TEXT SEARCH                                             ║");
    println!(
        "║    Build ({:>5} docs):    {:>6.0}ms                           ║",
        results.fulltext_search.docs_indexed, results.fulltext_search.build_time_ms
    );
    println!(
        "║    Search latency:       {:>6.0}µs                            ║",
        results.fulltext_search.search_us
    );
    println!("║                                                               ║");
    println!(
        "║  VECTOR SEARCH ({}D, {} vectors)                         ║",
        results.vector_search.dim, results.vector_search.count
    );
    println!(
        "║    HNSW build:           {:>6.0}ms                            ║",
        results.vector_search.build_time_ms
    );
    println!(
        "║    Brute force (top-10): {:>6.0}µs                            ║",
        results.vector_search.brute_force_us
    );
    println!(
        "║    HNSW ANN (top-10):    {:>6.0}µs                            ║",
        results.vector_search.hnsw_us
    );
    println!(
        "║    Speedup:              {:>6.1}x                             ║",
        results.vector_search.speedup
    );
    println!(
        "║    Filtered search:      {:>6.0}µs                            ║",
        results.vector_search.filtered_us
    );
    println!("║                                                               ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();
    println!("  Results saved to: {}", output_path);

    std::fs::remove_dir_all(db_path)?;
    Ok(())
}

//! Fire Safety Data Test - Live testing with real data from Vercel Blob
//!
//! This example demonstrates AresaDB's capabilities by importing 550K+
//! fire dispatch records from Pittsburgh and running various queries.

use aresadb::{Database, Value};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

const BLOB_URL: &str =
    "https://lgn0alpssagu0n2c.public.blob.vercel-storage.com/fire_dispatches_fresh.csv";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║           AresaDB Fire Safety Data Test                      ║");
    println!("║   Importing records from Vercel Blob Storage                 ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // Setup database path
    let db_path = Path::new("/tmp/aresadb-fire-test/fire_db");

    println!("📁 Opening/creating database at {:?}...", db_path);
    let start = Instant::now();

    // Create or open database
    let db = if db_path.join(".aresadb/config.toml").exists() {
        Database::open(db_path).await?
    } else {
        // Create parent directory if needed
        std::fs::create_dir_all(db_path)?;
        Database::create(db_path, "fire_safety_test").await?
    };
    println!("   ✓ Database ready in {:?}\n", start.elapsed());

    // Fetch data from Vercel Blob
    println!("🌐 Fetching CSV data from Vercel Blob...");
    println!("   URL: {}", BLOB_URL);
    let fetch_start = Instant::now();

    let response = reqwest::get(BLOB_URL).await?;
    let csv_data = response.text().await?;
    let lines: Vec<&str> = csv_data.lines().collect();
    let total_lines = lines.len() - 1; // Exclude header

    println!(
        "   ✓ Fetched {} records in {:?}\n",
        total_lines,
        fetch_start.elapsed()
    );

    // Parse header
    let header: Vec<&str> = lines[0].split(',').collect();
    println!("📋 CSV Headers ({} columns):", header.len());
    for (i, col) in header.iter().enumerate().take(8) {
        println!("   {}. {}", i + 1, col);
    }
    if header.len() > 8 {
        println!("   ... and {} more\n", header.len() - 8);
    }

    // Import data
    println!("📥 Importing fire dispatch records...");
    let import_start = Instant::now();

    let batch_size = 5_000;
    let mut imported = 0;
    let mut errors = 0;

    // Import a subset for testing (first 25,000 records for speed)
    let limit = std::cmp::min(25_000, total_lines);

    for (i, line) in lines.iter().skip(1).take(limit).enumerate() {
        let values = parse_csv_line(line);

        if values.len() < header.len() {
            errors += 1;
            continue;
        }

        // Build JSON properties
        let mut props = serde_json::Map::new();

        for (j, &col) in header.iter().enumerate() {
            if j < values.len() {
                let val = values[j].trim_matches('"');
                props.insert(col.to_string(), serde_json::json!(val));
            }
        }

        // Add computed category
        let category = classify_fire_category(&props);
        props.insert("fire_category".to_string(), serde_json::json!(category));

        // Create node
        match db
            .insert_node("fire_dispatch", serde_json::Value::Object(props))
            .await
        {
            Ok(_) => imported += 1,
            Err(e) => {
                if errors < 5 {
                    eprintln!("   ⚠ Error importing row {}: {}", i, e);
                }
                errors += 1;
            }
        }

        // Progress reporting
        if (i + 1) % batch_size == 0 {
            let elapsed = import_start.elapsed();
            let rate = (i + 1) as f64 / elapsed.as_secs_f64();
            println!(
                "   → Imported {}/{} records ({:.0} records/sec)",
                i + 1,
                limit,
                rate
            );
        }
    }

    let import_elapsed = import_start.elapsed();
    let rate = imported as f64 / import_elapsed.as_secs_f64();

    println!("\n   ✓ Import complete!");
    println!("   ├── Total imported: {}", imported);
    println!("   ├── Errors: {}", errors);
    println!("   ├── Time: {:?}", import_elapsed);
    println!("   └── Rate: {:.0} records/sec\n", rate);

    // Run queries
    println!("🔍 Running queries...\n");

    // Query 1: Database status
    println!("Query 1: Database Status");
    let q1_start = Instant::now();
    let status = db.status().await?;
    println!("   ├── Name: {}", status.name);
    println!("   ├── Nodes: {}", status.node_count);
    println!("   ├── Edges: {}", status.edge_count);
    println!("   ├── Schemas: {}", status.schema_count);
    println!("   └── Size: {} bytes", status.size_bytes);
    println!("   Time: {:?}\n", q1_start.elapsed());

    // Query 2: Get sample records
    println!("Query 2: Sample fire dispatch records");
    let q2_start = Instant::now();
    let nodes = db.get_all_by_type("fire_dispatch", Some(3)).await?;
    for (idx, node) in nodes.iter().enumerate() {
        println!("   Record {}:", idx + 1);
        println!("     ID: {}", node.id);
        println!("     Type: {}", node.node_type);
        // Show a few key properties
        for key in &[
            "call_year",
            "city_name",
            "priority",
            "fire_category",
            "description_short",
        ] {
            if let Some(val) = node.properties.get(*key) {
                println!("     {}: {:?}", key, val);
            }
        }
        println!();
    }
    println!("   Time: {:?}\n", q2_start.elapsed());

    // Query 3: Count by fire category
    println!("Query 3: Count by fire category");
    let q3_start = Instant::now();
    let all_nodes = db.get_all_by_type("fire_dispatch", Some(50_000)).await?;

    let mut category_counts: BTreeMap<String, usize> = BTreeMap::new();
    for node in &all_nodes {
        if let Some(Value::String(cat)) = node.properties.get("fire_category") {
            *category_counts.entry(cat.clone()).or_insert(0) += 1;
        }
    }

    println!("   Results:");
    let mut sorted: Vec<_> = category_counts.iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(a.1));
    for (cat, count) in sorted.iter().take(10) {
        let pct = (**count as f64 / all_nodes.len() as f64) * 100.0;
        println!("     {}: {} ({:.1}%)", cat, count, pct);
    }
    println!("   Time: {:?}\n", q3_start.elapsed());

    // Query 4: Count by city
    println!("Query 4: Top 10 cities by incident count");
    let q4_start = Instant::now();
    let mut city_counts: BTreeMap<String, usize> = BTreeMap::new();
    for node in &all_nodes {
        if let Some(Value::String(city)) = node.properties.get("city_name") {
            *city_counts.entry(city.clone()).or_insert(0) += 1;
        }
    }

    let mut sorted_cities: Vec<_> = city_counts.iter().collect();
    sorted_cities.sort_by(|a, b| b.1.cmp(a.1));
    println!("   Top 10 cities:");
    for (city, count) in sorted_cities.iter().take(10) {
        let pct = (**count as f64 / all_nodes.len() as f64) * 100.0;
        println!("     {}: {} ({:.1}%)", city, count, pct);
    }
    println!("   Time: {:?}\n", q4_start.elapsed());

    // Query 5: High priority incidents
    println!("Query 5: High priority incidents (F1)");
    let q5_start = Instant::now();
    let mut high_priority = 0;
    for node in &all_nodes {
        if let Some(Value::String(priority)) = node.properties.get("priority") {
            if priority == "F1" {
                high_priority += 1;
            }
        }
    }
    let pct = (high_priority as f64 / all_nodes.len() as f64) * 100.0;
    println!("   Result: {} incidents ({:.1}%)", high_priority, pct);
    println!("   Time: {:?}\n", q5_start.elapsed());

    // Query 6: Count by year
    println!("Query 6: Incidents by year");
    let q6_start = Instant::now();
    let mut year_counts: BTreeMap<String, usize> = BTreeMap::new();
    for node in &all_nodes {
        if let Some(Value::String(year)) = node.properties.get("call_year") {
            *year_counts.entry(year.clone()).or_insert(0) += 1;
        }
    }

    let mut sorted_years: Vec<_> = year_counts.iter().collect();
    sorted_years.sort_by(|a, b| a.0.cmp(b.0));
    for (year, count) in &sorted_years {
        println!("     {}: {}", year, count);
    }
    println!("   Time: {:?}\n", q6_start.elapsed());

    // Summary
    let total_time = start.elapsed();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                      Test Summary                            ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!(
        "║  Records imported:    {:>10}                            ║",
        imported
    );
    println!(
        "║  Total nodes:         {:>10}                            ║",
        status.node_count
    );
    println!(
        "║  Import rate:         {:>10.0} rec/sec                   ║",
        rate
    );
    println!(
        "║  Total time:          {:>10.2?}                          ║",
        total_time
    );
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    println!("✅ Fire Safety Data Test Complete!");

    Ok(())
}

/// Parse a CSV line handling quoted fields
fn parse_csv_line(line: &str) -> Vec<&str> {
    let mut fields = Vec::new();
    let mut in_quotes = false;
    let mut start = 0;
    let chars: Vec<char> = line.chars().collect();

    for (i, &c) in chars.iter().enumerate() {
        if c == '"' {
            in_quotes = !in_quotes;
        } else if c == ',' && !in_quotes {
            fields.push(&line[start..i]);
            start = i + 1;
        }
    }
    // Don't forget the last field
    if start <= line.len() {
        fields.push(&line[start..]);
    }

    fields
}

/// Classify fire incidents (mirrors the fire-safety app logic)
fn classify_fire_category(props: &serde_json::Map<String, serde_json::Value>) -> String {
    let desc = props
        .get("description_short")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_uppercase();

    let year: i32 = props
        .get("call_year")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // EMS exclusion
    if desc.contains("EMS") {
        return "Excluded".to_string();
    }

    // Fire alarms (handles pre/post 2020 reclassification)
    let is_pre_alarm = year < 2020 && desc.contains("ALARM");
    let is_post_alarm = year >= 2020 && desc == "REMOVED";
    if is_pre_alarm || is_post_alarm {
        return "Fire Alarms".to_string();
    }

    // Structure fires
    if desc.contains("DWELLING")
        || desc.contains("STRUCTURE")
        || desc.contains("BUILDING")
        || desc.contains("APARTMENT")
    {
        return "Structure Fires".to_string();
    }

    // Outdoor/brush fires
    if desc.contains("BRUSH")
        || desc.contains("GRASS")
        || desc.contains("MULCH")
        || desc.contains("OUTSIDE")
        || desc.contains("OUTDOOR")
        || desc.contains("ILLEGAL FIRE")
    {
        return "Outdoor/Brush Fires".to_string();
    }

    // Electrical issues
    if desc.contains("WIRE")
        || desc.contains("ELECTRICAL")
        || desc.contains("ARCING")
        || desc.contains("TRANSFORMER")
    {
        return "Electrical Issues".to_string();
    }

    // Vehicle fires
    if desc.contains("VEHICLE") || desc.contains("AUTO") || desc.contains("CAR") {
        return "Vehicle Fires".to_string();
    }

    // Gas issues
    if desc.contains("GAS") || desc.contains("NATURAL GAS") {
        return "Gas Issues".to_string();
    }

    // Hazmat/CO
    if desc.contains("HAZMAT") || desc.contains("CO OR HAZMAT") {
        return "Hazmat/CO Issues".to_string();
    }

    // Smoke investigation
    if desc.contains("SMOKE") {
        return "Smoke Investigation".to_string();
    }

    // Uncategorized fire
    if desc.contains("FIRE") {
        return "Uncategorized Fire".to_string();
    }

    "Other".to_string()
}

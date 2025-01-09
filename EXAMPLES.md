# AresaDB Examples

Real-world usage examples and patterns for AresaDB.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Data Modeling](#data-modeling)
3. [CRUD Operations](#crud-operations)
4. [SQL Queries](#sql-queries)
5. [Graph Operations](#graph-operations)
6. [Cloud Storage](#cloud-storage)
7. [Real-World Use Cases](#real-world-use-cases)
8. [Performance Patterns](#performance-patterns)

---

## Quick Start

### Create Your First Database

```bash
# Initialize a new database
aresadb init ./myapp --name "My Application"

# Check status
aresadb -d ./myapp status
```

Output:
```
Database Status
─────────────────────────────────────
  Name: My Application
  Path: ./myapp
  Nodes: 0
  Edges: 0
  Schemas: 0
  Size: 0 B
```

---

## Data Modeling

### E-Commerce Example

```bash
# Users
aresadb -d ./shop insert user --props '{
  "email": "alice@example.com",
  "name": "Alice Johnson",
  "tier": "premium",
  "created": "2024-01-15"
}'

aresadb -d ./shop insert user --props '{
  "email": "bob@example.com",
  "name": "Bob Smith",
  "tier": "standard",
  "created": "2024-02-20"
}'

# Products
aresadb -d ./shop insert product --props '{
  "sku": "LAPTOP-001",
  "name": "Pro Laptop 15",
  "price": 1299.99,
  "category": "Electronics",
  "stock": 50
}'

aresadb -d ./shop insert product --props '{
  "sku": "PHONE-001",
  "name": "SmartPhone X",
  "price": 899.99,
  "category": "Electronics",
  "stock": 100
}'

# Orders
aresadb -d ./shop insert order --props '{
  "order_id": "ORD-2024-001",
  "user_email": "alice@example.com",
  "total": 2199.98,
  "status": "delivered",
  "items": ["LAPTOP-001", "PHONE-001"]
}'
```

### Blog Platform Example

```bash
# Authors
aresadb -d ./blog insert author --props '{
  "username": "techwriter",
  "name": "Tech Writer",
  "bio": "Writing about technology and programming",
  "followers": 5000
}'

# Posts
aresadb -d ./blog insert post --props '{
  "slug": "intro-to-rust",
  "title": "Introduction to Rust Programming",
  "author": "techwriter",
  "published": "2024-03-01",
  "tags": ["rust", "programming", "tutorial"],
  "views": 15000
}'

# Comments
aresadb -d ./blog insert comment --props '{
  "post_slug": "intro-to-rust",
  "author": "reader123",
  "content": "Great article! Very helpful.",
  "likes": 42
}'
```

### IoT Sensor Data Example

```bash
# Devices
aresadb -d ./iot insert device --props '{
  "device_id": "SENSOR-001",
  "type": "temperature",
  "location": "Building A, Floor 2",
  "status": "active"
}'

# Readings (time-series style)
aresadb -d ./iot insert reading --props '{
  "device_id": "SENSOR-001",
  "timestamp": "2024-11-28T10:00:00Z",
  "temperature": 22.5,
  "humidity": 45.2,
  "battery": 85
}'

aresadb -d ./iot insert reading --props '{
  "device_id": "SENSOR-001",
  "timestamp": "2024-11-28T10:05:00Z",
  "temperature": 22.7,
  "humidity": 44.8,
  "battery": 85
}'
```

---

## CRUD Operations

### Create (Insert)

```bash
# Simple insert
aresadb -d ./db insert user --props '{"name": "John", "age": 30}'

# Complex nested data
aresadb -d ./db insert profile --props '{
  "user": "john",
  "preferences": {
    "theme": "dark",
    "notifications": true,
    "language": "en"
  },
  "tags": ["developer", "rust", "databases"]
}'
```

### Read (Get/Query)

```bash
# Get by ID
aresadb -d ./db get 123e4567-e89b-12d3-a456-426614174000

# Get all of a type
aresadb -d ./db view user --as table

# SQL query
aresadb -d ./db query "SELECT * FROM user WHERE age > 25"

# JSON output
aresadb -d ./db -f json query "SELECT name, email FROM user"
```

### Update

Using the library (Rust)

```rust
let updated = db.update_node(
    "123e4567-e89b-12d3-a456-426614174000",
    serde_json::json!({"age": 31, "status": "verified"})
).await?;
```

### Delete

```bash
# Delete by ID
aresadb -d ./db delete 123e4567-e89b-12d3-a456-426614174000
```

---

## SQL Queries

### Basic Queries

```bash
# Select all columns
aresadb -d ./db query "SELECT * FROM users"

# Select specific columns
aresadb -d ./db query "SELECT name, email FROM users"

# With WHERE clause
aresadb -d ./db query "SELECT * FROM products WHERE price > 100"

# Multiple conditions
aresadb -d ./db query "SELECT * FROM orders WHERE status = 'pending' AND total > 50"

# Ordering
aresadb -d ./db query "SELECT * FROM products ORDER BY price DESC"

# Limiting results
aresadb -d ./db query "SELECT * FROM logs LIMIT 100"
```

### Complex Queries

```bash
# String matching (exact)
aresadb -d ./db query "SELECT * FROM users WHERE email = 'john@example.com'"

# Numeric comparisons
aresadb -d ./db query "SELECT * FROM products WHERE price >= 10 AND price <= 100"

# Combined sorting and limiting
aresadb -d ./db query "SELECT * FROM posts ORDER BY views DESC LIMIT 10"
```

### Output Formats

```bash
# Table format (default)
aresadb -d ./db query "SELECT * FROM users"

# JSON format
aresadb -d ./db -f json query "SELECT * FROM users"

# CSV format (for export)
aresadb -d ./db -f csv query "SELECT * FROM users" > users.csv
```

---

## Graph Operations

### Creating Relationships

```rust
// Rust library usage
use aresadb::Database;

// Create users
let alice = db.insert_node("user", serde_json::json!({
    "name": "Alice",
    "email": "alice@example.com"
})).await?;

let bob = db.insert_node("user", serde_json::json!({
    "name": "Bob",
    "email": "bob@example.com"
})).await?;

// Create "follows" relationship
db.create_edge(
    &alice.id.to_string(),
    &bob.id.to_string(),
    "follows",
    Some(serde_json::json!({"since": "2024-01-01"}))
).await?;

// Create order for alice
let order = db.insert_node("order", serde_json::json!({
    "total": 150.00,
    "status": "delivered"
})).await?;

// Link order to user
db.create_edge(
    &alice.id.to_string(),
    &order.id.to_string(),
    "placed",
    None
).await?;
```

### Traversing Relationships

```bash
# CLI traversal
aresadb -d ./db traverse <user_id> --depth 2

# Traverse from a node, filtering by edge type
aresadb -d ./db traverse <user_id> --depth 1 --edges placed
```

```rust
// Rust library
// Get all orders placed by user
let orders = db.get_edges_from(&user_id, Some("placed")).await?;

// Get all followers of user
let followers = db.get_edges_to(&user_id, Some("follows")).await?;
```

### Graph View

```bash
# View data as graph
aresadb -d ./db view user --as graph
```

---

## Cloud Storage

### AWS S3

```bash
# Set credentials
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_REGION=us-east-1

# Push local database to S3
aresadb -d ./myapp push s3://my-bucket/databases/myapp

# Connect to a remote database and launch REPL
aresadb connect s3://my-bucket/databases/myapp --readonly
# Then use the REPL to query

# Sync changes
aresadb -d ./myapp sync s3://my-bucket/databases/myapp
```

### Google Cloud Storage

```bash
# Set credentials
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Push to GCS
aresadb -d ./myapp push gs://my-bucket/databases/myapp

aresadb connect gs://my-bucket/databases/myapp
# Use the REPL to query interactively
```

### Workflow: Local Development → Cloud Deploy

```bash
# 1. Develop locally
aresadb init ./dev-db --name "my-app-dev"
aresadb -d ./dev-db insert user --props '{"name": "Test User"}'

# 2. Test your changes
aresadb -d ./dev-db query "SELECT * FROM user"

# 3. Push to staging
aresadb -d ./dev-db push s3://my-bucket/staging/my-app

# 4. Push to production
aresadb -d ./dev-db push s3://my-bucket/production/my-app
```

---

## Real-World Use Cases

### Research Data Management

```bash
# Initialize research database
aresadb init ./research --name "ML Experiments"

# Store experiment configurations
aresadb -d ./research insert experiment --props '{
  "name": "bert-fine-tuning-v1",
  "model": "bert-base-uncased",
  "dataset": "squad-v2",
  "hyperparameters": {
    "learning_rate": 0.00002,
    "batch_size": 32,
    "epochs": 3
  },
  "started_at": "2024-11-28T10:00:00Z"
}'

# Store results
aresadb -d ./research insert result --props '{
  "experiment": "bert-fine-tuning-v1",
  "metrics": {
    "accuracy": 0.892,
    "f1_score": 0.876,
    "loss": 0.234
  },
  "completed_at": "2024-11-28T14:30:00Z"
}'

# Query best experiments
aresadb -d ./research query "SELECT * FROM result ORDER BY accuracy DESC LIMIT 5"
```

### Analytics Data Pipeline

```bash
# Initialize analytics database
aresadb init ./analytics --name "Web Analytics"

# Store page views
for i in {1..1000}; do
  aresadb -d ./analytics insert pageview --props "{
    \"page\": \"/products/$i\",
    \"user_agent\": \"Mozilla/5.0\",
    \"timestamp\": \"2024-11-28T$((10 + i % 14)):$((i % 60)):00Z\",
    \"duration_seconds\": $((30 + RANDOM % 300))
  }"
done

# Analyze
aresadb -d ./analytics query "SELECT * FROM pageview ORDER BY duration_seconds DESC LIMIT 10"
```

### Fire Safety Data (Real Example)

```rust
// This example imports 550K+ fire dispatch records
use aresadb::Database;
use std::path::Path;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Fetch data from Vercel Blob
    let csv_url = "https://lgn0alpssagu0n2c.public.blob.vercel-storage.com/fire_dispatches_fresh.csv";
    let response = reqwest::get(csv_url).await?;
    let csv_data = response.text().await?;

    // Create database
    let db = Database::create(Path::new("./fire_db"), "fire_safety").await?;

    // Parse and import
    for line in csv_data.lines().skip(1).take(25000) {
        let fields: Vec<&str> = line.split(',').collect();

        db.insert_node("fire_dispatch", serde_json::json!({
            "call_year": fields[6],
            "city_name": fields[8],
            "priority": fields[3],
            "description": fields[7]
        })).await?;
    }

    // Query
    let status = db.status().await?;
    println!("Imported {} records", status.node_count);

    // Analyze by city
    let nodes = db.get_all_by_type("fire_dispatch", None).await?;
    // ... aggregation logic

    Ok(())
}
```

---

## Performance Patterns

### Batch Inserts

```rust
// For bulk data loading, batch your inserts
use futures::future::join_all;

async fn batch_insert(db: &Database, items: Vec<serde_json::Value>) -> Result<()> {
    let batch_size = 100;

    for chunk in items.chunks(batch_size) {
        let futures: Vec<_> = chunk
            .iter()
            .map(|item| db.insert_node("item", item.clone()))
            .collect();

        join_all(futures).await;
    }

    Ok(())
}
```

### Efficient Queries

```bash
# Use LIMIT to avoid loading everything
aresadb -d ./db query "SELECT * FROM logs LIMIT 1000"

# Select only needed columns
aresadb -d ./db query "SELECT id, name FROM users"  # faster than SELECT *

# Filter early
aresadb -d ./db query "SELECT * FROM events WHERE date > '2024-01-01'"
```

### Caching Patterns

```rust
// The database has built-in caching, but you can optimize access patterns
use std::collections::HashMap;

// Cache frequently accessed data
let mut user_cache: HashMap<String, Node> = HashMap::new();

async fn get_user_cached(
    db: &Database,
    cache: &mut HashMap<String, Node>,
    user_id: &str
) -> Result<Node> {
    if let Some(user) = cache.get(user_id) {
        return Ok(user.clone());
    }

    let user = db.get_node(user_id).await?.unwrap();
    cache.insert(user_id.to_string(), user.clone());
    Ok(user)
}
```

### Index Optimization

```bash
# Use type-based queries which leverage indexes
aresadb -d ./db view user --as table          # Uses type index ✓
aresadb -d ./db query "SELECT * FROM user"    # Uses type index ✓
aresadb -d ./db get <uuid>                     # Direct lookup ✓
```

---

## Tips and Best Practices

### Data Modeling Tips

1. **Use meaningful node types**: `user`, `order`, `product` not `data1`, `data2`
2. **Keep properties flat when possible**: Easier to query
3. **Use arrays for lists**: `["tag1", "tag2"]` works well
4. **Store timestamps as ISO strings**: `"2024-11-28T10:00:00Z"`

### Query Tips

1. **Always use LIMIT for exploratory queries**
2. **Use JSON output for programmatic access**: `-f json`
3. **Export large results to CSV**: `-f csv > output.csv`

### Performance Tips

1. **Batch inserts for bulk data**
2. **Use specific columns in SELECT**
3. **Filter early with WHERE**
4. **Consider data locality for related items**

---

## Troubleshooting

### Common Issues

**Database locked:**
```bash
# Another process has the database open
# Close other aresadb processes or wait
lsof +D ./mydb  # Find processes using the database
```

**Query returns empty:**
```bash
# Check if data exists
aresadb -d ./db status
aresadb -d ./db view <type> --as table

# Check query syntax
aresadb -d ./db query "SELECT * FROM \"order\""  # Reserved word needs quotes
```

**Cloud sync fails:**
```bash
# Check credentials
echo $AWS_ACCESS_KEY_ID
echo $GOOGLE_APPLICATION_CREDENTIALS

# Check bucket permissions
aws s3 ls s3://my-bucket/
gsutil ls gs://my-bucket/
```

---

*For more examples, see the `examples/` directory in the repository.*


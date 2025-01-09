# aresadb

Python bindings for [AresaDB](https://github.com/yoreai/aresadb) — a high-performance multi-model database engine in Rust.

**Key-Value · Graph · SQL · Vector Search · Full-Text Search — all from Python.**

## Install

```bash
pip install aresadb
# or
uv add aresadb
```

## Quick Start

```python
from aresadb_python import Database

db = Database.create("./mydata", "demo")

# Insert nodes
user = db.insert_dict("user", {"name": "Alice", "age": 30})

# Batch insert
nodes = db.insert_batch([
    ("user", {"name": "Bob", "age": 25}),
    ("user", {"name": "Charlie", "age": 35}),
])

# Get / Update / Delete
node = db.get(user.id)
updated = db.update(user.id, {"name": "Alice", "age": 31})
db.delete(user.id)

# List by type
users = db.get_by_type("user", limit=100)
```

## SQL Queries

```python
result = db.query("SELECT * FROM user WHERE age > 28 ORDER BY name LIMIT 10")
for row in result.rows:
    print(dict(zip(result.columns, row)))

db.query("INSERT INTO product (name, price) VALUES ('Widget', 9.99)")
db.query("UPDATE user SET age = 31 WHERE name = 'Alice'")
db.query("DELETE FROM user WHERE age < 18")
```

## Graph (Edges & Traversal)

```python
alice = db.insert_dict("user", {"name": "Alice"})
bob = db.insert_dict("user", {"name": "Bob"})
charlie = db.insert_dict("user", {"name": "Charlie"})

# Create edges (with optional properties)
edge = db.create_edge(alice.id, bob.id, "follows", {"since": "2024"})

# Batch edges
db.create_edges_batch([
    (bob.id, charlie.id, "follows"),
    (alice.id, charlie.id, "knows"),
])

# Query edges
outgoing = db.get_edges_from(alice.id)
incoming = db.get_edges_to(charlie.id, edge_type="follows")
db.delete_edge(edge.id)

# BFS traversal
result = db.traverse(alice.id, max_depth=3, edge_types=["follows"])
print(f"Visited {len(result.nodes)} nodes over {result.depth} hops")
print(result.adjacency)  # {node_id: [neighbor_ids]}

# Shortest path
path = db.shortest_path(alice.id, charlie.id)
if path:
    print(" -> ".join(n.properties.get("name", n.id) for n in path))

# Connected components
components = db.connected_components("user")
print(f"{len(components)} component(s)")
```

## Secondary Indexes

```python
db.create_index("user", "age")        # B-tree index
results = db.index_lookup("user", "age", 30)
db.list_indexes()                      # [("user", "age")]
db.drop_index("user", "age")
```

## Full-Text Search (BM25)

```python
db.insert("article", '{"title": "Rust", "body": "Rust is a systems language"}')
db.insert("article", '{"title": "Python", "body": "Python is great for ML"}')

db.create_fulltext_index("article", "body")

results = db.fulltext_search("article", "body", "Rust systems", limit=5)
for r in results:
    print(f"Score: {r.score:.3f}  Node: {r.node.properties}")

db.list_fulltext_indexes()  # [("article", "body")]
```

## Vector Search (HNSW)

```python
# Insert with embedding
doc = db.insert_with_embedding(
    "document",
    '{"title": "ML Intro"}',
    "embedding",
    [0.9, 0.1, 0.0, 0.0],
)

# k-NN search (HNSW-accelerated)
results = db.similarity_search(
    query_vector=[1.0, 0.0, 0.0, 0.0],
    node_type="document",
    field="embedding",
    k=10,
    metric="cosine",  # cosine | euclidean | dot | manhattan
)

# Radius search (exact, brute-force)
near = db.similarity_search_radius(
    [1.0, 0.0, 0.0, 0.0], "document", "embedding",
    max_distance=0.5, metric="euclidean",
)

# Retrieve node + embedding
node, vec = db.get_node_with_embedding(doc.id, "embedding")

# Rebuild HNSW index after bulk inserts
stats = db.rebuild_vector_index("document", "embedding")
print(f"{stats.num_vectors} vectors, dim={stats.dimension}")
```

## Introspection

```python
status = db.status()
print(f"{status.name}: {status.node_count} nodes, {status.edge_count} edges, {status.size_bytes} bytes")
print(db.name(), db.path())
```

## Type Stubs

A `.pyi` stub file ships with the package for full IDE autocompletion and type checking.

## Running Tests

```bash
cd python/
uv venv .venv && source .venv/bin/activate
uv pip install maturin pytest
maturin develop --release
pytest tests/ -v
```

## License

MIT

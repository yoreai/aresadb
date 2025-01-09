"""Type stubs for the AresaDB Python bindings (PyO3)."""

from __future__ import annotations
from typing import Any, Optional

class PyNode:
    """A node in the AresaDB property graph."""

    id: str
    node_type: str
    properties: dict[str, Any]
    created_at: str
    updated_at: str

class PyEdge:
    """An edge (relationship) between two nodes."""

    id: str
    from_id: str
    to_id: str
    edge_type: str
    properties: dict[str, Any]
    created_at: str

class PySearchResult:
    """A single vector similarity search result."""

    node_id: str
    score: float
    distance: float

class PyFulltextResult:
    """A single full-text search result with BM25 score."""

    node: PyNode
    score: float

class PyQueryResult:
    """Result of a SQL query execution."""

    columns: list[str]
    rows: list[list[Any]]
    rows_affected: int
    execution_time_ms: int

class PyDatabaseStatus:
    """Database introspection snapshot."""

    name: str
    path: str
    node_count: int
    edge_count: int
    schema_count: int
    size_bytes: int

class PyTraversalResult:
    """Result of a graph traversal (BFS)."""

    root: PyNode
    nodes: list[PyNode]
    edges: list[PyEdge]
    depth: int
    adjacency: dict[str, list[str]]

class PyIndexStats:
    """Statistics for a vector HNSW index."""

    num_vectors: int
    dimension: int
    max_connections: int
    max_layers: int
    total_connections: int
    avg_connections: float

class Database:
    """AresaDB database handle.

    Provides access to all storage models: KV, Graph, SQL, Vector Search,
    and Full-Text Search through a unified property-graph engine.
    """

    # --- Constructors ---

    @staticmethod
    def create(path: str, name: str) -> Database:
        """Create a new database at ``path`` with the given ``name``."""
        ...

    @staticmethod
    def open(path: str) -> Database:
        """Open an existing database at ``path``."""
        ...

    # --- Node Operations ---

    def insert(self, node_type: str, properties: str) -> PyNode:
        """Insert a node.  ``properties`` is a JSON string."""
        ...

    def insert_dict(self, node_type: str, properties: dict[str, Any]) -> PyNode:
        """Insert a node with a native Python dict for properties."""
        ...

    def insert_batch(self, items: list[tuple[str, dict[str, Any]]]) -> list[PyNode]:
        """Batch-insert nodes. Each item is ``(node_type, properties_dict)``."""
        ...

    def get(self, id: str) -> Optional[PyNode]:
        """Retrieve a node by ID, or ``None`` if not found."""
        ...

    def update(self, id: str, properties: dict[str, Any]) -> PyNode:
        """Update a node's properties (merge semantics)."""
        ...

    def delete(self, id: str) -> None:
        """Delete a node by ID."""
        ...

    def get_by_type(self, node_type: str, limit: Optional[int] = None) -> list[PyNode]:
        """List nodes of a given type, optionally limited."""
        ...

    # --- Edge Operations ---

    def create_edge(
        self,
        from_id: str,
        to_id: str,
        edge_type: str,
        properties: Optional[dict[str, Any]] = None,
    ) -> PyEdge:
        """Create a directed edge between two nodes."""
        ...

    def create_edges_batch(
        self, edges: list[tuple[str, str, str]]
    ) -> list[PyEdge]:
        """Batch-create edges. Each item is ``(from_id, to_id, edge_type)``."""
        ...

    def get_edges_from(
        self, node_id: str, edge_type: Optional[str] = None
    ) -> list[PyEdge]:
        """Get outgoing edges from a node, optionally filtered by type."""
        ...

    def get_edges_to(
        self, node_id: str, edge_type: Optional[str] = None
    ) -> list[PyEdge]:
        """Get incoming edges to a node, optionally filtered by type."""
        ...

    def delete_edge(self, edge_id: str) -> None:
        """Delete an edge by ID."""
        ...

    # --- SQL ---

    def query(self, sql: str, limit: Optional[int] = None) -> PyQueryResult:
        """Execute a SQL statement and return the result."""
        ...

    # --- Graph Traversal ---

    def traverse(
        self,
        start_node_id: str,
        max_depth: int,
        edge_types: Optional[list[str]] = None,
    ) -> PyTraversalResult:
        """BFS traversal from a start node up to ``max_depth`` hops."""
        ...

    def shortest_path(
        self, from_id: str, to_id: str, max_depth: int = 10
    ) -> Optional[list[PyNode]]:
        """Find the shortest path between two nodes (BFS)."""
        ...

    def connected_components(self, node_type: str) -> list[list[PyNode]]:
        """Return connected components for nodes of a given type."""
        ...

    # --- Secondary Indexes ---

    def create_index(self, node_type: str, field: str) -> int:
        """Create a B-tree secondary index. Returns count of indexed nodes."""
        ...

    def drop_index(self, node_type: str, field: str) -> None:
        """Drop a secondary index."""
        ...

    def list_indexes(self) -> list[tuple[str, str]]:
        """List all secondary indexes as ``(node_type, field)`` pairs."""
        ...

    def index_lookup(
        self, node_type: str, field: str, value: Any
    ) -> Optional[list[PyNode]]:
        """Exact-match lookup via a secondary index."""
        ...

    # --- Full-Text Search ---

    def create_fulltext_index(self, node_type: str, field: str) -> int:
        """Create an inverted full-text index. Returns count of indexed docs."""
        ...

    def fulltext_search(
        self,
        node_type: str,
        field: str,
        query: str,
        limit: int = 10,
    ) -> list[PyFulltextResult]:
        """BM25-ranked full-text search."""
        ...

    def list_fulltext_indexes(self) -> list[tuple[str, str]]:
        """List all full-text indexes as ``(node_type, field)`` pairs."""
        ...

    # --- Vector / Embedding Operations ---

    def insert_with_embedding(
        self,
        node_type: str,
        properties: str,
        field: str,
        vector: list[float],
    ) -> PyNode:
        """Insert a node with a vector embedding (JSON string props)."""
        ...

    def similarity_search(
        self,
        query_vector: list[float],
        node_type: str,
        field: str,
        k: int,
        metric: str = "cosine",
    ) -> list[PySearchResult]:
        """HNSW-accelerated k-NN search. Metrics: cosine, euclidean, dot, manhattan."""
        ...

    def similarity_search_radius(
        self,
        query_vector: list[float],
        node_type: str,
        field: str,
        max_distance: float,
        metric: str = "cosine",
    ) -> list[PySearchResult]:
        """Find all vectors within ``max_distance`` (brute-force exact)."""
        ...

    def get_node_with_embedding(
        self, id: str, embedding_field: str
    ) -> Optional[tuple[PyNode, Optional[list[float]]]]:
        """Retrieve a node and its embedding vector."""
        ...

    def rebuild_vector_index(
        self, node_type: str, embedding_field: str
    ) -> PyIndexStats:
        """Rebuild the HNSW index from stored vectors."""
        ...

    # --- Introspection ---

    def status(self) -> PyDatabaseStatus:
        """Get database status (counts, size, etc.)."""
        ...

    def name(self) -> str:
        """Get the database name."""
        ...

    def path(self) -> str:
        """Get the database filesystem path."""
        ...

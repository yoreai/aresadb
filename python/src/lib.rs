use pyo3::prelude::*;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::types::{PyDict, PyList};
use std::path::PathBuf;
use std::sync::Arc;
use aresadb::query::DistanceMetric;

fn runtime() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to create tokio runtime")
    })
}

fn to_pyerr(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

fn value_to_py(py: Python<'_>, val: &aresadb::Value) -> PyResult<PyObject> {
    let json_val = val.to_json();
    let json_str = serde_json::to_string(&json_val).map_err(to_pyerr)?;
    let json_mod = py.import_bound("json")?;
    Ok(json_mod.call_method1("loads", (json_str,))?.unbind())
}

fn props_to_py(py: Python<'_>, props: &std::collections::BTreeMap<String, aresadb::Value>) -> PyResult<PyObject> {
    let json_map: serde_json::Map<String, serde_json::Value> = props
        .iter()
        .map(|(k, v)| (k.clone(), v.to_json()))
        .collect();
    let json_val = serde_json::Value::Object(json_map);
    let json_str = serde_json::to_string(&json_val).map_err(to_pyerr)?;
    let json_mod = py.import_bound("json")?;
    Ok(json_mod.call_method1("loads", (json_str,))?.unbind())
}

fn py_to_json_string(py: Python<'_>, obj: PyObject) -> PyResult<String> {
    let json_mod = py.import_bound("json")?;
    let json_str: String = json_mod.call_method1("dumps", (obj,))?.extract()?;
    Ok(json_str)
}

// ========== PyNode ==========

#[pyclass]
struct PyNode {
    #[pyo3(get)]
    id: String,
    #[pyo3(get)]
    node_type: String,
    #[pyo3(get)]
    properties: PyObject,
    #[pyo3(get)]
    created_at: String,
    #[pyo3(get)]
    updated_at: String,
}

#[pymethods]
impl PyNode {
    fn __repr__(&self) -> String {
        format!("Node(id='{}', type='{}')", self.id, self.node_type)
    }
}

impl PyNode {
    fn from_node(py: Python<'_>, node: &aresadb::Node) -> PyResult<Self> {
        let properties = props_to_py(py, &node.properties)?;
        Ok(PyNode {
            id: node.id.to_string(),
            node_type: node.node_type.clone(),
            properties,
            created_at: node.created_at.to_datetime().to_rfc3339(),
            updated_at: node.updated_at.to_datetime().to_rfc3339(),
        })
    }
}

// ========== PyEdge ==========

#[pyclass]
struct PyEdge {
    #[pyo3(get)]
    id: String,
    #[pyo3(get)]
    from_id: String,
    #[pyo3(get)]
    to_id: String,
    #[pyo3(get)]
    edge_type: String,
    #[pyo3(get)]
    properties: PyObject,
    #[pyo3(get)]
    created_at: String,
}

#[pymethods]
impl PyEdge {
    fn __repr__(&self) -> String {
        format!("Edge(id='{}', type='{}', {} -> {})", self.id, self.edge_type, self.from_id, self.to_id)
    }
}

impl PyEdge {
    fn from_edge(py: Python<'_>, edge: &aresadb::Edge) -> PyResult<Self> {
        let properties = props_to_py(py, &edge.properties)?;
        Ok(PyEdge {
            id: edge.id.to_string(),
            from_id: edge.from.to_string(),
            to_id: edge.to.to_string(),
            edge_type: edge.edge_type.clone(),
            properties,
            created_at: edge.created_at.to_datetime().to_rfc3339(),
        })
    }
}

// ========== PySearchResult ==========

#[pyclass]
struct PySearchResult {
    #[pyo3(get)]
    node_id: String,
    #[pyo3(get)]
    score: f64,
    #[pyo3(get)]
    distance: f64,
}

#[pymethods]
impl PySearchResult {
    fn __repr__(&self) -> String {
        format!("SearchResult(node_id='{}', score={:.4})", self.node_id, self.score)
    }
}

// ========== PyFulltextResult ==========

#[pyclass]
struct PyFulltextResult {
    #[pyo3(get)]
    node: PyObject,
    #[pyo3(get)]
    score: f64,
}

#[pymethods]
impl PyFulltextResult {
    fn __repr__(&self) -> String {
        format!("FulltextResult(score={:.4})", self.score)
    }
}

// ========== PyQueryResult ==========

#[pyclass]
struct PyQueryResult {
    #[pyo3(get)]
    columns: Vec<String>,
    #[pyo3(get)]
    rows: PyObject,
    #[pyo3(get)]
    rows_affected: u64,
    #[pyo3(get)]
    execution_time_ms: u64,
}

#[pymethods]
impl PyQueryResult {
    fn __repr__(&self) -> String {
        format!("QueryResult(columns={:?}, rows_affected={})", self.columns, self.rows_affected)
    }
}

// ========== PyDatabaseStatus ==========

#[pyclass]
struct PyDatabaseStatus {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    path: String,
    #[pyo3(get)]
    node_count: u64,
    #[pyo3(get)]
    edge_count: u64,
    #[pyo3(get)]
    schema_count: u64,
    #[pyo3(get)]
    size_bytes: u64,
}

#[pymethods]
impl PyDatabaseStatus {
    fn __repr__(&self) -> String {
        format!(
            "DatabaseStatus(name='{}', nodes={}, edges={}, size={})",
            self.name, self.node_count, self.edge_count, self.size_bytes
        )
    }
}

// ========== PyTraversalResult ==========

#[pyclass]
struct PyTraversalResult {
    #[pyo3(get)]
    root: PyObject,
    #[pyo3(get)]
    nodes: PyObject,
    #[pyo3(get)]
    edges: PyObject,
    #[pyo3(get)]
    depth: u32,
    #[pyo3(get)]
    adjacency: PyObject,
}

#[pymethods]
impl PyTraversalResult {
    fn __repr__(&self) -> String {
        format!("TraversalResult(depth={})", self.depth)
    }
}

// ========== PyIndexStats ==========

#[pyclass]
struct PyIndexStats {
    #[pyo3(get)]
    num_vectors: usize,
    #[pyo3(get)]
    dimension: usize,
    #[pyo3(get)]
    max_connections: usize,
    #[pyo3(get)]
    max_layers: usize,
    #[pyo3(get)]
    total_connections: usize,
    #[pyo3(get)]
    avg_connections: f64,
}

#[pymethods]
impl PyIndexStats {
    fn __repr__(&self) -> String {
        format!("IndexStats(vectors={}, dim={})", self.num_vectors, self.dimension)
    }
}

// ========== Database ==========

struct DbInner {
    db: aresadb::Database,
    engine: aresadb::QueryEngine,
}

#[pyclass]
struct Database {
    inner: Arc<DbInner>,
}

impl Database {
    fn db(&self) -> &aresadb::Database {
        &self.inner.db
    }

    fn engine(&self) -> &aresadb::QueryEngine {
        &self.inner.engine
    }
}

fn parse_metric(metric: &str) -> PyResult<DistanceMetric> {
    match metric {
        "cosine" => Ok(DistanceMetric::Cosine),
        "euclidean" | "l2" => Ok(DistanceMetric::Euclidean),
        "dot" | "dotproduct" => Ok(DistanceMetric::DotProduct),
        "manhattan" | "l1" => Ok(DistanceMetric::Manhattan),
        _ => Err(PyValueError::new_err(
            format!("Unknown metric '{}'. Use: cosine, euclidean, dot, manhattan", metric)
        )),
    }
}

#[pymethods]
impl Database {
    // ===== Constructors =====

    #[staticmethod]
    #[pyo3(signature = (path, name))]
    fn create(path: &str, name: &str) -> PyResult<Self> {
        let db = runtime()
            .block_on(aresadb::Database::create(PathBuf::from(path), name))
            .map_err(to_pyerr)?;
        let engine = aresadb::QueryEngine::new(db.clone());
        Ok(Database {
            inner: Arc::new(DbInner { db, engine }),
        })
    }

    #[staticmethod]
    #[pyo3(signature = (path,))]
    fn open(path: &str) -> PyResult<Self> {
        let db = runtime()
            .block_on(aresadb::Database::open(PathBuf::from(path)))
            .map_err(to_pyerr)?;
        let engine = aresadb::QueryEngine::new(db.clone());
        Ok(Database {
            inner: Arc::new(DbInner { db, engine }),
        })
    }

    // ===== Node Operations =====

    #[pyo3(signature = (node_type, properties))]
    fn insert(&self, py: Python<'_>, node_type: &str, properties: &str) -> PyResult<PyNode> {
        let json_val: serde_json::Value = serde_json::from_str(properties)
            .map_err(|e| PyValueError::new_err(format!("Invalid JSON: {e}")))?;
        let node = runtime()
            .block_on(self.db().insert_node(node_type, json_val))
            .map_err(to_pyerr)?;
        PyNode::from_node(py, &node)
    }

    #[pyo3(signature = (node_type, properties))]
    fn insert_dict(&self, py: Python<'_>, node_type: &str, properties: PyObject) -> PyResult<PyNode> {
        let json_str = py_to_json_string(py, properties)?;
        self.insert(py, node_type, &json_str)
    }

    #[pyo3(signature = (items,))]
    fn insert_batch(&self, py: Python<'_>, items: &Bound<'_, PyList>) -> PyResult<Vec<PyNode>> {
        let mut batch: Vec<(String, serde_json::Value)> = Vec::with_capacity(items.len());
        for item in items.iter() {
            let tuple = item.downcast::<pyo3::types::PyTuple>()
                .map_err(|_| PyValueError::new_err("Each item must be a tuple of (node_type, properties_dict)"))?;
            if tuple.len() != 2 {
                return Err(PyValueError::new_err("Each tuple must have exactly 2 elements: (node_type, properties_dict)"));
            }
            let node_type: String = tuple.get_item(0)?.extract()?;
            let props_obj: PyObject = tuple.get_item(1)?.unbind();
            let json_str = py_to_json_string(py, props_obj)?;
            let json_val: serde_json::Value = serde_json::from_str(&json_str)
                .map_err(|e| PyValueError::new_err(format!("Invalid JSON: {e}")))?;
            batch.push((node_type, json_val));
        }

        let refs: Vec<(&str, serde_json::Value)> = batch.iter()
            .map(|(t, v)| (t.as_str(), v.clone()))
            .collect();
        let nodes = runtime()
            .block_on(self.db().insert_nodes_batch(refs))
            .map_err(to_pyerr)?;
        nodes.iter().map(|n| PyNode::from_node(py, n)).collect()
    }

    #[pyo3(signature = (id,))]
    fn get(&self, py: Python<'_>, id: &str) -> PyResult<Option<PyNode>> {
        let node = runtime()
            .block_on(self.db().get_node(id))
            .map_err(to_pyerr)?;
        match node {
            Some(n) => Ok(Some(PyNode::from_node(py, &n)?)),
            None => Ok(None),
        }
    }

    #[pyo3(signature = (id, properties))]
    fn update(&self, py: Python<'_>, id: &str, properties: PyObject) -> PyResult<PyNode> {
        let json_str = py_to_json_string(py, properties)?;
        let json_val: serde_json::Value = serde_json::from_str(&json_str)
            .map_err(|e| PyValueError::new_err(format!("Invalid JSON: {e}")))?;
        let node = runtime()
            .block_on(self.db().update_node(id, json_val))
            .map_err(to_pyerr)?;
        PyNode::from_node(py, &node)
    }

    #[pyo3(signature = (id,))]
    fn delete(&self, id: &str) -> PyResult<()> {
        runtime()
            .block_on(self.db().delete_node(id))
            .map_err(to_pyerr)
    }

    #[pyo3(signature = (node_type, limit=None))]
    fn get_by_type(&self, py: Python<'_>, node_type: &str, limit: Option<usize>) -> PyResult<Vec<PyNode>> {
        let nodes = runtime()
            .block_on(self.db().get_all_by_type(node_type, limit))
            .map_err(to_pyerr)?;
        nodes.iter().map(|n| PyNode::from_node(py, n)).collect()
    }

    // ===== Edge Operations =====

    #[pyo3(signature = (from_id, to_id, edge_type, properties=None))]
    fn create_edge(
        &self,
        py: Python<'_>,
        from_id: &str,
        to_id: &str,
        edge_type: &str,
        properties: Option<PyObject>,
    ) -> PyResult<PyEdge> {
        let props = match properties {
            Some(obj) => {
                let json_str = py_to_json_string(py, obj)?;
                Some(serde_json::from_str(&json_str)
                    .map_err(|e| PyValueError::new_err(format!("Invalid JSON: {e}")))?)
            }
            None => None,
        };
        let edge = runtime()
            .block_on(self.db().create_edge(from_id, to_id, edge_type, props))
            .map_err(to_pyerr)?;
        PyEdge::from_edge(py, &edge)
    }

    #[pyo3(signature = (edges,))]
    fn create_edges_batch(&self, py: Python<'_>, edges: &Bound<'_, PyList>) -> PyResult<Vec<PyEdge>> {
        let mut batch: Vec<(String, String, String)> = Vec::with_capacity(edges.len());
        for item in edges.iter() {
            let tuple = item.downcast::<pyo3::types::PyTuple>()
                .map_err(|_| PyValueError::new_err("Each edge must be a tuple of (from_id, to_id, edge_type)"))?;
            if tuple.len() != 3 {
                return Err(PyValueError::new_err("Each tuple must have 3 elements: (from_id, to_id, edge_type)"));
            }
            let from_id: String = tuple.get_item(0)?.extract()?;
            let to_id: String = tuple.get_item(1)?.extract()?;
            let edge_type: String = tuple.get_item(2)?.extract()?;
            batch.push((from_id, to_id, edge_type));
        }

        let refs: Vec<(&str, &str, &str)> = batch.iter()
            .map(|(f, t, e)| (f.as_str(), t.as_str(), e.as_str()))
            .collect();
        let result = runtime()
            .block_on(self.db().create_edges_batch(refs))
            .map_err(to_pyerr)?;
        result.iter().map(|e| PyEdge::from_edge(py, e)).collect()
    }

    #[pyo3(signature = (node_id, edge_type=None))]
    fn get_edges_from(&self, py: Python<'_>, node_id: &str, edge_type: Option<&str>) -> PyResult<Vec<PyEdge>> {
        let edges = runtime()
            .block_on(self.db().get_edges_from(node_id, edge_type))
            .map_err(to_pyerr)?;
        edges.iter().map(|e| PyEdge::from_edge(py, e)).collect()
    }

    #[pyo3(signature = (node_id, edge_type=None))]
    fn get_edges_to(&self, py: Python<'_>, node_id: &str, edge_type: Option<&str>) -> PyResult<Vec<PyEdge>> {
        let edges = runtime()
            .block_on(self.db().get_edges_to(node_id, edge_type))
            .map_err(to_pyerr)?;
        edges.iter().map(|e| PyEdge::from_edge(py, e)).collect()
    }

    #[pyo3(signature = (edge_id,))]
    fn delete_edge(&self, edge_id: &str) -> PyResult<()> {
        runtime()
            .block_on(self.db().delete_edge(edge_id))
            .map_err(to_pyerr)
    }

    // ===== SQL =====

    #[pyo3(signature = (sql, limit=None))]
    fn query(&self, py: Python<'_>, sql: &str, limit: Option<usize>) -> PyResult<PyQueryResult> {
        let result = runtime()
            .block_on(self.engine().execute_sql(sql, limit))
            .map_err(to_pyerr)?;

        let py_rows = PyList::empty_bound(py);
        for row in &result.rows {
            let py_row = PyList::empty_bound(py);
            for val in row {
                py_row.append(value_to_py(py, val)?)?;
            }
            py_rows.append(py_row)?;
        }

        Ok(PyQueryResult {
            columns: result.columns,
            rows: py_rows.unbind().into(),
            rows_affected: result.rows_affected,
            execution_time_ms: result.execution_time_ms,
        })
    }

    // ===== Graph Traversal =====

    #[pyo3(signature = (start_node_id, max_depth, edge_types=None))]
    fn traverse(
        &self,
        py: Python<'_>,
        start_node_id: &str,
        max_depth: u32,
        edge_types: Option<Vec<String>>,
    ) -> PyResult<PyTraversalResult> {
        let et_refs: Option<Vec<&str>> = edge_types.as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        let result = runtime()
            .block_on(self.engine().traverse(start_node_id, max_depth, et_refs))
            .map_err(to_pyerr)?;

        let root = PyNode::from_node(py, &result.root)?;
        let nodes_list = PyList::empty_bound(py);
        for n in &result.nodes {
            nodes_list.append(PyNode::from_node(py, n)?.into_py(py))?;
        }
        let edges_list = PyList::empty_bound(py);
        for e in &result.edges {
            edges_list.append(PyEdge::from_edge(py, e)?.into_py(py))?;
        }

        let adj_dict = PyDict::new_bound(py);
        for (k, v) in &result.adjacency {
            let py_list = PyList::new_bound(py, v);
            adj_dict.set_item(k, py_list)?;
        }

        Ok(PyTraversalResult {
            root: root.into_py(py),
            nodes: nodes_list.unbind().into(),
            edges: edges_list.unbind().into(),
            depth: result.depth,
            adjacency: adj_dict.unbind().into(),
        })
    }

    #[pyo3(signature = (from_id, to_id, max_depth=10))]
    fn shortest_path(
        &self,
        py: Python<'_>,
        from_id: &str,
        to_id: &str,
        max_depth: u32,
    ) -> PyResult<Option<Vec<PyNode>>> {
        let result = runtime()
            .block_on(self.engine().shortest_path(from_id, to_id, max_depth))
            .map_err(to_pyerr)?;
        match result {
            Some(nodes) => {
                let py_nodes: Vec<PyNode> = nodes.iter()
                    .map(|n| PyNode::from_node(py, n))
                    .collect::<PyResult<Vec<_>>>()?;
                Ok(Some(py_nodes))
            }
            None => Ok(None),
        }
    }

    #[pyo3(signature = (node_type,))]
    fn connected_components(
        &self,
        py: Python<'_>,
        node_type: &str,
    ) -> PyResult<Vec<Vec<PyNode>>> {
        let result = runtime()
            .block_on(self.engine().connected_components(node_type))
            .map_err(to_pyerr)?;
        result.into_iter()
            .map(|component| {
                component.iter()
                    .map(|n| PyNode::from_node(py, n))
                    .collect::<PyResult<Vec<_>>>()
            })
            .collect()
    }

    // ===== Secondary Indexes =====

    #[pyo3(signature = (node_type, field))]
    fn create_index(&self, node_type: &str, field: &str) -> PyResult<u64> {
        runtime()
            .block_on(self.db().create_index(node_type, field))
            .map_err(to_pyerr)
    }

    #[pyo3(signature = (node_type, field))]
    fn drop_index(&self, node_type: &str, field: &str) -> PyResult<()> {
        runtime()
            .block_on(self.db().drop_index(node_type, field))
            .map_err(to_pyerr)
    }

    fn list_indexes(&self) -> PyResult<Vec<(String, String)>> {
        self.db().list_indexes().map_err(to_pyerr)
    }

    #[pyo3(signature = (node_type, field, value))]
    fn index_lookup(
        &self,
        py: Python<'_>,
        node_type: &str,
        field: &str,
        value: PyObject,
    ) -> PyResult<Option<Vec<PyNode>>> {
        let json_str = py_to_json_string(py, value)?;
        let json_val: serde_json::Value = serde_json::from_str(&json_str)
            .map_err(|e| PyValueError::new_err(format!("Invalid JSON: {e}")))?;
        let val = aresadb::Value::from_json(json_val).map_err(to_pyerr)?;

        let result = runtime()
            .block_on(self.db().index_lookup(node_type, field, &val))
            .map_err(to_pyerr)?;
        match result {
            Some(nodes) => {
                let py_nodes: Vec<PyNode> = nodes.iter()
                    .map(|n| PyNode::from_node(py, n))
                    .collect::<PyResult<Vec<_>>>()?;
                Ok(Some(py_nodes))
            }
            None => Ok(None),
        }
    }

    // ===== Full-Text Search =====

    #[pyo3(signature = (node_type, field))]
    fn create_fulltext_index(&self, node_type: &str, field: &str) -> PyResult<u64> {
        runtime()
            .block_on(self.db().create_fulltext_index(node_type, field))
            .map_err(to_pyerr)
    }

    #[pyo3(signature = (node_type, field, query, limit=10))]
    fn fulltext_search(
        &self,
        py: Python<'_>,
        node_type: &str,
        field: &str,
        query: &str,
        limit: usize,
    ) -> PyResult<Vec<PyFulltextResult>> {
        let results = runtime()
            .block_on(self.db().fulltext_search(node_type, field, query, limit))
            .map_err(to_pyerr)?;
        results.into_iter()
            .map(|(node, score)| {
                let py_node = PyNode::from_node(py, &node)?;
                Ok(PyFulltextResult {
                    node: py_node.into_py(py),
                    score,
                })
            })
            .collect()
    }

    fn list_fulltext_indexes(&self) -> PyResult<Vec<(String, String)>> {
        self.db().list_fulltext_indexes().map_err(to_pyerr)
    }

    // ===== Vector / Embedding Operations =====

    #[pyo3(signature = (node_type, properties, field, vector))]
    fn insert_with_embedding(
        &self,
        py: Python<'_>,
        node_type: &str,
        properties: &str,
        field: &str,
        vector: Vec<f32>,
    ) -> PyResult<PyNode> {
        let json_val: serde_json::Value = serde_json::from_str(properties)
            .map_err(|e| PyValueError::new_err(format!("Invalid JSON: {e}")))?;
        let node = runtime()
            .block_on(self.db().insert_with_embedding(node_type, json_val, field, vector))
            .map_err(to_pyerr)?;
        PyNode::from_node(py, &node)
    }

    #[pyo3(signature = (query_vector, node_type, field, k, metric="cosine"))]
    fn similarity_search(
        &self,
        query_vector: Vec<f32>,
        node_type: &str,
        field: &str,
        k: usize,
        metric: &str,
    ) -> PyResult<Vec<PySearchResult>> {
        let distance_metric = parse_metric(metric)?;
        let results = runtime()
            .block_on(self.db().similarity_search(&query_vector, node_type, field, k, distance_metric))
            .map_err(to_pyerr)?;
        Ok(results.into_iter().map(|r| PySearchResult {
            node_id: r.node_id.to_string(),
            score: r.score,
            distance: r.distance,
        }).collect())
    }

    #[pyo3(signature = (query_vector, node_type, field, max_distance, metric="cosine"))]
    fn similarity_search_radius(
        &self,
        query_vector: Vec<f32>,
        node_type: &str,
        field: &str,
        max_distance: f64,
        metric: &str,
    ) -> PyResult<Vec<PySearchResult>> {
        let distance_metric = parse_metric(metric)?;
        let results = runtime()
            .block_on(self.db().similarity_search_radius(&query_vector, node_type, field, max_distance, distance_metric))
            .map_err(to_pyerr)?;
        Ok(results.into_iter().map(|r| PySearchResult {
            node_id: r.node_id.to_string(),
            score: r.score,
            distance: r.distance,
        }).collect())
    }

    #[pyo3(signature = (id, embedding_field))]
    fn get_node_with_embedding(
        &self,
        py: Python<'_>,
        id: &str,
        embedding_field: &str,
    ) -> PyResult<Option<(PyNode, Option<Vec<f32>>)>> {
        let result = runtime()
            .block_on(self.db().get_node_with_embedding(id, embedding_field))
            .map_err(to_pyerr)?;
        match result {
            Some((node, embedding)) => {
                let py_node = PyNode::from_node(py, &node)?;
                Ok(Some((py_node, embedding)))
            }
            None => Ok(None),
        }
    }

    #[pyo3(signature = (node_type, embedding_field))]
    fn rebuild_vector_index(
        &self,
        node_type: &str,
        embedding_field: &str,
    ) -> PyResult<PyIndexStats> {
        let stats = runtime()
            .block_on(self.db().rebuild_vector_index(node_type, embedding_field))
            .map_err(to_pyerr)?;
        Ok(PyIndexStats {
            num_vectors: stats.num_vectors,
            dimension: stats.dimension,
            max_connections: stats.max_connections,
            max_layers: stats.max_layers,
            total_connections: stats.total_connections,
            avg_connections: stats.avg_connections,
        })
    }

    // ===== Introspection =====

    fn status(&self) -> PyResult<PyDatabaseStatus> {
        let status = runtime()
            .block_on(self.db().status())
            .map_err(to_pyerr)?;
        Ok(PyDatabaseStatus {
            name: status.name,
            path: status.path,
            node_count: status.node_count,
            edge_count: status.edge_count,
            schema_count: status.schema_count,
            size_bytes: status.size_bytes,
        })
    }

    fn name(&self) -> String {
        self.db().name()
    }

    fn path(&self) -> String {
        self.db().path().to_string_lossy().to_string()
    }

    fn __repr__(&self) -> String {
        format!("Database(name='{}', path='{}')", self.db().name(), self.db().path().display())
    }
}

// ========== Module Registration ==========

#[pymodule]
fn aresadb_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Database>()?;
    m.add_class::<PyNode>()?;
    m.add_class::<PyEdge>()?;
    m.add_class::<PySearchResult>()?;
    m.add_class::<PyFulltextResult>()?;
    m.add_class::<PyQueryResult>()?;
    m.add_class::<PyDatabaseStatus>()?;
    m.add_class::<PyTraversalResult>()?;
    m.add_class::<PyIndexStats>()?;
    Ok(())
}

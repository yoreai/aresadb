//! Query executor
//!
//! Executes query plans against the storage engine.

#![allow(dead_code)]

use anyhow::Result;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::time::Instant;

use super::planner::PlanStep;
use super::{
    Condition, Operator, ParsedQuery, QueryOperation, QueryParser, QueryPlan, QueryPlanner,
    QueryResult, TraversalResult,
};
use crate::storage::{Database, Edge, Node, NodeId, SimilarityResult, Value};

/// Query executor
pub struct QueryEngine {
    db: Database,
    parser: QueryParser,
    planner: QueryPlanner,
}

impl QueryEngine {
    /// Create a new query engine
    pub fn new(db: Database) -> Self {
        // Load existing indexes into the planner
        let mut planner = QueryPlanner::new();
        if let Ok(indexes) = db.list_indexes() {
            for (node_type, field) in indexes {
                planner.add_indexed_field(node_type, field);
            }
        }

        Self {
            db,
            parser: QueryParser::new(),
            planner,
        }
    }

    /// Refresh the planner's knowledge of available indexes
    pub fn refresh_indexes(&mut self) {
        if let Ok(indexes) = self.db.list_indexes() {
            let mut planner = QueryPlanner::new();
            for (node_type, field) in indexes {
                planner.add_indexed_field(node_type, field);
            }
            self.planner = planner;
        }
    }

    /// Execute a SQL query
    pub async fn execute_sql(&self, sql: &str, limit: Option<usize>) -> Result<QueryResult> {
        let start = Instant::now();

        // Parse SQL
        let mut query = self.parser.parse(sql)?;

        // Apply external limit if provided
        if let Some(l) = limit {
            query.limit = Some(query.limit.map(|ql| ql.min(l)).unwrap_or(l));
        }

        // Handle vector search separately
        if query.operation == QueryOperation::VectorSearch {
            let results = self.execute_vector_search(&query).await?;
            let mut result = self.vector_results_to_query_result(results).await?;
            result.execution_time_ms = start.elapsed().as_millis() as u64;
            return Ok(result);
        }

        // Handle index commands
        if query.operation == QueryOperation::CreateIndex
            || query.operation == QueryOperation::DropIndex
            || query.operation == QueryOperation::CreateFullTextIndex
        {
            let mut result = self.execute_index_command(&query).await?;
            result.execution_time_ms = start.elapsed().as_millis() as u64;
            return Ok(result);
        }

        // Handle full-text search
        if query.operation == QueryOperation::FullTextSearch {
            let mut result = self.execute_fulltext_search(&query).await?;
            result.execution_time_ms = start.elapsed().as_millis() as u64;
            return Ok(result);
        }

        // Plan and execute
        let plan = self.planner.plan(&query)?;
        let mut result = self.execute_plan(&plan, &query).await?;

        result.execution_time_ms = start.elapsed().as_millis() as u64;
        Ok(result)
    }

    /// Execute a parsed query
    pub async fn execute_parsed(
        &self,
        query: &ParsedQuery,
        limit: Option<usize>,
    ) -> Result<QueryResult> {
        let start = Instant::now();

        let mut query = query.clone();
        if let Some(l) = limit {
            query.limit = Some(query.limit.map(|ql| ql.min(l)).unwrap_or(l));
        }

        // Handle vector search separately
        if query.operation == QueryOperation::VectorSearch {
            let results = self.execute_vector_search(&query).await?;
            let mut result = self.vector_results_to_query_result(results).await?;
            result.execution_time_ms = start.elapsed().as_millis() as u64;
            return Ok(result);
        }

        let plan = self.planner.plan(&query)?;
        let mut result = self.execute_plan(&plan, &query).await?;

        result.execution_time_ms = start.elapsed().as_millis() as u64;
        Ok(result)
    }

    /// Execute a CREATE INDEX or DROP INDEX command
    async fn execute_index_command(&self, query: &ParsedQuery) -> Result<QueryResult> {
        let data = query
            .data
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Missing index field specification"))?;
        let field = match data.get("field") {
            Some(Value::String(f)) => f.clone(),
            _ => anyhow::bail!("Missing field name in index command"),
        };

        match query.operation {
            QueryOperation::CreateIndex => {
                let count = self.db.create_index(&query.target, &field).await?;
                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String(format!(
                        "Index created on {}.{} ({} entries indexed)",
                        query.target, field, count
                    ))]],
                    rows_affected: count,
                    execution_time_ms: 0,
                })
            }
            QueryOperation::DropIndex => {
                self.db.drop_index(&query.target, &field).await?;
                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String(format!(
                        "Index on {}.{} dropped",
                        query.target, field
                    ))]],
                    rows_affected: 0,
                    execution_time_ms: 0,
                })
            }
            QueryOperation::CreateFullTextIndex => {
                let count = self.db.create_fulltext_index(&query.target, &field).await?;
                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String(format!(
                        "Full-text index created on {}.{} ({} documents indexed)",
                        query.target, field, count
                    ))]],
                    rows_affected: count,
                    execution_time_ms: 0,
                })
            }
            _ => unreachable!(),
        }
    }

    /// Execute a full-text search query with BM25 ranking
    async fn execute_fulltext_search(&self, query: &ParsedQuery) -> Result<QueryResult> {
        let params = query
            .fulltext_search
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Missing full-text search parameters"))?;

        let results = self
            .db
            .fulltext_search(&query.target, &params.field, &params.query, params.limit)
            .await?;

        if results.is_empty() {
            return Ok(QueryResult::empty());
        }

        // Build result with all node properties + score
        let mut all_keys: Vec<String> = Vec::new();
        for (node, _) in &results {
            for key in node.properties.keys() {
                if !all_keys.contains(key) {
                    all_keys.push(key.clone());
                }
            }
        }
        all_keys.sort();

        let mut columns = vec![
            "rank".to_string(),
            "id".to_string(),
            "type".to_string(),
            "score".to_string(),
        ];
        columns.extend(all_keys.iter().cloned());

        let rows: Vec<Vec<Value>> = results
            .iter()
            .enumerate()
            .map(|(i, (node, score))| {
                let mut row = vec![
                    Value::Int((i + 1) as i64),
                    Value::String(node.id.to_string()),
                    Value::String(node.node_type.clone()),
                    Value::Float(*score),
                ];
                for key in &all_keys {
                    row.push(node.properties.get(key).cloned().unwrap_or(Value::Null));
                }
                row
            })
            .collect();

        Ok(QueryResult {
            columns,
            rows,
            rows_affected: 0,
            execution_time_ms: 0,
        })
    }

    /// Execute a vector search query with optional WHERE filtering
    pub async fn execute_vector_search(
        &self,
        query: &ParsedQuery,
    ) -> Result<Vec<SimilarityResult>> {
        let params = query
            .vector_search
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Missing vector search parameters"))?;

        if params.filters.is_empty() {
            // No filters — use the fast HNSW path directly
            return self
                .db
                .similarity_search(
                    &params.query_vector,
                    &query.target,
                    &params.embedding_field,
                    params.k,
                    params.metric,
                )
                .await;
        }

        // Filtered vector search: pre-filter nodes, then compute similarity
        let all_nodes = self.db.get_all_by_type(&query.target, None).await?;
        let filtered: Vec<_> = all_nodes
            .into_iter()
            .filter(|node| self.node_matches_conditions(node, &params.filters))
            .collect();

        // Brute-force search on filtered subset
        let search = crate::storage::VectorSearch::new(params.metric);
        let results = search.search(
            &params.query_vector,
            &filtered,
            &params.embedding_field,
            params.k,
        );
        Ok(results)
    }

    /// Check if a node matches a set of conditions
    fn node_matches_conditions(&self, node: &Node, conditions: &[Condition]) -> bool {
        conditions.iter().all(|cond| {
            if let Some(value) = node.properties.get(&cond.column) {
                match cond.operator {
                    Operator::Eq => value == &cond.value,
                    Operator::Ne => value != &cond.value,
                    Operator::Lt => value_cmp(value, &cond.value) == Some(std::cmp::Ordering::Less),
                    Operator::Le => matches!(
                        value_cmp(value, &cond.value),
                        Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                    ),
                    Operator::Gt => {
                        value_cmp(value, &cond.value) == Some(std::cmp::Ordering::Greater)
                    }
                    Operator::Ge => matches!(
                        value_cmp(value, &cond.value),
                        Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                    ),
                    Operator::Like => {
                        if let (Some(text), Some(pattern)) = (value.as_str(), cond.value.as_str()) {
                            text.contains(pattern.trim_matches('%'))
                        } else {
                            false
                        }
                    }
                    Operator::IsNull => value == &Value::Null,
                    Operator::IsNotNull => value != &Value::Null,
                    _ => false,
                }
            } else {
                matches!(cond.operator, Operator::IsNull)
            }
        })
    }

    /// Convert vector search results to QueryResult, including matched node properties
    async fn vector_results_to_query_result(
        &self,
        results: Vec<SimilarityResult>,
    ) -> Result<QueryResult> {
        let mut all_property_keys: Vec<String> = Vec::new();
        let mut node_cache: Vec<(usize, Node, f64, f64)> = Vec::new();

        // Fetch all matched nodes and collect property keys
        for (i, result) in results.iter().enumerate() {
            if let Some(node) = self.db.get_node(&result.node_id.to_string()).await? {
                for key in node.properties.keys() {
                    if !all_property_keys.contains(key)
                        && key != "embedding"
                        && !key.starts_with("$")
                    {
                        all_property_keys.push(key.clone());
                    }
                }
                node_cache.push((i, node, result.score, result.distance));
            }
        }
        all_property_keys.sort();

        let mut columns = vec![
            "rank".to_string(),
            "id".to_string(),
            "type".to_string(),
            "score".to_string(),
            "distance".to_string(),
        ];
        columns.extend(all_property_keys.iter().cloned());

        let mut rows = Vec::new();
        for (i, node, score, distance) in &node_cache {
            let mut row = vec![
                Value::Int((*i + 1) as i64),
                Value::String(node.id.to_string()),
                Value::String(node.node_type.clone()),
                Value::Float(*score),
                Value::Float(*distance),
            ];
            for key in &all_property_keys {
                row.push(node.properties.get(key).cloned().unwrap_or(Value::Null));
            }
            rows.push(row);
        }

        Ok(QueryResult {
            columns,
            rows,
            rows_affected: 0,
            execution_time_ms: 0,
        })
    }

    /// Execute a query plan
    async fn execute_plan(&self, plan: &QueryPlan, query: &ParsedQuery) -> Result<QueryResult> {
        let mut nodes: Option<Vec<Node>> = None;
        let mut insert_result: Option<Node> = None;
        let mut rows_affected: u64 = 0;

        for step in &plan.steps {
            match step {
                PlanStep::FullScan { node_type } => {
                    nodes = Some(self.db.get_all_by_type(node_type, None).await?);
                }

                PlanStep::IndexLookup {
                    node_type,
                    field,
                    value,
                } => {
                    match self.db.index_lookup(node_type, field, value).await? {
                        Some(indexed_nodes) => {
                            nodes = Some(indexed_nodes);
                        }
                        None => {
                            // No index available, fall back to full scan
                            nodes = Some(self.db.get_all_by_type(node_type, None).await?);
                        }
                    }
                }

                PlanStep::Filter { conditions } => {
                    if let Some(ref mut n) = nodes {
                        *n = n
                            .drain(..)
                            .filter(|node| self.matches_conditions(node, conditions))
                            .collect();
                    }
                }

                PlanStep::Sort { field, descending } => {
                    if let Some(ref mut n) = nodes {
                        n.sort_by(|a, b| {
                            let va = a.get(field).unwrap_or(&Value::Null);
                            let vb = b.get(field).unwrap_or(&Value::Null);
                            let cmp = self.compare_values(va, vb);
                            if *descending {
                                cmp.reverse()
                            } else {
                                cmp
                            }
                        });
                    }
                }

                PlanStep::Limit { count, offset } => {
                    if let Some(ref mut n) = nodes {
                        *n = n.drain(..).skip(*offset).take(*count).collect();
                    }
                }

                PlanStep::Project { columns } => {
                    // Projection is handled at result rendering time
                    // Just validate columns exist
                    let _ = columns;
                }

                PlanStep::InsertNode { node_type, data } => {
                    let props = Value::Object(data.clone());
                    let node = self.db.insert_node(node_type, props.to_json()).await?;
                    insert_result = Some(node);
                    rows_affected = 1;
                }

                PlanStep::UpdateNodes { data } => {
                    if let Some(ref n) = nodes {
                        for node in n {
                            let props = Value::Object(data.clone());
                            self.db
                                .update_node(&node.id.to_string(), props.to_json())
                                .await?;
                            rows_affected += 1;
                        }
                    }
                }

                PlanStep::DeleteNodes => {
                    if let Some(ref n) = nodes {
                        for node in n {
                            self.db.delete_node(&node.id.to_string()).await?;
                            rows_affected += 1;
                        }
                    }
                }

                PlanStep::Traverse { .. } => {
                    // Handled separately by traverse method
                }
            }
        }

        // Build result
        let mut result = if let Some(node) = insert_result {
            QueryResult::from_nodes(vec![node])
        } else if let Some(n) = nodes {
            let mut r = QueryResult::from_nodes(n);

            // Apply column projection
            if !query.columns.is_empty() {
                let col_set: HashSet<&String> = query.columns.iter().collect();
                r.columns
                    .retain(|c| col_set.contains(c) || c == "id" || c == "type");

                let keep_indices: Vec<usize> = r
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| col_set.contains(*c) || *c == "id" || *c == "type")
                    .map(|(i, _)| i)
                    .collect();

                r.rows = r
                    .rows
                    .into_iter()
                    .map(|row| keep_indices.iter().map(|&i| row[i].clone()).collect())
                    .collect();
            }

            r
        } else {
            QueryResult::empty()
        };

        result.rows_affected = rows_affected;
        Ok(result)
    }

    /// Check if a node matches all conditions
    fn matches_conditions(&self, node: &Node, conditions: &[Condition]) -> bool {
        for condition in conditions {
            let value = if condition.column == "id" {
                Value::String(node.id.to_string())
            } else if condition.column == "type" {
                Value::String(node.node_type.clone())
            } else {
                node.get(&condition.column).cloned().unwrap_or(Value::Null)
            };

            if !condition.operator.matches(&value, &condition.value) {
                return false;
            }
        }
        true
    }

    /// Compare two values for sorting
    fn compare_values(&self, a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) {
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Null, _) => std::cmp::Ordering::Less,
            (_, Value::Null) => std::cmp::Ordering::Greater,
            (Value::Int(ai), Value::Int(bi)) => ai.cmp(bi),
            (Value::Float(af), Value::Float(bf)) => {
                af.partial_cmp(bf).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Value::Int(ai), Value::Float(bf)) => (*ai as f64)
                .partial_cmp(bf)
                .unwrap_or(std::cmp::Ordering::Equal),
            (Value::Float(af), Value::Int(bi)) => af
                .partial_cmp(&(*bi as f64))
                .unwrap_or(std::cmp::Ordering::Equal),
            (Value::String(as_), Value::String(bs)) => as_.cmp(bs),
            (Value::Bool(ab), Value::Bool(bb)) => ab.cmp(bb),
            _ => std::cmp::Ordering::Equal,
        }
    }

    /// Perform graph traversal from a starting node
    pub async fn traverse(
        &self,
        start_node_id: &str,
        max_depth: u32,
        edge_types: Option<Vec<&str>>,
    ) -> Result<TraversalResult> {
        let start_id = NodeId::parse(start_node_id)?;
        let root = self
            .db
            .get_node(start_node_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Start node not found: {}", start_node_id))?;

        let mut visited_nodes: BTreeMap<String, Node> = BTreeMap::new();
        let mut all_edges: Vec<Edge> = Vec::new();
        let mut adjacency: BTreeMap<String, Vec<String>> = BTreeMap::new();

        // BFS traversal
        let mut queue: VecDeque<(NodeId, u32)> = VecDeque::new();
        let mut visited_ids: HashSet<String> = HashSet::new();

        queue.push_back((start_id, 0));

        while let Some((current_id, depth)) = queue.pop_front() {
            let id_str = current_id.to_string();

            if visited_ids.contains(&id_str) {
                continue;
            }
            visited_ids.insert(id_str.clone());

            // Get the node
            if let Some(node) = self.db.get_node(&id_str).await? {
                visited_nodes.insert(id_str.clone(), node);
            }

            // Stop if max depth reached
            if depth >= max_depth {
                continue;
            }

            // Get outgoing edges
            let edges = self.db.get_edges_from(&id_str, None).await?;

            let mut neighbors = Vec::new();

            for edge in edges {
                // Filter by edge type if specified
                if let Some(ref types) = edge_types {
                    if !types.contains(&edge.edge_type.as_str()) {
                        continue;
                    }
                }

                let to_str = edge.to.to_string();
                neighbors.push(to_str.clone());

                if !visited_ids.contains(&to_str) {
                    queue.push_back((edge.to.clone(), depth + 1));
                }

                all_edges.push(edge);
            }

            adjacency.insert(id_str, neighbors);
        }

        let nodes: Vec<Node> = visited_nodes.into_values().collect();

        Ok(TraversalResult {
            root,
            nodes,
            edges: all_edges,
            depth: max_depth,
            adjacency,
        })
    }

    /// Find shortest path between two nodes
    pub async fn shortest_path(
        &self,
        from_id: &str,
        to_id: &str,
        max_depth: u32,
    ) -> Result<Option<Vec<Node>>> {
        let from = NodeId::parse(from_id)?;
        let to_str = to_id.to_string();

        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<(NodeId, Vec<String>)> = VecDeque::new();

        queue.push_back((from, vec![from_id.to_string()]));

        while let Some((current, path)) = queue.pop_front() {
            let current_str = current.to_string();

            if current_str == to_str {
                // Found path - resolve node IDs to nodes
                let mut nodes = Vec::new();
                for id in path {
                    if let Some(node) = self.db.get_node(&id).await? {
                        nodes.push(node);
                    }
                }
                return Ok(Some(nodes));
            }

            if visited.contains(&current_str) || path.len() > max_depth as usize {
                continue;
            }
            visited.insert(current_str.clone());

            // Get neighbors
            let edges = self.db.get_edges_from(&current_str, None).await?;

            for edge in edges {
                let neighbor_str = edge.to.to_string();
                if !visited.contains(&neighbor_str) {
                    let mut new_path = path.clone();
                    new_path.push(neighbor_str);
                    queue.push_back((edge.to, new_path));
                }
            }
        }

        Ok(None) // No path found
    }

    /// Get connected components
    pub async fn connected_components(&self, node_type: &str) -> Result<Vec<Vec<Node>>> {
        let all_nodes = self.db.get_all_by_type(node_type, None).await?;
        let mut visited: HashSet<String> = HashSet::new();
        let mut components: Vec<Vec<Node>> = Vec::new();

        for node in &all_nodes {
            let id_str = node.id.to_string();

            if visited.contains(&id_str) {
                continue;
            }

            // BFS to find all connected nodes
            let mut component: Vec<Node> = Vec::new();
            let mut queue: VecDeque<NodeId> = VecDeque::new();
            queue.push_back(node.id.clone());

            while let Some(current) = queue.pop_front() {
                let current_str = current.to_string();

                if visited.contains(&current_str) {
                    continue;
                }
                visited.insert(current_str.clone());

                if let Some(n) = self.db.get_node(&current_str).await? {
                    component.push(n);
                }

                // Get all connected nodes (both directions)
                let edges_from = self.db.get_edges_from(&current_str, None).await?;
                let edges_to = self.db.get_edges_to(&current_str, None).await?;

                for edge in edges_from {
                    let neighbor = edge.to.to_string();
                    if !visited.contains(&neighbor) {
                        queue.push_back(edge.to);
                    }
                }

                for edge in edges_to {
                    let neighbor = edge.from.to_string();
                    if !visited.contains(&neighbor) {
                        queue.push_back(edge.from);
                    }
                }
            }

            if !component.is_empty() {
                components.push(component);
            }
        }

        Ok(components)
    }
}

fn value_cmp(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => a.partial_cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)),
        (Value::String(a), Value::String(b)) => a.partial_cmp(b),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_execute_select() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "test").await.unwrap();

        // Insert some data
        let props = serde_json::json!({"name": "Alice", "age": 30});
        db.insert_node("user", props).await.unwrap();

        let props = serde_json::json!({"name": "Bob", "age": 25});
        db.insert_node("user", props).await.unwrap();

        let engine = QueryEngine::new(db);

        // Query all
        let result = engine
            .execute_sql("SELECT * FROM user", None)
            .await
            .unwrap();
        assert_eq!(result.row_count(), 2);

        // Query with condition
        let result = engine
            .execute_sql("SELECT * FROM user WHERE age > 28", None)
            .await
            .unwrap();
        assert_eq!(result.row_count(), 1);
    }

    #[tokio::test]
    async fn test_execute_insert() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "test").await.unwrap();
        let engine = QueryEngine::new(db);

        let result = engine
            .execute_sql("INSERT INTO user (name, age) VALUES ('Charlie', 35)", None)
            .await
            .unwrap();

        assert_eq!(result.rows_affected, 1);
    }

    #[tokio::test]
    async fn test_traverse() {
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
        let charlie = db
            .insert_node("user", serde_json::json!({"name": "Charlie"}))
            .await
            .unwrap();

        // Create edges
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

        let engine = QueryEngine::new(db);

        // Traverse from Alice
        let result = engine
            .traverse(&alice.id.to_string(), 2, None)
            .await
            .unwrap();

        assert_eq!(result.nodes.len(), 3); // Alice, Bob, Charlie
        assert_eq!(result.edges.len(), 2);
    }
}

//! Query Engine
//!
//! Provides SQL parsing, query planning, and execution with graph traversal.

#![allow(dead_code)]
#![allow(unused_imports)]

mod executor;
mod parser;
mod planner;

pub use executor::QueryEngine;
pub use parser::QueryParser;
pub use planner::{PlanStep, QueryPlan, QueryPlanner};

use crate::storage::{Edge, Node, Value};

// Re-export vector search types from storage
pub use crate::storage::{DistanceMetric, SimilarityResult};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Result of a query execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Column names
    pub columns: Vec<String>,
    /// Rows of data
    pub rows: Vec<Vec<Value>>,
    /// Number of rows affected (for INSERT/UPDATE/DELETE)
    pub rows_affected: u64,
    /// Query execution time in milliseconds
    pub execution_time_ms: u64,
}

impl QueryResult {
    /// Create an empty result
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: 0,
            execution_time_ms: 0,
        }
    }

    /// Create from nodes
    pub fn from_nodes(nodes: Vec<Node>) -> Self {
        if nodes.is_empty() {
            return Self::empty();
        }

        // Collect all unique column names
        let mut column_set = std::collections::HashSet::new();
        column_set.insert("id".to_string());
        column_set.insert("type".to_string());

        for node in &nodes {
            for key in node.properties.keys() {
                column_set.insert(key.clone());
            }
        }

        let mut columns: Vec<String> = column_set.into_iter().collect();
        columns.sort();

        // Ensure id and type are first
        columns.retain(|c| c != "id" && c != "type");
        columns.insert(0, "type".to_string());
        columns.insert(0, "id".to_string());

        // Build rows
        let rows: Vec<Vec<Value>> = nodes
            .iter()
            .map(|node| {
                columns
                    .iter()
                    .map(|col| {
                        if col == "id" {
                            Value::String(node.id.to_string())
                        } else if col == "type" {
                            Value::String(node.node_type.clone())
                        } else {
                            node.properties.get(col).cloned().unwrap_or(Value::Null)
                        }
                    })
                    .collect()
            })
            .collect();

        Self {
            columns,
            rows,
            rows_affected: 0,
            execution_time_ms: 0,
        }
    }

    /// Create from edges
    pub fn from_edges(edges: Vec<Edge>) -> Self {
        if edges.is_empty() {
            return Self::empty();
        }

        let columns = vec![
            "id".to_string(),
            "from".to_string(),
            "to".to_string(),
            "type".to_string(),
        ];

        let rows: Vec<Vec<Value>> = edges
            .iter()
            .map(|edge| {
                vec![
                    Value::String(edge.id.to_string()),
                    Value::String(edge.from.to_string()),
                    Value::String(edge.to.to_string()),
                    Value::String(edge.edge_type.clone()),
                ]
            })
            .collect();

        Self {
            columns,
            rows,
            rows_affected: 0,
            execution_time_ms: 0,
        }
    }

    /// Get row count
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Check if result is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Convert to JSON
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "columns": self.columns,
            "rows": self.rows.iter().map(|row| {
                row.iter().map(|v| v.to_json()).collect::<Vec<_>>()
            }).collect::<Vec<_>>(),
            "rows_affected": self.rows_affected,
            "execution_time_ms": self.execution_time_ms,
        })
    }
}

/// Traversal result for graph queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraversalResult {
    /// Starting node
    pub root: Node,
    /// All visited nodes
    pub nodes: Vec<Node>,
    /// All traversed edges
    pub edges: Vec<Edge>,
    /// Depth of traversal
    pub depth: u32,
    /// Node adjacency map (node_id -> connected node_ids)
    pub adjacency: BTreeMap<String, Vec<String>>,
}

impl TraversalResult {
    /// Convert to QueryResult for display
    pub fn to_query_result(&self) -> QueryResult {
        QueryResult::from_nodes(self.nodes.clone())
    }
}

/// Parsed query from natural language or SQL
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    /// Type of operation
    pub operation: QueryOperation,
    /// Target table/node type
    pub target: String,
    /// Selected columns (empty = all)
    pub columns: Vec<String>,
    /// Filter conditions
    pub conditions: Vec<Condition>,
    /// Order by clauses
    pub order_by: Vec<OrderBy>,
    /// Limit
    pub limit: Option<usize>,
    /// Offset
    pub offset: Option<usize>,
    /// Data for INSERT/UPDATE
    pub data: Option<BTreeMap<String, Value>>,
    /// Vector search parameters
    pub vector_search: Option<VectorSearchParams>,
    /// Full-text search parameters
    pub fulltext_search: Option<FullTextSearchParams>,
}

/// Query operation type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryOperation {
    /// Retrieve nodes matching conditions
    Select,
    /// Insert a new node
    Insert,
    /// Update existing nodes
    Update,
    /// Delete nodes
    Delete,
    /// Traverse graph edges from a starting node
    Traverse,
    /// Create a new node schema
    CreateSchema,
    /// Drop an existing schema
    DropSchema,
    /// Perform vector similarity search
    VectorSearch,
    /// Create a B-tree index on a field
    CreateIndex,
    /// Drop an existing index
    DropIndex,
    /// Perform full-text search on a field
    FullTextSearch,
    /// Create a full-text search index
    CreateFullTextIndex,
}

/// Parameters for vector similarity search
#[derive(Debug, Clone)]
pub struct VectorSearchParams {
    /// The query vector
    pub query_vector: Vec<f32>,
    /// Field containing embeddings
    pub embedding_field: String,
    /// Number of results to return
    pub k: usize,
    /// Distance metric
    pub metric: DistanceMetric,
    /// Optional WHERE filters applied before similarity ranking
    pub filters: Vec<Condition>,
}

/// Parameters for full-text search
#[derive(Debug, Clone)]
pub struct FullTextSearchParams {
    /// Field to search
    pub field: String,
    /// Search query text
    pub query: String,
    /// Maximum number of results
    pub limit: usize,
}

/// Filter condition
#[derive(Debug, Clone)]
pub struct Condition {
    /// Column name to filter on
    pub column: String,
    /// Comparison operator
    pub operator: Operator,
    /// Value to compare against
    pub value: Value,
}

/// Comparison operator
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operator {
    /// Equal (`=`)
    Eq,
    /// Not equal (`!=`)
    Ne,
    /// Less than (`<`)
    Lt,
    /// Less than or equal (`<=`)
    Le,
    /// Greater than (`>`)
    Gt,
    /// Greater than or equal (`>=`)
    Ge,
    /// Pattern match with `%` and `_` wildcards
    Like,
    /// Value is contained in an array
    In,
    /// Value is null
    IsNull,
    /// Value is not null
    IsNotNull,
}

impl Operator {
    /// Check if a value matches the condition
    pub fn matches(&self, left: &Value, right: &Value) -> bool {
        match self {
            Operator::Eq => left == right,
            Operator::Ne => left != right,
            Operator::Lt => match (left, right) {
                (Value::Int(l), Value::Int(r)) => l < r,
                (Value::Float(l), Value::Float(r)) => l < r,
                (Value::Int(l), Value::Float(r)) => (*l as f64) < *r,
                (Value::Float(l), Value::Int(r)) => *l < (*r as f64),
                (Value::String(l), Value::String(r)) => l < r,
                _ => false,
            },
            Operator::Le => match (left, right) {
                (Value::Int(l), Value::Int(r)) => l <= r,
                (Value::Float(l), Value::Float(r)) => l <= r,
                (Value::Int(l), Value::Float(r)) => (*l as f64) <= *r,
                (Value::Float(l), Value::Int(r)) => *l <= (*r as f64),
                (Value::String(l), Value::String(r)) => l <= r,
                _ => false,
            },
            Operator::Gt => match (left, right) {
                (Value::Int(l), Value::Int(r)) => l > r,
                (Value::Float(l), Value::Float(r)) => l > r,
                (Value::Int(l), Value::Float(r)) => (*l as f64) > *r,
                (Value::Float(l), Value::Int(r)) => *l > (*r as f64),
                (Value::String(l), Value::String(r)) => l > r,
                _ => false,
            },
            Operator::Ge => match (left, right) {
                (Value::Int(l), Value::Int(r)) => l >= r,
                (Value::Float(l), Value::Float(r)) => l >= r,
                (Value::Int(l), Value::Float(r)) => (*l as f64) >= *r,
                (Value::Float(l), Value::Int(r)) => *l >= (*r as f64),
                (Value::String(l), Value::String(r)) => l >= r,
                _ => false,
            },
            Operator::Like => {
                match (left, right) {
                    (Value::String(l), Value::String(pattern)) => {
                        // Simple LIKE implementation with % wildcards
                        let pattern = pattern.replace("%", ".*").replace("_", ".");
                        regex::Regex::new(&format!("^{}$", pattern))
                            .map(|re| re.is_match(l))
                            .unwrap_or(false)
                    }
                    _ => false,
                }
            }
            Operator::In => match right {
                Value::Array(arr) => arr.contains(left),
                _ => false,
            },
            Operator::IsNull => left.is_null(),
            Operator::IsNotNull => !left.is_null(),
        }
    }
}

/// Order by clause
#[derive(Debug, Clone)]
pub struct OrderBy {
    /// Column to sort by
    pub column: String,
    /// Sort in descending order when true
    pub descending: bool,
}

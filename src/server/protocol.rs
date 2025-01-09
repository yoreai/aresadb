//! Wire Protocol for Client-Server Communication
//!
//! Binary protocol using bincode for efficient serialization.

use crate::storage::{Edge, Node, Value};
use serde::{Deserialize, Serialize};

/// Request types from client to server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Ping to check server health
    Ping,

    /// Disconnect from server
    Disconnect,

    /// Insert a new node
    InsertNode {
        /// Type label for the new node
        node_type: String,
        /// Property map for the new node
        properties: Value,
    },

    /// Get a node by ID
    GetNode {
        /// Unique node identifier
        id: String,
    },

    /// Update a node
    UpdateNode {
        /// Unique node identifier
        id: String,
        /// Updated property map
        properties: Value,
    },

    /// Delete a node
    DeleteNode {
        /// Unique node identifier
        id: String,
    },

    /// Get nodes by type
    GetNodesByType {
        /// Type label to filter by
        node_type: String,
        /// Maximum number of results
        limit: Option<usize>,
    },

    /// Create an edge
    CreateEdge {
        /// Source node identifier
        from_id: String,
        /// Target node identifier
        to_id: String,
        /// Relationship type label
        edge_type: String,
        /// Optional property map for the edge
        properties: Option<Value>,
    },

    /// Get edges from a node
    GetEdgesFrom {
        /// Source node identifier
        node_id: String,
        /// Optional edge type filter
        edge_type: Option<String>,
    },

    /// Get edges to a node
    GetEdgesTo {
        /// Target node identifier
        node_id: String,
        /// Optional edge type filter
        edge_type: Option<String>,
    },

    /// Delete an edge
    DeleteEdge {
        /// Unique edge identifier
        edge_id: String,
    },

    /// Execute SQL query
    Query {
        /// SQL query string
        sql: String,
        /// Maximum number of result rows
        limit: Option<usize>,
    },

    /// Graph traversal
    Traverse {
        /// Starting node identifier
        start_id: String,
        /// Maximum traversal depth
        depth: u32,
        /// Optional edge type filter list
        edge_types: Option<Vec<String>>,
    },

    /// Get database status
    Status,

    /// Begin a transaction
    BeginTransaction,

    /// Commit a transaction
    CommitTransaction {
        /// Transaction identifier
        tx_id: u64,
    },

    /// Rollback a transaction
    RollbackTransaction {
        /// Transaction identifier
        tx_id: u64,
    },
}

/// Response types from server to client
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    /// Pong response
    Pong,

    /// Goodbye (connection closing)
    Goodbye,

    /// Success with a single node
    Node(Node),

    /// Success with optional node
    MaybeNode(Option<Node>),

    /// Success with multiple nodes
    Nodes(Vec<Node>),

    /// Success with a single edge
    Edge(Edge),

    /// Success with multiple edges
    Edges(Vec<Edge>),

    /// Success with no data
    Ok,

    /// Query results
    QueryResult {
        /// Column names in the result set
        columns: Vec<String>,
        /// Row data as nested value arrays
        rows: Vec<Vec<Value>>,
        /// Number of rows affected by the query
        rows_affected: u64,
        /// Query execution time in milliseconds
        execution_time_ms: u64,
    },

    /// Traversal results
    TraversalResult {
        /// Nodes discovered during traversal
        nodes: Vec<Node>,
        /// Edges discovered during traversal
        edges: Vec<Edge>,
        /// Actual traversal depth reached
        depth: u32,
    },

    /// Database status
    Status {
        /// Database name
        name: String,
        /// Total number of nodes
        node_count: u64,
        /// Total number of edges
        edge_count: u64,
        /// Total storage size in bytes
        size_bytes: u64,
    },

    /// Transaction started
    TransactionStarted {
        /// Assigned transaction identifier
        tx_id: u64,
    },

    /// Transaction committed
    TransactionCommitted,

    /// Transaction rolled back
    TransactionRolledBack,

    /// Error response
    Error {
        /// Categorized error code
        code: ErrorCode,
        /// Human-readable error description
        message: String,
    },
}

/// Error codes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    /// Unknown error
    Unknown = 0,
    /// Invalid request format
    InvalidRequest = 1,
    /// Node not found
    NodeNotFound = 2,
    /// Edge not found
    EdgeNotFound = 3,
    /// Query parse error
    QueryParseError = 4,
    /// Query execution error
    QueryExecutionError = 5,
    /// Transaction error
    TransactionError = 6,
    /// Permission denied
    PermissionDenied = 7,
    /// Server overloaded
    ServerOverloaded = 8,
    /// Internal error
    InternalError = 9,
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorCode::Unknown => write!(f, "Unknown error"),
            ErrorCode::InvalidRequest => write!(f, "Invalid request"),
            ErrorCode::NodeNotFound => write!(f, "Node not found"),
            ErrorCode::EdgeNotFound => write!(f, "Edge not found"),
            ErrorCode::QueryParseError => write!(f, "Query parse error"),
            ErrorCode::QueryExecutionError => write!(f, "Query execution error"),
            ErrorCode::TransactionError => write!(f, "Transaction error"),
            ErrorCode::PermissionDenied => write!(f, "Permission denied"),
            ErrorCode::ServerOverloaded => write!(f, "Server overloaded"),
            ErrorCode::InternalError => write!(f, "Internal error"),
        }
    }
}

impl Response {
    /// Create an error response
    pub fn error(code: ErrorCode, message: impl Into<String>) -> Self {
        Response::Error {
            code,
            message: message.into(),
        }
    }

    /// Check if this is an error response
    pub fn is_error(&self) -> bool {
        matches!(self, Response::Error { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_serialization_json() {
        let request = Request::InsertNode {
            node_type: "user".to_string(),
            properties: Value::from_json(serde_json::json!({"name": "Alice"})).unwrap(),
        };

        let json = serde_json::to_vec(&request).unwrap();
        let deserialized: Request = serde_json::from_slice(&json).unwrap();

        match deserialized {
            Request::InsertNode { node_type, .. } => {
                assert_eq!(node_type, "user");
            }
            _ => panic!("Wrong request type"),
        }
    }

    #[test]
    fn test_request_serialization_simple() {
        let request = Request::Ping;
        let bytes = bincode::serialize(&request).unwrap();
        let deserialized: Request = bincode::deserialize(&bytes).unwrap();
        assert!(matches!(deserialized, Request::Ping));

        let request = Request::GetNode {
            id: "abc-123".to_string(),
        };
        let bytes = bincode::serialize(&request).unwrap();
        let deserialized: Request = bincode::deserialize(&bytes).unwrap();
        match deserialized {
            Request::GetNode { id } => assert_eq!(id, "abc-123"),
            _ => panic!("Wrong type"),
        }
    }

    #[test]
    fn test_response_serialization() {
        let response = Response::Status {
            name: "test".to_string(),
            node_count: 100,
            edge_count: 50,
            size_bytes: 1024,
        };

        let bytes = bincode::serialize(&response).unwrap();
        let deserialized: Response = bincode::deserialize(&bytes).unwrap();

        match deserialized {
            Response::Status {
                name, node_count, ..
            } => {
                assert_eq!(name, "test");
                assert_eq!(node_count, 100);
            }
            _ => panic!("Wrong response type"),
        }
    }

    #[test]
    fn test_error_response() {
        let response = Response::error(ErrorCode::NodeNotFound, "Node not found");
        assert!(response.is_error());

        match response {
            Response::Error { code, message } => {
                assert_eq!(code, ErrorCode::NodeNotFound);
                assert_eq!(message, "Node not found");
            }
            _ => panic!("Expected error response"),
        }
    }
}

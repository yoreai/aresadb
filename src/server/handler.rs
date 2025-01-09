//! Request Handler
//!
//! Processes incoming requests and interacts with storage.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use super::protocol::{ErrorCode, Request, Response};
use crate::distributed::ShardManager;
use crate::query::QueryEngine;
use crate::storage::{Database, Edge, Node, Value};

/// Request handler for processing client requests
pub struct RequestHandler {
    /// Database (single node mode)
    db: Option<Database>,
    /// Query engine (for SQL and traversal in single-node mode)
    query_engine: Option<QueryEngine>,
    /// Shard manager (distributed mode)
    shards: Option<ShardManager>,
    /// Active transactions
    transactions: RwLock<HashMap<u64, Transaction>>,
    /// Transaction ID counter
    tx_counter: AtomicU64,
}

struct Transaction {
    // For future use with actual transaction support
    #[allow(dead_code)]
    started_at: std::time::Instant,
}

impl RequestHandler {
    /// Create handler with a database
    pub fn new(db: Database) -> Self {
        let query_engine = QueryEngine::new(db.clone());
        Self {
            db: Some(db),
            query_engine: Some(query_engine),
            shards: None,
            transactions: RwLock::new(HashMap::new()),
            tx_counter: AtomicU64::new(1),
        }
    }

    /// Create handler with shards
    pub fn with_shards(shards: ShardManager) -> Self {
        Self {
            db: None,
            query_engine: None,
            shards: Some(shards),
            transactions: RwLock::new(HashMap::new()),
            tx_counter: AtomicU64::new(1),
        }
    }

    /// Handle a request
    pub async fn handle(&self, request: Request) -> Response {
        match request {
            Request::Ping => Response::Pong,
            Request::Disconnect => Response::Goodbye,

            Request::InsertNode {
                node_type,
                properties,
            } => self.handle_insert_node(&node_type, properties).await,

            Request::GetNode { id } => self.handle_get_node(&id).await,

            Request::UpdateNode { id, properties } => {
                self.handle_update_node(&id, properties).await
            }

            Request::DeleteNode { id } => self.handle_delete_node(&id).await,

            Request::GetNodesByType { node_type, limit } => {
                self.handle_get_nodes_by_type(&node_type, limit).await
            }

            Request::CreateEdge {
                from_id,
                to_id,
                edge_type,
                properties,
            } => {
                self.handle_create_edge(&from_id, &to_id, &edge_type, properties)
                    .await
            }

            Request::GetEdgesFrom { node_id, edge_type } => {
                self.handle_get_edges_from(&node_id, edge_type.as_deref())
                    .await
            }

            Request::GetEdgesTo { node_id, edge_type } => {
                self.handle_get_edges_to(&node_id, edge_type.as_deref())
                    .await
            }

            Request::DeleteEdge { edge_id } => self.handle_delete_edge(&edge_id).await,

            Request::Query { sql, limit } => self.handle_query(&sql, limit).await,

            Request::Traverse {
                start_id,
                depth,
                edge_types,
            } => self.handle_traverse(&start_id, depth, edge_types).await,

            Request::Status => self.handle_status().await,

            Request::BeginTransaction => self.handle_begin_transaction(),

            Request::CommitTransaction { tx_id } => self.handle_commit_transaction(tx_id),

            Request::RollbackTransaction { tx_id } => self.handle_rollback_transaction(tx_id),
        }
    }

    async fn handle_insert_node(&self, node_type: &str, properties: Value) -> Response {
        let props_json = properties.to_json();

        let result = if let Some(ref db) = self.db {
            db.insert_node(node_type, props_json).await
        } else if let Some(ref shards) = self.shards {
            let node = Node::new(node_type, properties);
            shards.insert_node(&node).await.map(|_| node)
        } else {
            return Response::error(ErrorCode::InternalError, "No storage configured");
        };

        match result {
            Ok(node) => Response::Node(node),
            Err(e) => Response::error(ErrorCode::InternalError, e.to_string()),
        }
    }

    async fn handle_get_node(&self, id: &str) -> Response {
        let result = if let Some(ref db) = self.db {
            db.get_node(id).await
        } else if let Some(ref shards) = self.shards {
            match crate::storage::NodeId::parse(id) {
                Ok(node_id) => shards.get_node(&node_id).await,
                Err(e) => return Response::error(ErrorCode::InvalidRequest, e.to_string()),
            }
        } else {
            return Response::error(ErrorCode::InternalError, "No storage configured");
        };

        match result {
            Ok(node) => Response::MaybeNode(node),
            Err(e) => Response::error(ErrorCode::InternalError, e.to_string()),
        }
    }

    async fn handle_update_node(&self, id: &str, properties: Value) -> Response {
        let props_json = properties.to_json();

        let result = if let Some(ref db) = self.db {
            db.update_node(id, props_json).await
        } else if let Some(ref shards) = self.shards {
            match crate::storage::NodeId::parse(id) {
                Ok(node_id) => shards.update_node(&node_id, properties).await,
                Err(e) => return Response::error(ErrorCode::InvalidRequest, e.to_string()),
            }
        } else {
            return Response::error(ErrorCode::InternalError, "No storage configured");
        };

        match result {
            Ok(node) => Response::Node(node),
            Err(e) => Response::error(ErrorCode::NodeNotFound, e.to_string()),
        }
    }

    async fn handle_delete_node(&self, id: &str) -> Response {
        let result = if let Some(ref db) = self.db {
            db.delete_node(id).await
        } else if let Some(ref shards) = self.shards {
            match crate::storage::NodeId::parse(id) {
                Ok(node_id) => shards.delete_node(&node_id).await,
                Err(e) => return Response::error(ErrorCode::InvalidRequest, e.to_string()),
            }
        } else {
            return Response::error(ErrorCode::InternalError, "No storage configured");
        };

        match result {
            Ok(_) => Response::Ok,
            Err(e) => Response::error(ErrorCode::NodeNotFound, e.to_string()),
        }
    }

    async fn handle_get_nodes_by_type(&self, node_type: &str, limit: Option<usize>) -> Response {
        let result = if let Some(ref db) = self.db {
            db.get_all_by_type(node_type, limit).await
        } else if let Some(ref shards) = self.shards {
            shards.get_nodes_by_type(node_type, limit).await
        } else {
            return Response::error(ErrorCode::InternalError, "No storage configured");
        };

        match result {
            Ok(nodes) => Response::Nodes(nodes),
            Err(e) => Response::error(ErrorCode::InternalError, e.to_string()),
        }
    }

    async fn handle_create_edge(
        &self,
        from_id: &str,
        to_id: &str,
        edge_type: &str,
        properties: Option<Value>,
    ) -> Response {
        let result = if let Some(ref db) = self.db {
            let props_json = properties.map(|p| p.to_json());
            db.create_edge(from_id, to_id, edge_type, props_json).await
        } else if let Some(ref shards) = self.shards {
            let from = match crate::storage::NodeId::parse(from_id) {
                Ok(id) => id,
                Err(e) => return Response::error(ErrorCode::InvalidRequest, e.to_string()),
            };
            let to = match crate::storage::NodeId::parse(to_id) {
                Ok(id) => id,
                Err(e) => return Response::error(ErrorCode::InvalidRequest, e.to_string()),
            };
            let edge = Edge::new(from, to, edge_type, properties.unwrap_or(Value::Null));
            shards.insert_edge(&edge).await.map(|_| edge)
        } else {
            return Response::error(ErrorCode::InternalError, "No storage configured");
        };

        match result {
            Ok(edge) => Response::Edge(edge),
            Err(e) => Response::error(ErrorCode::InternalError, e.to_string()),
        }
    }

    async fn handle_get_edges_from(&self, node_id: &str, edge_type: Option<&str>) -> Response {
        let result = if let Some(ref db) = self.db {
            db.get_edges_from(node_id, edge_type).await
        } else if let Some(ref shards) = self.shards {
            match crate::storage::NodeId::parse(node_id) {
                Ok(id) => shards.get_edges_from(&id, edge_type).await,
                Err(e) => return Response::error(ErrorCode::InvalidRequest, e.to_string()),
            }
        } else {
            return Response::error(ErrorCode::InternalError, "No storage configured");
        };

        match result {
            Ok(edges) => Response::Edges(edges),
            Err(e) => Response::error(ErrorCode::InternalError, e.to_string()),
        }
    }

    async fn handle_get_edges_to(&self, node_id: &str, edge_type: Option<&str>) -> Response {
        let result = if let Some(ref db) = self.db {
            db.get_edges_to(node_id, edge_type).await
        } else {
            return Response::error(
                ErrorCode::InternalError,
                "Sharded mode doesn't support edges_to",
            );
        };

        match result {
            Ok(edges) => Response::Edges(edges),
            Err(e) => Response::error(ErrorCode::InternalError, e.to_string()),
        }
    }

    async fn handle_delete_edge(&self, edge_id: &str) -> Response {
        if let Some(ref db) = self.db {
            match db.delete_edge(edge_id).await {
                Ok(_) => Response::Ok,
                Err(e) => Response::error(ErrorCode::EdgeNotFound, e.to_string()),
            }
        } else {
            Response::error(
                ErrorCode::InternalError,
                "Edge deletion not supported in sharded mode",
            )
        }
    }

    async fn handle_query(&self, sql: &str, limit: Option<usize>) -> Response {
        if let Some(ref engine) = self.query_engine {
            match engine.execute_sql(sql, limit).await {
                Ok(result) => Response::QueryResult {
                    columns: result.columns,
                    rows: result.rows,
                    rows_affected: result.rows_affected,
                    execution_time_ms: result.execution_time_ms,
                },
                Err(e) => Response::error(ErrorCode::QueryExecutionError, e.to_string()),
            }
        } else {
            Response::error(
                ErrorCode::InternalError,
                "Query execution not available in sharded mode",
            )
        }
    }

    async fn handle_traverse(
        &self,
        start_id: &str,
        depth: u32,
        edge_types: Option<Vec<String>>,
    ) -> Response {
        if let Some(ref engine) = self.query_engine {
            let types_refs: Option<Vec<&str>> = edge_types
                .as_ref()
                .map(|v| v.iter().map(|s| s.as_str()).collect());

            match engine.traverse(start_id, depth, types_refs).await {
                Ok(result) => Response::TraversalResult {
                    nodes: result.nodes,
                    edges: result.edges,
                    depth: result.depth,
                },
                Err(e) => Response::error(ErrorCode::QueryExecutionError, e.to_string()),
            }
        } else {
            Response::error(
                ErrorCode::InternalError,
                "Traversal not available in sharded mode",
            )
        }
    }

    async fn handle_status(&self) -> Response {
        if let Some(ref db) = self.db {
            match db.status().await {
                Ok(status) => Response::Status {
                    name: status.name,
                    node_count: status.node_count,
                    edge_count: status.edge_count,
                    size_bytes: status.size_bytes,
                },
                Err(e) => Response::error(ErrorCode::InternalError, e.to_string()),
            }
        } else if let Some(ref shards) = self.shards {
            match shards.stats().await {
                Ok(stats) => Response::Status {
                    name: "sharded".to_string(),
                    node_count: stats.total_nodes,
                    edge_count: stats.total_edges,
                    size_bytes: stats.total_size,
                },
                Err(e) => Response::error(ErrorCode::InternalError, e.to_string()),
            }
        } else {
            Response::error(ErrorCode::InternalError, "No storage configured")
        }
    }

    fn handle_begin_transaction(&self) -> Response {
        let tx_id = self.tx_counter.fetch_add(1, Ordering::SeqCst);
        self.transactions.write().insert(
            tx_id,
            Transaction {
                started_at: std::time::Instant::now(),
            },
        );
        Response::TransactionStarted { tx_id }
    }

    fn handle_commit_transaction(&self, tx_id: u64) -> Response {
        if self.transactions.write().remove(&tx_id).is_some() {
            Response::TransactionCommitted
        } else {
            Response::error(ErrorCode::TransactionError, "Transaction not found")
        }
    }

    fn handle_rollback_transaction(&self, tx_id: u64) -> Response {
        if self.transactions.write().remove(&tx_id).is_some() {
            Response::TransactionRolledBack
        } else {
            Response::error(ErrorCode::TransactionError, "Transaction not found")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_handler_ping() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "test").await.unwrap();
        let handler = RequestHandler::new(db);

        let response = handler.handle(Request::Ping).await;
        assert!(matches!(response, Response::Pong));
    }

    #[tokio::test]
    async fn test_handler_insert_get_node() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "test").await.unwrap();
        let handler = RequestHandler::new(db);

        // Insert
        let response = handler
            .handle(Request::InsertNode {
                node_type: "user".to_string(),
                properties: Value::from_json(serde_json::json!({"name": "Alice"})).unwrap(),
            })
            .await;

        let node_id = match response {
            Response::Node(node) => node.id.to_string(),
            _ => panic!("Expected Node response"),
        };

        // Get
        let response = handler.handle(Request::GetNode { id: node_id }).await;
        match response {
            Response::MaybeNode(Some(node)) => {
                assert_eq!(node.node_type, "user");
            }
            _ => panic!("Expected MaybeNode response"),
        }
    }

    #[tokio::test]
    async fn test_handler_query() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "test").await.unwrap();
        let handler = RequestHandler::new(db);

        // Insert some data first
        handler
            .handle(Request::InsertNode {
                node_type: "user".to_string(),
                properties: Value::from_json(serde_json::json!({"name": "Alice", "age": 30}))
                    .unwrap(),
            })
            .await;
        handler
            .handle(Request::InsertNode {
                node_type: "user".to_string(),
                properties: Value::from_json(serde_json::json!({"name": "Bob", "age": 25}))
                    .unwrap(),
            })
            .await;

        // Execute SQL query
        let response = handler
            .handle(Request::Query {
                sql: "SELECT * FROM user".to_string(),
                limit: None,
            })
            .await;

        match response {
            Response::QueryResult { columns, rows, .. } => {
                assert!(!columns.is_empty());
                assert_eq!(rows.len(), 2);
            }
            Response::Error { message, .. } => panic!("Query failed: {}", message),
            _ => panic!("Expected QueryResult response"),
        }
    }

    #[tokio::test]
    async fn test_handler_traverse() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "test").await.unwrap();
        let handler = RequestHandler::new(db);

        // Insert nodes
        let r1 = handler
            .handle(Request::InsertNode {
                node_type: "person".to_string(),
                properties: Value::from_json(serde_json::json!({"name": "Alice"})).unwrap(),
            })
            .await;
        let id1 = match r1 {
            Response::Node(n) => n.id.to_string(),
            _ => panic!("Expected node"),
        };

        let r2 = handler
            .handle(Request::InsertNode {
                node_type: "person".to_string(),
                properties: Value::from_json(serde_json::json!({"name": "Bob"})).unwrap(),
            })
            .await;
        let id2 = match r2 {
            Response::Node(n) => n.id.to_string(),
            _ => panic!("Expected node"),
        };

        // Create edge
        handler
            .handle(Request::CreateEdge {
                from_id: id1.clone(),
                to_id: id2.clone(),
                edge_type: "knows".to_string(),
                properties: None,
            })
            .await;

        // Traverse
        let response = handler
            .handle(Request::Traverse {
                start_id: id1,
                depth: 2,
                edge_types: None,
            })
            .await;

        match response {
            Response::TraversalResult { nodes, edges, .. } => {
                assert_eq!(nodes.len(), 2);
                assert_eq!(edges.len(), 1);
            }
            Response::Error { message, .. } => panic!("Traverse failed: {}", message),
            _ => panic!("Expected TraversalResult"),
        }
    }

    #[tokio::test]
    async fn test_handler_delete_edge() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "test").await.unwrap();
        let handler = RequestHandler::new(db);

        let r1 = handler
            .handle(Request::InsertNode {
                node_type: "person".to_string(),
                properties: Value::from_json(serde_json::json!({"name": "Alice"})).unwrap(),
            })
            .await;
        let id1 = match r1 {
            Response::Node(n) => n.id.to_string(),
            _ => panic!("Expected node"),
        };

        let r2 = handler
            .handle(Request::InsertNode {
                node_type: "person".to_string(),
                properties: Value::from_json(serde_json::json!({"name": "Bob"})).unwrap(),
            })
            .await;
        let id2 = match r2 {
            Response::Node(n) => n.id.to_string(),
            _ => panic!("Expected node"),
        };

        let edge_resp = handler
            .handle(Request::CreateEdge {
                from_id: id1.clone(),
                to_id: id2.clone(),
                edge_type: "knows".to_string(),
                properties: None,
            })
            .await;
        let edge_id = match edge_resp {
            Response::Edge(e) => e.id.to_string(),
            _ => panic!("Expected edge"),
        };

        // Delete the edge
        let response = handler.handle(Request::DeleteEdge { edge_id }).await;
        assert!(matches!(response, Response::Ok));

        // Verify it's gone
        let edges = handler
            .handle(Request::GetEdgesFrom {
                node_id: id1,
                edge_type: None,
            })
            .await;
        match edges {
            Response::Edges(e) => assert_eq!(e.len(), 0),
            _ => panic!("Expected Edges response"),
        }
    }

    #[tokio::test]
    async fn test_handler_transaction() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "test").await.unwrap();
        let handler = RequestHandler::new(db);

        // Begin
        let response = handler.handle(Request::BeginTransaction).await;
        let tx_id = match response {
            Response::TransactionStarted { tx_id } => tx_id,
            _ => panic!("Expected TransactionStarted response"),
        };

        // Commit
        let response = handler.handle(Request::CommitTransaction { tx_id }).await;
        assert!(matches!(response, Response::TransactionCommitted));
    }
}

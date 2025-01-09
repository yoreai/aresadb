//! AresaDB Client SDK
//!
//! Client library for connecting to AresaDB servers.

mod builder;
mod connection;

pub use builder::ClientBuilder;
pub use connection::Connection;

use anyhow::{bail, Context, Result};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::distributed::Compressor;
use crate::server::{Request, Response};
use crate::storage::{Edge, Node, Value};

/// AresaDB client for remote connections
pub struct Client {
    /// Server address
    addr: SocketAddr,
    /// Active TCP stream
    stream: TcpStream,
    /// Compressor for data transfer
    compressor: Option<Compressor>,
}

impl Client {
    /// Create a new client connected to the server
    pub async fn connect(addr: impl Into<SocketAddr>) -> Result<Self> {
        let addr = addr.into();
        let stream = TcpStream::connect(&addr)
            .await
            .context("Failed to connect to server")?;

        Ok(Self {
            addr,
            stream,
            compressor: Some(Compressor::new()),
        })
    }

    /// Create a client builder for configuration
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// Get the server address
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Ping the server
    pub async fn ping(&mut self) -> Result<()> {
        let response = self.send_request(Request::Ping).await?;
        match response {
            Response::Pong => Ok(()),
            Response::Error { message, .. } => bail!("Ping failed: {}", message),
            _ => bail!("Unexpected response"),
        }
    }

    /// Disconnect from the server
    pub async fn disconnect(mut self) -> Result<()> {
        let _ = self.send_request(Request::Disconnect).await;
        Ok(())
    }

    /// Insert a new node
    pub async fn insert_node(
        &mut self,
        node_type: &str,
        properties: serde_json::Value,
    ) -> Result<Node> {
        let props = Value::from_json(properties)?;
        let response = self
            .send_request(Request::InsertNode {
                node_type: node_type.to_string(),
                properties: props,
            })
            .await?;

        match response {
            Response::Node(node) => Ok(node),
            Response::Error { message, .. } => bail!("Insert failed: {}", message),
            _ => bail!("Unexpected response"),
        }
    }

    /// Get a node by ID
    pub async fn get_node(&mut self, id: &str) -> Result<Option<Node>> {
        let response = self
            .send_request(Request::GetNode { id: id.to_string() })
            .await?;

        match response {
            Response::MaybeNode(node) => Ok(node),
            Response::Error { message, .. } => bail!("Get failed: {}", message),
            _ => bail!("Unexpected response"),
        }
    }

    /// Update a node
    pub async fn update_node(&mut self, id: &str, properties: serde_json::Value) -> Result<Node> {
        let props = Value::from_json(properties)?;
        let response = self
            .send_request(Request::UpdateNode {
                id: id.to_string(),
                properties: props,
            })
            .await?;

        match response {
            Response::Node(node) => Ok(node),
            Response::Error { message, .. } => bail!("Update failed: {}", message),
            _ => bail!("Unexpected response"),
        }
    }

    /// Delete a node
    pub async fn delete_node(&mut self, id: &str) -> Result<()> {
        let response = self
            .send_request(Request::DeleteNode { id: id.to_string() })
            .await?;

        match response {
            Response::Ok => Ok(()),
            Response::Error { message, .. } => bail!("Delete failed: {}", message),
            _ => bail!("Unexpected response"),
        }
    }

    /// Get nodes by type
    pub async fn get_nodes_by_type(
        &mut self,
        node_type: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Node>> {
        let response = self
            .send_request(Request::GetNodesByType {
                node_type: node_type.to_string(),
                limit,
            })
            .await?;

        match response {
            Response::Nodes(nodes) => Ok(nodes),
            Response::Error { message, .. } => bail!("Query failed: {}", message),
            _ => bail!("Unexpected response"),
        }
    }

    /// Create an edge
    pub async fn create_edge(
        &mut self,
        from_id: &str,
        to_id: &str,
        edge_type: &str,
        properties: Option<serde_json::Value>,
    ) -> Result<Edge> {
        let props = properties.map(Value::from_json).transpose()?;
        let response = self
            .send_request(Request::CreateEdge {
                from_id: from_id.to_string(),
                to_id: to_id.to_string(),
                edge_type: edge_type.to_string(),
                properties: props,
            })
            .await?;

        match response {
            Response::Edge(edge) => Ok(edge),
            Response::Error { message, .. } => bail!("Create edge failed: {}", message),
            _ => bail!("Unexpected response"),
        }
    }

    /// Get edges from a node
    pub async fn get_edges_from(
        &mut self,
        node_id: &str,
        edge_type: Option<&str>,
    ) -> Result<Vec<Edge>> {
        let response = self
            .send_request(Request::GetEdgesFrom {
                node_id: node_id.to_string(),
                edge_type: edge_type.map(String::from),
            })
            .await?;

        match response {
            Response::Edges(edges) => Ok(edges),
            Response::Error { message, .. } => bail!("Query failed: {}", message),
            _ => bail!("Unexpected response"),
        }
    }

    /// Execute a SQL query
    pub async fn query(&mut self, sql: &str, limit: Option<usize>) -> Result<QueryResult> {
        let response = self
            .send_request(Request::Query {
                sql: sql.to_string(),
                limit,
            })
            .await?;

        match response {
            Response::QueryResult {
                columns,
                rows,
                rows_affected,
                execution_time_ms,
            } => Ok(QueryResult {
                columns,
                rows,
                rows_affected,
                execution_time_ms,
            }),
            Response::Error { message, .. } => bail!("Query failed: {}", message),
            _ => bail!("Unexpected response"),
        }
    }

    /// Get database status
    pub async fn status(&mut self) -> Result<DatabaseStatus> {
        let response = self.send_request(Request::Status).await?;

        match response {
            Response::Status {
                name,
                node_count,
                edge_count,
                size_bytes,
            } => Ok(DatabaseStatus {
                name,
                node_count,
                edge_count,
                size_bytes,
            }),
            Response::Error { message, .. } => bail!("Status failed: {}", message),
            _ => bail!("Unexpected response"),
        }
    }

    /// Begin a transaction
    pub async fn begin_transaction(&mut self) -> Result<u64> {
        let response = self.send_request(Request::BeginTransaction).await?;

        match response {
            Response::TransactionStarted { tx_id } => Ok(tx_id),
            Response::Error { message, .. } => bail!("Begin transaction failed: {}", message),
            _ => bail!("Unexpected response"),
        }
    }

    /// Commit a transaction
    pub async fn commit_transaction(&mut self, tx_id: u64) -> Result<()> {
        let response = self
            .send_request(Request::CommitTransaction { tx_id })
            .await?;

        match response {
            Response::TransactionCommitted => Ok(()),
            Response::Error { message, .. } => bail!("Commit failed: {}", message),
            _ => bail!("Unexpected response"),
        }
    }

    /// Rollback a transaction
    pub async fn rollback_transaction(&mut self, tx_id: u64) -> Result<()> {
        let response = self
            .send_request(Request::RollbackTransaction { tx_id })
            .await?;

        match response {
            Response::TransactionRolledBack => Ok(()),
            Response::Error { message, .. } => bail!("Rollback failed: {}", message),
            _ => bail!("Unexpected response"),
        }
    }

    // === Private methods ===

    async fn send_request(&mut self, request: Request) -> Result<Response> {
        // Serialize request
        let body = bincode::serialize(&request)?;

        // Compress if enabled
        let body = if let Some(ref comp) = self.compressor {
            comp.compress(&body)?
        } else {
            body
        };

        // Send length + body
        let len = body.len() as u32;
        self.stream.write_all(&len.to_le_bytes()).await?;
        self.stream.write_all(&body).await?;
        self.stream.flush().await?;

        // Read response length
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await?;
        let msg_len = u32::from_le_bytes(len_buf) as usize;

        // Read response body
        let mut body = vec![0u8; msg_len];
        self.stream.read_exact(&mut body).await?;

        // Decompress if enabled
        let body = if let Some(ref comp) = self.compressor {
            comp.decompress(&body)?
        } else {
            body
        };

        // Deserialize response
        let response: Response = bincode::deserialize(&body)?;
        Ok(response)
    }
}

/// Query result from the server
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Column names in the result set
    pub columns: Vec<String>,
    /// Row data as nested value arrays
    pub rows: Vec<Vec<Value>>,
    /// Number of rows affected by the query
    pub rows_affected: u64,
    /// Query execution time in milliseconds
    pub execution_time_ms: u64,
}

/// Database status
#[derive(Debug, Clone)]
pub struct DatabaseStatus {
    /// Database name
    pub name: String,
    /// Total number of nodes
    pub node_count: u64,
    /// Total number of edges
    pub edge_count: u64,
    /// Total storage size in bytes
    pub size_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_builder() {
        let builder = Client::builder()
            .host("localhost")
            .port(7432)
            .compression(true);

        // Builder should store configuration
        assert!(builder.compression);
    }
}

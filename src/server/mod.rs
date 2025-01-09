//! AresaDB Server
//!
//! TCP server for remote database access with connection pooling
//! and request handling.

mod handler;
mod pool;
mod protocol;

pub use handler::RequestHandler;
pub use pool::ConnectionPool;
pub use protocol::{ErrorCode, Request, Response};

use anyhow::{Context, Result};
use parking_lot::RwLock;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::distributed::ShardManager;
use crate::storage::Database;

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address to bind to
    pub bind_addr: SocketAddr,
    /// Maximum connections
    pub max_connections: usize,
    /// Read timeout in seconds
    pub read_timeout_secs: u64,
    /// Write timeout in seconds
    pub write_timeout_secs: u64,
    /// Enable compression
    pub compression: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:7432".parse().unwrap(),
            max_connections: 1000,
            read_timeout_secs: 30,
            write_timeout_secs: 30,
            compression: true,
        }
    }
}

/// AresaDB TCP Server
pub struct Server {
    config: ServerConfig,
    handler: Arc<RequestHandler>,
    pool: Arc<ConnectionPool>,
    /// Shutdown flag
    pub shutdown: Arc<RwLock<bool>>,
}

impl Server {
    /// Create a new server with a database
    pub fn new(db: Database, config: ServerConfig) -> Self {
        let handler = Arc::new(RequestHandler::new(db));
        let pool = Arc::new(ConnectionPool::new(config.max_connections));

        Self {
            config,
            handler,
            pool,
            shutdown: Arc::new(RwLock::new(false)),
        }
    }

    /// Create a new server with a shard manager
    pub fn with_shards(shards: ShardManager, config: ServerConfig) -> Self {
        let handler = Arc::new(RequestHandler::with_shards(shards));
        let pool = Arc::new(ConnectionPool::new(config.max_connections));

        Self {
            config,
            handler,
            pool,
            shutdown: Arc::new(RwLock::new(false)),
        }
    }

    /// Start the server
    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.config.bind_addr)
            .await
            .context("Failed to bind server")?;

        info!("AresaDB server listening on {}", self.config.bind_addr);

        while !*self.shutdown.read() {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    debug!("New connection from {}", addr);

                    // Check connection limit
                    if !self.pool.try_acquire() {
                        warn!("Connection limit reached, rejecting {}", addr);
                        continue;
                    }

                    let handler = Arc::clone(&self.handler);
                    let pool = Arc::clone(&self.pool);
                    let compression = self.config.compression;

                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, handler, compression).await {
                            warn!("Connection error from {}: {}", addr, e);
                        }
                        pool.release();
                        debug!("Connection closed: {}", addr);
                    });
                }
                Err(e) => {
                    error!("Accept error: {}", e);
                }
            }
        }

        info!("Server shutting down");
        Ok(())
    }

    /// Shutdown the server
    pub fn shutdown(&self) {
        *self.shutdown.write() = true;
    }

    /// Get current connection count
    pub fn connection_count(&self) -> usize {
        self.pool.active_count()
    }
}

/// Handle a single client connection
async fn handle_connection(
    mut stream: TcpStream,
    handler: Arc<RequestHandler>,
    compression: bool,
) -> Result<()> {
    let compressor = if compression {
        Some(crate::distributed::Compressor::new())
    } else {
        None
    };

    loop {
        // Read message length (4 bytes)
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        let msg_len = u32::from_le_bytes(len_buf) as usize;

        // Read message body
        let mut body = vec![0u8; msg_len];
        stream.read_exact(&mut body).await?;

        // Decompress if needed
        let body = if let Some(ref comp) = compressor {
            comp.decompress(&body)?
        } else {
            body
        };

        // Parse request
        let request: Request = match bincode::deserialize(&body) {
            Ok(req) => req,
            Err(e) => {
                let response = Response::Error {
                    code: ErrorCode::InvalidRequest,
                    message: format!("Failed to parse request: {}", e),
                };
                send_response(&mut stream, &response, compressor.as_ref()).await?;
                continue;
            }
        };

        // Handle request
        let response = handler.handle(request).await;

        // Send response
        send_response(&mut stream, &response, compressor.as_ref()).await?;

        // Check for disconnect request
        if matches!(response, Response::Goodbye) {
            break;
        }
    }

    Ok(())
}

/// Send a response to the client
async fn send_response(
    stream: &mut TcpStream,
    response: &Response,
    compressor: Option<&crate::distributed::Compressor>,
) -> Result<()> {
    let body = bincode::serialize(response)?;

    let body = if let Some(comp) = compressor {
        comp.compress(&body)?
    } else {
        body
    };

    let len = body.len() as u32;
    stream.write_all(&len.to_le_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_server_config() {
        let config = ServerConfig::default();
        assert_eq!(config.max_connections, 1000);
        assert!(config.compression);
    }

    #[tokio::test]
    async fn test_server_creation() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "test").await.unwrap();

        let config = ServerConfig::default();
        let server = Server::new(db, config);

        assert_eq!(server.connection_count(), 0);
    }
}

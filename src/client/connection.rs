//! Connection Management
//!
//! Low-level TCP connection handling with reconnection support.

use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;

/// Connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Not connected
    Disconnected,
    /// Connecting
    Connecting,
    /// Connected
    Connected,
    /// Connection error
    Error,
}

/// Connection wrapper with reconnection support
pub struct Connection {
    /// Server address
    addr: SocketAddr,
    /// TCP stream
    stream: Option<TcpStream>,
    /// Connection state
    state: ConnectionState,
    /// Connection timeout
    timeout_secs: u64,
    /// Max reconnection attempts
    max_retries: u32,
}

impl Connection {
    /// Create a new connection
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            stream: None,
            state: ConnectionState::Disconnected,
            timeout_secs: 10,
            max_retries: 3,
        }
    }

    /// Set connection timeout
    pub fn with_timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }

    /// Set max retries
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    /// Connect to the server
    pub async fn connect(&mut self) -> Result<()> {
        self.state = ConnectionState::Connecting;

        let connect_timeout = Duration::from_secs(self.timeout_secs);

        for attempt in 0..self.max_retries {
            match timeout(connect_timeout, TcpStream::connect(&self.addr)).await {
                Ok(Ok(stream)) => {
                    self.stream = Some(stream);
                    self.state = ConnectionState::Connected;
                    return Ok(());
                }
                Ok(Err(e)) => {
                    if attempt + 1 < self.max_retries {
                        tokio::time::sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
                        continue;
                    }
                    self.state = ConnectionState::Error;
                    return Err(e).context("Failed to connect");
                }
                Err(_) => {
                    if attempt + 1 < self.max_retries {
                        continue;
                    }
                    self.state = ConnectionState::Error;
                    anyhow::bail!("Connection timeout");
                }
            }
        }

        self.state = ConnectionState::Error;
        anyhow::bail!("Failed to connect after {} attempts", self.max_retries)
    }

    /// Disconnect
    pub fn disconnect(&mut self) {
        self.stream = None;
        self.state = ConnectionState::Disconnected;
    }

    /// Get connection state
    pub fn state(&self) -> ConnectionState {
        self.state
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        self.state == ConnectionState::Connected && self.stream.is_some()
    }

    /// Get the underlying stream
    pub fn stream(&mut self) -> Option<&mut TcpStream> {
        self.stream.as_mut()
    }

    /// Take the stream (for transferring ownership)
    pub fn take_stream(&mut self) -> Option<TcpStream> {
        self.state = ConnectionState::Disconnected;
        self.stream.take()
    }

    /// Reconnect if disconnected
    pub async fn ensure_connected(&mut self) -> Result<()> {
        if !self.is_connected() {
            self.connect().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_state() {
        let addr: SocketAddr = "127.0.0.1:7432".parse().unwrap();
        let conn = Connection::new(addr);

        assert_eq!(conn.state(), ConnectionState::Disconnected);
        assert!(!conn.is_connected());
    }

    #[test]
    fn test_connection_config() {
        let addr: SocketAddr = "127.0.0.1:7432".parse().unwrap();
        let conn = Connection::new(addr).with_timeout(30).with_retries(5);

        assert_eq!(conn.timeout_secs, 30);
        assert_eq!(conn.max_retries, 5);
    }
}

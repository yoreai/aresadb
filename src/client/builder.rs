//! Client Builder
//!
//! Fluent API for building AresaDB clients.

use anyhow::{Context, Result};
use std::net::SocketAddr;

use super::Client;

/// Builder for creating AresaDB clients
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    host: String,
    port: u16,
    pub(crate) compression: bool,
    timeout_secs: u64,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientBuilder {
    /// Create a new client builder with defaults
    pub fn new() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 7432,
            compression: true,
            timeout_secs: 10,
        }
    }

    /// Set the server host
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    /// Set the server port
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the server address from a string
    pub fn address(mut self, addr: &str) -> Self {
        if let Some((host, port)) = addr.rsplit_once(':') {
            self.host = host.to_string();
            if let Ok(p) = port.parse() {
                self.port = p;
            }
        } else {
            self.host = addr.to_string();
        }
        self
    }

    /// Enable or disable compression
    pub fn compression(mut self, enabled: bool) -> Self {
        self.compression = enabled;
        self
    }

    /// Set connection timeout in seconds
    pub fn timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }

    /// Build and connect the client
    pub async fn build(self) -> Result<Client> {
        let addr: SocketAddr = format!("{}:{}", self.host, self.port)
            .parse()
            .context("Invalid server address")?;

        let mut client = Client::connect(addr).await?;

        if !self.compression {
            client.compressor = None;
        }

        Ok(client)
    }

    /// Get the configured address
    pub fn get_address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_defaults() {
        let builder = ClientBuilder::new();

        assert_eq!(builder.host, "127.0.0.1");
        assert_eq!(builder.port, 7432);
        assert!(builder.compression);
    }

    #[test]
    fn test_builder_configuration() {
        let builder = ClientBuilder::new()
            .host("example.com")
            .port(8080)
            .compression(false)
            .timeout(30);

        assert_eq!(builder.host, "example.com");
        assert_eq!(builder.port, 8080);
        assert!(!builder.compression);
        assert_eq!(builder.timeout_secs, 30);
    }

    #[test]
    fn test_builder_address() {
        let builder = ClientBuilder::new().address("db.example.com:9000");

        assert_eq!(builder.host, "db.example.com");
        assert_eq!(builder.port, 9000);
    }

    #[test]
    fn test_get_address() {
        let builder = ClientBuilder::new().host("localhost").port(7432);

        assert_eq!(builder.get_address(), "localhost:7432");
    }
}

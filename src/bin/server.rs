//! AresaDB Server Binary
//!
//! Standalone server for remote database access.

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser)]
#[command(name = "aresadb-server")]
#[command(about = "AresaDB server for remote connections")]
struct Args {
    /// Database path
    #[arg(short, long, default_value = ".")]
    database: String,

    /// Bind address
    #[arg(short, long, default_value = "127.0.0.1:7432")]
    bind: String,

    /// Maximum connections
    #[arg(short, long, default_value = "1000")]
    max_connections: usize,

    /// Enable compression
    #[arg(short, long, default_value = "true")]
    compression: bool,

    /// Number of shards (0 for single-node mode)
    #[arg(short, long, default_value = "0")]
    shards: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "aresadb=info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();

    tracing::info!("Starting AresaDB server...");
    tracing::info!("Database path: {}", args.database);
    tracing::info!("Bind address: {}", args.bind);

    let config = aresadb::server::ServerConfig {
        bind_addr: args.bind.parse()?,
        max_connections: args.max_connections,
        compression: args.compression,
        ..Default::default()
    };

    let server = if args.shards > 0 {
        tracing::info!("Sharded mode with {} shards", args.shards);

        let shard_config = aresadb::distributed::ShardConfig {
            num_shards: args.shards,
            base_path: std::path::PathBuf::from(&args.database),
            ..Default::default()
        };

        let shards = aresadb::distributed::ShardManager::new(shard_config).await?;
        aresadb::server::Server::with_shards(shards, config)
    } else {
        tracing::info!("Single-node mode");

        // Try to open existing database, or create new one
        let db = match aresadb::storage::Database::open(&args.database).await {
            Ok(db) => db,
            Err(_) => {
                tracing::info!("Creating new database");
                aresadb::storage::Database::create(&args.database, "aresadb").await?
            }
        };

        aresadb::server::Server::new(db, config)
    };

    // Handle shutdown signal
    let shutdown = server.shutdown.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Shutdown signal received");
        *shutdown.write() = true;
    });

    server.run().await?;

    Ok(())
}

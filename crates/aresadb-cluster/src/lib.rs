//! Cluster lifecycle and admin API for AresaDB v2.
//!
//! See the crate README for the overall shape. The quickest way in:
//!
//! ```ignore
//! use aresadb_cluster::{ClusterNode, NodeConfig};
//!
//! # async fn demo() -> anyhow::Result<()> {
//! let cfg = NodeConfig::new(
//!     1,
//!     "127.0.0.1:7001".parse()?,
//!     "/var/lib/aresa/1",
//! );
//! let node = ClusterNode::bootstrap_single(cfg).await?;
//! // ... do work ...
//! node.shutdown().await?;
//! # Ok(()) }
//! ```

#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod admin;
pub mod config;
pub mod error;
pub mod node;
pub mod pd_supervisor;
pub mod range;

pub use admin::{AdminService, ClusterAdmin, ClusterAdminClient, ClusterAdminServer};
pub use config::{DataEngine, NodeConfig};
pub use error::{ClusterError, ClusterResult, ReadError, ReadResult, WriteError, WriteResult};
pub use node::{ClusterNode, DEFAULT_RAFT_GROUP_ID, DEFAULT_RANGE_ID};
pub use pd_supervisor::{PdSupervisor, PdSupervisorConfig, PdSupervisorError, PdSupervisorHandle};
pub use range::{LeadershipStatus, RangeDirectory, RangeDirectoryError, RangeRuntime};

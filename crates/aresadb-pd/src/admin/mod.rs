//! Placement-driver admin gRPC layer.
//!
//! This is the control-plane surface the operator CLI and client
//! libraries talk to. Split into three pieces so each one stays
//! small and independently testable:
//!
//! - [`pb`] — tonic-generated server and client stubs for
//!   `proto/pd.proto`.
//! - [`convert`] — total conversions between the protobuf messages
//!   and the Rust catalog types.
//! - [`server`] — [`PdAdminService`] adapts an `openraft::Raft`
//!   handle + the local [`crate::PdStateMachine`] to the tonic
//!   service trait. Mutations go through Raft, reads are served
//!   locally.
//! - [`client`] — [`PdAdminClient`] is a typed wrapper around the
//!   raw tonic client. Callers get Rust types back (not protobuf
//!   structs) and `ForwardToLeader` errors surface as a dedicated
//!   variant so clients can retry against the right endpoint.
//! - [`heartbeat`] — [`HeartbeatLoop`] spawns a background task
//!   that sends periodic `HeartbeatNode` RPCs, with a cancellation
//!   channel for graceful shutdown.

// Generated protobuf types don't come with doc comments; suppress
// the crate-wide `missing_docs` lint just for the pb module.
#[allow(missing_docs)]
pub mod pb {
    tonic::include_proto!("aresadb.pd.v1");
}

pub mod client;
pub mod convert;
pub mod heartbeat;
pub mod server;

pub use client::{PdAdminClient, PdAdminClientError};
pub use heartbeat::{ClockFn, EndpointResolver, HeartbeatConfig, HeartbeatHandle, HeartbeatLoop};
pub use pb::placement_driver_admin_client::PlacementDriverAdminClient;
pub use pb::placement_driver_admin_server::{PlacementDriverAdmin, PlacementDriverAdminServer};
pub use server::PdAdminService;

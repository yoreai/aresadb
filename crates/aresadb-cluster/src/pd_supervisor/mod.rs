//! Placement-Driver-driven orchestration for `ClusterNode`.
//!
//! Every node that participates in a PD-managed cluster spawns one
//! [`PdSupervisor`]. The supervisor owns two concurrent tasks:
//!
//! 1. A **heartbeat** task (wrapper around
//!    [`aresadb_pd::HeartbeatLoop`]) that keeps the PD catalog
//!    refreshed with this node's liveness timestamp.
//! 2. A **reconciliation** task that, at a fixed cadence, asks PD
//!    for the current catalog, diffs it against the local
//!    `RangeDirectory`, and converges — calling the in-process
//!    equivalent of `AddRange` for every range assigned to this
//!    node that isn't already open, and `RemoveRange` for every
//!    locally-open range that PD no longer assigns to us.
//!
//! The supervisor intentionally **never touches the default range**
//! ([`DEFAULT_RANGE_ID`](crate::DEFAULT_RANGE_ID)). That range is a
//! Phase 1 back-compat artefact — it spans the whole keyspace on
//! every node and isn't in the PD catalog. Ranges created by PD are
//! guaranteed to have `range_id >= 2` and live on a disjoint set of
//! backends; spans may overlap at the keyspace level but the
//! supervisor only cares about which `RangeRuntime` to open and
//! close.
//!
//! This module is organised into three layers:
//!
//! * [`reconciler`] — pure logic that turns a PD catalog view and a
//!   local directory view into a [`ReconcilePlan`]. Fully unit
//!   tested; knows nothing about I/O.
//! * [`executor`] — applies a [`ReconcilePlan`] against a real
//!   `RangeDirectory` by opening new `RangeRuntime` values and
//!   shutting down stale ones.
//! * [`supervisor`] — the long-running task that drives the above
//!   two on a timer and handles PD connectivity.

pub mod config;
pub mod executor;
pub mod reconciler;
pub mod supervisor;

pub use config::PdSupervisorConfig;
pub use executor::{ExecutorError, ExecutorReport};
pub use reconciler::{plan_reconcile, ReconcilePlan};
pub use supervisor::{PdSupervisor, PdSupervisorError, PdSupervisorHandle};

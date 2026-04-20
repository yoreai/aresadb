//! Configuration for [`PdSupervisor`](super::PdSupervisor).
//!
//! Kept separate from [`NodeConfig`](crate::NodeConfig) because PD
//! integration is opt-in: a node may run single-process (no PD at
//! all), managed by an external control plane, or orchestrated by
//! the `aresadb-pd` catalog. `PdSupervisorConfig` describes just the
//! last mode.

use std::collections::BTreeSet;
use std::time::Duration;

use aresadb_pd::types::{RangeId, StoreId};
use aresadb_raft::NodeId;

use crate::DEFAULT_RANGE_ID;

/// Default cadence for the PD heartbeat. Matches the one
/// `HeartbeatLoop` documentation recommends for production: low
/// enough that a failing node is detected within ~3 ticks of the
/// catalog's configured liveness timer, high enough that heartbeat
/// traffic stays negligible compared to actual replication.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(1_000);

/// Default cadence for the reconciler. Runs slower than the
/// heartbeat: catalog changes are rare in steady state, and
/// opening / closing [`RangeRuntime`](crate::RangeRuntime)s is
/// expensive enough that we don't want to thrash.
pub const DEFAULT_RECONCILE_INTERVAL: Duration = Duration::from_millis(1_000);

/// Everything the supervisor needs to know to drive PD integration
/// for one node. Construct via [`PdSupervisorConfig::new`] and tune
/// intervals via the builder methods.
#[derive(Debug, Clone)]
pub struct PdSupervisorConfig {
    /// Id this node advertises to the PD when registering /
    /// heartbeating. Must match
    /// [`NodeConfig::node_id`](crate::NodeConfig).
    pub node_id: NodeId,

    /// Storage-engine instance id — used by PD's catalog when
    /// placing replicas. A single-engine node passes the same value
    /// every time (typically `1`); multi-store deployments will
    /// advertise distinct store ids per local engine.
    pub store_id: StoreId,

    /// Address peers (and PD) should dial to reach this node. Same
    /// value as [`NodeConfig::effective_advertise_addr`](crate::NodeConfig::effective_advertise_addr)
    /// when a [`ClusterNode`](crate::ClusterNode) owns the supervisor.
    pub advertise_addr: String,

    /// Ordered list of PD admin endpoints. The first is the
    /// primary; any subsequent entries are tried as fallbacks on
    /// dial failure. Typical values are `http://host:port` URLs.
    pub pd_endpoints: Vec<String>,

    /// How often to send a `HeartbeatNode` RPC to the PD leader.
    /// Defaults to [`DEFAULT_HEARTBEAT_INTERVAL`].
    pub heartbeat_interval: Duration,

    /// How often to run the reconciliation tick. Defaults to
    /// [`DEFAULT_RECONCILE_INTERVAL`].
    pub reconcile_interval: Duration,

    /// Range ids the supervisor must never close locally, even if
    /// the PD catalog doesn't know about them. The default value
    /// contains [`DEFAULT_RANGE_ID`] so the back-compat default
    /// range survives every reconcile. Tests can extend it.
    pub skip_local_ranges: BTreeSet<RangeId>,
}

impl PdSupervisorConfig {
    /// Build a config with default intervals and the default skip
    /// list (`{DEFAULT_RANGE_ID}`).
    pub fn new(
        node_id: NodeId,
        advertise_addr: impl Into<String>,
        pd_endpoints: Vec<String>,
    ) -> Self {
        let mut skip = BTreeSet::new();
        skip.insert(DEFAULT_RANGE_ID);
        Self {
            node_id,
            store_id: 1,
            advertise_addr: advertise_addr.into(),
            pd_endpoints,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            reconcile_interval: DEFAULT_RECONCILE_INTERVAL,
            skip_local_ranges: skip,
        }
    }

    /// Override the store id. Useful for multi-store deployments
    /// where each local engine has its own identity in the catalog.
    pub fn with_store_id(mut self, store_id: StoreId) -> Self {
        self.store_id = store_id;
        self
    }

    /// Override the heartbeat cadence. Takes effect when the
    /// supervisor is next spawned.
    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Override the reconcile cadence. Takes effect when the
    /// supervisor is next spawned.
    pub fn with_reconcile_interval(mut self, interval: Duration) -> Self {
        self.reconcile_interval = interval;
        self
    }

    /// Replace the skip list wholesale. Callers that want to
    /// *extend* the defaults should read the existing set first.
    pub fn with_skip_local_ranges(mut self, ids: BTreeSet<RangeId>) -> Self {
        self.skip_local_ranges = ids;
        self
    }

    /// Add a single range id to the skip list. Idempotent.
    pub fn skip_local_range(mut self, id: RangeId) -> Self {
        self.skip_local_ranges.insert(id);
        self
    }

    /// Returns the primary PD endpoint, or `None` if none were
    /// configured. The supervisor refuses to spawn with an empty
    /// list; this accessor is for callers that want to preflight
    /// the value.
    pub fn primary_endpoint(&self) -> Option<&str> {
        self.pd_endpoints.first().map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_populates_defaults() {
        let cfg = PdSupervisorConfig::new(
            7,
            "http://127.0.0.1:7001",
            vec!["http://127.0.0.1:9000".to_string()],
        );
        assert_eq!(cfg.node_id, 7);
        assert_eq!(cfg.store_id, 1);
        assert_eq!(cfg.advertise_addr, "http://127.0.0.1:7001");
        assert_eq!(cfg.pd_endpoints, vec!["http://127.0.0.1:9000".to_string()]);
        assert_eq!(cfg.heartbeat_interval, DEFAULT_HEARTBEAT_INTERVAL);
        assert_eq!(cfg.reconcile_interval, DEFAULT_RECONCILE_INTERVAL);
        assert_eq!(cfg.skip_local_ranges, {
            let mut s = BTreeSet::new();
            s.insert(DEFAULT_RANGE_ID);
            s
        });
    }

    #[test]
    fn builders_override_fields() {
        let cfg = PdSupervisorConfig::new(1, "addr", vec!["ep".to_string()])
            .with_store_id(3)
            .with_heartbeat_interval(Duration::from_millis(250))
            .with_reconcile_interval(Duration::from_millis(500))
            .skip_local_range(42);
        assert_eq!(cfg.store_id, 3);
        assert_eq!(cfg.heartbeat_interval, Duration::from_millis(250));
        assert_eq!(cfg.reconcile_interval, Duration::from_millis(500));
        assert!(cfg.skip_local_ranges.contains(&DEFAULT_RANGE_ID));
        assert!(cfg.skip_local_ranges.contains(&42));
    }

    #[test]
    fn with_skip_local_ranges_replaces_defaults() {
        let mut custom = BTreeSet::new();
        custom.insert(10);
        custom.insert(20);
        let cfg = PdSupervisorConfig::new(1, "addr", vec!["ep".to_string()])
            .with_skip_local_ranges(custom.clone());
        assert_eq!(cfg.skip_local_ranges, custom);
    }

    #[test]
    fn primary_endpoint_reflects_list() {
        let cfg = PdSupervisorConfig::new(
            1,
            "addr",
            vec!["http://a:1".to_string(), "http://b:2".to_string()],
        );
        assert_eq!(cfg.primary_endpoint(), Some("http://a:1"));
        let empty = PdSupervisorConfig::new(1, "addr", vec![]);
        assert_eq!(empty.primary_endpoint(), None);
    }
}

//! Declarative node configuration.
//!
//! Everything callers need to describe a node lives here. The goal is
//! that the CLI, the test harness, and the eventual Kubernetes operator
//! can all construct [`NodeConfig`] programmatically without duplicating
//! defaults.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use aresadb_pd::types::RangeId;
use aresadb_raft::NodeId;

/// Which engine `aresadb-cluster` opens for a range's **data**
/// backend (the state-machine key/value store). The Raft log backend
/// is intentionally excluded from this knob and always uses redb —
/// the log is append-heavy with single-writer semantics and one
/// fsync per commit, which is exactly redb's sweet spot, so switching
/// to an LSM engine would only add write amplification for no win.
///
/// Defaults to [`DataEngine::Redb`] so Phase 1-2c deployments upgrade
/// to Phase 2d without any config change. Operators who want the LSM
/// engine on hot data ranges flip a single knob
/// (`NodeConfig::with_data_engine(DataEngine::Lsm)`) and the per-
/// range directory layout adjusts accordingly (see
/// [`NodeConfig::range_data_path`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DataEngine {
    /// redb — default. Single-file, embedded, fsync-per-commit. Best
    /// for metadata stores and small embedded deployments.
    #[default]
    Redb,
    /// fjall — LSM tree. Journal + memtable + levelled SSTables.
    /// Amortises writes, wins on hot data ranges. Phase 2d opt-in.
    Lsm,
}

impl DataEngine {
    /// Lower-case identifier used in filesystem path extensions so
    /// the two engines can coexist under `<range>/data/` without
    /// stomping each other. `data.redb` / `data.lsm`.
    pub fn path_suffix(&self) -> &'static str {
        match self {
            DataEngine::Redb => "data.redb",
            DataEngine::Lsm => "data.lsm",
        }
    }

    /// Human-friendly label used in logs / CLI output.
    pub fn label(&self) -> &'static str {
        match self {
            DataEngine::Redb => "redb",
            DataEngine::Lsm => "lsm",
        }
    }
}

/// Full description of a node's runtime identity.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Stable, cluster-wide node identifier. Operators are responsible
    /// for choosing one that's never been used in this cluster before —
    /// reusing an id would confuse openraft's log indexing.
    pub node_id: NodeId,

    /// Socket address the gRPC server binds to locally. Must be a
    /// concrete `SocketAddr` (not a DNS name) so there's no
    /// ambiguity about where we listen; the advertised address can
    /// still be different.
    pub listen_addr: SocketAddr,

    /// How other nodes should reach this one. Typically the same host
    /// as `listen_addr` but with a public IP / DNS name and the same
    /// port. Falls back to `http://<listen_addr>` if unset.
    pub advertise_addr: Option<String>,

    /// Directory that owns the node's on-disk state. Two sub-directories
    /// live inside: `raft-log/` and `state-machine/`.
    pub data_dir: PathBuf,

    /// Human-friendly cluster label, used by openraft for log messages
    /// and metrics.
    pub cluster_name: String,

    /// Engine used for per-range **data** backends. The Raft log and
    /// the default-range state machine stay on redb; this knob only
    /// affects ranges opened via `range_data_path`. Defaults to
    /// [`DataEngine::Redb`], keeping every existing deployment on
    /// the same engine it bootstrapped on.
    pub data_engine: DataEngine,
}

impl NodeConfig {
    /// Build a fresh config with sensible defaults.
    pub fn new(node_id: NodeId, listen_addr: SocketAddr, data_dir: impl Into<PathBuf>) -> Self {
        Self {
            node_id,
            listen_addr,
            advertise_addr: None,
            data_dir: data_dir.into(),
            cluster_name: "aresadb".to_string(),
            data_engine: DataEngine::default(),
        }
    }

    /// Override the advertised address. Most useful when the node
    /// binds to `0.0.0.0` but peers need to reach it via a specific
    /// external hostname.
    pub fn with_advertise_addr(mut self, addr: impl Into<String>) -> Self {
        self.advertise_addr = Some(addr.into());
        self
    }

    /// Override the cluster name.
    pub fn with_cluster_name(mut self, name: impl Into<String>) -> Self {
        self.cluster_name = name.into();
        self
    }

    /// Select the engine used for per-range data backends. See
    /// [`DataEngine`] for the behavioural differences.
    pub fn with_data_engine(mut self, engine: DataEngine) -> Self {
        self.data_engine = engine;
        self
    }

    /// Return the address other nodes should dial to reach us.
    pub fn effective_advertise_addr(&self) -> String {
        self.advertise_addr
            .clone()
            .unwrap_or_else(|| format!("http://{}", self.listen_addr))
    }

    /// Directory that holds the Raft log backend.
    pub fn raft_log_dir(&self) -> PathBuf {
        self.data_dir.join("raft-log")
    }

    /// Directory that holds the state-machine backend.
    pub fn state_machine_dir(&self) -> PathBuf {
        self.data_dir.join("state-machine")
    }

    /// File path for the Raft log redb database.
    pub fn raft_log_path(&self) -> PathBuf {
        self.raft_log_dir().join("log.redb")
    }

    /// File path for the state machine redb database.
    pub fn state_machine_path(&self) -> PathBuf {
        self.state_machine_dir().join("data.redb")
    }

    /// Parent directory that owns every per-range subdirectory. Ranges
    /// spawn under `<data-dir>/ranges/<range_id>/`; this accessor lets
    /// administrative tools enumerate them without reaching into
    /// filesystem helpers directly.
    pub fn ranges_root(&self) -> PathBuf {
        self.data_dir.join("ranges")
    }

    /// Directory that owns a single range's entire persistent state
    /// (log backend + data backend).
    pub fn range_dir(&self, range_id: RangeId) -> PathBuf {
        self.ranges_root().join(range_id.to_string())
    }

    /// Directory that holds the Raft log backend for the given range.
    /// Kept on a distinct subdirectory from `range_data_dir` so a
    /// future migration can point each at a different engine without
    /// churning paths.
    pub fn range_log_dir(&self, range_id: RangeId) -> PathBuf {
        self.range_dir(range_id).join("log")
    }

    /// Directory that holds the state-machine backend for the given
    /// range.
    pub fn range_data_dir(&self, range_id: RangeId) -> PathBuf {
        self.range_dir(range_id).join("data")
    }

    /// File path for the per-range Raft log redb database. The log
    /// backend is always redb (see [`DataEngine`] for why), so this
    /// never varies.
    pub fn range_log_path(&self, range_id: RangeId) -> PathBuf {
        self.range_log_dir(range_id).join("log.redb")
    }

    /// Filesystem target for the per-range state-machine data
    /// backend.
    ///
    /// The suffix is engine-specific — `data.redb` when
    /// [`NodeConfig::data_engine`] is [`DataEngine::Redb`],
    /// `data.lsm` (a directory fjall manages) when it's
    /// [`DataEngine::Lsm`]. Switching engines therefore doesn't
    /// stomp an existing range's on-disk layout; operators who
    /// migrate have to explicitly move / drop the old directory.
    pub fn range_data_path(&self, range_id: RangeId) -> PathBuf {
        self.range_data_dir(range_id)
            .join(self.data_engine.path_suffix())
    }

    /// Create the per-range `log/` and `data/` subdirectories for
    /// `range_id`. Idempotent — safe to call on every open, including
    /// reopens.
    pub fn ensure_range_dirs(&self, range_id: RangeId) -> std::io::Result<()> {
        for dir in [self.range_log_dir(range_id), self.range_data_dir(range_id)] {
            ensure_dir(&dir)?;
        }
        Ok(())
    }

    /// Create every directory the backend expects to find.
    pub fn ensure_dirs(&self) -> std::io::Result<()> {
        for dir in [self.raft_log_dir(), self.state_machine_dir()] {
            ensure_dir(&dir)?;
        }
        Ok(())
    }
}

fn ensure_dir(path: &Path) -> std::io::Result<()> {
    match std::fs::create_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_advertise_addr_matches_listen_addr() {
        let cfg = NodeConfig::new(1, "127.0.0.1:7001".parse().unwrap(), "/tmp/nonexistent");
        assert_eq!(cfg.effective_advertise_addr(), "http://127.0.0.1:7001");
    }

    #[test]
    fn with_advertise_addr_overrides_default() {
        let cfg = NodeConfig::new(1, "0.0.0.0:7001".parse().unwrap(), "/tmp/nonexistent")
            .with_advertise_addr("http://node1.aresadb.example:7001");
        assert_eq!(
            cfg.effective_advertise_addr(),
            "http://node1.aresadb.example:7001"
        );
    }

    #[test]
    fn paths_live_under_data_dir() {
        let cfg = NodeConfig::new(1, "127.0.0.1:7001".parse().unwrap(), "/var/lib/aresa/1");
        assert_eq!(
            cfg.raft_log_dir(),
            PathBuf::from("/var/lib/aresa/1/raft-log")
        );
        assert_eq!(
            cfg.state_machine_dir(),
            PathBuf::from("/var/lib/aresa/1/state-machine")
        );
    }

    #[test]
    fn ensure_dirs_creates_nested_structure() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = NodeConfig::new(1, "127.0.0.1:7001".parse().unwrap(), dir.path());
        cfg.ensure_dirs().unwrap();
        assert!(cfg.raft_log_dir().is_dir());
        assert!(cfg.state_machine_dir().is_dir());
    }

    #[test]
    fn range_paths_live_under_ranges_root() {
        let cfg = NodeConfig::new(1, "127.0.0.1:7001".parse().unwrap(), "/var/lib/aresa/1");
        assert_eq!(cfg.ranges_root(), PathBuf::from("/var/lib/aresa/1/ranges"));
        assert_eq!(
            cfg.range_dir(42),
            PathBuf::from("/var/lib/aresa/1/ranges/42")
        );
        assert_eq!(
            cfg.range_log_dir(42),
            PathBuf::from("/var/lib/aresa/1/ranges/42/log")
        );
        assert_eq!(
            cfg.range_data_dir(42),
            PathBuf::from("/var/lib/aresa/1/ranges/42/data")
        );
        assert_eq!(
            cfg.range_log_path(42),
            PathBuf::from("/var/lib/aresa/1/ranges/42/log/log.redb")
        );
        assert_eq!(
            cfg.range_data_path(42),
            PathBuf::from("/var/lib/aresa/1/ranges/42/data/data.redb")
        );
    }

    #[test]
    fn default_data_engine_is_redb() {
        let cfg = NodeConfig::new(1, "127.0.0.1:7001".parse().unwrap(), "/var/lib/aresa/1");
        assert_eq!(cfg.data_engine, DataEngine::Redb);
        assert_eq!(
            cfg.range_data_path(42),
            PathBuf::from("/var/lib/aresa/1/ranges/42/data/data.redb")
        );
    }

    #[test]
    fn lsm_data_engine_uses_lsm_suffix() {
        let cfg = NodeConfig::new(1, "127.0.0.1:7001".parse().unwrap(), "/var/lib/aresa/1")
            .with_data_engine(DataEngine::Lsm);
        assert_eq!(cfg.data_engine, DataEngine::Lsm);
        assert_eq!(
            cfg.range_data_path(42),
            PathBuf::from("/var/lib/aresa/1/ranges/42/data/data.lsm")
        );
        // The log path is unaffected by the engine choice.
        assert_eq!(
            cfg.range_log_path(42),
            PathBuf::from("/var/lib/aresa/1/ranges/42/log/log.redb")
        );
    }

    #[test]
    fn data_engine_labels_are_stable() {
        assert_eq!(DataEngine::Redb.label(), "redb");
        assert_eq!(DataEngine::Lsm.label(), "lsm");
    }

    #[test]
    fn ranges_with_different_ids_do_not_collide() {
        let cfg = NodeConfig::new(1, "127.0.0.1:7001".parse().unwrap(), "/var/lib/aresa/1");
        assert_ne!(cfg.range_dir(1), cfg.range_dir(2));
        assert_ne!(cfg.range_log_path(1), cfg.range_log_path(2));
        assert_ne!(cfg.range_data_path(1), cfg.range_data_path(2));
    }

    #[test]
    fn ensure_range_dirs_creates_nested_structure_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = NodeConfig::new(1, "127.0.0.1:7001".parse().unwrap(), dir.path());
        cfg.ensure_range_dirs(7).unwrap();
        assert!(cfg.range_log_dir(7).is_dir());
        assert!(cfg.range_data_dir(7).is_dir());
        // Second call is a no-op.
        cfg.ensure_range_dirs(7).unwrap();
        assert!(cfg.range_log_dir(7).is_dir());
        assert!(cfg.range_data_dir(7).is_dir());
    }
}

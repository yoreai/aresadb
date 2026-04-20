//! Persistent placement-driver state machine.
//!
//! Wraps a [`Catalog`] in a durable adapter over any
//! [`aresadb_core::StorageBackend`]. Every accepted [`PdCommand`]
//! mutates the in-memory catalog and writes the touched rows to the
//! backend in a single atomic `WriteBatch`, so recovery is just a
//! scan of `/m/pd/` followed by [`Catalog::load`].
//!
//! Phase 2b-2 is the persistent-catalog adapter; Phase 2b-3 adds a
//! thin set of Raft-facing entry points ([`PdStateMachine::apply_with_meta`],
//! [`PdStateMachine::apply_meta_only`], [`PdStateMachine::install_catalog_snapshot`])
//! that let the `aresadb_pd::raft::PdRaftStateMachine` adapter persist
//! `last_applied` / `last_membership` atomically with every catalog
//! mutation, without baking Raft knowledge into the catalog core.
//!
//! The two concerns remain split deliberately: `PdStateMachine` has
//! to be unit-testable end-to-end without Raft in the loop, so bugs
//! in persistence can't hide behind bugs in replication.
//!
//! ## Consistency model
//!
//! Applies serialize on an internal `tokio::sync::Mutex`. Inside the
//! critical section:
//!
//! 1. The catalog mutation runs to completion (pure logic, no
//!    `await`).
//! 2. The derived `WriteBatch` is built from the post-apply
//!    descriptors.
//! 3. The batch is committed atomically + flushed.
//!
//! If step 3 fails, the in-memory catalog is one apply ahead of disk.
//! That is a **fatal** state — the caller must drop this state
//! machine, `open` a fresh instance against the same backend, and
//! let Raft re-deliver the entry. See [`PdApplyError::Backend`].
//!
//! Reads are served directly from the in-memory catalog via
//! [`PdStateMachine::read`]. They don't take the apply lock so they
//! never block on backend I/O.
//!
//! ## Reserved meta prefix
//!
//! The `0xff`-prefixed keyspace on the data backend is reserved for
//! adapter-layer metadata (Raft applied-log-id, membership, etc.).
//! The catalog itself only ever writes `/m/pd/*`-prefixed keys, so
//! the reserved prefix is effectively disjoint. See
//! [`PD_RAFT_META_KEY`].

use std::sync::Arc;

use aresadb_core::{Error as BackendError, KeyRange, StorageBackend, WriteBatch};
use bytes::Bytes;
use futures::StreamExt;
use parking_lot::RwLock;
use thiserror::Error;
use tokio::sync::Mutex;

use crate::{
    catalog::Catalog,
    command::{PdCommand, PdResponse},
    error::CatalogError,
    persist::{
        node_id_from_key, node_key, prefix_upper_bound, range_id_from_key, range_key, NODE_PREFIX,
        RANGE_PREFIX,
    },
    types::{NodeInfo, RangeDescriptor},
};

/// Reserved row key where the PD Raft adapter stores its bincode-
/// encoded `{ last_applied, last_membership }` payload.
///
/// Lives in the `0xff` reserved prefix to keep it disjoint from both
/// the catalog's `/m/pd/*` rows and the user-data state-machine's
/// `b"\xff/sm/meta"` row. Different PD state-machine instances on
/// the same backend would trample each other; that's fine — a
/// backend is only ever owned by one PD Raft member.
pub const PD_RAFT_META_KEY: &[u8] = b"\xff/pd/sm/meta";

/// Reason a persisted apply failed.
///
/// `Catalog` errors are *recoverable* — the state machine's in-memory
/// state and its on-disk state are still in sync; the caller tried
/// something the catalog forbids. `Backend` errors are *fatal* — the
/// in-memory state mutated but the disk write did not land; the only
/// safe recovery is to drop this state machine and re-open.
#[derive(Debug, Error)]
pub enum PdApplyError {
    /// The catalog rejected the command. The state machine is still
    /// in a consistent on-disk + in-memory state.
    #[error("catalog rejected command: {0}")]
    Catalog(#[from] CatalogError),

    /// The backend write failed after the in-memory mutation was
    /// already applied. The state machine is no longer safe to use;
    /// discard it, re-open against the same backend, and let the
    /// replicator re-deliver the entry.
    #[error("backend write failed: {0}")]
    Backend(#[from] BackendError),

    /// A bincode encode failed while preparing the write batch. In
    /// practice this only happens if the host is out of memory while
    /// serialising; treat the same as `Backend`.
    #[error("bincode encode failed: {0}")]
    Encode(#[from] bincode::Error),
}

/// Persistent placement-driver state machine.
///
/// `Arc`-wrap it and share across tasks; the type is `Send + Sync` by
/// construction (every field is either `Arc` or a sync-lock-guarded
/// cell).
pub struct PdStateMachine {
    /// Backend that durably holds `/m/pd/r/*` and `/m/pd/n/*`.
    data: Arc<dyn StorageBackend>,
    /// In-memory catalog. Under the apply mutex when mutating; the
    /// `parking_lot` `RwLock` lets read-only callers grab a consistent
    /// view cheaply.
    catalog: RwLock<Catalog>,
    /// Serializes `apply` calls so the catalog mutation and the
    /// derived backend write form one logical operation.
    apply_lock: Mutex<()>,
}

impl PdStateMachine {
    /// Open a state machine against `data`, rehydrating the catalog
    /// from any existing rows. Creates no new rows itself — a fresh
    /// backend yields an empty catalog, which is exactly what
    /// bootstrap wants.
    pub async fn open(data: Arc<dyn StorageBackend>) -> Result<Arc<Self>, PdApplyError> {
        let catalog = Self::rehydrate(&*data).await?;
        Ok(Arc::new(Self {
            data,
            catalog: RwLock::new(catalog),
            apply_lock: Mutex::new(()),
        }))
    }

    /// Apply a replicated command. Serializes with every other apply
    /// on this state machine and returns once the write + flush are
    /// durable.
    ///
    /// Convenience shortcut for [`Self::apply_with_meta`] with no
    /// accompanying Raft meta row — admin and unit-test callers that
    /// don't have a Raft log can use this directly.
    pub async fn apply(&self, cmd: PdCommand) -> Result<PdResponse, PdApplyError> {
        self.apply_inner(cmd, None).await
    }

    /// Apply a replicated command, atomically persisting an opaque
    /// meta row alongside the catalog mutation.
    ///
    /// The meta payload is written at [`PD_RAFT_META_KEY`] in the same
    /// `WriteBatch` as the catalog changes, so a crash between the
    /// catalog write and the meta write is impossible — either both
    /// land or neither does. The payload is opaque to the state
    /// machine; the Raft adapter on top encodes its own
    /// `{ last_applied, last_membership }` there.
    pub async fn apply_with_meta(
        &self,
        cmd: PdCommand,
        meta: &[u8],
    ) -> Result<PdResponse, PdApplyError> {
        self.apply_inner(cmd, Some(meta)).await
    }

    /// Persist an opaque meta row atomically, without mutating the
    /// catalog. Used by the Raft adapter to bump `last_applied` for
    /// entries that don't touch the catalog (Raft blank entries and
    /// membership changes).
    pub async fn apply_meta_only(&self, meta: &[u8]) -> Result<(), PdApplyError> {
        let _g = self.apply_lock.lock().await;
        let mut batch = WriteBatch::new();
        batch.put(
            Bytes::from_static(PD_RAFT_META_KEY),
            Bytes::copy_from_slice(meta),
        );
        self.data.write_batch(batch).await?;
        self.data.flush().await?;
        Ok(())
    }

    /// Read the opaque Raft meta row. Returns `None` if no meta has
    /// ever been persisted (fresh backend).
    pub async fn read_raft_meta(&self) -> Result<Option<Bytes>, PdApplyError> {
        Ok(self.data.get(PD_RAFT_META_KEY).await?)
    }

    /// Replace the entire catalog with a snapshot's contents in one
    /// atomic batch.
    ///
    /// Used by the Raft adapter's `install_snapshot`: it wipes the
    /// existing catalog rows (`/m/pd/r/*` + `/m/pd/n/*`), writes the
    /// snapshot's ranges and nodes, and lands the new meta row — all
    /// inside a single `WriteBatch`. The in-memory catalog is
    /// replaced with a fresh [`Catalog::load`] built from the same
    /// descriptors, under the apply lock.
    ///
    /// `next_range_id` is the monotonic id counter the Raft leader
    /// had when the snapshot was built; callers can derive it from
    /// `ranges` (max id + 1) but pass it explicitly so the snapshot
    /// wire format stays the source of truth.
    pub async fn install_catalog_snapshot(
        &self,
        ranges: Vec<RangeDescriptor>,
        nodes: Vec<NodeInfo>,
        next_range_id: crate::types::RangeId,
        meta: &[u8],
    ) -> Result<(), PdApplyError> {
        let _g = self.apply_lock.lock().await;

        let mut batch = WriteBatch::new();
        // Wipe catalog rows. Each prefix gets its own delete_range so
        // we don't accidentally sweep the `0xff` reserved meta keys.
        let range_end = prefix_upper_bound(RANGE_PREFIX);
        batch.delete_range(Bytes::from_static(RANGE_PREFIX), range_end);
        let node_end = prefix_upper_bound(NODE_PREFIX);
        batch.delete_range(Bytes::from_static(NODE_PREFIX), node_end);

        for desc in &ranges {
            put_range(&mut batch, desc)?;
        }
        for info in &nodes {
            put_node(&mut batch, info)?;
        }
        batch.put(
            Bytes::from_static(PD_RAFT_META_KEY),
            Bytes::copy_from_slice(meta),
        );

        self.data.write_batch(batch).await?;
        self.data.flush().await?;

        // Swap the in-memory catalog only after the durable write
        // landed. A crash between the batch and the swap re-runs
        // rehydrate on next `open`, which reads the exact rows we
        // just wrote, so the two paths converge.
        let mut guard = self.catalog.write();
        *guard = Catalog::load(ranges, nodes, next_range_id);
        Ok(())
    }

    /// Borrow the catalog for reads. The callback runs under a shared
    /// read lock; it must not call back into [`Self::apply`] (would
    /// deadlock). Intended for admin queries and range lookups.
    pub fn read<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Catalog) -> R,
    {
        let guard = self.catalog.read();
        f(&guard)
    }

    /// Borrow the data backend. Intended for the Phase 2b-3 Raft
    /// state-machine adapter, which needs to persist its own meta
    /// rows alongside the catalog. External callers almost certainly
    /// want [`Self::read`] instead.
    pub fn data_backend(&self) -> &Arc<dyn StorageBackend> {
        &self.data
    }

    async fn apply_inner(
        &self,
        cmd: PdCommand,
        meta: Option<&[u8]>,
    ) -> Result<PdResponse, PdApplyError> {
        let _g = self.apply_lock.lock().await;

        let (resp, mut batch) = {
            let mut catalog = self.catalog.write();
            let resp = catalog.apply(cmd.clone())?;
            let batch = self.build_write_batch(&cmd, &resp, &catalog)?;
            (resp, batch)
        };

        if let Some(meta_bytes) = meta {
            batch.put(
                Bytes::from_static(PD_RAFT_META_KEY),
                Bytes::copy_from_slice(meta_bytes),
            );
        }

        self.data.write_batch(batch).await?;
        self.data.flush().await?;

        Ok(resp)
    }

    // ------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------

    async fn rehydrate(data: &dyn StorageBackend) -> Result<Catalog, PdApplyError> {
        let ranges = scan_ranges(data).await?;
        let nodes = scan_nodes(data).await?;

        let next_hint = ranges
            .iter()
            .map(|r| r.range_id)
            .max()
            .map(|m| m + 1)
            .unwrap_or(1);

        tracing::debug!(
            ranges = ranges.len(),
            nodes = nodes.len(),
            next_range_id = next_hint,
            "placement-driver catalog rehydrated"
        );

        Ok(Catalog::load(ranges, nodes, next_hint))
    }

    /// Translate a freshly-applied command into the set of row
    /// writes / deletes that mirror the catalog's new state. `catalog`
    /// is the **post-apply** view.
    ///
    /// The mapping is exhaustive: every `PdCommand` variant produces
    /// a deterministic set of touched rows.
    fn build_write_batch(
        &self,
        cmd: &PdCommand,
        resp: &PdResponse,
        catalog: &Catalog,
    ) -> Result<WriteBatch, PdApplyError> {
        let mut batch = WriteBatch::new();
        match cmd {
            PdCommand::RegisterNode(info) => {
                // Register may have preserved a prior heartbeat — re-
                // read the stored copy rather than serializing the
                // inbound value directly.
                let persisted = catalog
                    .get_node(info.node_id)
                    .expect("register_node just inserted the node");
                put_node(&mut batch, persisted)?;
            }
            PdCommand::HeartbeatNode { node_id, .. } => {
                let persisted = catalog
                    .get_node(*node_id)
                    .expect("heartbeat_node rejected missing nodes above");
                put_node(&mut batch, persisted)?;
            }
            PdCommand::CreateRange(desc) => {
                put_range(&mut batch, desc)?;
            }
            PdCommand::SplitRange {
                parent_range_id, ..
            } => {
                let rhs = match resp {
                    PdResponse::Range(r) => r,
                    _ => unreachable!("split_range always responds with the new RHS"),
                };
                let parent = catalog
                    .get_range(*parent_range_id)
                    .expect("parent existed before split and still does after");
                put_range(&mut batch, parent)?;
                put_range(&mut batch, rhs)?;
            }
            PdCommand::MergeRanges { left, right } => {
                let merged_left = catalog
                    .get_range(*left)
                    .expect("merge retains the left range");
                put_range(&mut batch, merged_left)?;
                batch.delete(range_key(*right));
            }
            PdCommand::UpdateMembership { range_id, .. }
            | PdCommand::UpdateLease { range_id, .. } => {
                let desc = catalog
                    .get_range(*range_id)
                    .expect("update commands reject missing ranges above");
                put_range(&mut batch, desc)?;
            }
        }
        Ok(batch)
    }
}

/// Scan every `/m/pd/r/*` row and deserialize into descriptors.
async fn scan_ranges(data: &dyn StorageBackend) -> Result<Vec<RangeDescriptor>, PdApplyError> {
    let start = Bytes::from_static(RANGE_PREFIX);
    let end = prefix_upper_bound(RANGE_PREFIX);
    let mut stream = data.scan(KeyRange::new(start, end)).await?;

    let mut out: Vec<RangeDescriptor> = Vec::new();
    while let Some(item) = stream.next().await {
        let kv = item?;
        if range_id_from_key(&kv.key).is_none() {
            // Defensive: skip keys that fell inside the prefix but
            // aren't well-formed range rows. Shouldn't happen today.
            tracing::warn!(
                key = ?&kv.key[..kv.key.len().min(32)],
                "ignoring malformed range row during rehydrate"
            );
            continue;
        }
        let desc: RangeDescriptor = bincode::deserialize(&kv.value)?;
        out.push(desc);
    }
    Ok(out)
}

/// Scan every `/m/pd/n/*` row and deserialize into nodes.
async fn scan_nodes(data: &dyn StorageBackend) -> Result<Vec<NodeInfo>, PdApplyError> {
    let start = Bytes::from_static(NODE_PREFIX);
    let end = prefix_upper_bound(NODE_PREFIX);
    let mut stream = data.scan(KeyRange::new(start, end)).await?;

    let mut out: Vec<NodeInfo> = Vec::new();
    while let Some(item) = stream.next().await {
        let kv = item?;
        if node_id_from_key(&kv.key).is_none() {
            tracing::warn!(
                key = ?&kv.key[..kv.key.len().min(32)],
                "ignoring malformed node row during rehydrate"
            );
            continue;
        }
        let info: NodeInfo = bincode::deserialize(&kv.value)?;
        out.push(info);
    }
    Ok(out)
}

fn put_range(batch: &mut WriteBatch, desc: &RangeDescriptor) -> Result<(), PdApplyError> {
    let key = range_key(desc.range_id);
    let value: Bytes = bincode::serialize(desc)?.into();
    batch.put(key, value);
    Ok(())
}

fn put_node(batch: &mut WriteBatch, info: &NodeInfo) -> Result<(), PdApplyError> {
    let key = node_key(info.node_id);
    let value: Bytes = bincode::serialize(info)?.into();
    batch.put(key, value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use aresadb_core::MemoryBackend;
    use aresadb_core::StorageBackend;
    use tempfile::TempDir;

    use super::*;
    use crate::types::{LeaseInfo, RangeId, ReplicaPlacement};

    fn voters(ids: &[u64]) -> Vec<ReplicaPlacement> {
        ids.iter().map(|n| ReplicaPlacement::voter(*n, 1)).collect()
    }

    fn genesis() -> RangeDescriptor {
        RangeDescriptor::new(1, Vec::<u8>::new(), Vec::<u8>::new(), voters(&[1, 2, 3]))
    }

    fn memory_backend() -> Arc<dyn StorageBackend> {
        Arc::new(MemoryBackend::new())
    }

    // -------- fresh open --------

    #[tokio::test]
    async fn fresh_backend_yields_empty_catalog() {
        let sm = PdStateMachine::open(memory_backend()).await.unwrap();
        sm.read(|c| {
            assert_eq!(c.range_count(), 0);
            assert_eq!(c.peek_next_range_id(), 1);
        });
    }

    // -------- single apply --------

    #[tokio::test]
    async fn create_range_persists_row() {
        let backend = memory_backend();
        let sm = PdStateMachine::open(backend.clone()).await.unwrap();

        let resp = sm.apply(PdCommand::CreateRange(genesis())).await.unwrap();
        assert!(matches!(resp, PdResponse::Range(d) if d.range_id == 1));

        // Row lands on disk.
        let stored = backend.get(&range_key(1)).await.unwrap().unwrap();
        let decoded: RangeDescriptor = bincode::deserialize(&stored).unwrap();
        assert_eq!(decoded, genesis());
    }

    #[tokio::test]
    async fn catalog_rejection_does_not_write() {
        let backend = memory_backend();
        let sm = PdStateMachine::open(backend.clone()).await.unwrap();

        // Zero-width span — catalog rejects.
        let bad = RangeDescriptor::new(1, b"a".to_vec(), b"a".to_vec(), vec![]);
        let err = sm.apply(PdCommand::CreateRange(bad)).await.unwrap_err();
        assert!(matches!(
            err,
            PdApplyError::Catalog(CatalogError::InvalidSpan)
        ));

        // Disk is still pristine.
        assert!(backend.get(&range_key(1)).await.unwrap().is_none());
        sm.read(|c| assert_eq!(c.range_count(), 0));
    }

    // -------- multi-command sequence --------

    #[tokio::test]
    async fn split_persists_both_sides() {
        let backend = memory_backend();
        let sm = PdStateMachine::open(backend.clone()).await.unwrap();

        sm.apply(PdCommand::CreateRange(genesis())).await.unwrap();
        let resp = sm
            .apply(PdCommand::SplitRange {
                parent_range_id: 1,
                split_key: b"m".to_vec(),
            })
            .await
            .unwrap();
        let rhs = match resp {
            PdResponse::Range(r) => r,
            _ => panic!("split should return Range"),
        };
        assert_eq!(rhs.range_id, 2);

        // Both rows on disk.
        let row1 = backend.get(&range_key(1)).await.unwrap().unwrap();
        let row2 = backend.get(&range_key(2)).await.unwrap().unwrap();
        let p1: RangeDescriptor = bincode::deserialize(&row1).unwrap();
        let p2: RangeDescriptor = bincode::deserialize(&row2).unwrap();
        assert_eq!(p1.end_key, b"m".to_vec());
        assert_eq!(p2.start_key, b"m".to_vec());
        assert_eq!(p1.generation, p2.generation);
    }

    #[tokio::test]
    async fn merge_deletes_right_row() {
        let backend = memory_backend();
        let sm = PdStateMachine::open(backend.clone()).await.unwrap();

        sm.apply(PdCommand::CreateRange(genesis())).await.unwrap();
        sm.apply(PdCommand::SplitRange {
            parent_range_id: 1,
            split_key: b"m".to_vec(),
        })
        .await
        .unwrap();
        sm.apply(PdCommand::MergeRanges { left: 1, right: 2 })
            .await
            .unwrap();

        // Left row still present, right row gone.
        assert!(backend.get(&range_key(1)).await.unwrap().is_some());
        assert!(backend.get(&range_key(2)).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn membership_and_lease_updates_land_on_disk() {
        let backend = memory_backend();
        let sm = PdStateMachine::open(backend.clone()).await.unwrap();

        sm.apply(PdCommand::CreateRange(genesis())).await.unwrap();
        sm.apply(PdCommand::UpdateMembership {
            range_id: 1,
            new_replicas: voters(&[1, 2, 3, 4]),
            new_epoch: 1,
        })
        .await
        .unwrap();
        sm.apply(PdCommand::UpdateLease {
            range_id: 1,
            lease: Some(LeaseInfo {
                holder: 1,
                expires_at_millis: 1_700_000_000_000,
            }),
        })
        .await
        .unwrap();

        let row = backend.get(&range_key(1)).await.unwrap().unwrap();
        let desc: RangeDescriptor = bincode::deserialize(&row).unwrap();
        assert_eq!(desc.epoch, 1);
        assert_eq!(desc.replicas.len(), 4);
        assert!(desc.lease.is_some());
    }

    #[tokio::test]
    async fn register_and_heartbeat_persist() {
        let backend = memory_backend();
        let sm = PdStateMachine::open(backend.clone()).await.unwrap();

        sm.apply(PdCommand::RegisterNode(NodeInfo {
            node_id: 1,
            address: "127.0.0.1:7001".to_string(),
            stores: vec![1],
            last_heartbeat_millis: 0,
        }))
        .await
        .unwrap();
        sm.apply(PdCommand::HeartbeatNode {
            node_id: 1,
            last_seen_millis: 1_000,
        })
        .await
        .unwrap();

        let row = backend.get(&node_key(1)).await.unwrap().unwrap();
        let info: NodeInfo = bincode::deserialize(&row).unwrap();
        assert_eq!(info.last_heartbeat_millis, 1_000);
    }

    // -------- rehydrate --------

    #[tokio::test]
    async fn reopen_restores_catalog_state() {
        let backend = memory_backend();

        {
            let sm = PdStateMachine::open(backend.clone()).await.unwrap();
            sm.apply(PdCommand::CreateRange(genesis())).await.unwrap();
            sm.apply(PdCommand::SplitRange {
                parent_range_id: 1,
                split_key: b"m".to_vec(),
            })
            .await
            .unwrap();
            sm.apply(PdCommand::SplitRange {
                parent_range_id: 1,
                split_key: b"f".to_vec(),
            })
            .await
            .unwrap();
            sm.apply(PdCommand::UpdateMembership {
                range_id: 1,
                new_replicas: voters(&[1, 2, 3, 4]),
                new_epoch: 7,
            })
            .await
            .unwrap();
            sm.apply(PdCommand::RegisterNode(NodeInfo {
                node_id: 1,
                address: "host-1:7001".to_string(),
                stores: vec![1, 2],
                last_heartbeat_millis: 0,
            }))
            .await
            .unwrap();
            sm.apply(PdCommand::HeartbeatNode {
                node_id: 1,
                last_seen_millis: 1_700_000_000_000,
            })
            .await
            .unwrap();
        }

        // Drop + reopen against the same backend.
        let sm2 = PdStateMachine::open(backend.clone()).await.unwrap();
        sm2.read(|c| {
            assert_eq!(c.range_count(), 3);
            // peek should be strictly past max id.
            assert_eq!(c.peek_next_range_id(), 4);
            // Range 1's epoch survived.
            assert_eq!(c.get_range(1).unwrap().epoch, 7);
            // Node info survived.
            let node = c.get_node(1).unwrap();
            assert_eq!(node.address, "host-1:7001");
            assert_eq!(node.last_heartbeat_millis, 1_700_000_000_000);
            // Coverage map still resolves every key. Layout after
            // the two splits:
            //   range 1: [∅, "f")    (original, now shrunk twice)
            //   range 3: ["f", "m")  (allocated by split at "f")
            //   range 2: ["m", ∅)    (allocated by split at "m")
            assert_eq!(c.find_range_for_key(b"a").unwrap().range_id, 1);
            assert_eq!(c.find_range_for_key(b"h").unwrap().range_id, 3);
            assert_eq!(c.find_range_for_key(b"zzz").unwrap().range_id, 2);
        });
    }

    #[tokio::test]
    async fn reopen_allocates_range_ids_past_restored_max() {
        let backend = memory_backend();

        {
            let sm = PdStateMachine::open(backend.clone()).await.unwrap();
            // Create a range with a *non*-1 id to prove recovery also
            // advances the counter past explicit-id rows.
            let desc =
                RangeDescriptor::new(100, Vec::<u8>::new(), Vec::<u8>::new(), voters(&[1, 2, 3]));
            sm.apply(PdCommand::CreateRange(desc)).await.unwrap();
        }

        let sm2 = PdStateMachine::open(backend.clone()).await.unwrap();
        // Split: RHS must pick 101, not 2.
        let rhs = sm2
            .apply(PdCommand::SplitRange {
                parent_range_id: 100,
                split_key: b"m".to_vec(),
            })
            .await
            .unwrap();
        assert!(matches!(rhs, PdResponse::Range(r) if r.range_id == 101));
    }

    #[tokio::test]
    async fn reopen_with_empty_backend_is_clean() {
        let backend = memory_backend();
        let sm = PdStateMachine::open(backend).await.unwrap();
        sm.read(|c| {
            assert_eq!(c.range_count(), 0);
            assert_eq!(c.peek_next_range_id(), 1);
        });
    }

    // -------- durable backend --------

    #[tokio::test]
    async fn redb_backend_survives_restart() {
        // Use the real durable backend so we're not just proving the
        // memory backend to itself. End-to-end: write a handful of
        // commands, drop the process handle, reopen, verify.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pd.redb");

        {
            let backend: Arc<dyn StorageBackend> =
                aresadb_engine_redb::RedbBackend::open(&path).await.unwrap();
            let sm = PdStateMachine::open(backend).await.unwrap();
            sm.apply(PdCommand::CreateRange(genesis())).await.unwrap();
            sm.apply(PdCommand::SplitRange {
                parent_range_id: 1,
                split_key: b"m".to_vec(),
            })
            .await
            .unwrap();
            sm.apply(PdCommand::UpdateLease {
                range_id: 2,
                lease: Some(LeaseInfo {
                    holder: 3,
                    expires_at_millis: 1_700_000_001_000,
                }),
            })
            .await
            .unwrap();
        }

        {
            let backend: Arc<dyn StorageBackend> =
                aresadb_engine_redb::RedbBackend::open(&path).await.unwrap();
            let sm = PdStateMachine::open(backend).await.unwrap();
            sm.read(|c| {
                assert_eq!(c.range_count(), 2);
                assert_eq!(c.peek_next_range_id(), 3);
                let rhs = c.get_range(2).unwrap();
                assert_eq!(rhs.lease.as_ref().unwrap().holder, 3);
            });
        }
    }

    // -------- admin reads --------

    #[tokio::test]
    async fn read_exposes_consistent_view() {
        let backend = memory_backend();
        let sm = PdStateMachine::open(backend).await.unwrap();

        sm.apply(PdCommand::CreateRange(genesis())).await.unwrap();
        sm.apply(PdCommand::SplitRange {
            parent_range_id: 1,
            split_key: b"m".to_vec(),
        })
        .await
        .unwrap();

        let (count, by_start) = sm.read(|c| {
            let starts: Vec<RangeId> = c.iter_ranges_by_start().map(|r| r.range_id).collect();
            (c.range_count(), starts)
        });
        assert_eq!(count, 2);
        assert_eq!(by_start, vec![1, 2]);
    }
}

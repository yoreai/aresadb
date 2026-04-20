//! Raft state-machine adapter for [`PdStateMachine`].
//!
//! Wraps a shared [`Arc<PdStateMachine>`] and implements the two
//! openraft storage traits that complete the state-machine side of a
//! Raft group:
//!
//! - [`RaftStateMachine<PdTypeConfig>`] — `apply`, `applied_state`,
//!   `install_snapshot`, `begin_receiving_snapshot`,
//!   `get_current_snapshot`, `get_snapshot_builder`.
//! - [`RaftSnapshotBuilder<PdTypeConfig>`] — `build_snapshot`.
//!
//! ## Persistence layout
//!
//! The catalog rows (`/m/pd/r/*` and `/m/pd/n/*`) already live on the
//! state-machine's data backend. Raft-specific metadata —
//! `last_applied` log id and `last_membership` — rides along at the
//! reserved [`PD_RAFT_META_KEY`] row. Each apply bundles the catalog
//! mutation and the fresh meta into one [`aresadb_core::WriteBatch`]
//! via [`PdStateMachine::apply_with_meta`], so the two never drift
//! out of sync: either both land or neither does.
//!
//! Blank and membership entries don't touch the catalog but still
//! need to advance `last_applied`. They go through
//! [`PdStateMachine::apply_meta_only`], which writes just the meta
//! row in a one-row batch.
//!
//! ## Snapshots
//!
//! A snapshot is a bincode-encoded [`SnapshotPayload`] containing:
//!
//! - `last_applied` and `last_membership` at snapshot-build time;
//! - the `next_range_id` counter (so post-install `SplitRange`
//!   allocations pick up where the snapshot left off);
//! - every [`RangeDescriptor`] in the catalog, ordered by start key;
//! - every [`NodeInfo`], ordered by node id.
//!
//! `build_snapshot` reads this from the in-memory catalog and the
//! cached meta. `install_snapshot` wipes the `/m/pd/*` keyspace,
//! replays the descriptors, and writes the meta row — all inside a
//! single `WriteBatch` via [`PdStateMachine::install_catalog_snapshot`].
//!
//! ## Catalog rejections
//!
//! The catalog's `apply` returns a [`crate::CatalogError`] whenever a
//! command is invalid — overlapping ranges, epoch regression,
//! split-key outside span, etc. That's an **application-level**
//! rejection, not an I/O failure, so we surface it through
//! [`PdResponse::Error(msg)`] and let Raft commit the entry. This
//! keeps every replica's log semantics identical: the same log index
//! produces the same response everywhere, including the rejection.
//!
//! Actual backend failures (disk full, fsync error, bincode encoding
//! panic) are fatal — they propagate as openraft `StorageError`
//! values, which crash the state machine and force a re-open. The
//! atomic write-batch property protects us from half-applied state.

use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use aresadb_raft::{storage_err, storage_err_ctx, BincodeError};
use bincode;
use openraft::{
    storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot},
    BasicNode, EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend, SnapshotMeta,
    StorageError, StoredMembership,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    command::PdResponse,
    state_machine::{PdApplyError, PdStateMachine},
    types::{NodeInfo, RangeDescriptor, RangeId},
};

use super::config::{NodeId, PdTypeConfig};

/// Persisted `{ last_applied, last_membership }` row.
///
/// Stored bincode-encoded at [`PD_RAFT_META_KEY`]. Public so
/// integration tests and the admin surface can decode the row for
/// inspection.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PersistedPdMeta {
    /// Last Raft log id whose apply landed on disk. `None` means
    /// "no committed entry has been applied yet".
    pub last_applied: Option<LogId<NodeId>>,

    /// Applied membership (voter set + learners). Defaults to the
    /// empty stored-membership for fresh state machines.
    pub last_membership: StoredMembership<NodeId, BasicNode>,
}

impl PersistedPdMeta {
    // `StorageError` is a 200+ byte enum from openraft; every call
    // site here is a trait-impl-adjacent helper and can't change
    // the shape of the signature. Targeted allow keeps clippy
    // happy without boxing the error.
    #[allow(clippy::result_large_err)]
    fn to_bytes(&self) -> Result<Vec<u8>, StorageError<NodeId>> {
        bincode::serialize(self).map_err(|e| {
            storage_err_ctx::<NodeId, _>(
                ErrorSubject::StateMachine,
                ErrorVerb::Write,
                BincodeError(e),
            )
        })
    }

    #[allow(clippy::result_large_err)]
    fn from_bytes(raw: &[u8]) -> Result<Self, StorageError<NodeId>> {
        bincode::deserialize(raw).map_err(|e| {
            storage_err_ctx::<NodeId, _>(
                ErrorSubject::StateMachine,
                ErrorVerb::Read,
                BincodeError(e),
            )
        })
    }
}

/// Snapshot wire format. Contains the full replicated state of the
/// PD group: all range descriptors, all node rows, and the Raft meta
/// at snapshot-build time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotPayload {
    /// `last_applied` captured when the snapshot was built.
    pub last_applied: Option<LogId<NodeId>>,

    /// Applied membership at snapshot-build time.
    pub last_membership: StoredMembership<NodeId, BasicNode>,

    /// Next range id the catalog would allocate; persisted so a
    /// restored replica's subsequent splits allocate ids strictly
    /// past every id it already knows about.
    pub next_range_id: RangeId,

    /// Every range descriptor, ordered by `start_key` for stability.
    pub ranges: Vec<RangeDescriptor>,

    /// Every node row, ordered by `node_id` for stability.
    pub nodes: Vec<NodeInfo>,
}

impl SnapshotPayload {
    #[allow(clippy::result_large_err)]
    fn to_bytes(&self) -> Result<Vec<u8>, StorageError<NodeId>> {
        bincode::serialize(self).map_err(|e| {
            storage_err_ctx::<NodeId, _>(
                ErrorSubject::StateMachine,
                ErrorVerb::Write,
                BincodeError(e),
            )
        })
    }

    #[allow(clippy::result_large_err)]
    fn from_bytes(
        raw: &[u8],
        meta: &SnapshotMeta<NodeId, BasicNode>,
    ) -> Result<Self, StorageError<NodeId>> {
        bincode::deserialize(raw).map_err(|e| {
            storage_err_ctx::<NodeId, _>(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Read,
                BincodeError(e),
            )
        })
    }
}

/// In-memory record of the last snapshot this state machine produced
/// or installed. Exposed so tests can inspect snapshot metadata.
#[derive(Debug, Clone)]
pub struct StoredSnapshot {
    /// Openraft snapshot metadata (log id, membership, id string).
    pub meta: SnapshotMeta<NodeId, BasicNode>,

    /// bincode-encoded [`SnapshotPayload`] bytes. Held alongside the
    /// meta so `get_current_snapshot` can hand a fresh reader back to
    /// openraft without re-reading the backend.
    pub data: Vec<u8>,
}

/// Raft state-machine adapter for the placement-driver catalog.
///
/// Wrap the inner [`PdStateMachine`] once and hand out clones of the
/// `Arc<Self>` to both openraft (for applies / snapshots) and the PD
/// admin layer (for direct catalog reads via
/// [`PdStateMachine::read`]). The adapter owns the Raft meta cache
/// and the in-memory record of the most recent snapshot; everything
/// else delegates to the inner state machine.
pub struct PdRaftStateMachine {
    inner: Arc<PdStateMachine>,
    meta: RwLock<PersistedPdMeta>,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    snapshot_idx: AtomicU64,
}

impl PdRaftStateMachine {
    /// Build a Raft adapter over an already-open [`PdStateMachine`],
    /// rehydrating the persisted `last_applied` / `last_membership`
    /// from the same backend.
    ///
    /// Wraps both sides in `Arc`s so openraft can hold a shared,
    /// mutation-free handle while admin code keeps a parallel clone
    /// for reads.
    pub async fn open(inner: Arc<PdStateMachine>) -> Result<Arc<Self>, StorageError<NodeId>> {
        let raw = inner
            .read_raft_meta()
            .await
            .map_err(pd_apply_to_storage_err)?;
        let meta = match raw {
            Some(bytes) => PersistedPdMeta::from_bytes(&bytes)?,
            None => PersistedPdMeta::default(),
        };
        Ok(Arc::new(Self {
            inner,
            meta: RwLock::new(meta),
            current_snapshot: RwLock::new(None),
            snapshot_idx: AtomicU64::new(0),
        }))
    }

    /// Borrow the inner catalog state machine. Admin / gRPC code uses
    /// this to serve reads (`PdStateMachine::read`) and to inspect
    /// the data backend for diagnostics.
    pub fn inner(&self) -> &Arc<PdStateMachine> {
        &self.inner
    }

    /// Return a copy of the in-memory Raft meta. Primarily useful
    /// for tests — in production code prefer [`Self::applied_state`]
    /// which goes through the trait surface.
    pub async fn meta_snapshot(&self) -> PersistedPdMeta {
        self.meta.read().await.clone()
    }

    fn bump_snapshot_id(&self, last_applied: &Option<LogId<NodeId>>) -> String {
        let idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        match last_applied {
            Some(l) => format!("{}-{}-{}", l.leader_id, l.index, idx),
            None => format!("--{}", idx),
        }
    }
}

/// Map a [`PdApplyError`] into openraft's `StorageError`.
///
/// `Catalog` errors never reach this path — the apply adapter folds
/// them into `PdResponse::Error(msg)`. `Backend` and `Encode` errors
/// are fatal and surface as `StateMachine::Write` storage errors,
/// which is how openraft decides the state machine needs restart.
fn pd_apply_to_storage_err(err: PdApplyError) -> StorageError<NodeId> {
    match err {
        PdApplyError::Catalog(ce) => storage_err::<NodeId, _>(ce),
        PdApplyError::Backend(be) => {
            storage_err_ctx::<NodeId, _>(ErrorSubject::StateMachine, ErrorVerb::Write, be)
        }
        PdApplyError::Encode(ee) => storage_err_ctx::<NodeId, _>(
            ErrorSubject::StateMachine,
            ErrorVerb::Write,
            BincodeError(ee),
        ),
    }
}

impl RaftSnapshotBuilder<PdTypeConfig> for Arc<PdRaftStateMachine> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<Snapshot<PdTypeConfig>, StorageError<NodeId>> {
        let meta_snap = self.meta.read().await.clone();

        // Pull the full catalog state. The callback is cheap — just
        // clones the iterators into vectors — and runs under the
        // catalog's shared read lock, so no apply can sneak in during
        // snapshot assembly.
        let (ranges, nodes, next_range_id) = self.inner.read(|c| {
            let ranges: Vec<RangeDescriptor> = c.iter_ranges_by_start().cloned().collect();
            let nodes: Vec<NodeInfo> = c.iter_nodes().cloned().collect();
            let next_range_id = c.peek_next_range_id();
            (ranges, nodes, next_range_id)
        });

        let payload = SnapshotPayload {
            last_applied: meta_snap.last_applied,
            last_membership: meta_snap.last_membership.clone(),
            next_range_id,
            ranges,
            nodes,
        };
        let data = payload.to_bytes()?;

        let snapshot_id = self.bump_snapshot_id(&payload.last_applied);
        let snap_meta = SnapshotMeta {
            last_log_id: payload.last_applied,
            last_membership: payload.last_membership,
            snapshot_id,
        };

        let stored = StoredSnapshot {
            meta: snap_meta.clone(),
            data: data.clone(),
        };
        *self.current_snapshot.write().await = Some(stored);

        tracing::info!(
            last_applied = ?payload.last_applied,
            ranges = payload.ranges.len(),
            nodes = payload.nodes.len(),
            bytes = data.len(),
            "PD state-machine snapshot built",
        );

        Ok(Snapshot {
            meta: snap_meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<PdTypeConfig> for Arc<PdRaftStateMachine> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let m = self.meta.read().await;
        Ok((m.last_applied, m.last_membership.clone()))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<PdResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = super::config::typ::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut responses: Vec<PdResponse> = Vec::new();

        for entry in entries {
            let log_id = entry.log_id;

            // Build the meta payload that should land alongside this
            // entry's effects. We clone the current membership under
            // a short read lock, then swap it for the Membership
            // variant below if this entry is a membership change.
            let mut new_meta = {
                let m = self.meta.read().await;
                PersistedPdMeta {
                    last_applied: Some(log_id),
                    last_membership: m.last_membership.clone(),
                }
            };

            match entry.payload {
                EntryPayload::Blank => {
                    let encoded = new_meta.to_bytes()?;
                    self.inner
                        .apply_meta_only(&encoded)
                        .await
                        .map_err(pd_apply_to_storage_err)?;
                    responses.push(PdResponse::Ok);
                }
                EntryPayload::Normal(cmd) => {
                    // Encode the meta *before* delegating so the
                    // inner apply can land the catalog mutation and
                    // the meta in one batch.
                    let encoded = new_meta.to_bytes()?;
                    match self.inner.apply_with_meta(cmd, &encoded).await {
                        Ok(resp) => responses.push(resp),
                        Err(PdApplyError::Catalog(ce)) => {
                            // Catalog rejected — still need to persist
                            // last_applied so we don't reapply the
                            // entry after restart. Fold the rejection
                            // into a `PdResponse::Error`.
                            let encoded = new_meta.to_bytes()?;
                            self.inner
                                .apply_meta_only(&encoded)
                                .await
                                .map_err(pd_apply_to_storage_err)?;
                            responses.push(PdResponse::Error(ce.to_string()));
                        }
                        Err(other) => return Err(pd_apply_to_storage_err(other)),
                    }
                }
                EntryPayload::Membership(ref mem) => {
                    new_meta.last_membership = StoredMembership::new(Some(log_id), mem.clone());
                    let encoded = new_meta.to_bytes()?;
                    self.inner
                        .apply_meta_only(&encoded)
                        .await
                        .map_err(pd_apply_to_storage_err)?;
                    responses.push(PdResponse::Ok);
                }
            }

            // Commit the cached meta once the durable write landed.
            let mut m = self.meta.write().await;
            *m = new_meta;
        }

        Ok(responses)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let payload = SnapshotPayload::from_bytes(&data, meta)?;

        let persisted = PersistedPdMeta {
            last_applied: payload.last_applied,
            last_membership: payload.last_membership.clone(),
        };
        let encoded = persisted.to_bytes()?;

        self.inner
            .install_catalog_snapshot(
                payload.ranges.clone(),
                payload.nodes.clone(),
                payload.next_range_id,
                &encoded,
            )
            .await
            .map_err(pd_apply_to_storage_err)?;

        let mut m = self.meta.write().await;
        m.last_applied = payload.last_applied;
        m.last_membership = payload.last_membership;
        drop(m);

        let stored = StoredSnapshot {
            meta: meta.clone(),
            data,
        };
        *self.current_snapshot.write().await = Some(stored);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<PdTypeConfig>>, StorageError<NodeId>> {
        let guard = self.current_snapshot.read().await;
        Ok(guard.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use aresadb_core::{MemoryBackend, StorageBackend};
    use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, Membership};

    use super::*;
    use crate::state_machine::PD_RAFT_META_KEY;
    use crate::types::{LeaseInfo, RangeDescriptor, ReplicaPlacement};
    use crate::{PdCommand, PdResponse, PdStateMachine};

    fn memory_backend() -> Arc<dyn StorageBackend> {
        Arc::new(MemoryBackend::new())
    }

    fn voters(ids: &[u64]) -> Vec<ReplicaPlacement> {
        ids.iter().map(|n| ReplicaPlacement::voter(*n, 1)).collect()
    }

    fn genesis_range() -> RangeDescriptor {
        RangeDescriptor::new(1, Vec::<u8>::new(), Vec::<u8>::new(), voters(&[1, 2, 3]))
    }

    fn log_id(term: u64, index: u64) -> LogId<NodeId> {
        LogId::new(CommittedLeaderId::new(term, 0), index)
    }

    fn normal_entry(term: u64, index: u64, cmd: PdCommand) -> Entry<PdTypeConfig> {
        Entry {
            log_id: log_id(term, index),
            payload: EntryPayload::Normal(cmd),
        }
    }

    fn blank_entry(term: u64, index: u64) -> Entry<PdTypeConfig> {
        Entry {
            log_id: log_id(term, index),
            payload: EntryPayload::Blank,
        }
    }

    fn membership_entry(term: u64, index: u64, voters: &[NodeId]) -> Entry<PdTypeConfig> {
        let m = Membership::new(vec![voters.iter().copied().collect()], None);
        Entry {
            log_id: log_id(term, index),
            payload: EntryPayload::Membership(m),
        }
    }

    async fn setup() -> (Arc<dyn StorageBackend>, Arc<PdRaftStateMachine>) {
        let backend = memory_backend();
        let inner = PdStateMachine::open(backend.clone()).await.unwrap();
        let sm = PdRaftStateMachine::open(inner).await.unwrap();
        (backend, sm)
    }

    #[tokio::test]
    async fn fresh_adapter_reports_no_applied_state() {
        let (_, sm) = setup().await;
        let mut handle = sm.clone();
        let (last, mem) = handle.applied_state().await.unwrap();
        assert!(last.is_none());
        assert_eq!(mem.log_id(), &None);
    }

    #[tokio::test]
    async fn apply_normal_runs_catalog_and_persists_meta() {
        let (backend, sm) = setup().await;
        let mut handle = sm.clone();

        let resp = handle
            .apply([normal_entry(1, 1, PdCommand::CreateRange(genesis_range()))])
            .await
            .unwrap();
        assert_eq!(resp.len(), 1);
        assert!(matches!(&resp[0], PdResponse::Range(r) if r.range_id == 1));

        // Range row landed.
        let row = backend
            .get(&crate::persist::range_key(1))
            .await
            .unwrap()
            .expect("range row present");
        let desc: RangeDescriptor = bincode::deserialize(&row).unwrap();
        assert_eq!(desc, genesis_range());

        // Meta row landed with last_applied = (1,1).
        let raw = backend
            .get(PD_RAFT_META_KEY)
            .await
            .unwrap()
            .expect("meta row present");
        let meta: PersistedPdMeta = bincode::deserialize(&raw).unwrap();
        assert_eq!(meta.last_applied.unwrap().index, 1);
    }

    #[tokio::test]
    async fn catalog_rejection_folded_into_response_and_advances_meta() {
        let (backend, sm) = setup().await;
        let mut handle = sm.clone();

        // Zero-width range (start == end, both non-empty) — catalog
        // rejects with InvalidSpan.
        let bad = RangeDescriptor::new(1, b"a".to_vec(), b"a".to_vec(), voters(&[1]));
        let resp = handle
            .apply([normal_entry(1, 1, PdCommand::CreateRange(bad))])
            .await
            .unwrap();
        match &resp[0] {
            PdResponse::Error(msg) => assert!(msg.contains("span"), "got `{msg}`"),
            other => panic!("expected Error variant, got {other:?}"),
        }

        // No catalog row should exist…
        assert!(backend
            .get(&crate::persist::range_key(1))
            .await
            .unwrap()
            .is_none());

        // …but the meta row should still carry the advanced log id.
        let raw = backend.get(PD_RAFT_META_KEY).await.unwrap().unwrap();
        let meta: PersistedPdMeta = bincode::deserialize(&raw).unwrap();
        assert_eq!(meta.last_applied.unwrap().index, 1);
    }

    #[tokio::test]
    async fn blank_entry_advances_meta_only() {
        let (backend, sm) = setup().await;
        let mut handle = sm.clone();

        handle.apply([blank_entry(1, 7)]).await.unwrap();

        let raw = backend.get(PD_RAFT_META_KEY).await.unwrap().unwrap();
        let meta: PersistedPdMeta = bincode::deserialize(&raw).unwrap();
        assert_eq!(meta.last_applied.unwrap().index, 7);
        assert!(meta.last_membership.voter_ids().next().is_none());
    }

    #[tokio::test]
    async fn membership_entry_updates_last_membership() {
        let (backend, sm) = setup().await;
        let mut handle = sm.clone();

        handle
            .apply([membership_entry(1, 1, &[1, 2, 3])])
            .await
            .unwrap();
        let raw = backend.get(PD_RAFT_META_KEY).await.unwrap().unwrap();
        let meta: PersistedPdMeta = bincode::deserialize(&raw).unwrap();
        let voters: Vec<NodeId> = meta.last_membership.voter_ids().collect();
        assert_eq!(voters, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn reopen_restores_meta_and_catalog() {
        let backend = memory_backend();

        {
            let inner = PdStateMachine::open(backend.clone()).await.unwrap();
            let sm = PdRaftStateMachine::open(inner).await.unwrap();
            let mut h = sm.clone();
            h.apply([normal_entry(1, 1, PdCommand::CreateRange(genesis_range()))])
                .await
                .unwrap();
            h.apply([blank_entry(1, 2)]).await.unwrap();
            h.apply([membership_entry(1, 3, &[1, 2])]).await.unwrap();
        }

        let inner2 = PdStateMachine::open(backend.clone()).await.unwrap();
        let sm2 = PdRaftStateMachine::open(inner2).await.unwrap();
        let mut h2 = sm2.clone();

        let (last, mem) = h2.applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 3);
        let voters: Vec<NodeId> = mem.voter_ids().collect();
        assert_eq!(voters, vec![1, 2]);

        sm2.inner().read(|c| {
            assert_eq!(c.range_count(), 1);
            assert_eq!(c.get_range(1).unwrap(), &genesis_range());
        });
    }

    #[tokio::test]
    async fn snapshot_round_trip_preserves_catalog_and_meta() {
        let (_, sm) = setup().await;
        let mut h = sm.clone();

        // Build a non-trivial catalog: split once, register a node,
        // update a lease.
        h.apply([normal_entry(1, 1, PdCommand::CreateRange(genesis_range()))])
            .await
            .unwrap();
        h.apply([normal_entry(
            1,
            2,
            PdCommand::SplitRange {
                parent_range_id: 1,
                split_key: b"m".to_vec(),
            },
        )])
        .await
        .unwrap();
        h.apply([normal_entry(
            1,
            3,
            PdCommand::RegisterNode(NodeInfo {
                node_id: 9,
                address: "host-9:7001".to_string(),
                stores: vec![1],
                last_heartbeat_millis: 0,
            }),
        )])
        .await
        .unwrap();
        h.apply([normal_entry(
            1,
            4,
            PdCommand::UpdateLease {
                range_id: 2,
                lease: Some(LeaseInfo {
                    holder: 9,
                    expires_at_millis: 42,
                }),
            },
        )])
        .await
        .unwrap();
        h.apply([membership_entry(1, 5, &[1, 2, 3])]).await.unwrap();

        let snap = sm.clone().build_snapshot().await.unwrap();
        assert_eq!(snap.meta.last_log_id.unwrap().index, 5);

        // Install the snapshot on a fresh state machine.
        let fresh_backend = memory_backend();
        let fresh_inner = PdStateMachine::open(fresh_backend.clone()).await.unwrap();
        let fresh_sm = PdRaftStateMachine::open(fresh_inner).await.unwrap();
        let mut fresh_h = fresh_sm.clone();

        let snap_bytes = snap.snapshot.into_inner();
        let snap_meta = snap.meta.clone();
        fresh_h
            .install_snapshot(&snap_meta, Box::new(Cursor::new(snap_bytes)))
            .await
            .unwrap();

        // Meta matches.
        let (last, mem) = fresh_h.applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 5);
        let voters: Vec<NodeId> = mem.voter_ids().collect();
        assert_eq!(voters, vec![1, 2, 3]);

        // Catalog matches.
        fresh_sm.inner().read(|c| {
            assert_eq!(c.range_count(), 2);
            assert_eq!(c.get_range(1).unwrap().end_key, b"m".to_vec());
            assert_eq!(c.get_range(2).unwrap().start_key, b"m".to_vec());
            assert_eq!(c.get_range(2).unwrap().lease.as_ref().unwrap().holder, 9);
            assert_eq!(c.get_node(9).unwrap().address, "host-9:7001");
            // next_range_id round-trips, so a subsequent split picks
            // id 3, not 2.
            assert_eq!(c.peek_next_range_id(), 3);
        });
    }

    #[tokio::test]
    async fn install_snapshot_wipes_old_catalog() {
        let (backend, sm) = setup().await;
        let mut h = sm.clone();

        // Populate the receiver with pre-existing state that the
        // snapshot doesn't mention.
        h.apply([normal_entry(1, 1, PdCommand::CreateRange(genesis_range()))])
            .await
            .unwrap();
        assert!(backend
            .get(&crate::persist::range_key(1))
            .await
            .unwrap()
            .is_some());

        // Build an empty snapshot.
        let empty_snap = SnapshotPayload {
            last_applied: Some(log_id(2, 5)),
            last_membership: StoredMembership::default(),
            next_range_id: 10,
            ranges: Vec::new(),
            nodes: Vec::new(),
        };
        let bytes = bincode::serialize(&empty_snap).unwrap();
        let snap_meta = SnapshotMeta {
            last_log_id: empty_snap.last_applied,
            last_membership: empty_snap.last_membership.clone(),
            snapshot_id: "install-wipes-old".to_string(),
        };

        h.install_snapshot(&snap_meta, Box::new(Cursor::new(bytes.clone())))
            .await
            .unwrap();

        // Catalog row is gone.
        assert!(backend
            .get(&crate::persist::range_key(1))
            .await
            .unwrap()
            .is_none());
        // Meta matches snapshot.
        let (last, _) = h.applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 5);
        // Catalog in-memory is also empty with restored id counter.
        sm.inner().read(|c| {
            assert_eq!(c.range_count(), 0);
            assert_eq!(c.peek_next_range_id(), 10);
        });
    }

    #[tokio::test]
    async fn get_current_snapshot_surfaces_last_built() {
        let (_, sm) = setup().await;
        let mut h = sm.clone();

        assert!(h.get_current_snapshot().await.unwrap().is_none());

        h.apply([normal_entry(1, 1, PdCommand::CreateRange(genesis_range()))])
            .await
            .unwrap();
        let _ = sm.clone().build_snapshot().await.unwrap();

        let current = h.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(current.meta.last_log_id.unwrap().index, 1);
    }

    #[tokio::test]
    async fn applies_over_many_entries_batch_into_single_meta_tail() {
        let (backend, sm) = setup().await;
        let mut h = sm.clone();

        let entries = vec![
            normal_entry(1, 1, PdCommand::CreateRange(genesis_range())),
            normal_entry(
                1,
                2,
                PdCommand::SplitRange {
                    parent_range_id: 1,
                    split_key: b"m".to_vec(),
                },
            ),
            blank_entry(1, 3),
            membership_entry(1, 4, &[1, 2, 3]),
        ];

        let responses = h.apply(entries).await.unwrap();
        assert_eq!(responses.len(), 4);

        // Final meta reflects the last entry.
        let raw = backend.get(PD_RAFT_META_KEY).await.unwrap().unwrap();
        let meta: PersistedPdMeta = bincode::deserialize(&raw).unwrap();
        assert_eq!(meta.last_applied.unwrap().index, 4);
        let voters: Vec<NodeId> = meta.last_membership.voter_ids().collect();
        assert_eq!(voters, vec![1, 2, 3]);
    }
}

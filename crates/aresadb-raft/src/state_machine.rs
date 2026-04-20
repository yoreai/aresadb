//! `RaftStateMachine` implementation.
//!
//! The state machine applies committed [`AresaCommand`]s in Raft order
//! to a user-owned [`StorageBackend`]. Snapshots serialize the full
//! keyspace of that backend plus the applied-state metadata (last
//! applied log id, last membership).
//!
//! ## Persistence model
//!
//! State-machine metadata (`last_applied`, `last_membership`) is
//! persisted to the data backend under a reserved
//! `b"\xff/sm/"`-prefixed keyspace. Applying an entry writes the new
//! metadata together with the user-facing keys in a single
//! [`WriteBatch`], so recovery is straightforward: on startup we
//! read the metadata row back into memory and resume from there.
//!
//! User keys that start with the `0xff` byte are reserved for
//! internal metadata; application code should never produce such
//! keys. The state machine won't stop you from writing them — that's
//! the caller's contract.
//!
//! ## Determinism
//!
//! Every entry's `WriteBatch` is deterministic: `Put`/`Delete`/
//! `DeleteRange` are idempotent so re-applying a committed entry
//! after a crash yields the same final state as applying it once.
//! That's what lets the recovery path be simple — re-apply from log
//! index 1 up to the committed pointer — without worrying about
//! double-writes.

use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use aresadb_core::{KeyRange, StorageBackend, WriteBatch};
use bytes::Bytes;
use futures::StreamExt;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend, SnapshotMeta,
    StorageError, StoredMembership,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::command::{AresaCommand, AresaResponse};
use crate::error::{storage_err, storage_err_ctx, BincodeError};
use crate::types::{NodeId, TypeConfig};

/// Serializable snapshot payload.
///
/// Stored as the contents of [`openraft::storage::Snapshot`]. The
/// `data` field is every key/value pair in the application backend at
/// snapshot-build time; installing a snapshot clears the backend and
/// replays these pairs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotPayload {
    /// Last applied log id captured in the snapshot.
    pub last_applied: Option<LogId<NodeId>>,

    /// Applied membership at snapshot time.
    pub last_membership: StoredMembership<NodeId, BasicNode>,

    /// All application key/value pairs.
    ///
    /// We keep them as `Vec<u8>` so the payload is purely `serde` and
    /// doesn't need the `bytes` serde feature on the wire.
    pub data: Vec<(Vec<u8>, Vec<u8>)>,
}

/// An in-memory stored snapshot — metadata + the serialized payload.
#[derive(Debug, Clone)]
pub struct StoredSnapshot {
    /// Openraft snapshot metadata.
    pub meta: SnapshotMeta<NodeId, BasicNode>,
    /// bincode-encoded [`SnapshotPayload`].
    pub data: Vec<u8>,
}

#[derive(Default)]
struct Inner {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
}

/// Reserved prefix for state-machine internal metadata. Application
/// keys must not start with the `0xff` byte — every other byte is
/// free for callers.
pub(crate) const SM_META_KEY: &[u8] = b"\xff/sm/meta";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PersistedMeta {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
}

/// State machine over a [`StorageBackend`].
///
/// Construct one with [`StateMachineStore::new`] and wrap it in an
/// `Arc` — openraft takes ownership of an `Arc<Self>` when the
/// `RaftStateMachine` impls are written on the shared handle (just
/// like the upstream memstore example).
pub struct StateMachineStore {
    /// Application backend. The state machine owns no other key space
    /// on this backend — all application keys live here.
    data: Arc<dyn StorageBackend>,

    inner: RwLock<Inner>,

    /// Most recent snapshot produced or installed. `None` until the
    /// first snapshot exists.
    current_snapshot: RwLock<Option<StoredSnapshot>>,

    /// Monotonic counter used to disambiguate snapshot identifiers
    /// when multiple snapshots are built in quick succession. Purely
    /// cosmetic — it just shows up in the metadata's `snapshot_id`.
    snapshot_idx: AtomicU64,
}

impl StateMachineStore {
    /// Create a new state machine bound to `data`.
    ///
    /// On construction we read the persisted metadata row (the
    /// crate-private `SM_META_KEY` byte key) from the data backend so
    /// recovery after a restart resumes from exactly where we left
    /// off.
    pub fn new(data: Arc<dyn StorageBackend>) -> Arc<Self> {
        Arc::new(Self::new_sync(data))
    }

    fn new_sync(data: Arc<dyn StorageBackend>) -> Self {
        Self {
            data,
            inner: RwLock::new(Inner::default()),
            current_snapshot: RwLock::new(None),
            snapshot_idx: AtomicU64::new(0),
        }
    }

    /// Rehydrate in-memory metadata from the data backend. Called
    /// once by [`StateMachineStore::open`]; callers constructing via
    /// [`StateMachineStore::new`] must invoke it themselves before
    /// handing the store to openraft if they care about recovery.
    pub async fn load_persisted(&self) -> Result<(), StorageError<NodeId>> {
        let raw = self.data.get(SM_META_KEY).await.map_err(storage_err)?;
        let Some(raw) = raw else {
            return Ok(());
        };
        let meta: PersistedMeta = bincode::deserialize(&raw).map_err(|e| {
            storage_err_ctx(ErrorSubject::StateMachine, ErrorVerb::Read, BincodeError(e))
        })?;
        let mut inner = self.inner.write().await;
        inner.last_applied = meta.last_applied;
        inner.last_membership = meta.last_membership;
        Ok(())
    }

    /// Convenience constructor that creates the store and
    /// immediately rehydrates any persisted metadata. Prefer this
    /// over [`StateMachineStore::new`] unless you have a specific
    /// reason to split the two steps.
    pub async fn open(data: Arc<dyn StorageBackend>) -> Result<Arc<Self>, StorageError<NodeId>> {
        let sm = Arc::new(Self::new_sync(data));
        sm.load_persisted().await?;
        Ok(sm)
    }

    /// Borrow the application backend.
    ///
    /// External readers (the SQL / graph / vector layers) use this to
    /// serve committed reads without going through Raft. It's
    /// intentionally read-only from their perspective; the state
    /// machine is the only writer.
    pub fn data_backend(&self) -> &Arc<dyn StorageBackend> {
        &self.data
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let (last_applied, last_membership) = {
            let inner = self.inner.read().await;
            (inner.last_applied, inner.last_membership.clone())
        };

        // Snapshot the user-visible keyspace. We stop at the
        // reserved `0xff` prefix so internal metadata (SM meta,
        // future indexes, etc.) doesn't leak into application
        // snapshots — callers installing this snapshot will
        // re-derive their own metadata.
        let mut stream = self
            .data
            .scan(KeyRange::to(Bytes::from(vec![0xffu8])))
            .await
            .map_err(storage_err)?;
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while let Some(item) = stream.next().await {
            let kv = item.map_err(storage_err)?;
            entries.push((kv.key.to_vec(), kv.value.to_vec()));
        }

        let payload = SnapshotPayload {
            last_applied,
            last_membership: last_membership.clone(),
            data: entries,
        };
        let data = bincode::serialize(&payload).map_err(|e| {
            storage_err_ctx(ErrorSubject::StateMachine, ErrorVerb::Read, BincodeError(e))
        })?;

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id,
        };

        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        *self.current_snapshot.write().await = Some(stored);

        tracing::info!(
            last_applied = ?last_applied,
            bytes = data.len(),
            "state machine snapshot built"
        );

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let inner = self.inner.read().await;
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<AresaResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = crate::types::typ::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut responses: Vec<AresaResponse> = Vec::new();
        let mut inner = self.inner.write().await;

        for entry in entries {
            inner.last_applied = Some(entry.log_id);

            let mut batch = WriteBatch::new();

            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(AresaResponse::default());
                }
                EntryPayload::Normal(AresaCommand::Noop) => {
                    responses.push(AresaResponse::default());
                }
                EntryPayload::Normal(AresaCommand::WriteBatch(sb)) => {
                    let ops_applied = sb.ops.len() as u32;
                    let user_batch: WriteBatch = sb.into();
                    for op in user_batch.into_ops() {
                        batch.push(op);
                    }
                    responses.push(AresaResponse { ops_applied });
                }
                EntryPayload::Membership(ref mem) => {
                    inner.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    responses.push(AresaResponse::default());
                }
            }

            // Persist the updated metadata atomically with any user
            // ops from this entry. A crash between apply and the
            // next restart would otherwise leave the state machine
            // thinking it's behind and openraft would try to replay
            // entries that had already landed in user keys.
            let meta = PersistedMeta {
                last_applied: inner.last_applied,
                last_membership: inner.last_membership.clone(),
            };
            let encoded = bincode::serialize(&meta).map_err(|e| {
                storage_err_ctx(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Write,
                    BincodeError(e),
                )
            })?;
            batch.put(Bytes::from_static(SM_META_KEY), Bytes::from(encoded));

            self.data
                .write_batch(batch)
                .await
                .map_err(|e| storage_err_ctx(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
        }

        // Ensure the backend has durably persisted the applied writes
        // before we acknowledge the apply. Without this flush a crash
        // between apply and the next snapshot could lose committed
        // writes on engines that buffer.
        self.data
            .flush()
            .await
            .map_err(|e| storage_err_ctx(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;

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
        let payload: SnapshotPayload = bincode::deserialize(&data).map_err(|e| {
            storage_err_ctx(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Read,
                BincodeError(e),
            )
        })?;

        // Wipe the application backend and repopulate from the
        // snapshot payload. This is intentionally atomic from the
        // caller's perspective (one `WriteBatch`).
        let mut batch = WriteBatch::with_capacity(payload.data.len() + 2);
        // Clear the user keyspace (everything before the reserved
        // `0xff` prefix). We leave existing metadata entries alone
        // until we overwrite them below — that way a mid-install
        // crash can't wipe our bookkeeping before replacing it.
        batch.delete_range(Bytes::new(), Bytes::from(vec![0xffu8]));
        for (k, v) in payload.data {
            batch.put(Bytes::from(k), Bytes::from(v));
        }
        // Overwrite the metadata row with the snapshot's view.
        let persisted = PersistedMeta {
            last_applied: payload.last_applied,
            last_membership: payload.last_membership.clone(),
        };
        let encoded = bincode::serialize(&persisted).map_err(|e| {
            storage_err_ctx(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Write,
                BincodeError(e),
            )
        })?;
        batch.put(Bytes::from_static(SM_META_KEY), Bytes::from(encoded));

        self.data.write_batch(batch).await.map_err(|e| {
            storage_err_ctx(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Write,
                e,
            )
        })?;
        self.data.flush().await.map_err(|e| {
            storage_err_ctx(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Write,
                e,
            )
        })?;

        let mut inner = self.inner.write().await;
        inner.last_applied = payload.last_applied;
        inner.last_membership = payload.last_membership;
        drop(inner);

        let stored = StoredSnapshot {
            meta: meta.clone(),
            data,
        };
        *self.current_snapshot.write().await = Some(stored);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
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
    use super::*;
    use aresadb_core::MemoryBackend;
    use openraft::EntryPayload;

    fn setup() -> (Arc<dyn StorageBackend>, Arc<StateMachineStore>) {
        let data: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let sm = StateMachineStore::new(data.clone());
        (data, sm)
    }

    fn normal_entry(term: u64, index: u64, cmd: AresaCommand) -> crate::types::typ::Entry {
        openraft::Entry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Normal(cmd),
        }
    }

    #[tokio::test]
    async fn apply_write_batch_writes_to_data_backend() {
        let (data, sm) = setup();
        let mut sm_mut = sm.clone();

        let mut batch = WriteBatch::new();
        batch.put("k", "v");
        let entry = normal_entry(1, 1, AresaCommand::batch(batch));

        let responses = sm_mut.apply([entry]).await.unwrap();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].ops_applied, 1);

        let got = data.get(b"k").await.unwrap().unwrap();
        assert_eq!(&got[..], b"v");
    }

    #[tokio::test]
    async fn applied_state_tracks_last_log_id() {
        let (_, sm) = setup();
        let mut sm_mut = sm.clone();

        let entry = normal_entry(1, 7, AresaCommand::Noop);
        sm_mut.apply([entry]).await.unwrap();

        let (last, _) = sm_mut.applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 7);
    }

    #[tokio::test]
    async fn snapshot_roundtrip_preserves_data_and_applied() {
        let (data, sm) = setup();
        let mut sm_mut = sm.clone();

        let mut b = WriteBatch::new();
        b.put("x", "1").put("y", "2");
        sm_mut
            .apply([normal_entry(1, 1, AresaCommand::batch(b))])
            .await
            .unwrap();

        let snap = sm_mut.clone().build_snapshot().await.unwrap();
        assert!(snap.meta.last_log_id.is_some());

        // Mutate after snapshotting — the snapshot should still see
        // the pre-snapshot state when installed.
        let mut b2 = WriteBatch::new();
        b2.put("x", "post-snap");
        sm_mut
            .apply([normal_entry(1, 2, AresaCommand::batch(b2))])
            .await
            .unwrap();
        assert_eq!(&data.get(b"x").await.unwrap().unwrap()[..], b"post-snap");

        // Rebuild a fresh SM on a fresh backend and install the
        // snapshot. It must reach the pre-snapshot state.
        let fresh_backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let fresh_sm = StateMachineStore::new(fresh_backend.clone());
        let mut fresh_mut = fresh_sm.clone();

        let snap_bytes = snap.snapshot.into_inner();
        let meta = snap.meta.clone();
        fresh_mut
            .install_snapshot(&meta, Box::new(Cursor::new(snap_bytes)))
            .await
            .unwrap();

        assert_eq!(&fresh_backend.get(b"x").await.unwrap().unwrap()[..], b"1");
        assert_eq!(&fresh_backend.get(b"y").await.unwrap().unwrap()[..], b"2");
        let (last, _) = fresh_mut.applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 1);
    }

    #[tokio::test]
    async fn install_snapshot_clears_previous_state() {
        let (data, sm) = setup();
        let mut sm_mut = sm.clone();

        // Apply some pre-snapshot data.
        let mut b = WriteBatch::new();
        b.put("old", "data");
        sm_mut
            .apply([normal_entry(1, 1, AresaCommand::batch(b))])
            .await
            .unwrap();
        assert!(data.get(b"old").await.unwrap().is_some());

        // Build a snapshot reflecting that state.
        let snap = sm_mut.clone().build_snapshot().await.unwrap();
        let snap_bytes = snap.snapshot.into_inner();
        let meta = snap.meta.clone();

        // Write more data the snapshot doesn't know about.
        let mut b2 = WriteBatch::new();
        b2.put("extra", "post-snap");
        sm_mut
            .apply([normal_entry(1, 2, AresaCommand::batch(b2))])
            .await
            .unwrap();
        assert!(data.get(b"extra").await.unwrap().is_some());

        // Installing the earlier snapshot must wipe the extra key.
        sm_mut
            .install_snapshot(&meta, Box::new(Cursor::new(snap_bytes)))
            .await
            .unwrap();
        assert!(data.get(b"extra").await.unwrap().is_none());
        assert_eq!(&data.get(b"old").await.unwrap().unwrap()[..], b"data");
    }

    #[tokio::test]
    async fn current_snapshot_starts_none_and_updates_after_build() {
        let (_, sm) = setup();
        let mut sm_mut = sm.clone();

        assert!(sm_mut.get_current_snapshot().await.unwrap().is_none());

        sm_mut
            .apply([normal_entry(1, 1, AresaCommand::Noop)])
            .await
            .unwrap();
        let _ = sm_mut.clone().build_snapshot().await.unwrap();
        assert!(sm_mut.get_current_snapshot().await.unwrap().is_some());
    }
}

//! `RaftLogStorage` + `RaftLogReader` over a [`StorageBackend`].
//!
//! ## Layout
//!
//! The backend's keyspace is partitioned so Raft log entries never
//! collide with the meta keys the log store needs:
//!
//! | Prefix            | Content                                   |
//! |-------------------|-------------------------------------------|
//! | `0x00 <index:8>`  | bincode-encoded `Entry<C>`                |
//! | `0x01 0x01`       | bincode-encoded `Vote`                    |
//! | `0x01 0x02`       | bincode-encoded `Option<LogId>` (committed) |
//! | `0x01 0x03`       | bincode-encoded `Option<LogId>` (purged)  |
//!
//! Index bytes are big-endian so lexicographic ordering matches
//! numeric ordering — that lets us translate a `RangeBounds<u64>`
//! directly into a byte range for the backend's `scan`.
//!
//! ## Durability
//!
//! Every mutating openraft call issues exactly one
//! [`StorageBackend::write_batch`], which backends are required to
//! apply atomically. For the v1 redb-backed wrapper (Phase 1c) the
//! batch is also `fsync`ed before the call returns, so by the time
//! `append` resolves the `LogFlushed` callback the entries are
//! durable.
//!
//! The in-memory `MemoryBackend` used in tests satisfies the same
//! API but drops data on restart; it exists to make the openraft
//! [testing suite] fast and deterministic.
//!
//! ## Type-config parameterization
//!
//! The underlying [`LogStoreGeneric`] is generic over any
//! [`openraft::RaftTypeConfig`], which is what lets the PD Raft
//! group (Phase 2b-3) reuse the exact same log persistence as the
//! user-data Raft groups without any code duplication. The default
//! alias [`LogStore`] keeps the single-group Phase 1 call sites
//! compiling unchanged.
//!
//! [testing suite]: openraft::testing::Suite

use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use aresadb_core::{KeyRange, StorageBackend, WriteBatch};
use bytes::Bytes;
use futures::StreamExt;
use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{
    ErrorSubject, ErrorVerb, LogId, OptionalSend, RaftLogId, RaftTypeConfig, StorageError, Vote,
};

use crate::error::{storage_err, storage_err_ctx, BincodeError};
use crate::types::TypeConfig;

const LOG_PREFIX: u8 = 0x00;
const META_PREFIX: u8 = 0x01;
const META_VOTE: u8 = 0x01;
const META_COMMITTED: u8 = 0x02;
const META_PURGED: u8 = 0x03;

/// Build the log-entry key for `index` (`[0x00, index_be_bytes]`).
fn log_key(index: u64) -> Bytes {
    let mut v = Vec::with_capacity(9);
    v.push(LOG_PREFIX);
    v.extend_from_slice(&index.to_be_bytes());
    Bytes::from(v)
}

/// Build a meta key for `tag` under the meta prefix.
fn meta_key(tag: u8) -> Bytes {
    Bytes::from(vec![META_PREFIX, tag])
}

/// Translate a `u64` range-bounds into the backend `KeyRange` that
/// covers the requested log entries.
fn log_range<RB: RangeBounds<u64>>(range: RB) -> KeyRange {
    let start = match range.start_bound() {
        Bound::Included(&i) => log_key(i),
        Bound::Excluded(&i) => log_key(i.saturating_add(1)),
        Bound::Unbounded => Bytes::from(vec![LOG_PREFIX]),
    };
    let end = match range.end_bound() {
        Bound::Included(&i) => {
            if i == u64::MAX {
                Bytes::from(vec![LOG_PREFIX + 1])
            } else {
                log_key(i + 1)
            }
        }
        Bound::Excluded(&i) => log_key(i),
        Bound::Unbounded => Bytes::from(vec![LOG_PREFIX + 1]),
    };
    KeyRange::new(start, end)
}

/// Log storage backed by a generic [`StorageBackend`], parameterized
/// over any openraft [`RaftTypeConfig`].
///
/// Clones are cheap — they share the same `Arc<dyn StorageBackend>`
/// inside — which lets openraft create read views via
/// `get_log_reader` without any extra allocation.
///
/// Phase 1 and Phase 2b-3 use the same underlying struct with
/// different type configs (`TypeConfig` for the user-data group,
/// `PdTypeConfig` for the placement-driver group). See [`LogStore`]
/// for the default alias pinned to `TypeConfig`.
pub struct LogStoreGeneric<C: RaftTypeConfig> {
    backend: Arc<dyn StorageBackend>,
    _marker: PhantomData<C>,
}

/// Convenience alias pinned to the user-data [`TypeConfig`]. Phase 1
/// callers reach for this name directly; the generic form is spelled
/// [`LogStoreGeneric`].
pub type LogStore = LogStoreGeneric<TypeConfig>;

impl<C: RaftTypeConfig> Clone for LogStoreGeneric<C> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            _marker: PhantomData,
        }
    }
}

impl<C: RaftTypeConfig> Debug for LogStoreGeneric<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogStoreGeneric").finish_non_exhaustive()
    }
}

impl<C: RaftTypeConfig> LogStoreGeneric<C>
where
    C::NodeId: Copy,
{
    /// Create a new log store over `backend`.
    ///
    /// The backend is assumed empty (or previously owned by a
    /// [`LogStoreGeneric`]). Mixing Raft log entries with
    /// application data on the same backend is not supported — the
    /// higher layer must keep the log on its own dedicated backend.
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            backend,
            _marker: PhantomData,
        }
    }

    async fn read_vote_inner(&self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
        let bytes = self
            .backend
            .get(&meta_key(META_VOTE))
            .await
            .map_err(storage_err)?;
        match bytes {
            None => Ok(None),
            Some(b) => {
                let v: Vote<C::NodeId> =
                    bincode::deserialize(&b).map_err(|e| storage_err(BincodeError(e)))?;
                Ok(Some(v))
            }
        }
    }

    async fn read_meta_log_id(
        &self,
        tag: u8,
        subject: ErrorSubject<C::NodeId>,
    ) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
        let bytes = self
            .backend
            .get(&meta_key(tag))
            .await
            .map_err(|e| storage_err_ctx(subject.clone(), ErrorVerb::Read, e))?;
        match bytes {
            None => Ok(None),
            Some(b) => {
                let v: Option<LogId<C::NodeId>> = bincode::deserialize(&b)
                    .map_err(|e| storage_err_ctx(subject, ErrorVerb::Read, BincodeError(e)))?;
                Ok(v)
            }
        }
    }

    async fn last_log_id_on_disk(
        &self,
    ) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
        let mut stream = self
            .backend
            .scan(log_range::<(Bound<u64>, Bound<u64>)>((
                Bound::Unbounded,
                Bound::Unbounded,
            )))
            .await
            .map_err(storage_err)?;
        let mut last: Option<LogId<C::NodeId>> = None;
        while let Some(item) = stream.next().await {
            let kv = item.map_err(storage_err)?;
            let entry: C::Entry =
                bincode::deserialize(&kv.value).map_err(|e| storage_err(BincodeError(e)))?;
            last = Some(*entry.get_log_id());
        }
        Ok(last)
    }
}

impl<C: RaftTypeConfig> RaftLogReader<C> for LogStoreGeneric<C>
where
    C::NodeId: Copy,
{
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let krange = log_range(range);
        let mut stream = self.backend.scan(krange).await.map_err(storage_err)?;
        let mut entries = Vec::new();
        while let Some(item) = stream.next().await {
            let kv = item.map_err(storage_err)?;
            let entry: C::Entry =
                bincode::deserialize(&kv.value).map_err(|e| storage_err(BincodeError(e)))?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

impl<C: RaftTypeConfig> RaftLogStorage<C> for LogStoreGeneric<C>
where
    C::NodeId: Copy,
{
    type LogReader = Self;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>> {
        let last_purged = self
            .read_meta_log_id(META_PURGED, ErrorSubject::Store)
            .await?;
        let last_entry = self.last_log_id_on_disk().await?;
        let last = match (last_entry, last_purged) {
            (None, purged) => purged,
            (some @ Some(_), _) => some,
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        let bytes = bincode::serialize(vote)
            .map_err(|e| storage_err_ctx(ErrorSubject::Vote, ErrorVerb::Write, BincodeError(e)))?;
        let mut batch = WriteBatch::new();
        batch.put(meta_key(META_VOTE), Bytes::from(bytes));
        self.backend
            .write_batch(batch)
            .await
            .map_err(|e| storage_err_ctx(ErrorSubject::Vote, ErrorVerb::Write, e))?;
        self.backend
            .flush()
            .await
            .map_err(|e| storage_err_ctx(ErrorSubject::Vote, ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
        self.read_vote_inner().await
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<C::NodeId>>,
    ) -> Result<(), StorageError<C::NodeId>> {
        let bytes = bincode::serialize(&committed)
            .map_err(|e| storage_err_ctx(ErrorSubject::Store, ErrorVerb::Write, BincodeError(e)))?;
        let mut batch = WriteBatch::new();
        batch.put(meta_key(META_COMMITTED), Bytes::from(bytes));
        self.backend.write_batch(batch).await.map_err(storage_err)?;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
        self.read_meta_log_id(META_COMMITTED, ErrorSubject::Store)
            .await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> Result<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut batch = WriteBatch::new();
        let mut staged_ids: Vec<LogId<C::NodeId>> = Vec::new();

        for entry in entries {
            let log_id = *entry.get_log_id();
            let key = log_key(log_id.index);
            let value = bincode::serialize(&entry).map_err(|e| {
                storage_err_ctx(ErrorSubject::Log(log_id), ErrorVerb::Write, BincodeError(e))
            })?;
            batch.put(key, Bytes::from(value));
            staged_ids.push(log_id);
        }

        if batch.is_empty() {
            callback.log_io_completed(Ok(()));
            return Ok(());
        }

        // Apply atomically.
        if let Err(e) = self.backend.write_batch(batch).await {
            return Err(storage_err_ctx(ErrorSubject::Logs, ErrorVerb::Write, e));
        }
        if let Err(e) = self.backend.flush().await {
            return Err(storage_err_ctx(ErrorSubject::Logs, ErrorVerb::Write, e));
        }

        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        // Delete every log entry with index >= log_id.index.
        let start = log_key(log_id.index);
        let end = Bytes::from(vec![LOG_PREFIX + 1]);
        let mut batch = WriteBatch::new();
        batch.delete_range(start, end);
        self.backend
            .write_batch(batch)
            .await
            .map_err(|e| storage_err_ctx(ErrorSubject::Log(log_id), ErrorVerb::Delete, e))?;
        self.backend
            .flush()
            .await
            .map_err(|e| storage_err_ctx(ErrorSubject::Log(log_id), ErrorVerb::Delete, e))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        // Sanity: openraft never moves the purge pointer backwards.
        if let Some(prev) = self
            .read_meta_log_id(META_PURGED, ErrorSubject::Store)
            .await?
        {
            assert!(
                prev.index <= log_id.index,
                "purge must advance last_purged_log_id (prev={:?}, new={:?})",
                prev,
                log_id,
            );
        }

        // Update the purge pointer first so a crash between these two
        // writes can only leave stale entries behind (harmless).
        let purged_bytes = bincode::serialize(&Some(log_id))
            .map_err(|e| storage_err_ctx(ErrorSubject::Store, ErrorVerb::Write, BincodeError(e)))?;
        let mut meta = WriteBatch::new();
        meta.put(meta_key(META_PURGED), Bytes::from(purged_bytes));
        self.backend
            .write_batch(meta)
            .await
            .map_err(|e| storage_err_ctx(ErrorSubject::Store, ErrorVerb::Write, e))?;

        // Delete [0, log_id.index] — hence exclusive end at log_id.index+1.
        let end_index = log_id.index.saturating_add(1);
        let start = Bytes::from(vec![LOG_PREFIX]);
        let end = log_key(end_index);
        let mut batch = WriteBatch::new();
        batch.delete_range(start, end);
        self.backend
            .write_batch(batch)
            .await
            .map_err(|e| storage_err_ctx(ErrorSubject::Logs, ErrorVerb::Delete, e))?;

        self.backend
            .flush()
            .await
            .map_err(|e| storage_err_ctx(ErrorSubject::Logs, ErrorVerb::Delete, e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::typ;
    use aresadb_core::MemoryBackend;
    use openraft::storage::RaftLogStorageExt;
    use openraft::testing;
    use openraft::EntryPayload;

    fn backend() -> Arc<dyn StorageBackend> {
        Arc::new(MemoryBackend::new())
    }

    fn blank_ent(term: u64, index: u64) -> typ::Entry {
        // openraft's `blank_ent` returns a type-correct Entry wrapped
        // around a blank payload — exactly what we need for log-level
        // roundtrip testing without touching the command enum.
        testing::blank_ent::<TypeConfig>(term, 0, index)
    }

    #[tokio::test]
    async fn append_and_read_roundtrip() {
        let mut store = LogStore::new(backend());
        store
            .blocking_append([blank_ent(1, 1), blank_ent(1, 2), blank_ent(1, 3)])
            .await
            .unwrap();

        let got = store.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].log_id.index, 1);
        assert_eq!(got[2].log_id.index, 3);
    }

    #[tokio::test]
    async fn get_log_state_returns_last_entry_when_nothing_purged() {
        let mut store = LogStore::new(backend());
        store
            .blocking_append([blank_ent(1, 1), blank_ent(1, 2)])
            .await
            .unwrap();

        let state = store.get_log_state().await.unwrap();
        assert!(state.last_purged_log_id.is_none());
        assert_eq!(state.last_log_id.unwrap().index, 2);
    }

    #[tokio::test]
    async fn truncate_removes_tail_only() {
        let mut store = LogStore::new(backend());
        store
            .blocking_append([blank_ent(1, 1), blank_ent(1, 2), blank_ent(1, 3)])
            .await
            .unwrap();

        store
            .truncate(openraft::LogId::new(
                openraft::CommittedLeaderId::new(1, 0),
                2,
            ))
            .await
            .unwrap();

        let remaining = store.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].log_id.index, 1);
    }

    #[tokio::test]
    async fn purge_cleans_prefix_and_advances_pointer() {
        let mut store = LogStore::new(backend());
        store
            .blocking_append([
                blank_ent(1, 1),
                blank_ent(1, 2),
                blank_ent(1, 3),
                blank_ent(1, 4),
            ])
            .await
            .unwrap();

        let purge_up_to = openraft::LogId::new(openraft::CommittedLeaderId::new(1, 0), 2);
        store.purge(purge_up_to).await.unwrap();

        let remaining = store.try_get_log_entries(1..=4).await.unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].log_id.index, 3);
        assert_eq!(remaining[1].log_id.index, 4);

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 2);
        assert_eq!(state.last_log_id.unwrap().index, 4);
    }

    #[tokio::test]
    async fn vote_roundtrip() {
        let mut store = LogStore::new(backend());
        let vote = Vote::new(7, 3);
        store.save_vote(&vote).await.unwrap();
        let got = store.read_vote().await.unwrap().unwrap();
        assert_eq!(got.leader_id.term, 7);
    }

    #[tokio::test]
    async fn committed_roundtrip() {
        let mut store = LogStore::new(backend());
        assert!(store.read_committed().await.unwrap().is_none());

        let cid = openraft::LogId::new(openraft::CommittedLeaderId::new(2, 1), 42);
        store.save_committed(Some(cid)).await.unwrap();
        assert_eq!(store.read_committed().await.unwrap().unwrap().index, 42);
    }

    #[tokio::test]
    async fn get_log_state_reflects_purged_when_log_empty() {
        let mut store = LogStore::new(backend());
        store.blocking_append([blank_ent(1, 1)]).await.unwrap();

        let purge_up_to = openraft::LogId::new(openraft::CommittedLeaderId::new(1, 0), 1);
        store.purge(purge_up_to).await.unwrap();

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 1);
        assert_eq!(state.last_log_id.unwrap().index, 1);
    }

    #[tokio::test]
    async fn empty_append_is_a_noop() {
        let mut store = LogStore::new(backend());
        let empty: Vec<typ::Entry> = Vec::new();
        store.blocking_append(empty).await.unwrap();
        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());
    }

    #[tokio::test]
    async fn append_preserves_payload_shape() {
        let mut store = LogStore::new(backend());
        let mut e = blank_ent(1, 1);
        e.payload = EntryPayload::Normal(crate::AresaCommand::Noop);
        store.blocking_append([e]).await.unwrap();

        let entries = store.try_get_log_entries(1..=1).await.unwrap();
        assert!(matches!(
            entries[0].payload,
            EntryPayload::Normal(crate::AresaCommand::Noop)
        ));
    }

    #[tokio::test]
    async fn reader_sees_appends_through_shared_backend() {
        // `get_log_reader` clones the store — both views must see the
        // same data because they share the same backend.
        let mut store = LogStore::new(backend());
        store.blocking_append([blank_ent(1, 1)]).await.unwrap();
        let mut reader = store.get_log_reader().await;
        let got = reader.try_get_log_entries(1..=1).await.unwrap();
        assert_eq!(got.len(), 1);
    }

    /// Proves the generic parameterization by standing the same log
    /// store up over two distinct type configs and showing that each
    /// round-trips entries of its own payload shape. A regression
    /// where the generic was accidentally monomorphized to
    /// `TypeConfig` would surface here as a compile error.
    #[tokio::test]
    async fn generic_log_store_round_trips_on_custom_type_config() {
        let mut store: LogStoreGeneric<custom_cfg::DummyConfig> = LogStoreGeneric::new(backend());
        let entry = testing::blank_ent::<custom_cfg::DummyConfig>(1, 0, 1);
        store.blocking_append([entry]).await.unwrap();

        let got = store.try_get_log_entries(1..=1).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].log_id.index, 1);
    }

    /// `declare_raft_types!` emits a `pub` type that bounds the
    /// payload types as `pub`, so the helper sits in its own module
    /// where everything can be exported without leaking out of the
    /// test-only module gate.
    mod custom_cfg {
        use std::io::Cursor;

        use openraft::{BasicNode, TokioRuntime};
        use serde::{Deserialize, Serialize};

        #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
        pub struct Dummy;

        openraft::declare_raft_types!(
            pub DummyConfig:
                D            = Dummy,
                R            = Dummy,
                NodeId       = u64,
                Node         = BasicNode,
                Entry        = openraft::Entry<DummyConfig>,
                SnapshotData = Cursor<Vec<u8>>,
                AsyncRuntime = TokioRuntime,
        );
    }
}

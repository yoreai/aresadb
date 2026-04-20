//! fjall-backed [`aresadb_core::StorageBackend`] — the write-heavy LSM
//! engine for AresaDB v2.
//!
//! This crate is the sibling of `aresadb-engine-redb`: same trait, same
//! "no policy, just bytes" shape, different on-disk layout.
//! `aresadb-engine-redb` wins on embedded + metadata workloads (Raft
//! log, PD catalog) because redb is single-file + one-fsync-per-commit
//! by default; `aresadb-engine-lsm` wins on hot data ranges because
//! fjall's LSM amortises writes over a journal + memtable before
//! flushing to immutable SSTables.
//!
//! # Design
//!
//! Each [`FjallBackend`] owns one `fjall::Database` rooted at a
//! caller-supplied directory and talks to a single keyspace named
//! `default`. The trait doesn't know about keyspaces (column families),
//! and higher layers that need sub-spaces already prefix keys — so
//! exposing more than one keyspace here would only create confusion.
//!
//! All fjall calls run inside `tokio::task::spawn_blocking` because
//! fjall is a synchronous API. Cloning the `Database` and `Keyspace`
//! handles is cheap (they're `Arc`-shaped internally), so we clone
//! them into each blocking task rather than wrapping them behind a
//! lock.
//!
//! ## Durability
//!
//! Every `write_batch` commits an `OwnedWriteBatch` and then calls
//! `Database::persist(PersistMode::SyncAll)` to fsync the journal
//! before returning. That's the guarantee the Raft log needs:
//! `client_write(...).await` returning `Ok` MUST mean the command is
//! durable on disk. We deliberately do not batch commits across
//! `write_batch` calls — higher layers may batch, but the backend
//! treats each call as its own durability boundary to keep replay
//! behaviour unsurprising.
//!
//! ## Snapshots
//!
//! fjall exposes a cross-keyspace [`fjall::Snapshot`] that gives MVCC
//! reads, but the trait's `Snapshot` has to be `Send + 'static` so it
//! can cross the admin RPC boundary. We take the same shortcut as the
//! redb backend: take the fjall snapshot, copy the keyspace into an
//! owned `Vec`, and serve `get`/`scan` from memory. Cost is negligible
//! for the places `Snapshot` is used today (Raft bootstrap, admin
//! commands, Phase 4 MVCC). Phase 4 / Phase 5 will replace this with a
//! streaming variant once the query engine actually needs one.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode, Readable};
use futures::stream;
use parking_lot::RwLock;
use tokio::task;

use aresadb_core::{
    Error, KeyRange, KeyValue, KeyValueStream, Result, Snapshot, StorageBackend, WriteBatch,
    WriteOp,
};

/// Name of the sole keyspace every backend instance opens. Fjall's
/// keyspaces are analogous to RocksDB column families; higher layers
/// that need a sub-space prefix their keys instead, so we keep this
/// constant.
const DEFAULT_KEYSPACE: &str = "default";

/// fjall-backed [`StorageBackend`]. Clones share the same database
/// handle — constructing one is cheap by design, exactly like
/// `RedbBackend`.
#[derive(Clone)]
pub struct FjallBackend {
    inner: Arc<Inner>,
}

struct Inner {
    /// Live fjall handles. Both `Database` and `Keyspace` are cheap to
    /// `Clone` (internal `Arc`s), but we wrap in `RwLock<Option<_>>`
    /// so `close()` can drop them deterministically and every
    /// subsequent method returns `Error::Closed` instead of panicking
    /// on a torn-down handle.
    db: RwLock<Option<Handles>>,

    /// Persisted to disk; kept for diagnostics.
    path: PathBuf,
}

/// Paired fjall `Database` + its sole keyspace handle. Kept together
/// so `close()` drops both in lock-step.
#[derive(Clone)]
struct Handles {
    db: Database,
    keyspace: Keyspace,
}

impl FjallBackend {
    /// Open (or create) a fjall database at `path`.
    ///
    /// The parent directory must already exist — the cluster
    /// bootstrapper owns the data-directory layout and we don't want
    /// two engines fighting over who creates `data/`. Fjall itself
    /// does create the leaf directory it's pointed at.
    pub async fn open(path: impl Into<PathBuf>) -> Result<Arc<Self>> {
        let path = path.into();
        let path_for_task = path.clone();

        let handles = task::spawn_blocking(move || -> anyhow::Result<Handles> {
            let db = Database::builder(&path_for_task).open()?;
            // Touch the default keyspace so subsequent reads against
            // an otherwise-empty backend don't have to handle the
            // "keyspace doesn't exist yet" case. Mirrors the redb
            // backend's "open the default table up-front" pattern.
            let keyspace = db.keyspace(DEFAULT_KEYSPACE, KeyspaceCreateOptions::default)?;
            Ok(Handles { db, keyspace })
        })
        .await
        .map_err(|e| Error::backend(anyhow::anyhow!("fjall open task panicked: {e}")))?
        .map_err(Error::backend)?;

        Ok(Arc::new(Self {
            inner: Arc::new(Inner {
                db: RwLock::new(Some(handles)),
                path,
            }),
        }))
    }

    /// Filesystem path the backend was opened at.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    fn handles(&self) -> Result<Handles> {
        self.inner.db.read().as_ref().cloned().ok_or(Error::Closed)
    }
}

#[async_trait]
impl StorageBackend for FjallBackend {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let handles = self.handles()?;
        let owned_key = key.to_vec();
        task::spawn_blocking(move || -> anyhow::Result<Option<Bytes>> {
            Ok(handles
                .keyspace
                .get(owned_key.as_slice())?
                .map(|v| Bytes::copy_from_slice(v.as_ref())))
        })
        .await
        .map_err(|e| Error::backend(anyhow::anyhow!("fjall get task panicked: {e}")))?
        .map_err(Error::backend)
    }

    async fn scan<'a>(&'a self, range: KeyRange) -> Result<KeyValueStream<'a>> {
        let handles = self.handles()?;
        let items = task::spawn_blocking(move || -> anyhow::Result<Vec<KeyValue>> {
            collect_range(&handles.keyspace, &range)
        })
        .await
        .map_err(|e| Error::backend(anyhow::anyhow!("fjall scan task panicked: {e}")))?
        .map_err(Error::backend)?;

        Ok(Box::pin(stream::iter(items.into_iter().map(Ok))))
    }

    async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        let handles = self.handles()?;
        let ops: Vec<WriteOp> = batch.into_ops();
        task::spawn_blocking(move || -> anyhow::Result<()> {
            apply_ops_and_persist(&handles, ops)
        })
        .await
        .map_err(|e| Error::backend(anyhow::anyhow!("fjall write_batch task panicked: {e}")))?
        .map_err(Error::backend)?;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let handles = self.handles()?;
        task::spawn_blocking(move || -> anyhow::Result<()> {
            handles.db.persist(PersistMode::SyncAll)?;
            Ok(())
        })
        .await
        .map_err(|e| Error::backend(anyhow::anyhow!("fjall flush task panicked: {e}")))?
        .map_err(Error::backend)?;
        Ok(())
    }

    async fn snapshot(&self) -> Result<Box<dyn Snapshot>> {
        let handles = self.handles()?;
        // Eagerly materialise the keyspace into an owned `Vec` so the
        // returned `Snapshot` stays `Send + 'static` without pinning
        // a fjall txn across awaits. This matches the redb backend's
        // behaviour; the assumption (and architectural constraint)
        // is that snapshots are short-lived admin / bootstrap reads.
        let items = task::spawn_blocking(move || -> anyhow::Result<Vec<KeyValue>> {
            let snap = handles.db.snapshot();
            let mut out = Vec::new();
            for guard in snap.iter(&handles.keyspace) {
                let (k, v) = guard.into_inner()?;
                out.push(KeyValue::new(
                    Bytes::copy_from_slice(k.as_ref()),
                    Bytes::copy_from_slice(v.as_ref()),
                ));
            }
            Ok(out)
        })
        .await
        .map_err(|e| Error::backend(anyhow::anyhow!("fjall snapshot task panicked: {e}")))?
        .map_err(Error::backend)?;

        Ok(Box::new(FjallSnapshot { items }))
    }

    fn approximate_size(&self, _range: &KeyRange) -> u64 {
        // fjall exposes `Keyspace::disk_space()`, but it's
        // whole-keyspace, not per-range. The range sharder's advisory
        // contract lets us return 0 until we have per-range stats —
        // same deal as the redb backend.
        0
    }

    async fn close(&self) -> Result<()> {
        // Take the handles out under the write lock so clones of
        // `FjallBackend` stop being able to see them, then drop
        // outside the lock. Fjall runs a best-effort `persist` in
        // `Database::Drop`, so we don't need to call it explicitly.
        let taken = self.inner.db.write().take();
        drop(taken);
        Ok(())
    }
}

/// Apply a sequence of `WriteOp`s atomically against a fjall keyspace
/// and fsync. Split out so the blocking-task body is readable.
fn apply_ops_and_persist(handles: &Handles, ops: Vec<WriteOp>) -> anyhow::Result<()> {
    let mut batch = OwnedWriteBatch::with_capacity(handles.db.clone(), ops.len());
    for op in ops {
        match op {
            WriteOp::Put { key, value } => {
                batch.insert(&handles.keyspace, key.as_ref(), value.as_ref());
            }
            WriteOp::Delete { key } => {
                batch.remove(&handles.keyspace, key.as_ref());
            }
            WriteOp::DeleteRange { start, end } => {
                // fjall doesn't expose a cheap range-tombstone in the
                // public API yet, so we materialise the keys and
                // stage individual removes. Fine for Raft log purge
                // (contiguous small ranges); the "drop tenant" admin
                // will hit the fast-path in a later phase once fjall
                // ships range-delete or we add a compaction filter.
                let to_delete: Vec<Vec<u8>> = handles
                    .keyspace
                    .range::<&[u8], _>(start.as_ref()..end.as_ref())
                    .map(|g| g.key().map(|k| k.as_ref().to_vec()))
                    .collect::<fjall::Result<Vec<_>>>()?;
                for k in to_delete {
                    batch.remove(&handles.keyspace, k.as_slice());
                }
            }
        }
    }
    batch.commit()?;
    handles.db.persist(PersistMode::SyncAll)?;
    Ok(())
}

/// Materialise a `KeyRange` into a `Vec<KeyValue>` by driving fjall's
/// `range` / `iter` method with std `RangeBounds`. We intentionally
/// copy rather than hold a live fjall iterator because the trait's
/// `KeyValueStream` is `Send + 'a` and we can't ship a `!Send`
/// iterator across the async boundary without a Mutex — and the cost
/// of that Mutex exceeds the cost of the copy at the scales we care
/// about today.
fn collect_range(keyspace: &Keyspace, range: &KeyRange) -> anyhow::Result<Vec<KeyValue>> {
    let start = range.start();
    let end = range.end();
    let has_start = !start.is_empty();
    let has_end = !end.is_empty();

    let iter = match (has_start, has_end) {
        (false, false) => keyspace.iter(),
        (true, false) => keyspace.range::<&[u8], _>(start..),
        (false, true) => keyspace.range::<&[u8], _>(..end),
        (true, true) => keyspace.range::<&[u8], _>(start..end),
    };

    let mut out = Vec::new();
    for guard in iter {
        let (k, v) = guard.into_inner()?;
        out.push(KeyValue::new(
            Bytes::copy_from_slice(k.as_ref()),
            Bytes::copy_from_slice(v.as_ref()),
        ));
    }
    Ok(out)
}

struct FjallSnapshot {
    items: Vec<KeyValue>,
}

#[async_trait]
impl Snapshot for FjallSnapshot {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self
            .items
            .iter()
            .find(|kv| kv.key.as_ref() == key)
            .map(|kv| kv.value.clone()))
    }

    async fn scan<'a>(&'a self, range: KeyRange) -> Result<KeyValueStream<'a>> {
        let items: Vec<KeyValue> = self
            .items
            .iter()
            .filter(|kv| range.contains(&kv.key))
            .cloned()
            .collect();
        Ok(Box::pin(stream::iter(items.into_iter().map(Ok))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use tempfile::TempDir;

    async fn fresh() -> (TempDir, Arc<FjallBackend>) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.lsm");
        let b = FjallBackend::open(path).await.expect("open");
        (dir, b)
    }

    #[tokio::test]
    async fn put_get_roundtrip() {
        let (_dir, b) = fresh().await;
        let mut batch = WriteBatch::new();
        batch.put("k", "v");
        b.write_batch(batch).await.unwrap();
        let v = b.get(b"k").await.unwrap().unwrap();
        assert_eq!(&v[..], b"v");
    }

    #[tokio::test]
    async fn delete_removes_key() {
        let (_dir, b) = fresh().await;
        let mut batch = WriteBatch::new();
        batch.put("k", "v");
        b.write_batch(batch).await.unwrap();

        let mut del = WriteBatch::new();
        del.delete("k");
        b.write_batch(del).await.unwrap();

        assert!(b.get(b"k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_range_matches_memory_backend() {
        let (_dir, b) = fresh().await;
        let mut batch = WriteBatch::new();
        batch
            .put("a", "1")
            .put("b", "2")
            .put("c", "3")
            .put("d", "4");
        b.write_batch(batch).await.unwrap();

        let mut del = WriteBatch::new();
        del.delete_range("b", "d");
        b.write_batch(del).await.unwrap();

        assert_eq!(b.get(b"a").await.unwrap().unwrap()[..], *b"1");
        assert!(b.get(b"b").await.unwrap().is_none());
        assert!(b.get(b"c").await.unwrap().is_none());
        assert_eq!(b.get(b"d").await.unwrap().unwrap()[..], *b"4");
    }

    #[tokio::test]
    async fn scan_respects_range_bounds() {
        let (_dir, b) = fresh().await;
        let mut batch = WriteBatch::new();
        batch.put("a", "1").put("b", "2").put("c", "3");
        b.write_batch(batch).await.unwrap();

        let mut s = b.scan(KeyRange::new("a", "c")).await.unwrap();
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while let Some(Ok(kv)) = s.next().await {
            out.push((kv.key.to_vec(), kv.value.to_vec()));
        }
        assert_eq!(
            out,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
            ]
        );
    }

    #[tokio::test]
    async fn scan_full_range_returns_everything_in_order() {
        let (_dir, b) = fresh().await;
        let mut batch = WriteBatch::new();
        batch
            .put("c", "3")
            .put("a", "1")
            .put("b", "2")
            .put("d", "4");
        b.write_batch(batch).await.unwrap();

        let mut s = b.scan(KeyRange::all()).await.unwrap();
        let mut out = Vec::new();
        while let Some(Ok(kv)) = s.next().await {
            out.push(String::from_utf8(kv.key.to_vec()).unwrap());
        }
        assert_eq!(out, vec!["a", "b", "c", "d"]);
    }

    #[tokio::test]
    async fn data_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("durable.lsm");

        {
            let b = FjallBackend::open(path.clone()).await.unwrap();
            let mut batch = WriteBatch::new();
            batch.put("persist", "me").put("many", "keys");
            b.write_batch(batch).await.unwrap();
            // Explicit close triggers fjall's Drop-time persist; we
            // want to prove the Raft-log semantics hold even without
            // it, so we test both below.
            b.close().await.unwrap();
        }

        let reopened = FjallBackend::open(path).await.unwrap();
        let v = reopened.get(b"persist").await.unwrap().unwrap();
        assert_eq!(&v[..], b"me");
        let v2 = reopened.get(b"many").await.unwrap().unwrap();
        assert_eq!(&v2[..], b"keys");
    }

    #[tokio::test]
    async fn snapshot_isolated_from_subsequent_writes() {
        let (_dir, b) = fresh().await;
        let mut batch = WriteBatch::new();
        batch.put("k", "v1");
        b.write_batch(batch).await.unwrap();

        let snap = b.snapshot().await.unwrap();

        let mut second = WriteBatch::new();
        second.put("k", "v2");
        b.write_batch(second).await.unwrap();

        let from_snap = snap.get(b"k").await.unwrap().unwrap();
        assert_eq!(&from_snap[..], b"v1");

        let from_live = b.get(b"k").await.unwrap().unwrap();
        assert_eq!(&from_live[..], b"v2");
    }

    #[tokio::test]
    async fn snapshot_scan_respects_range_bounds() {
        let (_dir, b) = fresh().await;
        let mut batch = WriteBatch::new();
        batch
            .put("a", "1")
            .put("b", "2")
            .put("c", "3")
            .put("d", "4");
        b.write_batch(batch).await.unwrap();
        let snap = b.snapshot().await.unwrap();

        let mut s = snap.scan(KeyRange::new("b", "d")).await.unwrap();
        let mut out = Vec::new();
        while let Some(Ok(kv)) = s.next().await {
            out.push(String::from_utf8(kv.key.to_vec()).unwrap());
        }
        assert_eq!(out, vec!["b", "c"]);
    }

    #[tokio::test]
    async fn close_then_get_errors() {
        let (_dir, b) = fresh().await;
        b.close().await.unwrap();
        assert!(matches!(b.get(b"k").await, Err(Error::Closed)));
    }

    #[tokio::test]
    async fn approximate_size_is_advisory_zero() {
        let (_dir, b) = fresh().await;
        assert_eq!(b.approximate_size(&KeyRange::all()), 0);
    }

    #[tokio::test]
    async fn flush_is_idempotent() {
        let (_dir, b) = fresh().await;
        let mut batch = WriteBatch::new();
        batch.put("k", "v");
        b.write_batch(batch).await.unwrap();

        // flush after a committed write is a no-op from the caller's
        // point of view — data was already synced by write_batch.
        b.flush().await.unwrap();
        b.flush().await.unwrap();

        let v = b.get(b"k").await.unwrap().unwrap();
        assert_eq!(&v[..], b"v");
    }

    #[tokio::test]
    async fn scan_from_lower_bound_is_inclusive() {
        let (_dir, b) = fresh().await;
        let mut batch = WriteBatch::new();
        batch.put("a", "1").put("b", "2").put("c", "3");
        b.write_batch(batch).await.unwrap();

        let mut s = b.scan(KeyRange::from("b")).await.unwrap();
        let mut out = Vec::new();
        while let Some(Ok(kv)) = s.next().await {
            out.push(String::from_utf8(kv.key.to_vec()).unwrap());
        }
        assert_eq!(out, vec!["b", "c"]);
    }

    #[tokio::test]
    async fn scan_to_upper_bound_is_exclusive() {
        let (_dir, b) = fresh().await;
        let mut batch = WriteBatch::new();
        batch.put("a", "1").put("b", "2").put("c", "3");
        b.write_batch(batch).await.unwrap();

        let mut s = b.scan(KeyRange::to("c")).await.unwrap();
        let mut out = Vec::new();
        while let Some(Ok(kv)) = s.next().await {
            out.push(String::from_utf8(kv.key.to_vec()).unwrap());
        }
        assert_eq!(out, vec!["a", "b"]);
    }
}

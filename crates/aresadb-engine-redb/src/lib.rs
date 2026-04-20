//! redb-backed [`aresadb_core::StorageBackend`] — the default durable
//! engine for AresaDB v2.
//!
//! This crate intentionally carries no policy: it's a raw key/value
//! store and nothing else. Higher layers (`aresadb-raft`, the range
//! sharder) prefix their keys to carve out sub-spaces, and the engine
//! stays blissfully ignorant of what any given byte-sequence means.
//!
//! # Design
//!
//! Each `RedbBackend` owns one `redb::Database` file. redb is a B-tree
//! on a mapped file with single-writer/many-reader semantics, so the
//! backend funnels writes through an in-process lock and lets reads
//! parallelise naturally. We run redb operations inside
//! `tokio::task::spawn_blocking` so a stall on fsync or compaction
//! doesn't block the whole async runtime.
//!
//! ## Durability
//!
//! Every `write_batch` runs inside a redb write transaction and
//! commits before returning `Ok(())`. redb's `commit()` fsyncs by
//! default, which is exactly what the Raft log needs — one fsync per
//! log append is what makes the protocol safe after a crash. We
//! deliberately do **not** batch commits across calls; that would
//! hide correctness bugs during replay. A separate group-commit layer
//! (Phase 2) will sit *above* the backend rather than inside it.
//!
//! ## Snapshots
//!
//! `redb::ReadTransaction` gives us free MVCC read snapshots, so the
//! [`Snapshot`] impl just holds a `ReadTransaction` handle open. Cost
//! is negligible unless the snapshot lives for a long time while heavy
//! writes stream in — which is exactly what the architecture doc
//! warned about, so higher layers must close snapshots promptly.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use parking_lot::RwLock;
use redb::{Database, ReadableTable, TableDefinition};
use tokio::task;

use aresadb_core::{
    Error, KeyRange, KeyValue, KeyValueStream, Result, Snapshot, StorageBackend, WriteBatch,
    WriteOp,
};

/// Name of the sole table every backend instance uses. Keeping it
/// fixed keeps the public API tiny; if callers ever need to multiplex
/// logical key-spaces they should prefix keys (the Raft log does
/// exactly this).
const DEFAULT_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("default");

/// redb-backed [`StorageBackend`]. Clones share the same database
/// handle — constructing one is cheap by design.
#[derive(Clone)]
pub struct RedbBackend {
    inner: Arc<Inner>,
}

struct Inner {
    /// redb handle. Wrapped in `RwLock` only so we can swap it to
    /// `None` on `close()` without leaking the file descriptor until
    /// process exit. Access to the live handle holds a read lock.
    db: RwLock<Option<Arc<Database>>>,

    /// Persisted to disk; kept for diagnostics and for opening a
    /// fresh handle if we ever add hot-reload.
    path: PathBuf,
}

impl RedbBackend {
    /// Open (or create) a redb database at `path`.
    ///
    /// The parent directory must already exist — we don't silently
    /// create directory structure because the cluster bootstrapper
    /// is the piece that owns the data directory layout.
    pub async fn open(path: impl Into<PathBuf>) -> Result<Arc<Self>> {
        let path = path.into();
        let path_for_task = path.clone();

        let db = task::spawn_blocking(move || -> anyhow::Result<Database> {
            let db = Database::create(&path_for_task)?;
            // Touch the default table so subsequent reads against an
            // empty database don't have to handle the "no table yet"
            // case.
            let tx = db.begin_write()?;
            {
                let _ = tx.open_table(DEFAULT_TABLE)?;
            }
            tx.commit()?;
            Ok(db)
        })
        .await
        .map_err(|e| Error::backend(anyhow::anyhow!("redb open task panicked: {e}")))?
        .map_err(Error::backend)?;

        Ok(Arc::new(Self {
            inner: Arc::new(Inner {
                db: RwLock::new(Some(Arc::new(db))),
                path,
            }),
        }))
    }

    /// Filesystem path the backend was opened at.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    fn handle(&self) -> Result<Arc<Database>> {
        self.inner.db.read().as_ref().cloned().ok_or(Error::Closed)
    }
}

#[async_trait]
impl StorageBackend for RedbBackend {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let db = self.handle()?;
        let owned_key = key.to_vec();
        task::spawn_blocking(move || -> anyhow::Result<Option<Bytes>> {
            let tx = db.begin_read()?;
            let table = tx.open_table(DEFAULT_TABLE)?;
            Ok(table
                .get(owned_key.as_slice())?
                .map(|g| Bytes::copy_from_slice(g.value())))
        })
        .await
        .map_err(|e| Error::backend(anyhow::anyhow!("redb get task panicked: {e}")))?
        .map_err(Error::backend)
    }

    async fn scan<'a>(&'a self, range: KeyRange) -> Result<KeyValueStream<'a>> {
        let db = self.handle()?;
        // Materialize the range into memory. This is on par with the
        // reference MemoryBackend and avoids the lifetime problem of
        // yielding redb iterators through an async stream. Phase 2
        // will promote scan to a proper streaming iterator once the
        // query engine actually needs it.
        let items = task::spawn_blocking(move || -> anyhow::Result<Vec<KeyValue>> {
            let tx = db.begin_read()?;
            let table = tx.open_table(DEFAULT_TABLE)?;

            let bounds = range_bounds(&range);
            let iter = match bounds {
                RangeBounds::Full => table.range::<&[u8]>(..)?,
                RangeBounds::From(ref s) => table.range::<&[u8]>(s.as_slice()..)?,
                RangeBounds::Until(ref e) => table.range::<&[u8]>(..e.as_slice())?,
                RangeBounds::Between(ref s, ref e) => {
                    table.range::<&[u8]>(s.as_slice()..e.as_slice())?
                }
            };

            let mut out = Vec::new();
            for pair in iter {
                let (k, v) = pair?;
                out.push(KeyValue::new(
                    Bytes::copy_from_slice(k.value()),
                    Bytes::copy_from_slice(v.value()),
                ));
            }
            Ok(out)
        })
        .await
        .map_err(|e| Error::backend(anyhow::anyhow!("redb scan task panicked: {e}")))?
        .map_err(Error::backend)?;

        Ok(Box::pin(stream::iter(items.into_iter().map(Ok))))
    }

    async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        let db = self.handle()?;
        let ops: Vec<WriteOp> = batch.into_ops();
        task::spawn_blocking(move || -> anyhow::Result<()> {
            let tx = db.begin_write()?;
            {
                let mut table = tx.open_table(DEFAULT_TABLE)?;
                for op in ops {
                    match op {
                        WriteOp::Put { key, value } => {
                            table.insert(key.as_ref(), value.as_ref())?;
                        }
                        WriteOp::Delete { key } => {
                            let _ = table.remove(key.as_ref())?;
                        }
                        WriteOp::DeleteRange { start, end } => {
                            // redb 2.x doesn't expose a cheap
                            // range-delete; we collect and remove
                            // instead. Cheap enough at the scale
                            // Raft log purges work at, and the
                            // state machine seldom uses it.
                            let to_delete: Vec<Vec<u8>> = {
                                let iter = table.range::<&[u8]>(start.as_ref()..end.as_ref())?;
                                iter.map(|r| r.map(|(k, _)| k.value().to_vec()))
                                    .collect::<std::result::Result<_, _>>()?
                            };
                            for k in to_delete {
                                table.remove(k.as_slice())?;
                            }
                        }
                    }
                }
            }
            tx.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| Error::backend(anyhow::anyhow!("redb write_batch task panicked: {e}")))?
        .map_err(Error::backend)?;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        // Every `write_batch` already commits, which in redb means
        // fsync. `flush` is therefore a no-op — kept only to honour
        // the trait.
        self.handle()?;
        Ok(())
    }

    async fn snapshot(&self) -> Result<Box<dyn Snapshot>> {
        let db = self.handle()?;
        // redb read-transactions are MVCC snapshots. We copy the data
        // out eagerly instead of holding the txn live across awaits
        // so the `Snapshot` trait stays `Send + 'static` without
        // lifetime gymnastics. Copying the whole keyspace is only
        // reasonable because snapshots are used for Raft bootstrap
        // and admin commands today — hot path is `get`/`scan` against
        // the live backend.
        let items = task::spawn_blocking(move || -> anyhow::Result<Vec<KeyValue>> {
            let tx = db.begin_read()?;
            let table = tx.open_table(DEFAULT_TABLE)?;
            let mut out = Vec::new();
            for pair in table.range::<&[u8]>(..)? {
                let (k, v) = pair?;
                out.push(KeyValue::new(
                    Bytes::copy_from_slice(k.value()),
                    Bytes::copy_from_slice(v.value()),
                ));
            }
            Ok(out)
        })
        .await
        .map_err(|e| Error::backend(anyhow::anyhow!("redb snapshot task panicked: {e}")))?
        .map_err(Error::backend)?;

        Ok(Box::new(RedbSnapshot { items }))
    }

    fn approximate_size(&self, _range: &KeyRange) -> u64 {
        // redb doesn't give us a cheap per-range size estimate in the
        // public API. Returning 0 is safe here — the caller (the
        // range sharder in Phase 2) treats this as an advisory hint
        // and has a fallback path that falls back to exact scans
        // when the estimate is missing. We'll replace this with a
        // rolling size cache once the sharder actually lands.
        0
    }

    async fn close(&self) -> Result<()> {
        // Drop the live database handle so the file descriptor can
        // be reclaimed even if some clone of the backend sticks
        // around.
        let taken = self.inner.db.write().take();
        drop(taken);
        Ok(())
    }
}

enum RangeBounds {
    Full,
    From(Vec<u8>),
    Until(Vec<u8>),
    Between(Vec<u8>, Vec<u8>),
}

fn range_bounds(range: &KeyRange) -> RangeBounds {
    // `KeyRange` uses empty-byte sentinels for "open":
    //   start == b""  ⇒  no lower bound
    //   end   == b""  ⇒  no upper bound
    // Translate those into redb-friendly slice ranges.
    let start = range.start();
    let end = range.end();
    let has_start = !start.is_empty();
    let has_end = !end.is_empty();

    match (has_start, has_end) {
        (false, false) => RangeBounds::Full,
        (true, false) => RangeBounds::From(start.to_vec()),
        (false, true) => RangeBounds::Until(end.to_vec()),
        (true, true) => RangeBounds::Between(start.to_vec(), end.to_vec()),
    }
}

struct RedbSnapshot {
    items: Vec<KeyValue>,
}

#[async_trait]
impl Snapshot for RedbSnapshot {
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

    async fn fresh() -> (TempDir, Arc<RedbBackend>) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.redb");
        let b = RedbBackend::open(path).await.expect("open");
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
    async fn data_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("durable.redb");

        {
            let b = RedbBackend::open(path.clone()).await.unwrap();
            let mut batch = WriteBatch::new();
            batch.put("persist", "me");
            b.write_batch(batch).await.unwrap();
            b.close().await.unwrap();
        }

        let reopened = RedbBackend::open(path).await.unwrap();
        let v = reopened.get(b"persist").await.unwrap().unwrap();
        assert_eq!(&v[..], b"me");
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
    async fn close_then_get_errors() {
        let (_dir, b) = fresh().await;
        b.close().await.unwrap();
        assert!(matches!(b.get(b"k").await, Err(Error::Closed)));
    }
}

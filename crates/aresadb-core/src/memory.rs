//! Reference in-memory [`StorageBackend`] implementation.
//!
//! This backend exists for three reasons:
//!
//! 1. It's a small, easy-to-audit reference implementation of the
//!    trait's semantics — other engines can point at it as the "what
//!    should this do?" source of truth.
//! 2. Integration tests that don't care about durability (Raft state
//!    machine tests, query-engine tests against a single backend) use
//!    it to run fast without touching disk.
//! 3. Deterministic simulation (`aresadb-sim`) runs cluster scenarios
//!    against it because there's no non-determinism in an in-memory
//!    `BTreeMap`.
//!
//! It is not intended to be used in production.

use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use parking_lot::RwLock;

use crate::{
    Error, KeyRange, KeyValue, KeyValueStream, Result, Snapshot, StorageBackend, WriteBatch,
    WriteOp,
};

/// Shared inner state so the backend can be cloned cheaply while every
/// clone sees the same data.
#[derive(Default)]
struct Inner {
    data: RwLock<BTreeMap<Bytes, Bytes>>,
    closed: AtomicBool,
}

/// In-memory [`StorageBackend`]. Cheap to clone; all clones share state.
#[derive(Clone, Default)]
pub struct MemoryBackend {
    inner: Arc<Inner>,
}

impl MemoryBackend {
    /// Fresh empty backend.
    pub fn new() -> Self {
        Self::default()
    }

    fn ensure_open(&self) -> Result<()> {
        if self.inner.closed.load(Ordering::Acquire) {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }

    fn collect_range(&self, range: &KeyRange) -> Vec<KeyValue> {
        let data = self.inner.data.read();
        data.iter()
            .filter(|(k, _)| range.contains(k))
            .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
            .collect()
    }
}

#[async_trait]
impl StorageBackend for MemoryBackend {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.ensure_open()?;
        Ok(self.inner.data.read().get(key).cloned())
    }

    async fn scan<'a>(&'a self, range: KeyRange) -> Result<KeyValueStream<'a>> {
        self.ensure_open()?;
        let items = self.collect_range(&range);
        Ok(Box::pin(stream::iter(items.into_iter().map(Ok))))
    }

    async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.ensure_open()?;
        let mut data = self.inner.data.write();
        for op in batch.into_ops() {
            match op {
                WriteOp::Put { key, value } => {
                    data.insert(key, value);
                }
                WriteOp::Delete { key } => {
                    data.remove(&key);
                }
                WriteOp::DeleteRange { start, end } => {
                    let keys: Vec<Bytes> = data
                        .range(start.clone()..end.clone())
                        .map(|(k, _)| k.clone())
                        .collect();
                    for k in keys {
                        data.remove(&k);
                    }
                }
            }
        }
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        self.ensure_open()?;
        Ok(())
    }

    async fn snapshot(&self) -> Result<Box<dyn Snapshot>> {
        self.ensure_open()?;
        let data = self.inner.data.read().clone();
        Ok(Box::new(MemorySnapshot { data }))
    }

    fn approximate_size(&self, range: &KeyRange) -> u64 {
        let data = self.inner.data.read();
        data.iter()
            .filter(|(k, _)| range.contains(k))
            .map(|(k, v)| (k.len() + v.len()) as u64)
            .sum()
    }

    async fn close(&self) -> Result<()> {
        self.inner.closed.store(true, Ordering::Release);
        Ok(())
    }
}

struct MemorySnapshot {
    data: BTreeMap<Bytes, Bytes>,
}

#[async_trait]
impl Snapshot for MemorySnapshot {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.data.get(key).cloned())
    }

    async fn scan<'a>(&'a self, range: KeyRange) -> Result<KeyValueStream<'a>> {
        let items: Vec<KeyValue> = self
            .data
            .iter()
            .filter(|(k, _)| range.contains(k))
            .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
            .collect();
        Ok(Box::pin(stream::iter(items.into_iter().map(Ok))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn put_get_roundtrip() {
        let b = MemoryBackend::new();
        let mut batch = WriteBatch::new();
        batch.put("k", "v");
        b.write_batch(batch).await.unwrap();
        let v = b.get(b"k").await.unwrap().unwrap();
        assert_eq!(&v[..], b"v");
    }

    #[tokio::test]
    async fn delete_range_removes_keys() {
        let b = MemoryBackend::new();
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
    async fn scan_respects_range() {
        let b = MemoryBackend::new();
        let mut batch = WriteBatch::new();
        batch.put("a", "1").put("b", "2").put("c", "3");
        b.write_batch(batch).await.unwrap();

        let mut stream = b.scan(KeyRange::new("a", "c")).await.unwrap();
        let mut collected: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while let Some(Ok(kv)) = stream.next().await {
            collected.push((kv.key.to_vec(), kv.value.to_vec()));
        }
        assert_eq!(
            collected,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec())
            ]
        );
    }

    #[tokio::test]
    async fn snapshot_is_isolated_from_subsequent_writes() {
        let b = MemoryBackend::new();
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
        let b = MemoryBackend::new();
        b.close().await.unwrap();
        assert!(matches!(b.get(b"k").await, Err(Error::Closed)));
    }

    #[tokio::test]
    async fn approximate_size_counts_key_and_value_bytes() {
        let b = MemoryBackend::new();
        let mut batch = WriteBatch::new();
        batch.put("aa", "bbb").put("cc", "d");
        b.write_batch(batch).await.unwrap();
        let size = b.approximate_size(&KeyRange::all());
        assert_eq!(size, 2 + 3 + 2 + 1);
    }
}

//! The [`StorageBackend`] trait — the contract every engine implements.
//!
//! See the crate-level doc for the rationale. The trait is deliberately
//! minimal: anything that higher layers can build on top of these
//! primitives lives there, not here.

use async_trait::async_trait;
use bytes::Bytes;

use crate::{KeyRange, KeyValueStream, Result, Snapshot, WriteBatch};

/// Engine-agnostic contract for a local key-value store.
///
/// Every method is async because backends may do I/O. For in-memory
/// backends the futures resolve immediately.
///
/// A backend must be cheap to clone (typically by holding an `Arc`
/// internally) because the higher layers share a single handle across
/// many tasks.
#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    /// Point lookup. Returns `Ok(None)` if the key is absent.
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;

    /// Ordered scan over `range`.
    ///
    /// Results are yielded in lexicographic key order. The stream is
    /// backed by the backend's own iterators — callers don't need to
    /// hold a lock on the backend while consuming it.
    async fn scan<'a>(&'a self, range: KeyRange) -> Result<KeyValueStream<'a>>;

    /// Apply a batch atomically.
    ///
    /// Either every `WriteOp` becomes visible or none does. The
    /// ordering within the batch matters: later ops shadow earlier
    /// ones on the same key.
    async fn write_batch(&self, batch: WriteBatch) -> Result<()>;

    /// Flush all pending writes to durable storage.
    ///
    /// Backends that already sync each `write_batch` may implement
    /// this as a no-op.
    async fn flush(&self) -> Result<()>;

    /// Create a consistent point-in-time snapshot.
    async fn snapshot(&self) -> Result<Box<dyn Snapshot>>;

    /// Approximate size in bytes of the keys+values in `range`.
    ///
    /// Used by the range sharder to decide when to split/merge.
    /// Backends may return rough estimates — this is intentionally a
    /// synchronous method because callers poll it frequently on the
    /// scheduler tick.
    fn approximate_size(&self, range: &KeyRange) -> u64;

    /// Close the backend. After this returns, every method may return
    /// [`crate::Error::Closed`].
    async fn close(&self) -> Result<()>;
}

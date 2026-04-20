//! Point-in-time read views.
//!
//! A [`Snapshot`] is what `StorageBackend::snapshot` hands back: a
//! logically-frozen view of the keyspace at the moment the snapshot was
//! taken. Subsequent writes by the backend MUST NOT be visible through
//! the snapshot.
//!
//! Snapshots are how Raft's log-shipping and range-split protocols ship
//! data between nodes without blocking user traffic.

use async_trait::async_trait;
use bytes::Bytes;

use crate::{KeyRange, KeyValueStream, Result};

/// A consistent point-in-time read view over a [`crate::StorageBackend`].
#[async_trait]
pub trait Snapshot: Send + Sync {
    /// Point lookup. Returns `None` if `key` was absent at snapshot time.
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;

    /// Ordered scan over `range`, honouring the snapshot's frozen view.
    async fn scan<'a>(&'a self, range: KeyRange) -> Result<KeyValueStream<'a>>;
}

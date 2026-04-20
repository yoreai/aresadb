//! Async streams over key-value pairs.
//!
//! `StorageBackend::scan` returns a boxed [`KeyValueStream`] so callers
//! can consume results lazily without materializing the full range in
//! memory. This is what makes range-shipping between nodes (Raft
//! snapshots, range splits) cheap.

use bytes::Bytes;
use futures::stream::BoxStream;

use crate::Result;

/// A scanned key/value pair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyValue {
    /// Key bytes (lexicographically ordered in the scan output).
    pub key: Bytes,
    /// Value bytes.
    pub value: Bytes,
}

impl KeyValue {
    /// Construct a new pair.
    pub fn new(key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// Boxed async stream of [`KeyValue`] items from a backend scan.
///
/// We box the stream so [`crate::StorageBackend`] methods stay object-safe and
/// callers don't have to name a concrete stream type. The cost (one
/// heap allocation per scan) is fine for our use case; the hot path is
/// the body of the scan, not its construction.
pub type KeyValueStream<'a> = BoxStream<'a, Result<KeyValue>>;

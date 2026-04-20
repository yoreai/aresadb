//! Atomic write batches.
//!
//! A [`WriteBatch`] is the unit of atomic write that a `StorageBackend`
//! accepts. Backends MUST apply the operations in a batch such that
//! either all of them become visible or none of them do.
//!
//! Batches are *ordered*: later operations on the same key shadow
//! earlier ones. That lets higher layers build up a batch of index
//! maintenance plus user writes without having to deduplicate.

use bytes::Bytes;

/// A single element of a [`WriteBatch`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteOp {
    /// Insert or overwrite `(key, value)`.
    Put {
        /// Key to write.
        key: Bytes,
        /// Value to associate with the key.
        value: Bytes,
    },
    /// Remove `key`. No-op if the key is absent.
    Delete {
        /// Key to remove.
        key: Bytes,
    },
    /// Remove every key in `range`. Cheaper than many `Delete`s when the
    /// backend supports it natively.
    DeleteRange {
        /// Start of the range (inclusive).
        start: Bytes,
        /// End of the range (exclusive).
        end: Bytes,
    },
}

/// A batch of [`WriteOp`]s applied atomically.
#[derive(Clone, Debug, Default)]
pub struct WriteBatch {
    ops: Vec<WriteOp>,
}

impl WriteBatch {
    /// Create an empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a batch pre-sized for `n` operations.
    pub fn with_capacity(n: usize) -> Self {
        Self {
            ops: Vec::with_capacity(n),
        }
    }

    /// Append a put.
    pub fn put(&mut self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> &mut Self {
        self.ops.push(WriteOp::Put {
            key: key.into(),
            value: value.into(),
        });
        self
    }

    /// Append a delete.
    pub fn delete(&mut self, key: impl Into<Bytes>) -> &mut Self {
        self.ops.push(WriteOp::Delete { key: key.into() });
        self
    }

    /// Append a range delete.
    pub fn delete_range(&mut self, start: impl Into<Bytes>, end: impl Into<Bytes>) -> &mut Self {
        self.ops.push(WriteOp::DeleteRange {
            start: start.into(),
            end: end.into(),
        });
        self
    }

    /// Append a pre-built [`WriteOp`]. Useful when composing batches
    /// from other batches.
    pub fn push(&mut self, op: WriteOp) -> &mut Self {
        self.ops.push(op);
        self
    }

    /// Number of operations queued.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// True if no operations have been queued.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Consume the batch, returning the ordered operation list.
    pub fn into_ops(self) -> Vec<WriteOp> {
        self.ops
    }

    /// Borrow the ordered operation list.
    pub fn ops(&self) -> &[WriteOp] {
        &self.ops
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_ops_preserved() {
        let mut b = WriteBatch::new();
        b.put("a", "1").put("b", "2").delete("a");

        let ops = b.into_ops();
        assert_eq!(ops.len(), 3);
        assert!(matches!(ops[0], WriteOp::Put { .. }));
        assert!(matches!(ops[2], WriteOp::Delete { .. }));
    }

    #[test]
    fn delete_range_records_bounds() {
        let mut b = WriteBatch::new();
        b.delete_range("a", "z");
        let ops = b.into_ops();
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            WriteOp::DeleteRange { start, end } => {
                assert_eq!(&start[..], b"a");
                assert_eq!(&end[..], b"z");
            }
            _ => panic!("expected DeleteRange"),
        }
    }
}

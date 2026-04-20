//! The replicated command type.
//!
//! Every write that goes through Raft is wrapped in an [`AresaCommand`].
//! The command is the *unit of replication*: the leader serializes it
//! into a Raft log entry, followers persist the entry, and once
//! committed every node applies the command deterministically on its
//! local state machine.
//!
//! In Phase 1 a command is always a [`WriteBatch`]. Phase 2 will extend
//! this to carry cross-shard coordination commands (range split /
//! merge, schema changes); those extensions land as new variants of
//! this enum.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use aresadb_core::{WriteBatch, WriteOp};

/// A command that flows through the Raft log.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AresaCommand {
    /// No-op. Useful for heartbeats and leader-lease testing; applying
    /// it produces an empty response.
    Noop,

    /// Apply this write batch atomically to the state-machine backend.
    WriteBatch(SerializableWriteBatch),
}

impl AresaCommand {
    /// Convenience constructor: wrap a `WriteBatch` into a command.
    pub fn batch(batch: WriteBatch) -> Self {
        Self::WriteBatch(batch.into())
    }

    /// Number of operations inside the command, for observability.
    pub fn ops_count(&self) -> usize {
        match self {
            Self::Noop => 0,
            Self::WriteBatch(b) => b.ops.len(),
        }
    }
}

/// Response returned by the state machine after applying an
/// [`AresaCommand`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AresaResponse {
    /// How many ops the state machine actually executed.
    ///
    /// Zero for `Noop`, the number of inner `WriteOp`s for
    /// `WriteBatch`. Callers don't usually inspect this; it's here so
    /// integration tests can assert exact counts.
    pub ops_applied: u32,
}

/// A `WriteBatch` shaped for `serde`.
///
/// [`aresadb_core::WriteBatch`] stores keys and values as `Bytes`, which
/// deserialize cheaply into `Bytes` only with the `bytes/serde` feature
/// *and* serde-compatible formats. To keep the on-wire format compact
/// and bincode-friendly we translate once here instead of leaking that
/// detail everywhere.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SerializableWriteBatch {
    pub(crate) ops: Vec<SerializableWriteOp>,
}

/// Serialization-friendly mirror of [`aresadb_core::WriteOp`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SerializableWriteOp {
    /// Insert / overwrite a key.
    Put {
        /// Key bytes.
        key: Vec<u8>,
        /// Value bytes.
        value: Vec<u8>,
    },
    /// Delete a single key.
    Delete {
        /// Key bytes.
        key: Vec<u8>,
    },
    /// Delete every key in `[start, end)`.
    DeleteRange {
        /// Inclusive start.
        start: Vec<u8>,
        /// Exclusive end.
        end: Vec<u8>,
    },
}

impl From<WriteBatch> for SerializableWriteBatch {
    fn from(b: WriteBatch) -> Self {
        let ops = b
            .into_ops()
            .into_iter()
            .map(|op| match op {
                WriteOp::Put { key, value } => SerializableWriteOp::Put {
                    key: key.to_vec(),
                    value: value.to_vec(),
                },
                WriteOp::Delete { key } => SerializableWriteOp::Delete { key: key.to_vec() },
                WriteOp::DeleteRange { start, end } => SerializableWriteOp::DeleteRange {
                    start: start.to_vec(),
                    end: end.to_vec(),
                },
            })
            .collect();
        Self { ops }
    }
}

impl From<SerializableWriteBatch> for WriteBatch {
    fn from(s: SerializableWriteBatch) -> Self {
        let mut b = WriteBatch::with_capacity(s.ops.len());
        for op in s.ops {
            match op {
                SerializableWriteOp::Put { key, value } => {
                    b.put(Bytes::from(key), Bytes::from(value));
                }
                SerializableWriteOp::Delete { key } => {
                    b.delete(Bytes::from(key));
                }
                SerializableWriteOp::DeleteRange { start, end } => {
                    b.delete_range(Bytes::from(start), Bytes::from(end));
                }
            }
        }
        b
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aresadb_core::WriteBatch;

    #[test]
    fn noop_ops_count_is_zero() {
        assert_eq!(AresaCommand::Noop.ops_count(), 0);
    }

    #[test]
    fn write_batch_roundtrip_preserves_ops() {
        let mut b = WriteBatch::new();
        b.put("a", "1").delete("b").delete_range("c", "d");
        let cmd = AresaCommand::batch(b);
        assert_eq!(cmd.ops_count(), 3);

        let encoded = bincode::serialize(&cmd).unwrap();
        let decoded: AresaCommand = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded.ops_count(), 3);
    }

    #[test]
    fn into_write_batch_preserves_content() {
        let mut original = WriteBatch::new();
        original.put("k", "v");
        let wrapper: SerializableWriteBatch = original.clone().into();
        let back: WriteBatch = wrapper.into();

        assert_eq!(back.ops().len(), 1);
        match &back.ops()[0] {
            WriteOp::Put { key, value } => {
                assert_eq!(&key[..], b"k");
                assert_eq!(&value[..], b"v");
            }
            _ => panic!("expected Put"),
        }
    }
}

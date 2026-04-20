//! Distributed Database Components (v1 scaffolding)
//!
//! **Deprecated:** the types in this module were scaffolding for a v1
//! distributed story that was never finished. The real distributed
//! implementation lives in the v2 workspace crates under
//! [`crates/`](../../../crates/): `aresadb-core`, `aresadb-raft`,
//! `aresadb-net`, `aresadb-engine-redb`, `aresadb-cluster`,
//! `aresadb-sim`.
//!
//! The leftover helpers here (bloom filters, LZ4 compression, shard
//! config, streaming cursors, replica metadata) are retained for
//! backward compatibility of the embedded `aresadb` crate API; new
//! code should not depend on them.
//!
//! The `WriteAheadLog` stub was removed during Phase 1 closeout —
//! `aresadb-raft::LogStore` is the v2 Raft log.

#![allow(dead_code)]
#![allow(unused_imports)]

mod bloom;
mod compression;
mod replication;
mod shard;
mod streaming;

pub use bloom::{BloomFilter, CountingBloomFilter};
pub use compression::{CompressionStats, Compressor};
pub use replication::{ReplicaConfig, ReplicaSet, ReplicaState};
pub use shard::{Shard, ShardConfig, ShardManager};
pub use streaming::{Cursor, ResultStream, StreamSender};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let mut bloom = BloomFilter::new(1000, 0.01);
        bloom.insert(b"hello");
        bloom.insert(b"world");

        assert!(bloom.may_contain(b"hello"));
        assert!(bloom.may_contain(b"world"));
        // False positives are possible, but "definitely not in set" is reliable
    }

    #[test]
    fn test_compression_roundtrip() {
        let compressor = Compressor::new();
        let data = b"Hello, AresaDB! This is a test of compression.";

        let compressed = compressor.compress(data).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();

        assert_eq!(data.as_slice(), decompressed.as_slice());
    }
}

//! Distributed Database Components
//!
//! V2 features for scaling AresaDB across multiple nodes:
//! - Sharding with consistent hashing
//! - Write-ahead logging for durability
//! - Bloom filters for fast negative lookups
//! - LZ4 compression for storage efficiency
//! - Replication for fault tolerance
//! - Streaming for large result sets

#![allow(dead_code)]
#![allow(unused_imports)]

mod bloom;
mod compression;
mod replication;
mod shard;
mod streaming;
mod wal;

pub use bloom::{BloomFilter, CountingBloomFilter};
pub use compression::{CompressionStats, Compressor};
pub use replication::{ReplicaConfig, ReplicaSet, ReplicaState};
pub use shard::{Shard, ShardConfig, ShardManager};
pub use streaming::{Cursor, ResultStream, StreamSender};
pub use wal::{WalEntry, WalEntryType, WriteAheadLog};

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

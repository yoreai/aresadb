//! Property-Based Tests for AresaDB
//!
//! Using proptest for randomized invariant testing.

use aresadb::distributed::{BloomFilter, Compressor, ShardManager};
use proptest::collection::vec as prop_vec;
use proptest::prelude::*;

// ============================================================================
// Bloom Filter Properties
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Bloom filter never has false negatives
    #[test]
    fn bloom_filter_no_false_negatives(
        items in prop_vec("[a-z]{1,20}", 1..100)
    ) {
        let mut bloom = BloomFilter::new(1000, 0.01);

        // Insert all items
        for item in &items {
            bloom.insert(item.as_bytes());
        }

        // All inserted items MUST be found
        for item in &items {
            prop_assert!(
                bloom.may_contain(item.as_bytes()),
                "False negative for: {}", item
            );
        }
    }

    /// Bloom filter maintains properties after clear
    #[test]
    fn bloom_filter_clear(items in prop_vec("[a-z]{1,20}", 1..50)) {
        let mut bloom = BloomFilter::new(500, 0.01);

        for item in &items {
            bloom.insert(item.as_bytes());
        }

        bloom.clear();

        // After clear, nothing should be found
        for item in &items {
            prop_assert!(!bloom.may_contain(item.as_bytes()));
        }
    }

    /// Bloom filter serialization preserves state
    #[test]
    fn bloom_filter_serialization(items in prop_vec("[a-z]{1,20}", 1..50)) {
        let mut bloom = BloomFilter::new(500, 0.01);

        for item in &items {
            bloom.insert(item.as_bytes());
        }

        let bytes = bloom.to_bytes();
        let restored = BloomFilter::from_bytes(&bytes).unwrap();

        // All items should still be found after deserialization
        for item in &items {
            prop_assert!(
                restored.may_contain(item.as_bytes()),
                "Item lost in serialization: {}", item
            );
        }
    }
}

// ============================================================================
// Compression Properties
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// Compression round-trip is lossless
    #[test]
    fn compression_roundtrip(data in prop_vec(any::<u8>(), 0..1000)) {
        let compressor = Compressor::new();

        let compressed = compressor.compress(&data).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();

        prop_assert_eq!(data, decompressed);
    }

    /// Compression of repeated data is efficient
    #[test]
    fn compression_efficiency(byte: u8, count in 100usize..1000) {
        let data: Vec<u8> = vec![byte; count];
        let compressor = Compressor::new();

        let compressed = compressor.compress(&data).unwrap();

        // Highly repetitive data should compress well
        prop_assert!(
            compressed.len() < count,
            "Compression ineffective: {} -> {}",
            count,
            compressed.len()
        );
    }
}

// ============================================================================
// Sharding Properties
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Sharding is deterministic
    #[test]
    fn sharding_is_deterministic(key in "[a-z]{1,50}", num_shards in 2u32..16) {
        let shard1 = ShardManager::shard_for_key(key.as_bytes(), num_shards);
        let shard2 = ShardManager::shard_for_key(key.as_bytes(), num_shards);

        prop_assert_eq!(shard1, shard2, "Same key mapped to different shards");
    }

    /// All shards get some keys
    #[test]
    fn sharding_distribution(num_shards in 2u32..8, num_keys in 100usize..500) {
        let mut shard_counts = vec![0usize; num_shards as usize];

        for i in 0..num_keys {
            let key = format!("key_{}", i);
            let shard = ShardManager::shard_for_key(key.as_bytes(), num_shards) as usize;
            shard_counts[shard] += 1;
        }

        // Each shard should have at least some keys
        for (i, count) in shard_counts.iter().enumerate() {
            prop_assert!(
                *count > 0,
                "Shard {} has no keys out of {} total",
                i,
                num_keys
            );
        }
    }
}

// ============================================================================
// SQL Parsing Properties
// ============================================================================

mod sql_properties {
    use super::*;
    use aresadb::query::QueryParser;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        /// Valid table names parse correctly
        #[test]
        fn valid_table_names_parse(table in "[a-z][a-z0-9_]{0,20}") {
            let parser = QueryParser::new();
            let sql = format!("SELECT * FROM {}", table);

            let result = parser.parse(&sql);
            prop_assert!(result.is_ok(), "Failed to parse: {}", sql);
            prop_assert_eq!(result.unwrap().target, table);
        }

        /// Valid column names parse correctly
        #[test]
        fn valid_column_names_parse(col in "[a-z][a-z0-9_]{0,20}") {
            let parser = QueryParser::new();
            let sql = format!("SELECT {} FROM test", col);

            let result = parser.parse(&sql);
            prop_assert!(result.is_ok(), "Failed to parse: {}", sql);
            prop_assert!(result.unwrap().columns.contains(&col));
        }

        /// LIMIT values are preserved
        #[test]
        fn limit_values_preserved(limit in 1u32..10000) {
            let parser = QueryParser::new();
            let sql = format!("SELECT * FROM test LIMIT {}", limit);

            let result = parser.parse(&sql);
            prop_assert!(result.is_ok());
            prop_assert_eq!(result.unwrap().limit, Some(limit as usize));
        }
    }
}

// ============================================================================
// Idempotency Properties
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Multiple inserts into bloom filter are idempotent for lookups
    #[test]
    fn bloom_insert_idempotent(item in "[a-z]{1,30}", times in 1usize..10) {
        let mut bloom = BloomFilter::new(100, 0.01);

        for _ in 0..times {
            bloom.insert(item.as_bytes());
        }

        prop_assert!(bloom.may_contain(item.as_bytes()));
    }
}

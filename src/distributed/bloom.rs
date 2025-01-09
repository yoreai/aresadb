//! Bloom Filter Implementation
//!
//! Space-efficient probabilistic data structure for fast negative lookups.
//! Used to quickly check if a key definitely doesn't exist before doing disk I/O.

#![allow(dead_code)]

use siphasher::sip128::{Hasher128, SipHasher24};
use std::hash::Hasher;

/// Bloom filter for probabilistic set membership
///
/// Provides O(1) lookups with configurable false positive rate.
/// No false negatives - if `may_contain` returns false, the item is definitely not in the set.
#[derive(Clone)]
pub struct BloomFilter {
    /// Bit array for the filter
    bits: Vec<u64>,
    /// Number of bits in the filter
    num_bits: usize,
    /// Number of hash functions
    num_hashes: u32,
    /// Number of items inserted
    count: usize,
}

impl BloomFilter {
    /// Create a new Bloom filter with expected capacity and false positive rate
    ///
    /// # Arguments
    /// * `expected_items` - Expected number of items to insert
    /// * `false_positive_rate` - Desired false positive rate (e.g., 0.01 for 1%)
    ///
    /// # Example
    /// ```
    /// use aresadb::distributed::BloomFilter;
    /// let filter = BloomFilter::new(10000, 0.01);
    /// ```
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let num_bits = Self::optimal_num_bits(expected_items, false_positive_rate);
        let num_hashes = Self::optimal_num_hashes(num_bits, expected_items);

        let num_words = num_bits.div_ceil(64);

        Self {
            bits: vec![0u64; num_words],
            num_bits,
            num_hashes,
            count: 0,
        }
    }

    /// Create a Bloom filter with explicit parameters
    pub fn with_params(num_bits: usize, num_hashes: u32) -> Self {
        let num_words = num_bits.div_ceil(64);
        Self {
            bits: vec![0u64; num_words],
            num_bits,
            num_hashes,
            count: 0,
        }
    }

    /// Insert an item into the filter
    pub fn insert(&mut self, item: &[u8]) {
        let (h1, h2) = self.hash_pair(item);

        for i in 0..self.num_hashes {
            let bit_idx = self.get_bit_index(h1, h2, i);
            self.set_bit(bit_idx);
        }

        self.count += 1;
    }

    /// Check if an item may be in the filter
    ///
    /// Returns `true` if the item might be in the set (with some false positive probability).
    /// Returns `false` if the item is definitely not in the set.
    pub fn may_contain(&self, item: &[u8]) -> bool {
        let (h1, h2) = self.hash_pair(item);

        for i in 0..self.num_hashes {
            let bit_idx = self.get_bit_index(h1, h2, i);
            if !self.get_bit(bit_idx) {
                return false;
            }
        }

        true
    }

    /// Get the number of items inserted
    pub fn count(&self) -> usize {
        self.count
    }

    /// Estimate the current false positive rate
    pub fn estimated_fpp(&self) -> f64 {
        let m = self.num_bits as f64;
        let k = self.num_hashes as f64;
        let n = self.count as f64;

        (1.0 - (-k * n / m).exp()).powf(k)
    }

    /// Get memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        self.bits.len() * 8
    }

    /// Clear the filter
    pub fn clear(&mut self) {
        self.bits.fill(0);
        self.count = 0;
    }

    /// Merge another Bloom filter into this one (union)
    pub fn merge(&mut self, other: &BloomFilter) {
        assert_eq!(self.num_bits, other.num_bits);
        assert_eq!(self.num_hashes, other.num_hashes);

        for (a, b) in self.bits.iter_mut().zip(other.bits.iter()) {
            *a |= *b;
        }

        // Count is approximate after merge
        self.count += other.count;
    }

    /// Serialize the filter to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(12 + self.bits.len() * 8);

        result.extend_from_slice(&(self.num_bits as u32).to_le_bytes());
        result.extend_from_slice(&self.num_hashes.to_le_bytes());
        result.extend_from_slice(&(self.count as u32).to_le_bytes());

        for word in &self.bits {
            result.extend_from_slice(&word.to_le_bytes());
        }

        result
    }

    /// Deserialize a filter from bytes
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 12 {
            return None;
        }

        let num_bits = u32::from_le_bytes(data[0..4].try_into().ok()?) as usize;
        let num_hashes = u32::from_le_bytes(data[4..8].try_into().ok()?);
        let count = u32::from_le_bytes(data[8..12].try_into().ok()?) as usize;

        let num_words = num_bits.div_ceil(64);
        let expected_len = 12 + num_words * 8;

        if data.len() < expected_len {
            return None;
        }

        let mut bits = Vec::with_capacity(num_words);
        for i in 0..num_words {
            let start = 12 + i * 8;
            let word = u64::from_le_bytes(data[start..start + 8].try_into().ok()?);
            bits.push(word);
        }

        Some(Self {
            bits,
            num_bits,
            num_hashes,
            count,
        })
    }

    // === Private methods ===

    fn optimal_num_bits(n: usize, p: f64) -> usize {
        let ln2_squared = std::f64::consts::LN_2.powi(2);
        (-(n as f64) * p.ln() / ln2_squared).ceil() as usize
    }

    fn optimal_num_hashes(m: usize, n: usize) -> u32 {
        let k = (m as f64 / n as f64) * std::f64::consts::LN_2;
        k.ceil().max(1.0) as u32
    }

    fn hash_pair(&self, item: &[u8]) -> (u64, u64) {
        let mut hasher = SipHasher24::new();
        hasher.write(item);
        let hash = hasher.finish128();
        (hash.h1, hash.h2)
    }

    fn get_bit_index(&self, h1: u64, h2: u64, i: u32) -> usize {
        // Double hashing: h(i) = h1 + i * h2
        let hash = h1.wrapping_add((i as u64).wrapping_mul(h2));
        (hash as usize) % self.num_bits
    }

    fn set_bit(&mut self, idx: usize) {
        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        self.bits[word_idx] |= 1u64 << bit_idx;
    }

    fn get_bit(&self, idx: usize) -> bool {
        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        (self.bits[word_idx] >> bit_idx) & 1 == 1
    }
}

impl std::fmt::Debug for BloomFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BloomFilter")
            .field("num_bits", &self.num_bits)
            .field("num_hashes", &self.num_hashes)
            .field("count", &self.count)
            .field("estimated_fpp", &format!("{:.6}", self.estimated_fpp()))
            .field("memory_bytes", &self.memory_usage())
            .finish()
    }
}

/// Counting Bloom Filter - supports deletion
///
/// Each position is a counter instead of a single bit, allowing items to be removed.
/// Uses more memory than a standard Bloom filter.
#[derive(Clone)]
pub struct CountingBloomFilter {
    /// Counter array (4 bits per counter, packed into u64)
    counters: Vec<u64>,
    /// Number of counters
    num_counters: usize,
    /// Number of hash functions
    num_hashes: u32,
    /// Number of items
    count: usize,
}

impl CountingBloomFilter {
    /// Create a new counting Bloom filter
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let num_counters = BloomFilter::optimal_num_bits(expected_items, false_positive_rate);
        let num_hashes = BloomFilter::optimal_num_hashes(num_counters, expected_items);

        // 4 bits per counter, 16 counters per u64
        let num_words = num_counters.div_ceil(16);

        Self {
            counters: vec![0u64; num_words],
            num_counters,
            num_hashes,
            count: 0,
        }
    }

    /// Insert an item
    pub fn insert(&mut self, item: &[u8]) {
        let (h1, h2) = self.hash_pair(item);

        for i in 0..self.num_hashes {
            let idx = self.get_index(h1, h2, i);
            self.increment_counter(idx);
        }

        self.count += 1;
    }

    /// Remove an item
    pub fn remove(&mut self, item: &[u8]) -> bool {
        if !self.may_contain(item) {
            return false;
        }

        let (h1, h2) = self.hash_pair(item);

        for i in 0..self.num_hashes {
            let idx = self.get_index(h1, h2, i);
            self.decrement_counter(idx);
        }

        self.count = self.count.saturating_sub(1);
        true
    }

    /// Check if an item may be in the filter
    pub fn may_contain(&self, item: &[u8]) -> bool {
        let (h1, h2) = self.hash_pair(item);

        for i in 0..self.num_hashes {
            let idx = self.get_index(h1, h2, i);
            if self.get_counter(idx) == 0 {
                return false;
            }
        }

        true
    }

    /// Get the number of items
    pub fn count(&self) -> usize {
        self.count
    }

    /// Clear the filter
    pub fn clear(&mut self) {
        self.counters.fill(0);
        self.count = 0;
    }

    // === Private methods ===

    fn hash_pair(&self, item: &[u8]) -> (u64, u64) {
        let mut hasher = SipHasher24::new();
        hasher.write(item);
        let hash = hasher.finish128();
        (hash.h1, hash.h2)
    }

    fn get_index(&self, h1: u64, h2: u64, i: u32) -> usize {
        let hash = h1.wrapping_add((i as u64).wrapping_mul(h2));
        (hash as usize) % self.num_counters
    }

    fn get_counter(&self, idx: usize) -> u8 {
        let word_idx = idx / 16;
        let counter_idx = (idx % 16) * 4;
        ((self.counters[word_idx] >> counter_idx) & 0xF) as u8
    }

    fn increment_counter(&mut self, idx: usize) {
        let word_idx = idx / 16;
        let counter_idx = (idx % 16) * 4;
        let current = (self.counters[word_idx] >> counter_idx) & 0xF;

        if current < 15 {
            self.counters[word_idx] += 1u64 << counter_idx;
        }
    }

    fn decrement_counter(&mut self, idx: usize) {
        let word_idx = idx / 16;
        let counter_idx = (idx % 16) * 4;
        let current = (self.counters[word_idx] >> counter_idx) & 0xF;

        if current > 0 {
            self.counters[word_idx] -= 1u64 << counter_idx;
        }
    }
}

impl std::fmt::Debug for CountingBloomFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CountingBloomFilter")
            .field("num_counters", &self.num_counters)
            .field("num_hashes", &self.num_hashes)
            .field("count", &self.count)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_basic_operations() {
        let mut filter = BloomFilter::new(1000, 0.01);

        // Insert items
        filter.insert(b"apple");
        filter.insert(b"banana");
        filter.insert(b"cherry");

        // Check membership
        assert!(filter.may_contain(b"apple"));
        assert!(filter.may_contain(b"banana"));
        assert!(filter.may_contain(b"cherry"));

        // Items not inserted (false positives possible but unlikely)
        // We can't assert !may_contain for these due to false positives
        assert_eq!(filter.count(), 3);
    }

    #[test]
    fn test_bloom_false_positive_rate() {
        let mut filter = BloomFilter::new(10000, 0.01);

        // Insert 10000 items
        for i in 0_u64..10000 {
            filter.insert(&i.to_le_bytes());
        }

        // Count false positives for items not in set
        let mut false_positives = 0;
        for i in 10000_u64..20000 {
            if filter.may_contain(&i.to_le_bytes()) {
                false_positives += 1;
            }
        }

        // False positive rate should be close to 1%
        let fpp = false_positives as f64 / 10000.0;
        assert!(fpp < 0.02, "FPP too high: {}", fpp);
    }

    #[test]
    fn test_bloom_serialization() {
        let mut filter = BloomFilter::new(1000, 0.01);
        filter.insert(b"test");
        filter.insert(b"data");

        let bytes = filter.to_bytes();
        let restored = BloomFilter::from_bytes(&bytes).unwrap();

        assert!(restored.may_contain(b"test"));
        assert!(restored.may_contain(b"data"));
        assert_eq!(restored.count(), 2);
    }

    #[test]
    fn test_bloom_merge() {
        let mut filter1 = BloomFilter::new(1000, 0.01);
        filter1.insert(b"a");
        filter1.insert(b"b");

        let mut filter2 = BloomFilter::new(1000, 0.01);
        filter2.insert(b"c");
        filter2.insert(b"d");

        filter1.merge(&filter2);

        assert!(filter1.may_contain(b"a"));
        assert!(filter1.may_contain(b"b"));
        assert!(filter1.may_contain(b"c"));
        assert!(filter1.may_contain(b"d"));
    }

    #[test]
    fn test_counting_bloom_basic() {
        let mut filter = CountingBloomFilter::new(1000, 0.01);

        filter.insert(b"apple");
        filter.insert(b"banana");

        assert!(filter.may_contain(b"apple"));
        assert!(filter.may_contain(b"banana"));

        // Remove an item
        assert!(filter.remove(b"apple"));
        assert!(!filter.may_contain(b"apple"));
        assert!(filter.may_contain(b"banana"));
    }

    #[test]
    fn test_counting_bloom_multiple_inserts() {
        let mut filter = CountingBloomFilter::new(1000, 0.01);

        // Insert same item multiple times
        filter.insert(b"test");
        filter.insert(b"test");
        filter.insert(b"test");

        assert!(filter.may_contain(b"test"));

        // Remove once - should still exist
        filter.remove(b"test");
        assert!(filter.may_contain(b"test"));

        // Remove again - should still exist
        filter.remove(b"test");
        assert!(filter.may_contain(b"test"));

        // Remove third time - should be gone
        filter.remove(b"test");
        assert!(!filter.may_contain(b"test"));
    }
}

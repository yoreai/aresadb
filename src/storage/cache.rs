//! Cache layer for bucket storage
//!
//! Provides LRU caching for remote data to reduce latency and bandwidth.

#![allow(dead_code)]

use anyhow::Result;
use bytes::Bytes;
use moka::sync::Cache;
use std::sync::Arc;
use std::time::Duration;

/// Cache entry metadata
#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub data: Bytes,
    pub size: usize,
}

/// LRU cache layer for remote storage
pub struct CacheLayer {
    /// The cache store
    cache: Cache<String, Arc<CacheEntry>>,
    /// Maximum cache size in bytes
    max_size: u64,
    /// Current cache size in bytes
    current_size: std::sync::atomic::AtomicU64,
}

impl Clone for CacheLayer {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            max_size: self.max_size,
            current_size: std::sync::atomic::AtomicU64::new(
                self.current_size.load(std::sync::atomic::Ordering::Relaxed),
            ),
        }
    }
}

impl CacheLayer {
    /// Create a new cache layer with the given maximum size in bytes
    pub fn new(max_size_bytes: u64) -> Self {
        let cache = Cache::builder()
            .max_capacity(10000) // Max number of entries
            .time_to_idle(Duration::from_secs(3600)) // 1 hour TTL
            .weigher(|_key: &String, value: &Arc<CacheEntry>| -> u32 {
                // Weight by size (capped at u32::MAX)
                value.size.min(u32::MAX as usize) as u32
            })
            .build();

        Self {
            cache,
            max_size: max_size_bytes,
            current_size: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Get an entry from cache
    pub fn get(&self, key: &str) -> Option<Bytes> {
        self.cache.get(key).map(|entry| entry.data.clone())
    }

    /// Put an entry in cache
    pub fn put(&self, key: &str, data: Bytes) {
        let size = data.len();
        let entry = Arc::new(CacheEntry { data, size });

        self.cache.insert(key.to_string(), entry);
        self.current_size
            .fetch_add(size as u64, std::sync::atomic::Ordering::Relaxed);
    }

    /// Remove an entry from cache
    pub fn remove(&self, key: &str) {
        if let Some(entry) = self.cache.remove(key) {
            self.current_size
                .fetch_sub(entry.size as u64, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Clear all entries from cache
    pub fn clear(&self) {
        self.cache.invalidate_all();
        self.current_size
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get current cache size in bytes
    pub fn size(&self) -> u64 {
        self.current_size.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get maximum cache size in bytes
    pub fn max_size(&self) -> u64 {
        self.max_size
    }

    /// Get number of entries in cache
    pub fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }

    /// Check if key exists in cache
    pub fn contains(&self, key: &str) -> bool {
        self.cache.contains_key(key)
    }

    /// Get or fetch: returns cached value or fetches from provided async function
    pub async fn get_or_fetch<F, Fut>(&self, key: &str, fetch: F) -> Result<Bytes>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Bytes>>,
    {
        // Check cache first
        if let Some(data) = self.get(key) {
            return Ok(data);
        }

        // Fetch data
        let data = fetch().await?;

        // Store in cache
        self.put(key, data.clone());

        Ok(data)
    }
}

/// Stats about cache usage
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entries: u64,
    pub size_bytes: u64,
    pub max_size_bytes: u64,
    pub utilization_percent: f64,
}

impl CacheLayer {
    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let size = self.size();
        let max_size = self.max_size();

        CacheStats {
            entries: self.entry_count(),
            size_bytes: size,
            max_size_bytes: max_size,
            utilization_percent: (size as f64 / max_size as f64) * 100.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_basic() {
        let cache = CacheLayer::new(1024 * 1024); // 1MB

        // Put and get
        cache.put("key1", Bytes::from("hello"));
        let data = cache.get("key1").unwrap();
        assert_eq!(&data[..], b"hello");

        // Non-existent key
        assert!(cache.get("key2").is_none());
    }

    #[test]
    fn test_cache_remove() {
        let cache = CacheLayer::new(1024 * 1024);

        cache.put("key1", Bytes::from("hello"));
        assert!(cache.contains("key1"));

        cache.remove("key1");
        assert!(!cache.contains("key1"));
    }

    #[test]
    fn test_cache_clear() {
        let cache = CacheLayer::new(1024 * 1024);

        cache.put("key1", Bytes::from("hello"));
        cache.put("key2", Bytes::from("world"));
        // Note: moka cache may not immediately reflect entry count due to async nature
        assert!(cache.contains("key1"));
        assert!(cache.contains("key2"));

        cache.clear();
        assert!(!cache.contains("key1"));
        assert!(!cache.contains("key2"));
    }

    #[tokio::test]
    async fn test_get_or_fetch() {
        let cache = CacheLayer::new(1024 * 1024);

        // First call should fetch
        let data = cache
            .get_or_fetch("key1", || async { Ok(Bytes::from("fetched")) })
            .await
            .unwrap();
        assert_eq!(&data[..], b"fetched");

        // Second call should use cache
        let data = cache
            .get_or_fetch("key1", || async { Ok(Bytes::from("should not see this")) })
            .await
            .unwrap();
        assert_eq!(&data[..], b"fetched");
    }
}

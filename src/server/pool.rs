//! Connection Pool
//!
//! Manages concurrent connections with semaphore-based limiting.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Connection pool for limiting concurrent connections
pub struct ConnectionPool {
    max_connections: usize,
    active: AtomicUsize,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub fn new(max_connections: usize) -> Self {
        Self {
            max_connections,
            active: AtomicUsize::new(0),
        }
    }

    /// Try to acquire a connection slot (returns true if acquired)
    pub fn try_acquire(&self) -> bool {
        loop {
            let current = self.active.load(Ordering::SeqCst);
            if current >= self.max_connections {
                return false;
            }
            if self
                .active
                .compare_exchange(current, current + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Release a connection slot
    pub fn release(&self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }

    /// Get current active connection count
    pub fn active_count(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    /// Get maximum connections
    pub fn max_connections(&self) -> usize {
        self.max_connections
    }

    /// Get available slots
    pub fn available(&self) -> usize {
        self.max_connections - self.active_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_basic() {
        let pool = ConnectionPool::new(10);

        assert_eq!(pool.max_connections(), 10);
        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.available(), 10);
    }

    #[test]
    fn test_pool_acquire_release() {
        let pool = ConnectionPool::new(2);

        // Acquire first
        assert!(pool.try_acquire());
        assert_eq!(pool.active_count(), 1);

        // Acquire second
        assert!(pool.try_acquire());
        assert_eq!(pool.active_count(), 2);

        // Third should fail
        assert!(!pool.try_acquire());
        assert_eq!(pool.active_count(), 2);

        // Release one
        pool.release();
        assert_eq!(pool.active_count(), 1);

        // Now we can acquire again
        assert!(pool.try_acquire());
        assert_eq!(pool.active_count(), 2);
    }
}

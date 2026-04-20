//! Backend-level errors.
//!
//! Every `StorageBackend` method returns [`Result<T>`] with this error
//! type. Engine-specific errors are wrapped in [`Error::Backend`] so
//! callers can either match on the kind they care about or treat
//! everything as an opaque `anyhow::Error`.

use std::io;

/// Result alias used across the crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors that may be returned by a `StorageBackend`.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// I/O failure from the underlying filesystem or device.
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),

    /// The requested key was not found.
    ///
    /// Most `get`-like methods return `Ok(None)` instead, but operations
    /// that require an existing key (e.g. `cas`) use this variant.
    #[error("key not found")]
    NotFound,

    /// A write-batch violated a backend-specific invariant
    /// (e.g. duplicate deletes of the same key in one batch).
    #[error("invalid write batch: {0}")]
    InvalidWriteBatch(String),

    /// The backend was closed while the operation was in flight.
    #[error("backend is closed")]
    Closed,

    /// A snapshot or iterator outlived the transaction / epoch that
    /// created it.
    #[error("snapshot is no longer valid")]
    SnapshotInvalid,

    /// Engine-specific error. Keep the inner type opaque so callers
    /// don't accidentally depend on a specific engine.
    #[error("backend error: {0}")]
    Backend(#[source] anyhow::Error),
}

impl Error {
    /// Wrap any `anyhow`-shaped error as a backend error.
    pub fn backend<E: Into<anyhow::Error>>(err: E) -> Self {
        Self::Backend(err.into())
    }
}

/// Convenience conversion so `?` on `anyhow::Error` works.
impl From<anyhow::Error> for Error {
    fn from(err: anyhow::Error) -> Self {
        Self::Backend(err)
    }
}

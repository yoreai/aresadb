//! Byte-lexicographic key ranges.
//!
//! The v2 keyspace is a single sorted byte stream. Every scan, snapshot,
//! and range-replication operation uses [`KeyRange`] to describe the
//! half-open interval `[start, end)`.
//!
//! Sentinel values:
//! - `start = b""` means "from the beginning".
//! - `end = b""` means "to the end".
//! - If both are empty the range spans the full keyspace.

use bytes::Bytes;

/// A half-open byte range `[start, end)`.
///
/// `Bytes` is used rather than `Vec<u8>` so that ranges can be cloned
/// cheaply and so that callers can pass reference-counted buffers
/// without copying.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyRange {
    start: Bytes,
    end: Bytes,
}

impl KeyRange {
    /// Range covering the entire keyspace (`[b"", b"")` — both open).
    pub fn all() -> Self {
        Self {
            start: Bytes::new(),
            end: Bytes::new(),
        }
    }

    /// Range `[start, end)`.
    pub fn new(start: impl Into<Bytes>, end: impl Into<Bytes>) -> Self {
        Self {
            start: start.into(),
            end: end.into(),
        }
    }

    /// Range starting at `start` with no upper bound.
    pub fn from(start: impl Into<Bytes>) -> Self {
        Self {
            start: start.into(),
            end: Bytes::new(),
        }
    }

    /// Range from the beginning up to (but not including) `end`.
    pub fn to(end: impl Into<Bytes>) -> Self {
        Self {
            start: Bytes::new(),
            end: end.into(),
        }
    }

    /// All keys sharing a common prefix.
    ///
    /// `prefix("/n/")` is equivalent to `new("/n/", prefix_successor("/n/"))`.
    pub fn prefix(prefix: impl Into<Bytes>) -> Self {
        let start: Bytes = prefix.into();
        let end = prefix_successor(&start);
        Self { start, end }
    }

    /// Inclusive lower bound.
    pub fn start(&self) -> &[u8] {
        &self.start
    }

    /// Exclusive upper bound. Empty means "no upper bound".
    pub fn end(&self) -> &[u8] {
        &self.end
    }

    /// True if the range has no upper bound.
    pub fn is_open_ended(&self) -> bool {
        self.end.is_empty()
    }

    /// True if `key` falls inside the range.
    pub fn contains(&self, key: &[u8]) -> bool {
        let after_start = key >= &self.start[..];
        let before_end = self.end.is_empty() || key < &self.end[..];
        after_start && before_end
    }
}

/// Return the lexicographically-smallest key that does not share `prefix`
/// as a prefix. If `prefix` is all `0xff`, returns an empty slice to
/// signal "no upper bound" — the caller is expected to treat an empty
/// `end` as "to end of keyspace".
pub fn prefix_successor(prefix: &[u8]) -> Bytes {
    let mut out = prefix.to_vec();
    while let Some(last) = out.last_mut() {
        if *last != 0xff {
            *last += 1;
            return Bytes::from(out);
        }
        out.pop();
    }
    Bytes::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contains_respects_bounds() {
        let r = KeyRange::new(b"b".to_vec(), b"d".to_vec());
        assert!(!r.contains(b"a"));
        assert!(r.contains(b"b"));
        assert!(r.contains(b"c"));
        assert!(!r.contains(b"d"));
        assert!(!r.contains(b"e"));
    }

    #[test]
    fn open_ended_range_has_no_upper_limit() {
        let r = KeyRange::from(b"b".to_vec());
        assert!(r.contains(b"b"));
        assert!(r.contains(b"zzz"));
        assert!(r.is_open_ended());
    }

    #[test]
    fn prefix_range_uses_successor() {
        let r = KeyRange::prefix(b"/n/".to_vec());
        assert!(r.contains(b"/n/"));
        assert!(r.contains(b"/n/abc"));
        assert!(!r.contains(b"/o/"));
    }

    #[test]
    fn prefix_successor_bumps_last_byte() {
        assert_eq!(&prefix_successor(b"abc")[..], b"abd");
    }

    #[test]
    fn prefix_successor_handles_ff_overflow() {
        assert_eq!(&prefix_successor(&[0xff, 0xff])[..], b"");
        assert_eq!(&prefix_successor(&[0x01, 0xff])[..], &[0x02]);
    }
}

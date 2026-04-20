//! On-disk key layout for the placement-driver catalog.
//!
//! Every catalog row lives under the `/m/pd/` prefix so it falls
//! inside the unified keyspace's [`aresadb_core::keys::prefix::METADATA`]
//! region. Row keys are fixed-width after the prefix so point-lookup
//! remains a constant number of concatenations and an `O(log n)`
//! B-tree seek:
//!
//! ```text
//! /m/pd/r/<range_id_be:8>   -> bincode(RangeDescriptor)
//! /m/pd/n/<node_id_be:8>    -> bincode(NodeInfo)
//! ```
//!
//! No reserved "counter" row: the catalog's `next_range_id` counter is
//! reconstructed at open time by scanning every range row and taking
//! `max(range_id) + 1`. This keeps recovery trivial and avoids the
//! "counter lags live ranges after a partial split write" failure
//! mode.

use bytes::{BufMut, Bytes, BytesMut};

use crate::types::{NodeId, RangeId};

/// Prefix under which the catalog's range descriptors live.
pub const RANGE_PREFIX: &[u8] = b"/m/pd/r/";

/// Prefix under which the catalog's node inventory lives.
pub const NODE_PREFIX: &[u8] = b"/m/pd/n/";

/// Build the storage key for the given range id.
pub fn range_key(range_id: RangeId) -> Bytes {
    let mut b = BytesMut::with_capacity(RANGE_PREFIX.len() + 8);
    b.put_slice(RANGE_PREFIX);
    b.put_u64(range_id);
    b.freeze()
}

/// Build the storage key for the given node id.
pub fn node_key(node_id: NodeId) -> Bytes {
    let mut b = BytesMut::with_capacity(NODE_PREFIX.len() + 8);
    b.put_slice(NODE_PREFIX);
    b.put_u64(node_id);
    b.freeze()
}

/// Decode a range id from a storage key. Returns `None` if the key is
/// not a range row.
pub fn range_id_from_key(key: &[u8]) -> Option<RangeId> {
    let rest = key.strip_prefix(RANGE_PREFIX)?;
    if rest.len() != 8 {
        return None;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(rest);
    Some(u64::from_be_bytes(buf))
}

/// Decode a node id from a storage key. Returns `None` if the key is
/// not a node row.
pub fn node_id_from_key(key: &[u8]) -> Option<NodeId> {
    let rest = key.strip_prefix(NODE_PREFIX)?;
    if rest.len() != 8 {
        return None;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(rest);
    Some(u64::from_be_bytes(buf))
}

/// End key for a prefix scan, one past the prefix's lex maximum.
/// Equal to `prefix` with the final byte bumped. Panics if the
/// prefix is empty or ends in a byte that can't be incremented
/// (`0xff`), which never happens for our fixed prefixes.
pub fn prefix_upper_bound(prefix: &[u8]) -> Bytes {
    assert!(!prefix.is_empty(), "prefix must be non-empty");
    let mut out = prefix.to_vec();
    let last = out.last_mut().expect("non-empty per assert above");
    assert!(*last != 0xff, "prefix ends in 0xff; no lex successor");
    *last += 1;
    Bytes::from(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_key_round_trips() {
        for id in [0u64, 1, 42, u64::MAX / 2, u64::MAX] {
            let k = range_key(id);
            assert_eq!(range_id_from_key(&k), Some(id));
        }
    }

    #[test]
    fn node_key_round_trips() {
        for id in [0u64, 1, 7, u64::MAX] {
            let k = node_key(id);
            assert_eq!(node_id_from_key(&k), Some(id));
        }
    }

    #[test]
    fn range_key_sorts_by_id_big_endian() {
        // Big-endian encoding makes lex order match numeric order
        // over u64, which means a prefix scan of /m/pd/r/ returns
        // ranges in id-ascending order. Handy for recovery logs.
        let k_small = range_key(1);
        let k_big = range_key(1_000_000);
        assert!(k_small.as_ref() < k_big.as_ref());
    }

    #[test]
    fn decoders_reject_foreign_keys() {
        assert_eq!(range_id_from_key(b""), None);
        assert_eq!(range_id_from_key(b"/n/user"), None);
        assert_eq!(range_id_from_key(b"/m/pd/r/"), None, "missing id suffix");
        assert_eq!(range_id_from_key(b"/m/pd/r/12345"), None, "wrong id length");
        assert_eq!(node_id_from_key(b"/m/pd/r/00000000"), None, "wrong prefix");
    }

    #[test]
    fn prefix_upper_bound_is_exclusive_upper() {
        let ub = prefix_upper_bound(b"/m/pd/r/");
        // The key immediately after the prefix must be < ub.
        let last_in = range_key(u64::MAX);
        assert!(last_in.as_ref() < ub.as_ref());
        // The key immediately after the range prefix (e.g. /m/pd/s/)
        // must be >= ub, so a half-open scan over [prefix, ub) stays
        // inside the prefix.
        assert!(b"/m/pd/s/".as_ref() >= ub.as_ref());
    }

    #[test]
    fn range_and_node_prefixes_are_disjoint() {
        // Paranoid: nothing must alias a range row for a node key or
        // vice versa. Separate static prefixes + equal-length suffix
        // make this hold trivially.
        let r = range_key(42);
        let n = node_key(42);
        assert!(r.as_ref() != n.as_ref());
        assert!(range_id_from_key(&n).is_none());
        assert!(node_id_from_key(&r).is_none());
    }
}

//! Unified keyspace encoder / decoder.
//!
//! All data in a range-sharded v2 cluster lives in a single sorted
//! byte-keyspace. This module is the canonical implementation of §3.1
//! of `docs/architecture-v2.md`: one flat keyspace with ASCII prefixes
//! per model, so KV, graph, SQL, vector, and full-text entries all
//! coexist in a globally-sortable order.
//!
//! ```text
//! /m/<tail>                             cluster metadata
//! /n/<node_id>                          graph node payload
//! /i/<node_id>                          node index entry (tiered-storage pointer)
//! /e/<edge_id>                          edge record
//! /ef/<from_id>/<edge_id>               edge-by-from index
//! /et/<to_id>/<edge_id>                 edge-by-to index
//! /p/<type>/<field>/<value>/<node_id>   secondary property B-tree index
//! /ft/<type>/<field>/<token>/<node_id>  full-text inverted-index entry
//! /v/<type>/<field>/<node_id>           HNSW vector-index entry
//! /s/<type>                             schema registry entry
//! /x/<tx_id>                            transaction record (Phase 4)
//! ```
//!
//! # Why a single keyspace
//!
//! Range-based sharding (CockroachDB / TiKV style) only works when
//! every logical row has a predictable place in one global byte order.
//! Splits, merges, and rebalance decisions are then driven by the
//! byte prefix alone — the placement driver does not need to know
//! anything about the semantic model. This also lets graph traversal,
//! property lookups, and FTS posting walks exploit locality within a
//! single range whenever the schema co-locates related keys
//! (e.g. `/ef/<from_id>/...` groups a node's outgoing edges).
//!
//! # Encoding details
//!
//! Every key starts with an ASCII prefix from the table above. The
//! payload after the prefix is one or more *segments*:
//!
//! - A key with a single variable segment (e.g. `Node`, `Edge`,
//!   `Schema`) emits the segment bytes verbatim after the prefix. No
//!   escape, no terminator.
//! - A key with N ≥ 2 variable segments emits the first N − 1 segments
//!   with CRDB-style escape encoding (`0x00` within the segment becomes
//!   `0x00 0xff`) followed by a `0x00 0x01` terminator. The final
//!   segment is written raw.
//!
//! This layout has two useful properties:
//!
//! 1. **Lex order matches logical order.** Comparing two encoded keys
//!    byte-for-byte yields the same result as comparing the structured
//!    keys component-by-component. This is what lets the placement
//!    driver pick a split point inside a range without knowing the
//!    schema.
//! 2. **Prefix scans are O(prefix length).** Asking for "all edges out
//!    of node X" is a range scan on `/ef/<escaped X><0x00 0x01>..` —
//!    the escape/terminator scheme means no accidental collision with
//!    a different node's edges.
//!
//! Fixed-width identifiers (the v1 code uses 16-byte UUIDs) fit the
//! scheme for free: escape is a no-op on most UUID bytes, and where a
//! `0x00` happens to appear it simply gets emitted as `0x00 0xff`.
//!
//! # Phase 2a scope
//!
//! This module is self-contained. Nothing in Phase 1 uses it yet —
//! the Raft state machine still treats keys as opaque `Bytes`. Phase
//! 2c will wire it into `aresadb-cluster::ClusterNode` so the data
//! path writes encoded keys and range splits operate on the global
//! sort order.

use bytes::{BufMut, Bytes, BytesMut};

/// Byte that starts an escaped / terminator pair inside an encoded
/// segment. `0x00 0xff` is a literal null byte; `0x00 0x01` is a
/// segment terminator.
const NUL: u8 = 0x00;
/// Second byte of a segment terminator (see [`NUL`]).
const TERMINATOR: u8 = 0x01;
/// Second byte of an escape (see [`NUL`]).
const ESCAPE: u8 = 0xff;

/// ASCII prefixes that tag each keyspace region. Lookups of a key's
/// kind start by matching one of these.
pub mod prefix {
    /// Cluster metadata (placement driver catalog, topology snapshots).
    pub const METADATA: &[u8] = b"/m/";
    /// Graph node payload.
    pub const NODE: &[u8] = b"/n/";
    /// Node index entry (tiered-storage pointer for a node payload).
    pub const NODE_INDEX: &[u8] = b"/i/";
    /// Edge record (keyed by edge id).
    pub const EDGE: &[u8] = b"/e/";
    /// Edge-by-from index (groups outgoing edges of a source node).
    pub const EDGE_FROM: &[u8] = b"/ef/";
    /// Edge-by-to index (groups incoming edges of a target node).
    pub const EDGE_TO: &[u8] = b"/et/";
    /// Secondary property B-tree index.
    pub const PROPERTY: &[u8] = b"/p/";
    /// Full-text inverted-index entry.
    pub const FULLTEXT: &[u8] = b"/ft/";
    /// HNSW vector-index entry.
    pub const VECTOR: &[u8] = b"/v/";
    /// Schema registry entry (one per node type).
    pub const SCHEMA: &[u8] = b"/s/";
    /// Transaction record (Phase 4 MVCC / parallel commit).
    pub const TRANSACTION: &[u8] = b"/x/";
}

/// A structured key that can be round-tripped through its encoded
/// bytes form. Every variant corresponds to exactly one row in the
/// §3.1 layout table.
///
/// All fields are `Bytes` so callers can cheaply clone or slice into
/// an encoded buffer; the encoder never enforces a particular shape on
/// a segment's contents beyond requiring it to be bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Key {
    /// `/m/<tail>` — opaque metadata tail. The placement driver and
    /// schema catalog own this namespace.
    Metadata {
        /// The part of the key after `/m/`.
        tail: Bytes,
    },
    /// `/n/<node_id>` — graph node payload.
    Node {
        /// Binary node identifier (16-byte UUID in the v1 model).
        node_id: Bytes,
    },
    /// `/i/<node_id>` — node index entry (small pointer row used by
    /// the tiered-storage layer to locate a node's payload).
    NodeIndex {
        /// Binary node identifier.
        node_id: Bytes,
    },
    /// `/e/<edge_id>` — edge record.
    Edge {
        /// Binary edge identifier.
        edge_id: Bytes,
    },
    /// `/ef/<from_id>/<edge_id>` — edge-by-from index. Outgoing edges
    /// of a source node scan with prefix
    /// `/ef/<escaped from_id><0x00 0x01>`.
    EdgeFrom {
        /// Binary id of the edge's source node.
        from: Bytes,
        /// Binary edge identifier.
        edge_id: Bytes,
    },
    /// `/et/<to_id>/<edge_id>` — edge-by-to index. Incoming edges of
    /// a target node scan with prefix
    /// `/et/<escaped to_id><0x00 0x01>`.
    EdgeTo {
        /// Binary id of the edge's target node.
        to: Bytes,
        /// Binary edge identifier.
        edge_id: Bytes,
    },
    /// `/p/<type>/<field>/<value>/<node_id>` — secondary property
    /// index. Equality lookups scan with prefix up to `<value>`.
    PropertyIndex {
        /// Node type.
        type_: Bytes,
        /// Indexed field name.
        field: Bytes,
        /// Encoded field value.
        value: Bytes,
        /// Owning node id.
        node_id: Bytes,
    },
    /// `/ft/<type>/<field>/<token>/<node_id>` — full-text posting
    /// list entry.
    FulltextIndex {
        /// Node type.
        type_: Bytes,
        /// Indexed field name.
        field: Bytes,
        /// A single token from the inverted index.
        token: Bytes,
        /// Owning node id.
        node_id: Bytes,
    },
    /// `/v/<type>/<field>/<node_id>` — HNSW vector index entry.
    VectorIndex {
        /// Node type.
        type_: Bytes,
        /// Indexed embedding field name.
        field: Bytes,
        /// Owning node id.
        node_id: Bytes,
    },
    /// `/s/<type>` — schema registry entry.
    Schema {
        /// Node type whose schema this row describes.
        type_: Bytes,
    },
    /// `/x/<tx_id>` — transaction record (Phase 4 MVCC).
    Transaction {
        /// Transaction identifier.
        tx_id: Bytes,
    },
}

/// Errors surfaced by [`Key::decode`] when an input byte slice is not
/// a valid encoded key.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    /// Slice is too short to contain even a single prefix.
    #[error("encoded key is too short ({len} bytes)")]
    TooShort {
        /// Length of the offending slice.
        len: usize,
    },
    /// First bytes do not match any known prefix.
    #[error("unknown key prefix: {0:?}")]
    UnknownPrefix(Bytes),
    /// A segment terminator (`0x00 0x01`) was expected before the end
    /// of input but never appeared.
    #[error("missing segment terminator for segment {index}")]
    MissingTerminator {
        /// Zero-based index of the segment that lacked a terminator.
        index: usize,
    },
    /// An escape byte (`0x00`) was followed by a byte other than
    /// `0xff` (literal) or `0x01` (terminator).
    #[error("malformed escape sequence at byte {offset}: 0x00 0x{next:02x}")]
    MalformedEscape {
        /// Offset (inside the encoded body, after the prefix) of the
        /// first byte of the bad escape.
        offset: usize,
        /// The byte that followed `0x00`.
        next: u8,
    },
}

impl Key {
    /// Encode this key into its canonical byte representation. The
    /// returned bytes can be fed back to [`Key::decode`] for an exact
    /// round-trip.
    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(self.encoded_len_hint());
        match self {
            Self::Metadata { tail } => write_single(&mut out, prefix::METADATA, tail),
            Self::Node { node_id } => write_single(&mut out, prefix::NODE, node_id),
            Self::NodeIndex { node_id } => write_single(&mut out, prefix::NODE_INDEX, node_id),
            Self::Edge { edge_id } => write_single(&mut out, prefix::EDGE, edge_id),
            Self::EdgeFrom { from, edge_id } => {
                write_multi(&mut out, prefix::EDGE_FROM, &[from], edge_id);
            }
            Self::EdgeTo { to, edge_id } => {
                write_multi(&mut out, prefix::EDGE_TO, &[to], edge_id);
            }
            Self::PropertyIndex {
                type_,
                field,
                value,
                node_id,
            } => {
                write_multi(&mut out, prefix::PROPERTY, &[type_, field, value], node_id);
            }
            Self::FulltextIndex {
                type_,
                field,
                token,
                node_id,
            } => {
                write_multi(&mut out, prefix::FULLTEXT, &[type_, field, token], node_id);
            }
            Self::VectorIndex {
                type_,
                field,
                node_id,
            } => {
                write_multi(&mut out, prefix::VECTOR, &[type_, field], node_id);
            }
            Self::Schema { type_ } => write_single(&mut out, prefix::SCHEMA, type_),
            Self::Transaction { tx_id } => write_single(&mut out, prefix::TRANSACTION, tx_id),
        }
        out.freeze()
    }

    /// Decode an encoded key back into its structured form.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.len() < 3 || bytes[0] != b'/' {
            return Err(DecodeError::TooShort { len: bytes.len() });
        }

        // Match by the 2nd byte, then disambiguate /e/ vs /ef/ vs
        // /et/ and /ft/ on the 3rd byte.
        match bytes[1] {
            b'm' if bytes[2] == b'/' => Ok(Self::Metadata {
                tail: Bytes::copy_from_slice(&bytes[3..]),
            }),
            b'n' if bytes[2] == b'/' => Ok(Self::Node {
                node_id: Bytes::copy_from_slice(&bytes[3..]),
            }),
            b'i' if bytes[2] == b'/' => Ok(Self::NodeIndex {
                node_id: Bytes::copy_from_slice(&bytes[3..]),
            }),
            b'e' if bytes[2] == b'/' => Ok(Self::Edge {
                edge_id: Bytes::copy_from_slice(&bytes[3..]),
            }),
            b'e' if bytes.len() >= 4 && bytes[2] == b'f' && bytes[3] == b'/' => {
                let (segs, last) = split_segments(&bytes[4..], 1)?;
                Ok(Self::EdgeFrom {
                    from: segs.into_iter().next().expect("requested 1 segment"),
                    edge_id: last,
                })
            }
            b'e' if bytes.len() >= 4 && bytes[2] == b't' && bytes[3] == b'/' => {
                let (segs, last) = split_segments(&bytes[4..], 1)?;
                Ok(Self::EdgeTo {
                    to: segs.into_iter().next().expect("requested 1 segment"),
                    edge_id: last,
                })
            }
            b'p' if bytes[2] == b'/' => {
                let (segs, last) = split_segments(&bytes[3..], 3)?;
                let mut it = segs.into_iter();
                Ok(Self::PropertyIndex {
                    type_: it.next().expect("requested 3 segments"),
                    field: it.next().expect("requested 3 segments"),
                    value: it.next().expect("requested 3 segments"),
                    node_id: last,
                })
            }
            b'f' if bytes.len() >= 4 && bytes[2] == b't' && bytes[3] == b'/' => {
                let (segs, last) = split_segments(&bytes[4..], 3)?;
                let mut it = segs.into_iter();
                Ok(Self::FulltextIndex {
                    type_: it.next().expect("requested 3 segments"),
                    field: it.next().expect("requested 3 segments"),
                    token: it.next().expect("requested 3 segments"),
                    node_id: last,
                })
            }
            b'v' if bytes[2] == b'/' => {
                let (segs, last) = split_segments(&bytes[3..], 2)?;
                let mut it = segs.into_iter();
                Ok(Self::VectorIndex {
                    type_: it.next().expect("requested 2 segments"),
                    field: it.next().expect("requested 2 segments"),
                    node_id: last,
                })
            }
            b's' if bytes[2] == b'/' => Ok(Self::Schema {
                type_: Bytes::copy_from_slice(&bytes[3..]),
            }),
            b'x' if bytes[2] == b'/' => Ok(Self::Transaction {
                tx_id: Bytes::copy_from_slice(&bytes[3..]),
            }),
            _ => Err(DecodeError::UnknownPrefix(Bytes::copy_from_slice(
                &bytes[..bytes.len().min(4)],
            ))),
        }
    }

    fn encoded_len_hint(&self) -> usize {
        match self {
            Self::Metadata { tail } => 3 + tail.len(),
            Self::Node { node_id } | Self::NodeIndex { node_id } => 3 + node_id.len(),
            Self::Edge { edge_id } => 3 + edge_id.len(),
            Self::EdgeFrom { from, edge_id } => 4 + from.len() + 2 + edge_id.len(),
            Self::EdgeTo { to, edge_id } => 4 + to.len() + 2 + edge_id.len(),
            Self::PropertyIndex {
                type_,
                field,
                value,
                node_id,
            } => 3 + type_.len() + field.len() + value.len() + 6 + node_id.len(),
            Self::FulltextIndex {
                type_,
                field,
                token,
                node_id,
            } => 4 + type_.len() + field.len() + token.len() + 6 + node_id.len(),
            Self::VectorIndex {
                type_,
                field,
                node_id,
            } => 3 + type_.len() + field.len() + 4 + node_id.len(),
            Self::Schema { type_ } => 3 + type_.len(),
            Self::Transaction { tx_id } => 3 + tx_id.len(),
        }
    }
}

/// Build a prefix that bounds all [`Key::Node`] entries. Useful for
/// range-scanning the entire node space.
pub fn node_prefix() -> Bytes {
    Bytes::from_static(prefix::NODE)
}

/// Build a prefix that bounds all [`Key::Edge`] entries.
pub fn edge_prefix() -> Bytes {
    Bytes::from_static(prefix::EDGE)
}

/// Build a prefix that bounds the outgoing-edge index for a single
/// source node. Scanning this prefix yields every `Key::EdgeFrom`
/// whose `from` equals `from_id`.
pub fn edge_from_prefix(from_id: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(4 + from_id.len() + 2);
    out.put_slice(prefix::EDGE_FROM);
    escape_into(&mut out, from_id);
    out.put_slice(&[NUL, TERMINATOR]);
    out.freeze()
}

/// Build a prefix that bounds the incoming-edge index for a single
/// target node.
pub fn edge_to_prefix(to_id: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(4 + to_id.len() + 2);
    out.put_slice(prefix::EDGE_TO);
    escape_into(&mut out, to_id);
    out.put_slice(&[NUL, TERMINATOR]);
    out.freeze()
}

/// Build a prefix that bounds a full property-index equality lookup:
/// every [`Key::PropertyIndex`] entry with the given `(type, field,
/// value)` scans under this prefix.
pub fn property_equality_prefix(type_: &[u8], field: &[u8], value: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(
        prefix::PROPERTY.len() + type_.len() + field.len() + value.len() + 6,
    );
    out.put_slice(prefix::PROPERTY);
    escape_into(&mut out, type_);
    out.put_slice(&[NUL, TERMINATOR]);
    escape_into(&mut out, field);
    out.put_slice(&[NUL, TERMINATOR]);
    escape_into(&mut out, value);
    out.put_slice(&[NUL, TERMINATOR]);
    out.freeze()
}

/// Build a prefix that bounds every property-index entry for a single
/// `(type, field)` pair. A `BETWEEN`-style range scan on a property
/// uses this as its outer prefix.
pub fn property_field_prefix(type_: &[u8], field: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(prefix::PROPERTY.len() + type_.len() + field.len() + 4);
    out.put_slice(prefix::PROPERTY);
    escape_into(&mut out, type_);
    out.put_slice(&[NUL, TERMINATOR]);
    escape_into(&mut out, field);
    out.put_slice(&[NUL, TERMINATOR]);
    out.freeze()
}

/// Build a prefix that bounds every full-text posting-list entry for
/// a single `(type, field, token)` triple. The full-text query
/// executor scans this range to fetch all docs containing a token.
pub fn fulltext_token_prefix(type_: &[u8], field: &[u8], token: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(
        prefix::FULLTEXT.len() + type_.len() + field.len() + token.len() + 6,
    );
    out.put_slice(prefix::FULLTEXT);
    escape_into(&mut out, type_);
    out.put_slice(&[NUL, TERMINATOR]);
    escape_into(&mut out, field);
    out.put_slice(&[NUL, TERMINATOR]);
    escape_into(&mut out, token);
    out.put_slice(&[NUL, TERMINATOR]);
    out.freeze()
}

/// Build a prefix that bounds every vector-index entry for a single
/// `(type, field)` pair.
pub fn vector_field_prefix(type_: &[u8], field: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(prefix::VECTOR.len() + type_.len() + field.len() + 4);
    out.put_slice(prefix::VECTOR);
    escape_into(&mut out, type_);
    out.put_slice(&[NUL, TERMINATOR]);
    escape_into(&mut out, field);
    out.put_slice(&[NUL, TERMINATOR]);
    out.freeze()
}

// ---------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------

fn write_single(out: &mut BytesMut, prefix: &[u8], payload: &[u8]) {
    out.put_slice(prefix);
    out.put_slice(payload);
}

fn write_multi(out: &mut BytesMut, prefix: &[u8], head: &[&Bytes], last: &Bytes) {
    out.put_slice(prefix);
    for seg in head {
        escape_into(out, seg);
        out.put_slice(&[NUL, TERMINATOR]);
    }
    out.put_slice(last);
}

fn escape_into(out: &mut BytesMut, segment: &[u8]) {
    for &b in segment {
        if b == NUL {
            out.put_slice(&[NUL, ESCAPE]);
        } else {
            out.put_u8(b);
        }
    }
}

/// Parse `n` escape-encoded segments from `body`, followed by a raw
/// last segment. Returns the `n` parsed segments and the remainder.
fn split_segments(body: &[u8], n: usize) -> Result<(Vec<Bytes>, Bytes), DecodeError> {
    let mut segments = Vec::with_capacity(n);
    let mut current = BytesMut::new();
    let mut i = 0;
    let mut segment_idx = 0;

    while segment_idx < n {
        if i >= body.len() {
            return Err(DecodeError::MissingTerminator { index: segment_idx });
        }
        let b = body[i];
        if b == NUL {
            // Peek at the next byte to classify the 2-byte escape pair.
            if i + 1 >= body.len() {
                return Err(DecodeError::MalformedEscape { offset: i, next: 0 });
            }
            match body[i + 1] {
                ESCAPE => {
                    current.put_u8(NUL);
                    i += 2;
                }
                TERMINATOR => {
                    segments.push(current.split().freeze());
                    i += 2;
                    segment_idx += 1;
                }
                other => {
                    return Err(DecodeError::MalformedEscape {
                        offset: i,
                        next: other,
                    })
                }
            }
        } else {
            current.put_u8(b);
            i += 1;
        }
    }

    let tail = Bytes::copy_from_slice(&body[i..]);
    Ok((segments, tail))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &[u8]) -> Bytes {
        Bytes::copy_from_slice(s)
    }

    fn node(id: u8) -> Bytes {
        // Distinct 16-byte UUIDs for sort-order tests. We fill the
        // first byte with `id` so a byte-wise compare on the UUIDs
        // is a compare on `id`.
        let mut v = [0u8; 16];
        v[0] = id;
        Bytes::copy_from_slice(&v)
    }

    // -------- round-trip --------

    #[test]
    fn node_round_trips() {
        let k = Key::Node { node_id: b(b"abc") };
        let encoded = k.encode();
        assert_eq!(&encoded[..3], prefix::NODE);
        assert_eq!(Key::decode(&encoded).unwrap(), k);
    }

    #[test]
    fn edge_from_round_trips() {
        let k = Key::EdgeFrom {
            from: b(b"source"),
            edge_id: b(b"edge-007"),
        };
        let encoded = k.encode();
        assert_eq!(&encoded[..4], prefix::EDGE_FROM);
        assert_eq!(Key::decode(&encoded).unwrap(), k);
    }

    #[test]
    fn property_index_round_trips() {
        let k = Key::PropertyIndex {
            type_: b(b"user"),
            field: b(b"age"),
            value: b(b"42"),
            node_id: b(b"alice"),
        };
        let encoded = k.encode();
        assert_eq!(&encoded[..3], prefix::PROPERTY);
        assert_eq!(Key::decode(&encoded).unwrap(), k);
    }

    #[test]
    fn fulltext_round_trips_with_embedded_nulls() {
        // Exercise the escape path: every variable segment contains a
        // 0x00 byte that must survive encode->decode round-trip.
        let k = Key::FulltextIndex {
            type_: b(b"doc\x00type"),
            field: b(b"body\x00"),
            token: b(b"\x00hello"),
            node_id: b(b"\x00\x00\x00"),
        };
        let encoded = k.encode();
        assert_eq!(Key::decode(&encoded).unwrap(), k);
    }

    #[test]
    fn vector_round_trips() {
        let k = Key::VectorIndex {
            type_: b(b"image"),
            field: b(b"embedding"),
            node_id: b(b"img-001"),
        };
        assert_eq!(Key::decode(&k.encode()).unwrap(), k);
    }

    #[test]
    fn schema_and_transaction_round_trip() {
        assert_eq!(
            Key::decode(&Key::Schema { type_: b(b"post") }.encode()).unwrap(),
            Key::Schema { type_: b(b"post") }
        );
        assert_eq!(
            Key::decode(&Key::Transaction { tx_id: b(b"tx-42") }.encode()).unwrap(),
            Key::Transaction { tx_id: b(b"tx-42") }
        );
    }

    #[test]
    fn all_eleven_variants_round_trip() {
        // Belt-and-suspenders: tickle every variant so a future kind
        // that forgets encode/decode symmetry fails the suite
        // loudly.
        let keys = vec![
            Key::Metadata {
                tail: b(b"cluster/ranges/7"),
            },
            Key::Node {
                node_id: b(b"node-1"),
            },
            Key::NodeIndex {
                node_id: b(b"node-1"),
            },
            Key::Edge {
                edge_id: b(b"edge-1"),
            },
            Key::EdgeFrom {
                from: b(b"src"),
                edge_id: b(b"edge-1"),
            },
            Key::EdgeTo {
                to: b(b"dst"),
                edge_id: b(b"edge-1"),
            },
            Key::PropertyIndex {
                type_: b(b"user"),
                field: b(b"name"),
                value: b(b"alice"),
                node_id: b(b"n1"),
            },
            Key::FulltextIndex {
                type_: b(b"post"),
                field: b(b"body"),
                token: b(b"hello"),
                node_id: b(b"p1"),
            },
            Key::VectorIndex {
                type_: b(b"image"),
                field: b(b"embedding"),
                node_id: b(b"i1"),
            },
            Key::Schema { type_: b(b"user") },
            Key::Transaction { tx_id: b(b"tx-1") },
        ];
        for k in keys {
            assert_eq!(
                Key::decode(&k.encode()).unwrap(),
                k,
                "round-trip failed for {k:?}"
            );
        }
    }

    // -------- lex sort --------

    #[test]
    fn prefixes_partition_keyspace() {
        // Every model's encoded keys should byte-sort into a
        // contiguous block. Because the prefixes are distinct
        // ASCII strings, the starting byte of each prefix orders the
        // partitions. This test captures the contract so a future
        // refactor that changes a prefix can't silently break
        // range placement.
        let mut encoded: Vec<(Bytes, &str)> = vec![
            (Key::Metadata { tail: b(b"x") }.encode(), "metadata"),
            (Key::Node { node_id: b(b"x") }.encode(), "node"),
            (Key::NodeIndex { node_id: b(b"x") }.encode(), "node_index"),
            (Key::Edge { edge_id: b(b"x") }.encode(), "edge"),
            (
                Key::EdgeFrom {
                    from: b(b"x"),
                    edge_id: b(b"x"),
                }
                .encode(),
                "edge_from",
            ),
            (
                Key::EdgeTo {
                    to: b(b"x"),
                    edge_id: b(b"x"),
                }
                .encode(),
                "edge_to",
            ),
            (
                Key::PropertyIndex {
                    type_: b(b"x"),
                    field: b(b"x"),
                    value: b(b"x"),
                    node_id: b(b"x"),
                }
                .encode(),
                "property",
            ),
            (
                Key::FulltextIndex {
                    type_: b(b"x"),
                    field: b(b"x"),
                    token: b(b"x"),
                    node_id: b(b"x"),
                }
                .encode(),
                "fulltext",
            ),
            (
                Key::VectorIndex {
                    type_: b(b"x"),
                    field: b(b"x"),
                    node_id: b(b"x"),
                }
                .encode(),
                "vector",
            ),
            (Key::Schema { type_: b(b"x") }.encode(), "schema"),
            (Key::Transaction { tx_id: b(b"x") }.encode(), "transaction"),
        ];
        encoded.sort_by(|a, b| a.0.cmp(&b.0));

        // Observed order should match the prefix-byte order.
        // /e/ < /ef/ < /et/ < /ft/ < /i/ < /m/ < /n/ < /p/ < /s/ < /v/ < /x/
        let observed: Vec<&str> = encoded.iter().map(|(_, name)| *name).collect();
        assert_eq!(
            observed,
            vec![
                "edge",
                "edge_from",
                "edge_to",
                "fulltext",
                "node_index",
                "metadata",
                "node",
                "property",
                "schema",
                "vector",
                "transaction",
            ],
            "prefix byte-order changed; update partition contract"
        );
    }

    #[test]
    fn property_index_sorts_by_value_then_node() {
        // Same (type, field), distinct values: lex sort on value.
        let k_low = Key::PropertyIndex {
            type_: b(b"user"),
            field: b(b"age"),
            value: b(b"20"),
            node_id: node(1),
        }
        .encode();
        let k_high = Key::PropertyIndex {
            type_: b(b"user"),
            field: b(b"age"),
            value: b(b"30"),
            node_id: node(0),
        }
        .encode();
        assert!(
            k_low < k_high,
            "lex sort should follow value ordering (20 before 30)"
        );

        // Same (type, field, value): lex sort on node_id.
        let k_a = Key::PropertyIndex {
            type_: b(b"user"),
            field: b(b"age"),
            value: b(b"25"),
            node_id: node(1),
        }
        .encode();
        let k_b = Key::PropertyIndex {
            type_: b(b"user"),
            field: b(b"age"),
            value: b(b"25"),
            node_id: node(2),
        }
        .encode();
        assert!(
            k_a < k_b,
            "lex sort should follow node_id within equal value"
        );
    }

    #[test]
    fn edge_from_sorts_by_source_then_edge_id() {
        let a = Key::EdgeFrom {
            from: node(1),
            edge_id: node(9),
        }
        .encode();
        let b_ = Key::EdgeFrom {
            from: node(2),
            edge_id: node(0),
        }
        .encode();
        assert!(a < b_, "outer source node drives sort order");

        let c = Key::EdgeFrom {
            from: node(2),
            edge_id: node(0),
        }
        .encode();
        let d = Key::EdgeFrom {
            from: node(2),
            edge_id: node(1),
        }
        .encode();
        assert!(c < d, "edge_id resolves ties on source");
    }

    #[test]
    fn fulltext_token_collides_with_no_other_token() {
        // Two (token, node_id) pairs where one naive concatenation
        // scheme would accidentally merge them. Encoded correctly
        // they must differ.
        let k1 = Key::FulltextIndex {
            type_: b(b"t"),
            field: b(b"f"),
            token: b(b"ab"),
            node_id: b(b"cd"),
        }
        .encode();
        let k2 = Key::FulltextIndex {
            type_: b(b"t"),
            field: b(b"f"),
            token: b(b"abc"),
            node_id: b(b"d"),
        }
        .encode();
        assert_ne!(k1, k2, "escape+terminator must disambiguate segment splits");
        // Both round-trip.
        assert_eq!(
            Key::decode(&k1).unwrap(),
            Key::FulltextIndex {
                type_: b(b"t"),
                field: b(b"f"),
                token: b(b"ab"),
                node_id: b(b"cd"),
            }
        );
        assert_eq!(
            Key::decode(&k2).unwrap(),
            Key::FulltextIndex {
                type_: b(b"t"),
                field: b(b"f"),
                token: b(b"abc"),
                node_id: b(b"d"),
            }
        );
    }

    // -------- prefix helpers --------

    #[test]
    fn edge_from_prefix_bounds_outgoing_edges() {
        let from = node(7);
        let pref = edge_from_prefix(&from);

        // Two edges for this source should both start with the prefix.
        let e1 = Key::EdgeFrom {
            from: from.clone(),
            edge_id: node(1),
        }
        .encode();
        let e2 = Key::EdgeFrom {
            from: from.clone(),
            edge_id: node(2),
        }
        .encode();
        assert!(
            e1.starts_with(&pref),
            "encoded edge-from must start with the from-prefix"
        );
        assert!(e2.starts_with(&pref));

        // An edge for a different source must not.
        let other = Key::EdgeFrom {
            from: node(8),
            edge_id: node(1),
        }
        .encode();
        assert!(!other.starts_with(&pref));
    }

    #[test]
    fn property_equality_prefix_bounds_only_matching_entries() {
        let pref = property_equality_prefix(b"user", b"age", b"25");
        let match_ = Key::PropertyIndex {
            type_: b(b"user"),
            field: b(b"age"),
            value: b(b"25"),
            node_id: b(b"alice"),
        }
        .encode();
        let non_match = Key::PropertyIndex {
            type_: b(b"user"),
            field: b(b"age"),
            value: b(b"26"),
            node_id: b(b"alice"),
        }
        .encode();
        assert!(match_.starts_with(&pref));
        assert!(!non_match.starts_with(&pref));
    }

    #[test]
    fn property_field_prefix_is_parent_of_equality_prefix() {
        let field = property_field_prefix(b"user", b"age");
        let eq = property_equality_prefix(b"user", b"age", b"25");
        assert!(eq.starts_with(&field));

        // An entry for a different field must not match.
        let other_field = Key::PropertyIndex {
            type_: b(b"user"),
            field: b(b"name"),
            value: b(b"alice"),
            node_id: b(b"a"),
        }
        .encode();
        assert!(!other_field.starts_with(&field));
    }

    #[test]
    fn fulltext_token_prefix_matches_every_doc_for_token() {
        let pref = fulltext_token_prefix(b"post", b"body", b"hello");
        let m1 = Key::FulltextIndex {
            type_: b(b"post"),
            field: b(b"body"),
            token: b(b"hello"),
            node_id: b(b"p1"),
        }
        .encode();
        let m2 = Key::FulltextIndex {
            type_: b(b"post"),
            field: b(b"body"),
            token: b(b"hello"),
            node_id: b(b"p2"),
        }
        .encode();
        let non_match = Key::FulltextIndex {
            type_: b(b"post"),
            field: b(b"body"),
            token: b(b"world"),
            node_id: b(b"p1"),
        }
        .encode();
        assert!(m1.starts_with(&pref));
        assert!(m2.starts_with(&pref));
        assert!(!non_match.starts_with(&pref));
    }

    #[test]
    fn vector_field_prefix_matches_every_entry_for_field() {
        let pref = vector_field_prefix(b"image", b"embedding");
        let m = Key::VectorIndex {
            type_: b(b"image"),
            field: b(b"embedding"),
            node_id: b(b"i1"),
        }
        .encode();
        let non_match = Key::VectorIndex {
            type_: b(b"image"),
            field: b(b"thumbnail"),
            node_id: b(b"i1"),
        }
        .encode();
        assert!(m.starts_with(&pref));
        assert!(!non_match.starts_with(&pref));
    }

    // -------- decode errors --------

    #[test]
    fn decode_rejects_too_short() {
        assert!(matches!(
            Key::decode(b"/"),
            Err(DecodeError::TooShort { len: 1 })
        ));
        assert!(matches!(
            Key::decode(b""),
            Err(DecodeError::TooShort { len: 0 })
        ));
    }

    #[test]
    fn decode_rejects_unknown_prefix() {
        assert!(matches!(
            Key::decode(b"/z/anything"),
            Err(DecodeError::UnknownPrefix(_))
        ));
    }

    #[test]
    fn decode_rejects_missing_terminator() {
        // /ef/<from> with no terminator at all — decoder should
        // report the segment it was looking for.
        let bad = b"/ef/justfrombytes";
        assert!(matches!(
            Key::decode(bad),
            Err(DecodeError::MissingTerminator { index: 0 })
        ));
    }

    #[test]
    fn decode_rejects_malformed_escape() {
        // 0x00 followed by something other than 0xff/0x01 is invalid.
        let mut bad = Vec::from(b"/ef/".as_slice());
        bad.extend_from_slice(&[0x00, 0x42]); // not 0xff, not 0x01
        bad.extend_from_slice(b"more");
        assert!(matches!(
            Key::decode(&bad),
            Err(DecodeError::MalformedEscape { next: 0x42, .. })
        ));
    }

    // -------- escape invariants --------

    #[test]
    fn encoded_non_final_segments_never_contain_unescaped_null() {
        // A non-final segment with a literal 0x00 byte must appear in
        // the encoded output as 0x00 0xff (never 0x00 followed by
        // anything else).
        let k = Key::PropertyIndex {
            type_: b(b"\x00type"),
            field: b(b"field"),
            value: b(b"value"),
            node_id: b(b"node"),
        };
        let encoded = k.encode();
        // Walk the first segment's escape output. We know the prefix
        // `/p/` is 3 bytes; the first segment's first byte is `0x00`.
        assert_eq!(encoded[3], 0x00);
        assert_eq!(encoded[4], 0xff, "0x00 must be escaped as 0x00 0xff");
    }

    #[test]
    fn zero_length_segments_round_trip() {
        let k = Key::PropertyIndex {
            type_: b(b""),
            field: b(b"field"),
            value: b(b""),
            node_id: b(b""),
        };
        let encoded = k.encode();
        assert_eq!(Key::decode(&encoded).unwrap(), k);
    }

    #[test]
    fn last_segment_may_contain_null_bytes() {
        // The terminal segment is written raw, so a node_id starting
        // with 0x00 is allowed and must round-trip.
        let k = Key::Node {
            node_id: b(b"\x00\x00\x00"),
        };
        assert_eq!(Key::decode(&k.encode()).unwrap(), k);
    }
}

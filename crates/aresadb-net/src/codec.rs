//! bincode helpers shared by the server and client sides.
//!
//! Keeping them in one place guarantees the two ends of an RPC stay
//! in lock-step: both sides go through [`encode`] / [`decode`] for
//! both requests and responses, so a schema mismatch surfaces as a
//! single compile error rather than a silent protocol drift.

use serde::{de::DeserializeOwned, Serialize};

use thiserror::Error;

/// Errors that can be produced by encoding or decoding a bincode
/// payload. Kept separate from the openraft / tonic error types so
/// call sites can distinguish "transport mangled the bytes" from
/// "Raft said no".
#[derive(Debug, Error)]
pub enum CodecError {
    #[error("bincode encode error: {0}")]
    Encode(#[source] bincode::Error),

    #[error("bincode decode error: {0}")]
    Decode(#[source] bincode::Error),
}

/// Serialize a value with bincode.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    bincode::serialize(value).map_err(CodecError::Encode)
}

/// Deserialize a value with bincode.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    bincode::deserialize(bytes).map_err(CodecError::Decode)
}

/// Convert a [`CodecError`] into a `tonic::Status` so gRPC handlers
/// can use `?` without rolling a bespoke error type at every call
/// site. The mapping is chosen so a bad payload does not look like a
/// transient failure — replaying it will not help.
pub fn to_status(err: CodecError) -> tonic::Status {
    tonic::Status::invalid_argument(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_preserves_struct() {
        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        struct Msg {
            a: u64,
            b: String,
        }
        let m = Msg {
            a: 42,
            b: "hello".into(),
        };
        let bytes = encode(&m).unwrap();
        let back: Msg = decode(&bytes).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn bad_bytes_surface_as_decode_error() {
        let r: Result<u64, _> = decode(&[]);
        assert!(matches!(r, Err(CodecError::Decode(_))));
    }
}

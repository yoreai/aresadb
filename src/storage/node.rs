//! Node and Edge types - the core data model
//!
//! AresaDB uses a property graph model where:
//! - Nodes represent entities with typed properties
//! - Edges represent relationships between nodes

#![allow(dead_code)]

use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use rkyv::{Archive, Deserialize, Serialize};
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use std::collections::BTreeMap;
use std::fmt;
use uuid::Uuid;

// Re-export serde for external use
pub use serde;

/// Unique identifier for a node
#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct NodeId {
    /// Raw UUID bytes (big-endian)
    pub uuid: [u8; 16],
}

impl NodeId {
    /// Create a new random NodeId
    pub fn new() -> Self {
        Self {
            uuid: *Uuid::new_v4().as_bytes(),
        }
    }

    /// Parse a NodeId from a string (UUID format or type:uuid format)
    pub fn parse(s: &str) -> Result<Self> {
        // Support both "uuid" and "type:uuid" formats
        let uuid_str = if s.contains(':') {
            s.split(':').next_back().unwrap_or(s)
        } else if s.contains('/') {
            s.split('/').next_back().unwrap_or(s)
        } else {
            s
        };

        let uuid =
            Uuid::parse_str(uuid_str).map_err(|_| anyhow::anyhow!("Invalid node ID: {}", s))?;

        Ok(Self {
            uuid: *uuid.as_bytes(),
        })
    }

    /// Get as UUID
    pub fn as_uuid(&self) -> Uuid {
        Uuid::from_bytes(self.uuid)
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_uuid())
    }
}

impl SerdeSerialize for NodeId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> SerdeDeserialize<'de> for NodeId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = <String as SerdeDeserialize>::deserialize(deserializer)?;
        NodeId::parse(&s).map_err(serde::de::Error::custom)
    }
}

/// Unique identifier for an edge
#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct EdgeId {
    /// Raw UUID bytes (big-endian)
    pub uuid: [u8; 16],
}

impl EdgeId {
    /// Create a new random EdgeId
    pub fn new() -> Self {
        Self {
            uuid: *Uuid::new_v4().as_bytes(),
        }
    }

    /// Parse an EdgeId from a string
    pub fn parse(s: &str) -> Result<Self> {
        let uuid = Uuid::parse_str(s).map_err(|_| anyhow::anyhow!("Invalid edge ID: {}", s))?;

        Ok(Self {
            uuid: *uuid.as_bytes(),
        })
    }

    /// Get as UUID
    pub fn as_uuid(&self) -> Uuid {
        Uuid::from_bytes(self.uuid)
    }
}

impl Default for EdgeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for EdgeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_uuid())
    }
}

impl SerdeSerialize for EdgeId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> SerdeDeserialize<'de> for EdgeId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = <String as SerdeDeserialize>::deserialize(deserializer)?;
        EdgeId::parse(&s).map_err(serde::de::Error::custom)
    }
}

/// Timestamp wrapper for serialization
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct Timestamp {
    /// Milliseconds since Unix epoch (UTC)
    pub millis: i64,
}

impl Timestamp {
    /// Create timestamp for current time
    pub fn now() -> Self {
        Self {
            millis: Utc::now().timestamp_millis(),
        }
    }

    /// Create from DateTime
    pub fn from_datetime(dt: DateTime<Utc>) -> Self {
        Self {
            millis: dt.timestamp_millis(),
        }
    }

    /// Convert to DateTime
    #[allow(clippy::wrong_self_convention)]
    pub fn to_datetime(&self) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(self.millis).unwrap_or_default()
    }
}

impl Default for Timestamp {
    fn default() -> Self {
        Self::now()
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_datetime().format("%Y-%m-%d %H:%M:%S"))
    }
}

impl SerdeSerialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i64(self.millis)
    }
}

impl<'de> SerdeDeserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let millis = <i64 as SerdeDeserialize>::deserialize(deserializer)?;
        Ok(Self { millis })
    }
}

/// A flexible value type that can hold any data
///
/// Note: We use serde for serialization instead of rkyv for the Value type
/// because rkyv has issues with recursive types. The performance impact is
/// minimal since we batch serialize nodes/edges anyway.
#[derive(Debug, Clone, PartialEq, Default, SerdeSerialize, SerdeDeserialize)]
#[serde(untagged)]
pub enum Value {
    /// JSON null
    #[default]
    Null,
    /// Boolean value
    Bool(bool),
    /// 64-bit signed integer
    Int(i64),
    /// 64-bit floating point number
    Float(f64),
    /// UTF-8 string
    String(String),
    /// Raw binary data
    Bytes(Vec<u8>),
    /// Vector embedding for similarity search (RAG/ML)
    Vector(Vec<f32>),
    /// Ordered list of values
    Array(Vec<Value>),
    /// Sorted key-value map
    Object(BTreeMap<String, Value>),
}

/// Distance metrics for vector similarity search
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    /// Cosine similarity (1 - cosine_distance)
    Cosine,
    /// Euclidean (L2) distance
    Euclidean,
    /// Dot product (inner product)
    DotProduct,
    /// Manhattan (L1) distance
    Manhattan,
}

/// Result of a similarity search
#[derive(Debug, Clone, SerdeSerialize, SerdeDeserialize)]
pub struct SimilarityResult {
    /// ID of the matched node
    pub node_id: NodeId,
    /// Similarity score (metric-dependent; higher is more similar)
    pub score: f64,
    /// Raw distance from query vector (lower is closer)
    pub distance: f64,
}

impl Value {
    /// Convert from serde_json::Value
    pub fn from_json(v: serde_json::Value) -> Result<Self> {
        match v {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::Bool(b) => Ok(Value::Bool(b)),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Value::Int(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(Value::Float(f))
                } else {
                    bail!("Invalid number: {}", n)
                }
            }
            serde_json::Value::String(s) => Ok(Value::String(s)),
            serde_json::Value::Array(arr) => {
                // Check if this looks like a vector (array of numbers)
                // Use a special object format for explicit vectors
                let values: Result<Vec<_>> = arr.into_iter().map(Value::from_json).collect();
                Ok(Value::Array(values?))
            }
            serde_json::Value::Object(obj) => {
                // Check for special vector format: {"$vector": [1.0, 2.0, 3.0]}
                if obj.len() == 1 {
                    if let Some(serde_json::Value::Array(arr)) = obj.get("$vector") {
                        let floats: Result<Vec<f32>> = arr
                            .iter()
                            .map(|v| {
                                v.as_f64()
                                    .map(|f| f as f32)
                                    .ok_or_else(|| anyhow::anyhow!("Vector must contain numbers"))
                            })
                            .collect();
                        return Ok(Value::Vector(floats?));
                    }
                }

                let map: Result<BTreeMap<_, _>> = obj
                    .into_iter()
                    .map(|(k, v)| Value::from_json(v).map(|v| (k, v)))
                    .collect();
                Ok(Value::Object(map?))
            }
        }
    }

    /// Create a Value from a vector of f32
    pub fn from_vector(v: Vec<f32>) -> Self {
        Value::Vector(v)
    }

    /// Create a Value from a slice of f32
    pub fn from_vector_slice(v: &[f32]) -> Self {
        Value::Vector(v.to_vec())
    }

    /// Convert to serde_json::Value
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Int(i) => serde_json::Value::Number((*i).into()),
            Value::Float(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Bytes(b) => serde_json::Value::String(base64::encode(b)),
            Value::Vector(v) => {
                // Encode as special object format for round-trip
                let arr: Vec<serde_json::Value> = v
                    .iter()
                    .map(|f| {
                        serde_json::Number::from_f64(*f as f64)
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::Null)
                    })
                    .collect();
                let mut map = serde_json::Map::new();
                map.insert("$vector".to_string(), serde_json::Value::Array(arr));
                serde_json::Value::Object(map)
            }
            Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(|v| v.to_json()).collect())
            }
            Value::Object(obj) => {
                let map: serde_json::Map<_, _> =
                    obj.iter().map(|(k, v)| (k.clone(), v.to_json())).collect();
                serde_json::Value::Object(map)
            }
        }
    }

    /// Get as string if possible
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Get as i64 if possible
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            Value::Float(f) => Some(*f as i64),
            _ => None,
        }
    }

    /// Get as f64 if possible
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Int(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Get as bool if possible
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Get a field from an object
    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Object(obj) => obj.get(key),
            _ => None,
        }
    }

    /// Check if value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Check if value is a vector
    pub fn is_vector(&self) -> bool {
        matches!(self, Value::Vector(_))
    }

    /// Get as vector if possible
    pub fn as_vector(&self) -> Option<&[f32]> {
        match self {
            Value::Vector(v) => Some(v),
            _ => None,
        }
    }

    /// Create a vector value
    pub fn vector(data: Vec<f32>) -> Self {
        Value::Vector(data)
    }

    /// Get vector dimension (or None if not a vector)
    pub fn vector_dimension(&self) -> Option<usize> {
        match self {
            Value::Vector(v) => Some(v.len()),
            _ => None,
        }
    }
}

impl Value {
    /// Compute cosine similarity between two vectors
    /// Returns a value between -1 and 1 (1 = identical, 0 = orthogonal, -1 = opposite)
    pub fn cosine_similarity(a: &[f32], b: &[f32]) -> Option<f64> {
        if a.len() != b.len() || a.is_empty() {
            return None;
        }

        let mut dot = 0.0f64;
        let mut norm_a = 0.0f64;
        let mut norm_b = 0.0f64;

        for i in 0..a.len() {
            let ai = a[i] as f64;
            let bi = b[i] as f64;
            dot += ai * bi;
            norm_a += ai * ai;
            norm_b += bi * bi;
        }

        let magnitude = (norm_a * norm_b).sqrt();
        if magnitude == 0.0 {
            return None;
        }

        Some(dot / magnitude)
    }

    /// Compute euclidean (L2) distance between two vectors
    pub fn euclidean_distance(a: &[f32], b: &[f32]) -> Option<f64> {
        if a.len() != b.len() {
            return None;
        }

        let mut sum = 0.0f64;
        for i in 0..a.len() {
            let diff = (a[i] - b[i]) as f64;
            sum += diff * diff;
        }

        Some(sum.sqrt())
    }

    /// Compute dot product between two vectors
    pub fn dot_product(a: &[f32], b: &[f32]) -> Option<f64> {
        if a.len() != b.len() {
            return None;
        }

        let mut sum = 0.0f64;
        for i in 0..a.len() {
            sum += (a[i] as f64) * (b[i] as f64);
        }

        Some(sum)
    }

    /// Compute manhattan (L1) distance between two vectors
    pub fn manhattan_distance(a: &[f32], b: &[f32]) -> Option<f64> {
        if a.len() != b.len() {
            return None;
        }

        let mut sum = 0.0f64;
        for i in 0..a.len() {
            sum += ((a[i] - b[i]) as f64).abs();
        }

        Some(sum)
    }

    /// Normalize a vector to unit length
    pub fn normalize_vector(v: &[f32]) -> Vec<f32> {
        let norm: f64 = v.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
        if norm == 0.0 {
            return v.to_vec();
        }
        v.iter().map(|x| (*x as f64 / norm) as f32).collect()
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::String(s) => write!(f, "\"{}\"", s),
            Value::Bytes(b) => write!(f, "<{} bytes>", b.len()),
            Value::Vector(v) => write!(f, "<vector dim={}>", v.len()),
            Value::Array(arr) => {
                write!(f, "[")?;
                for (i, v) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")
            }
            Value::Object(obj) => {
                write!(f, "{{")?;
                for (i, (k, v)) in obj.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "\"{}\": {}", k, v)?;
                }
                write!(f, "}}")
            }
        }
    }
}

// Implement base64 encoding helper
mod base64 {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    pub fn encode(data: &[u8]) -> String {
        let mut result = String::new();
        for chunk in data.chunks(3) {
            let b0 = chunk[0] as usize;
            let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
            let b2 = chunk.get(2).copied().unwrap_or(0) as usize;

            result.push(ALPHABET[b0 >> 2] as char);
            result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

            if chunk.len() > 1 {
                result.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
            } else {
                result.push('=');
            }

            if chunk.len() > 2 {
                result.push(ALPHABET[b2 & 0x3f] as char);
            } else {
                result.push('=');
            }
        }
        result
    }
}

/// A node in the property graph
#[derive(Debug, Clone, SerdeSerialize, SerdeDeserialize)]
pub struct Node {
    /// Unique identifier
    pub id: NodeId,
    /// Node type (e.g., "user", "order", "product")
    pub node_type: String,
    /// Properties stored as key-value pairs
    pub properties: BTreeMap<String, Value>,
    /// Creation timestamp
    pub created_at: Timestamp,
    /// Last update timestamp
    pub updated_at: Timestamp,
}

impl Node {
    /// Create a new node
    pub fn new(node_type: &str, properties: Value) -> Self {
        let now = Timestamp::now();
        let props = match properties {
            Value::Object(obj) => obj,
            _ => {
                let mut map = BTreeMap::new();
                map.insert("value".to_string(), properties);
                map
            }
        };

        Self {
            id: NodeId::new(),
            node_type: node_type.to_string(),
            properties: props,
            created_at: now,
            updated_at: now,
        }
    }

    /// Create with a specific ID
    pub fn with_id(id: NodeId, node_type: &str, properties: BTreeMap<String, Value>) -> Self {
        let now = Timestamp::now();
        Self {
            id,
            node_type: node_type.to_string(),
            properties,
            created_at: now,
            updated_at: now,
        }
    }

    /// Get a property value
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.properties.get(key)
    }

    /// Set a property value
    pub fn set(&mut self, key: &str, value: Value) {
        self.properties.insert(key.to_string(), value);
        self.updated_at = Timestamp::now();
    }

    /// Remove a property
    pub fn remove(&mut self, key: &str) -> Option<Value> {
        self.updated_at = Timestamp::now();
        self.properties.remove(key)
    }

    /// Get all property keys
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.properties.keys()
    }

    /// Convert to JSON
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }
}

/// An edge connecting two nodes
#[derive(Debug, Clone, SerdeSerialize, SerdeDeserialize)]
pub struct Edge {
    /// Unique identifier
    pub id: EdgeId,
    /// Source node ID
    pub from: NodeId,
    /// Target node ID
    pub to: NodeId,
    /// Edge type (e.g., "purchased", "follows", "belongs_to")
    pub edge_type: String,
    /// Properties on the edge
    pub properties: BTreeMap<String, Value>,
    /// Creation timestamp
    pub created_at: Timestamp,
}

impl Edge {
    /// Create a new edge
    pub fn new(from: NodeId, to: NodeId, edge_type: &str, properties: Value) -> Self {
        let props = match properties {
            Value::Object(obj) => obj,
            Value::Null => BTreeMap::new(),
            _ => {
                let mut map = BTreeMap::new();
                map.insert("value".to_string(), properties);
                map
            }
        };

        Self {
            id: EdgeId::new(),
            from,
            to,
            edge_type: edge_type.to_string(),
            properties: props,
            created_at: Timestamp::now(),
        }
    }

    /// Get a property value
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.properties.get(key)
    }

    /// Set a property value
    pub fn set(&mut self, key: &str, value: Value) {
        self.properties.insert(key.to_string(), value);
    }

    /// Convert to JSON
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_creation() {
        let props = Value::from_json(serde_json::json!({
            "name": "John",
            "age": 30
        }))
        .unwrap();

        let node = Node::new("user", props);

        assert_eq!(node.node_type, "user");
        assert_eq!(node.get("name").unwrap().as_str(), Some("John"));
        assert_eq!(node.get("age").unwrap().as_int(), Some(30));
    }

    #[test]
    fn test_edge_creation() {
        let from = NodeId::new();
        let to = NodeId::new();

        let edge = Edge::new(from.clone(), to.clone(), "follows", Value::Null);

        assert_eq!(edge.from, from);
        assert_eq!(edge.to, to);
        assert_eq!(edge.edge_type, "follows");
    }

    #[test]
    fn test_value_conversion() {
        let json = serde_json::json!({
            "string": "hello",
            "number": 42,
            "float": 3.14,
            "bool": true,
            "null": null,
            "array": [1, 2, 3],
            "nested": {"a": 1}
        });

        let value = Value::from_json(json.clone()).unwrap();
        let back = value.to_json();

        assert_eq!(json, back);
    }

    #[test]
    fn test_node_id_parsing() {
        let id = NodeId::new();
        let str_id = id.to_string();

        let parsed = NodeId::parse(&str_id).unwrap();
        assert_eq!(id, parsed);

        // Test with type prefix
        let with_prefix = format!("user:{}", str_id);
        let parsed2 = NodeId::parse(&with_prefix).unwrap();
        assert_eq!(id, parsed2);
    }

    #[test]
    fn test_vector_creation() {
        let v = Value::vector(vec![1.0, 2.0, 3.0]);
        assert!(v.is_vector());
        assert_eq!(v.vector_dimension(), Some(3));
        assert_eq!(v.as_vector(), Some(&[1.0, 2.0, 3.0][..]));
    }

    #[test]
    fn test_vector_json_roundtrip() {
        let v = Value::vector(vec![1.0, 2.0, 3.0]);
        let json = v.to_json();
        let parsed = Value::from_json(json).unwrap();
        assert_eq!(v, parsed);
    }

    #[test]
    fn test_cosine_similarity() {
        // Identical vectors should have similarity 1.0
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        let sim = Value::cosine_similarity(&a, &b).unwrap();
        assert!((sim - 1.0).abs() < 1e-6);

        // Orthogonal vectors should have similarity 0.0
        let c = vec![0.0, 1.0, 0.0];
        let sim2 = Value::cosine_similarity(&a, &c).unwrap();
        assert!(sim2.abs() < 1e-6);

        // Opposite vectors should have similarity -1.0
        let d = vec![-1.0, 0.0, 0.0];
        let sim3 = Value::cosine_similarity(&a, &d).unwrap();
        assert!((sim3 + 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_euclidean_distance() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        let dist = Value::euclidean_distance(&a, &b).unwrap();
        assert!((dist - 1.0).abs() < 1e-6);

        let c = vec![3.0, 4.0, 0.0];
        let dist2 = Value::euclidean_distance(&a, &c).unwrap();
        assert!((dist2 - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_dot_product() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        let dot = Value::dot_product(&a, &b).unwrap();
        assert!((dot - 32.0).abs() < 1e-6); // 1*4 + 2*5 + 3*6 = 32
    }

    #[test]
    fn test_normalize_vector() {
        let v = vec![3.0, 4.0];
        let normalized = Value::normalize_vector(&v);
        let norm: f64 = normalized
            .iter()
            .map(|x| (*x as f64).powi(2))
            .sum::<f64>()
            .sqrt();
        assert!((norm - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_vector_display() {
        let v = Value::vector(vec![1.0; 768]); // Common embedding dimension
        let display = format!("{}", v);
        assert_eq!(display, "<vector dim=768>");
    }
}

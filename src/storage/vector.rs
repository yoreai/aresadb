//! Vector storage and similarity search for RAG/ML applications
//!
//! This module provides efficient vector storage and similarity search
//! for building RAG (Retrieval-Augmented Generation) systems.

#![allow(dead_code)]

use crate::storage::{DistanceMetric, Node, NodeId, SimilarityResult, Value};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// A scored node for similarity ranking
#[derive(Debug, Clone)]
struct ScoredNode {
    node_id: NodeId,
    score: f64,
    distance: f64,
}

impl PartialEq for ScoredNode {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Eq for ScoredNode {}

impl PartialOrd for ScoredNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredNode {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap behavior (we want top-k highest scores)
        other
            .score
            .partial_cmp(&self.score)
            .unwrap_or(Ordering::Equal)
    }
}

/// Vector search engine for similarity queries
pub struct VectorSearch {
    metric: DistanceMetric,
}

impl VectorSearch {
    /// Create a new vector search engine
    pub fn new(metric: DistanceMetric) -> Self {
        Self { metric }
    }

    /// Compute similarity/distance between two vectors
    pub fn compute_similarity(&self, a: &[f32], b: &[f32]) -> Option<(f64, f64)> {
        match self.metric {
            DistanceMetric::Cosine => {
                let sim = Value::cosine_similarity(a, b)?;
                // Convert to distance: 1 - similarity (range 0-2)
                Some((sim, 1.0 - sim))
            }
            DistanceMetric::Euclidean => {
                let dist = Value::euclidean_distance(a, b)?;
                // Convert to similarity: 1 / (1 + distance)
                Some((1.0 / (1.0 + dist), dist))
            }
            DistanceMetric::DotProduct => {
                let dot = Value::dot_product(a, b)?;
                // Higher dot product = more similar
                Some((dot, -dot)) // Negative for distance ordering
            }
            DistanceMetric::Manhattan => {
                let dist = Value::manhattan_distance(a, b)?;
                Some((1.0 / (1.0 + dist), dist))
            }
        }
    }

    /// Find the k most similar nodes to a query vector
    pub fn search(
        &self,
        query: &[f32],
        nodes: &[Node],
        vector_field: &str,
        k: usize,
    ) -> Vec<SimilarityResult> {
        let mut heap = BinaryHeap::with_capacity(k + 1);

        for node in nodes {
            if let Some(Value::Vector(vec)) = node.properties.get(vector_field) {
                if let Some((score, distance)) = self.compute_similarity(query, vec) {
                    heap.push(ScoredNode {
                        node_id: node.id.clone(),
                        score,
                        distance,
                    });

                    // Keep only top k
                    if heap.len() > k {
                        heap.pop();
                    }
                }
            }
        }

        // Convert to results (sorted by score descending)
        let mut results: Vec<_> = heap
            .into_iter()
            .map(|sn| SimilarityResult {
                node_id: sn.node_id,
                score: sn.score,
                distance: sn.distance,
            })
            .collect();

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
        results
    }

    /// Find all nodes within a distance threshold
    pub fn search_radius(
        &self,
        query: &[f32],
        nodes: &[Node],
        vector_field: &str,
        max_distance: f64,
    ) -> Vec<SimilarityResult> {
        let mut results = Vec::new();

        for node in nodes {
            if let Some(Value::Vector(vec)) = node.properties.get(vector_field) {
                if let Some((score, distance)) = self.compute_similarity(query, vec) {
                    if distance <= max_distance {
                        results.push(SimilarityResult {
                            node_id: node.id.clone(),
                            score,
                            distance,
                        });
                    }
                }
            }
        }

        // Sort by distance ascending
        results.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
        });
        results
    }
}

/// Builder for creating vector-enabled nodes
pub struct VectorNodeBuilder {
    node_type: String,
    properties: std::collections::BTreeMap<String, Value>,
}

impl VectorNodeBuilder {
    /// Create a new vector node builder
    pub fn new(node_type: &str) -> Self {
        Self {
            node_type: node_type.to_string(),
            properties: std::collections::BTreeMap::new(),
        }
    }

    /// Add a property
    pub fn property(mut self, key: &str, value: Value) -> Self {
        self.properties.insert(key.to_string(), value);
        self
    }

    /// Add a string property
    pub fn text(mut self, key: &str, value: &str) -> Self {
        self.properties
            .insert(key.to_string(), Value::String(value.to_string()));
        self
    }

    /// Add a vector embedding
    pub fn embedding(mut self, key: &str, vector: Vec<f32>) -> Self {
        self.properties
            .insert(key.to_string(), Value::Vector(vector));
        self
    }

    /// Build the node
    pub fn build(self) -> Node {
        Node::with_id(NodeId::new(), &self.node_type, self.properties)
    }
}

/// Utility functions for working with embeddings
pub mod utils {
    use super::*;

    /// Generate a random vector for testing
    pub fn random_vector(dim: usize) -> Vec<f32> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        use std::time::{SystemTime, UNIX_EPOCH};

        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let mut hasher = DefaultHasher::new();
        let mut vec = Vec::with_capacity(dim);

        for i in 0..dim {
            (seed, i).hash(&mut hasher);
            let hash = hasher.finish();
            // Convert to float in range [-1, 1]
            let f = ((hash % 20001) as f64 / 10000.0) - 1.0;
            vec.push(f as f32);
        }

        vec
    }

    /// Normalize a vector to unit length
    pub fn normalize(v: &[f32]) -> Vec<f32> {
        Value::normalize_vector(v)
    }

    /// Compute the centroid of multiple vectors
    pub fn centroid(vectors: &[Vec<f32>]) -> Option<Vec<f32>> {
        if vectors.is_empty() {
            return None;
        }

        let dim = vectors[0].len();
        if vectors.iter().any(|v| v.len() != dim) {
            return None;
        }

        let mut centroid = vec![0.0f64; dim];
        for v in vectors {
            for (i, &x) in v.iter().enumerate() {
                centroid[i] += x as f64;
            }
        }

        let n = vectors.len() as f64;
        Some(centroid.into_iter().map(|x| (x / n) as f32).collect())
    }

    /// Quantize a vector to reduce memory (simple uniform quantization)
    pub fn quantize_f32_to_u8(v: &[f32]) -> (Vec<u8>, f32, f32) {
        if v.is_empty() {
            return (vec![], 0.0, 0.0);
        }

        let min = v.iter().cloned().fold(f32::INFINITY, f32::min);
        let max = v.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        let range = max - min;

        if range == 0.0 {
            return (vec![128; v.len()], min, max);
        }

        let quantized: Vec<u8> = v
            .iter()
            .map(|&x| ((x - min) / range * 255.0).round() as u8)
            .collect();

        (quantized, min, max)
    }

    /// Dequantize u8 back to f32
    pub fn dequantize_u8_to_f32(quantized: &[u8], min: f32, max: f32) -> Vec<f32> {
        let range = max - min;
        quantized
            .iter()
            .map(|&q| min + (q as f32 / 255.0) * range)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_search_cosine() {
        let search = VectorSearch::new(DistanceMetric::Cosine);

        // Create test nodes with vectors
        let mut nodes = vec![];

        // Node similar to query
        let mut n1 = VectorNodeBuilder::new("doc")
            .text("content", "Hello world")
            .embedding("embedding", vec![1.0, 0.0, 0.0])
            .build();
        nodes.push(n1);

        // Node orthogonal to query
        let n2 = VectorNodeBuilder::new("doc")
            .text("content", "Goodbye world")
            .embedding("embedding", vec![0.0, 1.0, 0.0])
            .build();
        nodes.push(n2);

        // Node opposite to query
        let n3 = VectorNodeBuilder::new("doc")
            .text("content", "Different")
            .embedding("embedding", vec![-1.0, 0.0, 0.0])
            .build();
        nodes.push(n3);

        // Search for vectors similar to [1, 0, 0]
        let query = vec![1.0, 0.0, 0.0];
        let results = search.search(&query, &nodes, "embedding", 3);

        assert_eq!(results.len(), 3);
        // First result should be most similar (cosine ~ 1.0)
        assert!(results[0].score > 0.99);
    }

    #[test]
    fn test_vector_search_euclidean() {
        let search = VectorSearch::new(DistanceMetric::Euclidean);

        let nodes = vec![
            VectorNodeBuilder::new("doc")
                .embedding("embedding", vec![0.0, 0.0])
                .build(),
            VectorNodeBuilder::new("doc")
                .embedding("embedding", vec![1.0, 0.0])
                .build(),
            VectorNodeBuilder::new("doc")
                .embedding("embedding", vec![10.0, 10.0])
                .build(),
        ];

        let query = vec![0.0, 0.0];
        let results = search.search(&query, &nodes, "embedding", 2);

        assert_eq!(results.len(), 2);
        // First result should be at origin (distance 0)
        assert!(results[0].distance < 0.01);
    }

    #[test]
    fn test_radius_search() {
        let search = VectorSearch::new(DistanceMetric::Euclidean);

        let nodes = vec![
            VectorNodeBuilder::new("doc")
                .embedding("embedding", vec![0.0, 0.0])
                .build(),
            VectorNodeBuilder::new("doc")
                .embedding("embedding", vec![0.5, 0.0])
                .build(),
            VectorNodeBuilder::new("doc")
                .embedding("embedding", vec![10.0, 10.0])
                .build(),
        ];

        let query = vec![0.0, 0.0];
        let results = search.search_radius(&query, &nodes, "embedding", 1.0);

        // Only 2 nodes within distance 1.0
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_random_vector() {
        let v1 = utils::random_vector(384);
        assert_eq!(v1.len(), 384);

        // Values should be in range [-1, 1]
        assert!(v1.iter().all(|&x| x >= -1.0 && x <= 1.0));
    }

    #[test]
    fn test_centroid() {
        let vectors = vec![
            vec![0.0, 0.0],
            vec![2.0, 0.0],
            vec![0.0, 2.0],
            vec![2.0, 2.0],
        ];

        let centroid = utils::centroid(&vectors).unwrap();
        assert_eq!(centroid.len(), 2);
        assert!((centroid[0] - 1.0).abs() < 1e-6);
        assert!((centroid[1] - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_quantization() {
        let original = vec![0.0, 0.5, 1.0];
        let (quantized, min, max) = utils::quantize_f32_to_u8(&original);

        assert_eq!(quantized.len(), 3);
        assert_eq!(quantized[0], 0); // min -> 0
        assert_eq!(quantized[2], 255); // max -> 255

        let dequantized = utils::dequantize_u8_to_f32(&quantized, min, max);
        for (a, b) in original.iter().zip(dequantized.iter()) {
            assert!((a - b).abs() < 0.01);
        }
    }
}

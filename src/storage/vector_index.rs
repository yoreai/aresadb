//! Vector Index for fast similarity search
//!
//! Implements a simplified HNSW-like index for approximate nearest neighbor search.
//! For production use with very large datasets, consider integrating with
//! specialized libraries like hnswlib or faiss.

#![allow(dead_code)]

use anyhow::Result;
use parking_lot::RwLock;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use super::{DistanceMetric, NodeId};

/// A neighbor in the graph with distance
#[derive(Clone)]
struct Neighbor {
    id: NodeId,
    distance: f32,
}

impl PartialEq for Neighbor {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for Neighbor {}

impl PartialOrd for Neighbor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Neighbor {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap behavior in max-heap
        other
            .distance
            .partial_cmp(&self.distance)
            .unwrap_or(Ordering::Equal)
    }
}

/// Vector entry in the index
struct VectorEntry {
    #[allow(dead_code)]
    id: NodeId,
    vector: Vec<f32>,
    neighbors: Vec<Vec<NodeId>>, // Neighbors at each layer
}

/// HNSW-inspired vector index
pub struct VectorIndex {
    /// All vectors indexed by node ID
    vectors: RwLock<HashMap<NodeId, VectorEntry>>,
    /// Dimension of vectors
    dimension: usize,
    /// Maximum number of connections per node
    max_connections: usize,
    /// Maximum number of layers
    max_layers: usize,
    /// Entry point node ID
    entry_point: RwLock<Option<NodeId>>,
    /// Distance metric
    metric: DistanceMetric,
    /// Ef construction parameter (search width during construction)
    ef_construction: usize,
}

impl VectorIndex {
    /// Create a new vector index
    pub fn new(dimension: usize) -> Self {
        Self {
            vectors: RwLock::new(HashMap::new()),
            dimension,
            max_connections: 16,
            max_layers: 4,
            entry_point: RwLock::new(None),
            metric: DistanceMetric::Cosine,
            ef_construction: 100,
        }
    }

    /// Create with custom parameters
    pub fn with_params(
        dimension: usize,
        max_connections: usize,
        max_layers: usize,
        metric: DistanceMetric,
    ) -> Self {
        Self {
            vectors: RwLock::new(HashMap::new()),
            dimension,
            max_connections,
            max_layers,
            entry_point: RwLock::new(None),
            metric,
            ef_construction: 100,
        }
    }

    /// Insert a vector into the index
    pub fn insert(&self, id: NodeId, vector: Vec<f32>) -> Result<()> {
        if vector.len() != self.dimension {
            anyhow::bail!(
                "Vector dimension mismatch: expected {}, got {}",
                self.dimension,
                vector.len()
            );
        }

        // Normalize vector for cosine similarity
        let vector = self.normalize(&vector);

        // Random layer assignment (simplified - use hash for determinism)
        let layer = self.random_layer(&id);

        let entry = VectorEntry {
            id: id.clone(),
            vector,
            neighbors: vec![Vec::new(); layer + 1],
        };

        // Get write lock and insert
        let mut vectors = self.vectors.write();

        // If this is the first entry, set as entry point
        if vectors.is_empty() {
            *self.entry_point.write() = Some(id.clone());
            vectors.insert(id, entry);
            return Ok(());
        }

        // Find neighbors at each layer
        let entry_point = self.entry_point.read().clone();
        if let Some(ep) = entry_point {
            // Search for nearest neighbors
            let neighbors =
                self.search_layer(&vectors, &entry.vector, &ep, self.ef_construction, 0);

            // Connect to nearest neighbors
            let mut entry = entry;
            for (neighbor_id, _) in neighbors.iter().take(self.max_connections) {
                if entry.neighbors[0].len() < self.max_connections {
                    entry.neighbors[0].push(neighbor_id.clone());
                }

                // Bidirectional connection
                if let Some(neighbor) = vectors.get_mut(neighbor_id) {
                    if neighbor.neighbors[0].len() < self.max_connections {
                        neighbor.neighbors[0].push(id.clone());
                    }
                }
            }

            vectors.insert(id.clone(), entry);

            // Update entry point if new node is in higher layer
            if layer > 0 {
                *self.entry_point.write() = Some(id);
            }
        }

        Ok(())
    }

    /// Search for k nearest neighbors
    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<(NodeId, f32)>> {
        if query.len() != self.dimension {
            anyhow::bail!(
                "Query dimension mismatch: expected {}, got {}",
                self.dimension,
                query.len()
            );
        }

        let query = self.normalize(query);
        let vectors = self.vectors.read();

        if vectors.is_empty() {
            return Ok(Vec::new());
        }

        let entry_point = self.entry_point.read().clone();
        if let Some(ep) = entry_point {
            let results = self.search_layer(&vectors, &query, &ep, k.max(self.ef_construction), 0);
            Ok(results.into_iter().take(k).collect())
        } else {
            Ok(Vec::new())
        }
    }

    /// Remove a vector from the index
    pub fn remove(&self, id: &NodeId) -> bool {
        let mut vectors = self.vectors.write();

        if let Some(removed) = vectors.remove(id) {
            // Remove connections from neighbors
            for neighbor_id in removed.neighbors.iter().flatten() {
                if let Some(neighbor) = vectors.get_mut(neighbor_id) {
                    for layer_neighbors in &mut neighbor.neighbors {
                        layer_neighbors.retain(|n| n != id);
                    }
                }
            }

            // Update entry point if removed
            let mut entry_point = self.entry_point.write();
            if entry_point.as_ref() == Some(id) {
                *entry_point = vectors.keys().next().cloned();
            }

            true
        } else {
            false
        }
    }

    /// Get number of vectors in index
    pub fn len(&self) -> usize {
        self.vectors.read().len()
    }

    /// Check if index is empty
    pub fn is_empty(&self) -> bool {
        self.vectors.read().is_empty()
    }

    /// Search within a layer
    fn search_layer(
        &self,
        vectors: &HashMap<NodeId, VectorEntry>,
        query: &[f32],
        entry_point: &NodeId,
        ef: usize,
        _layer: usize,
    ) -> Vec<(NodeId, f32)> {
        let mut visited: HashMap<NodeId, f32> = HashMap::new();
        let mut candidates: BinaryHeap<Neighbor> = BinaryHeap::new();
        let mut results: BinaryHeap<Neighbor> = BinaryHeap::new();

        // Start with entry point
        if let Some(ep_entry) = vectors.get(entry_point) {
            let dist = self.distance(query, &ep_entry.vector);
            visited.insert(entry_point.clone(), dist);
            candidates.push(Neighbor {
                id: entry_point.clone(),
                distance: dist,
            });
            results.push(Neighbor {
                id: entry_point.clone(),
                distance: -dist, // Negate for max-heap
            });
        }

        while let Some(current) = candidates.pop() {
            // Get worst result distance
            let worst_dist = if let Some(worst) = results.peek() {
                -worst.distance
            } else {
                f32::INFINITY
            };

            if current.distance > worst_dist && results.len() >= ef {
                break;
            }

            // Explore neighbors
            if let Some(entry) = vectors.get(&current.id) {
                for layer_neighbors in &entry.neighbors {
                    for neighbor_id in layer_neighbors {
                        if visited.contains_key(neighbor_id) {
                            continue;
                        }

                        if let Some(neighbor_entry) = vectors.get(neighbor_id) {
                            let dist = self.distance(query, &neighbor_entry.vector);
                            visited.insert(neighbor_id.clone(), dist);

                            let worst_dist = if let Some(worst) = results.peek() {
                                -worst.distance
                            } else {
                                f32::INFINITY
                            };

                            if dist < worst_dist || results.len() < ef {
                                candidates.push(Neighbor {
                                    id: neighbor_id.clone(),
                                    distance: dist,
                                });
                                results.push(Neighbor {
                                    id: neighbor_id.clone(),
                                    distance: -dist,
                                });

                                if results.len() > ef {
                                    results.pop();
                                }
                            }
                        }
                    }
                }
            }
        }

        // Convert results to output format
        let mut output: Vec<(NodeId, f32)> =
            results.into_iter().map(|n| (n.id, -n.distance)).collect();

        // Sort by distance ascending
        output.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        output
    }

    /// Calculate distance between two vectors
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.metric {
            DistanceMetric::Cosine => {
                // For normalized vectors, cosine distance = 1 - dot product
                let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                1.0 - dot
            }
            DistanceMetric::Euclidean => a
                .iter()
                .zip(b.iter())
                .map(|(x, y)| (x - y) * (x - y))
                .sum::<f32>()
                .sqrt(),
            DistanceMetric::DotProduct => {
                let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                -dot // Negate so smaller is better
            }
            DistanceMetric::Manhattan => a.iter().zip(b.iter()).map(|(x, y)| (x - y).abs()).sum(),
        }
    }

    /// Normalize a vector
    fn normalize(&self, v: &[f32]) -> Vec<f32> {
        let mag: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if mag > 0.0 {
            v.iter().map(|x| x / mag).collect()
        } else {
            v.to_vec()
        }
    }

    /// Determine random layer for a new node (using hash for determinism)
    fn random_layer(&self, id: &NodeId) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        id.hash(&mut hasher);
        let hash = hasher.finish();

        // Exponential distribution simulation
        let ml = 1.0 / (self.max_connections as f64).ln();
        let level = (-(hash as f64 / u64::MAX as f64).ln() * ml) as usize;
        level.min(self.max_layers - 1)
    }

    /// Build index from existing vectors
    pub fn build_from_vectors(&self, vectors: Vec<(NodeId, Vec<f32>)>) -> Result<()> {
        for (id, vector) in vectors {
            self.insert(id, vector)?;
        }
        Ok(())
    }

    /// Get statistics about the index
    pub fn stats(&self) -> IndexStats {
        let vectors = self.vectors.read();
        let mut total_connections = 0;
        let mut max_connections = 0;

        for entry in vectors.values() {
            let conn_count: usize = entry.neighbors.iter().map(|n| n.len()).sum();
            total_connections += conn_count;
            max_connections = max_connections.max(conn_count);
        }

        let num_vectors = vectors.len();
        let avg_connections = if num_vectors > 0 {
            total_connections as f64 / num_vectors as f64
        } else {
            0.0
        };

        IndexStats {
            num_vectors,
            dimension: self.dimension,
            total_connections,
            avg_connections,
            max_connections,
            max_layers: self.max_layers,
        }
    }
}

/// Index statistics
#[derive(Debug, Clone)]
pub struct IndexStats {
    /// Number of vectors currently in the index
    pub num_vectors: usize,
    /// Dimensionality of each vector
    pub dimension: usize,
    /// Sum of all neighbor connections across the graph
    pub total_connections: usize,
    /// Mean neighbor count per node
    pub avg_connections: f64,
    /// Highest neighbor count for any single node
    pub max_connections: usize,
    /// Maximum number of HNSW layers
    pub max_layers: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn random_vector(dim: usize, seed: u64) -> Vec<f32> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        (0..dim)
            .map(|i| {
                let mut hasher = DefaultHasher::new();
                seed.hash(&mut hasher);
                i.hash(&mut hasher);
                let h = hasher.finish();
                (h as f64 / u64::MAX as f64 * 2.0 - 1.0) as f32
            })
            .collect()
    }

    #[test]
    fn test_index_insert_search() {
        let index = VectorIndex::new(64);

        // Insert some vectors
        for i in 0..100 {
            let id = NodeId::new();
            let vector = random_vector(64, i);
            index.insert(id, vector).unwrap();
        }

        assert_eq!(index.len(), 100);

        // Search
        let query = random_vector(64, 42);
        let results = index.search(&query, 10).unwrap();

        assert!(results.len() <= 10);
        // Results should be sorted by distance
        for i in 1..results.len() {
            assert!(results[i].1 >= results[i - 1].1);
        }
    }

    #[test]
    fn test_index_remove() {
        let index = VectorIndex::new(32);

        let id1 = NodeId::new();
        let id2 = NodeId::new();

        index.insert(id1.clone(), random_vector(32, 1)).unwrap();
        index.insert(id2.clone(), random_vector(32, 2)).unwrap();

        assert_eq!(index.len(), 2);

        assert!(index.remove(&id1));
        assert_eq!(index.len(), 1);

        assert!(!index.remove(&id1)); // Already removed
    }

    #[test]
    fn test_index_empty() {
        let index = VectorIndex::new(16);
        assert!(index.is_empty());

        let results = index.search(&random_vector(16, 0), 10).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_dimension_mismatch() {
        let index = VectorIndex::new(32);

        let id = NodeId::new();
        let wrong_dim_vector = vec![0.1f32; 64]; // Wrong dimension

        assert!(index.insert(id, wrong_dim_vector).is_err());
    }

    #[test]
    fn test_index_stats() {
        let index = VectorIndex::new(16);

        for i in 0..50 {
            let id = NodeId::new();
            index.insert(id, random_vector(16, i)).unwrap();
        }

        let stats = index.stats();
        assert_eq!(stats.num_vectors, 50);
        assert_eq!(stats.dimension, 16);
        assert!(stats.avg_connections >= 0.0);
    }

    #[test]
    fn test_search_accuracy() {
        let index = VectorIndex::new(8);

        // Insert a specific vector
        let target_id = NodeId::new();
        let target_vector = vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        index
            .insert(target_id.clone(), target_vector.clone())
            .unwrap();

        // Insert some noise
        for i in 0..20 {
            let id = NodeId::new();
            index.insert(id, random_vector(8, i)).unwrap();
        }

        // Search with the same vector - should find it as nearest
        let results = index.search(&target_vector, 1).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].0, target_id);
    }
}

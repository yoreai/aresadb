//! Hybrid Search - combining keyword and vector search
//!
//! Implements Reciprocal Rank Fusion (RRF) to combine results from
//! keyword-based (BM25-style) and vector-based similarity searches.

#![allow(dead_code)]

use anyhow::Result;
use std::collections::HashMap;

use crate::storage::{Database, DistanceMetric, Node, NodeId};

type RankScores = (f64, Option<usize>, Option<usize>, Option<f64>);
type RankedResult = (NodeId, f64, Option<usize>, Option<usize>, Option<f64>);

/// Result from hybrid search
#[derive(Debug, Clone)]
pub struct HybridSearchResult {
    /// Node ID
    pub node_id: NodeId,
    /// The node
    pub node: Option<Node>,
    /// Combined RRF score
    pub rrf_score: f64,
    /// Keyword search rank (if found)
    pub keyword_rank: Option<usize>,
    /// Vector search rank (if found)
    pub vector_rank: Option<usize>,
    /// Vector similarity score (if found)
    pub vector_score: Option<f64>,
}

/// Hybrid search configuration
#[derive(Debug, Clone)]
pub struct HybridSearchConfig {
    /// Weight for keyword results (0.0 to 1.0)
    pub keyword_weight: f64,
    /// Weight for vector results (0.0 to 1.0)
    pub vector_weight: f64,
    /// RRF k parameter (typically 60)
    pub rrf_k: f64,
    /// Minimum keyword matches required
    pub min_keyword_matches: usize,
    /// Vector distance metric
    pub metric: DistanceMetric,
}

impl Default for HybridSearchConfig {
    fn default() -> Self {
        Self {
            keyword_weight: 0.5,
            vector_weight: 0.5,
            rrf_k: 60.0,
            min_keyword_matches: 1,
            metric: DistanceMetric::Cosine,
        }
    }
}

impl HybridSearchConfig {
    /// Create a keyword-heavy config
    pub fn keyword_focused() -> Self {
        Self {
            keyword_weight: 0.7,
            vector_weight: 0.3,
            ..Default::default()
        }
    }

    /// Create a vector-heavy config
    pub fn vector_focused() -> Self {
        Self {
            keyword_weight: 0.3,
            vector_weight: 0.7,
            ..Default::default()
        }
    }
}

/// Hybrid search engine
pub struct HybridSearch<'a> {
    db: &'a Database,
    config: HybridSearchConfig,
}

impl<'a> HybridSearch<'a> {
    /// Create a new hybrid search engine
    pub fn new(db: &'a Database) -> Self {
        Self {
            db,
            config: HybridSearchConfig::default(),
        }
    }

    /// Create with custom config
    pub fn with_config(db: &'a Database, config: HybridSearchConfig) -> Self {
        Self { db, config }
    }

    /// Perform hybrid search
    pub async fn search(
        &self,
        query_text: &str,
        query_vector: &[f32],
        node_type: &str,
        content_field: &str,
        embedding_field: &str,
        k: usize,
    ) -> Result<Vec<HybridSearchResult>> {
        // Get more candidates than needed for fusion
        let candidate_k = k * 3;

        // Perform keyword search
        let keyword_results = self
            .keyword_search(query_text, node_type, content_field, candidate_k)
            .await?;

        // Perform vector search
        let vector_results = self
            .db
            .similarity_search(
                query_vector,
                node_type,
                embedding_field,
                candidate_k,
                self.config.metric,
            )
            .await?;

        // Fuse results using RRF
        let fused = self.reciprocal_rank_fusion(&keyword_results, &vector_results);

        // Take top k and fetch nodes
        let mut results = Vec::with_capacity(k);
        for (node_id, rrf_score, keyword_rank, vector_rank, vector_score) in
            fused.into_iter().take(k)
        {
            let node = self.db.get_node(&node_id.to_string()).await?;
            results.push(HybridSearchResult {
                node_id,
                node,
                rrf_score,
                keyword_rank,
                vector_rank,
                vector_score,
            });
        }

        Ok(results)
    }

    /// Simple keyword search using term matching
    async fn keyword_search(
        &self,
        query: &str,
        node_type: &str,
        content_field: &str,
        k: usize,
    ) -> Result<Vec<(NodeId, f64)>> {
        // Tokenize query
        let query_terms: Vec<String> = query
            .to_lowercase()
            .split_whitespace()
            .map(|s| s.to_string())
            .collect();

        if query_terms.is_empty() {
            return Ok(Vec::new());
        }

        // Get all nodes of the type
        let nodes = self.db.get_all_by_type(node_type, Some(1000)).await?;

        // Score each node using BM25-like scoring
        let mut scored: Vec<(NodeId, f64)> = Vec::new();

        for node in nodes {
            let content = node
                .properties
                .get(content_field)
                .and_then(|v| v.as_str())
                .unwrap_or("");

            let score = self.bm25_score(&query_terms, content);

            if score > 0.0 {
                scored.push((node.id, score));
            }
        }

        // Sort by score descending
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);

        Ok(scored)
    }

    /// BM25-like scoring
    fn bm25_score(&self, query_terms: &[String], content: &str) -> f64 {
        let k1 = 1.5;
        let b = 0.75;
        let avg_doc_len = 500.0;

        let content_lower = content.to_lowercase();
        let doc_len = content.chars().count() as f64;

        let mut score = 0.0;
        let mut matches = 0;

        for term in query_terms {
            let tf = content_lower.matches(term).count() as f64;
            if tf > 0.0 {
                matches += 1;
                let numerator = tf * (k1 + 1.0);
                let denominator = tf + k1 * (1.0 - b + b * doc_len / avg_doc_len);
                score += numerator / denominator;
            }
        }

        // Require minimum matches
        if matches < self.config.min_keyword_matches {
            return 0.0;
        }

        score
    }

    /// Reciprocal Rank Fusion
    fn reciprocal_rank_fusion(
        &self,
        keyword_results: &[(NodeId, f64)],
        vector_results: &[crate::storage::SimilarityResult],
    ) -> Vec<RankedResult> {
        let k = self.config.rrf_k;
        let kw_weight = self.config.keyword_weight;
        let vec_weight = self.config.vector_weight;

        // Map node_id -> (combined_score, keyword_rank, vector_rank, vector_score)
        let mut scores: HashMap<NodeId, RankScores> = HashMap::new();

        // Add keyword results
        for (rank, (node_id, _score)) in keyword_results.iter().enumerate() {
            let rrf_score = kw_weight * (1.0 / (k + rank as f64 + 1.0));
            scores.insert(node_id.clone(), (rrf_score, Some(rank + 1), None, None));
        }

        // Add vector results
        for (rank, result) in vector_results.iter().enumerate() {
            let rrf_score = vec_weight * (1.0 / (k + rank as f64 + 1.0));

            if let Some(entry) = scores.get_mut(&result.node_id) {
                entry.0 += rrf_score;
                entry.2 = Some(rank + 1);
                entry.3 = Some(result.score);
            } else {
                scores.insert(
                    result.node_id.clone(),
                    (rrf_score, None, Some(rank + 1), Some(result.score)),
                );
            }
        }

        // Convert to sorted vector
        let mut results: Vec<_> = scores
            .into_iter()
            .map(|(id, (score, kw_rank, vec_rank, vec_score))| {
                (id, score, kw_rank, vec_rank, vec_score)
            })
            .collect();

        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results
    }
}

/// Perform simple keyword-only search
pub fn keyword_search_sync(query: &str, documents: &[(String, String)]) -> Vec<(String, f64)> {
    let query_lower = query.to_lowercase();
    let query_terms: Vec<&str> = query_lower.split_whitespace().collect();

    if query_terms.is_empty() {
        return Vec::new();
    }

    let mut results: Vec<(String, f64)> = documents
        .iter()
        .filter_map(|(id, content)| {
            let content_lower = content.to_lowercase();
            let matches: usize = query_terms
                .iter()
                .filter(|term| content_lower.contains(*term))
                .count();

            if matches > 0 {
                let score = matches as f64 / query_terms.len() as f64;
                Some((id.clone(), score))
            } else {
                None
            }
        })
        .collect();

    results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyword_search_sync() {
        let documents = vec![
            (
                "doc1".to_string(),
                "Machine learning is a subset of artificial intelligence".to_string(),
            ),
            (
                "doc2".to_string(),
                "Deep learning uses neural networks".to_string(),
            ),
            (
                "doc3".to_string(),
                "Natural language processing for text analysis".to_string(),
            ),
        ];

        let results = keyword_search_sync("machine learning", &documents);

        assert!(!results.is_empty());
        assert_eq!(results[0].0, "doc1"); // Should match best
    }

    #[test]
    fn test_keyword_search_no_match() {
        let documents = vec![("doc1".to_string(), "Hello world".to_string())];

        let results = keyword_search_sync("quantum computing", &documents);
        assert!(results.is_empty());
    }

    #[test]
    fn test_hybrid_config_default() {
        let config = HybridSearchConfig::default();
        assert_eq!(config.keyword_weight, 0.5);
        assert_eq!(config.vector_weight, 0.5);
    }

    #[test]
    fn test_hybrid_config_focused() {
        let kw_config = HybridSearchConfig::keyword_focused();
        assert!(kw_config.keyword_weight > kw_config.vector_weight);

        let vec_config = HybridSearchConfig::vector_focused();
        assert!(vec_config.vector_weight > vec_config.keyword_weight);
    }
}

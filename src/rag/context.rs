//! Context retrieval for RAG applications
//!
//! Provides utilities for retrieving relevant context from the database
//! based on vector similarity.

#![allow(dead_code)]

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::storage::{Database, DistanceMetric, Node};

/// Retrieved context with relevance scores
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrievedContext {
    /// The chunks of context retrieved
    pub chunks: Vec<ContextChunk>,
    /// Total tokens in the context (estimated)
    pub estimated_tokens: usize,
    /// Query that was used
    pub query: String,
}

/// A single chunk of retrieved context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextChunk {
    /// The node ID from the database
    pub node_id: String,
    /// The text content
    pub content: String,
    /// Similarity score (0-1 for cosine)
    pub score: f64,
    /// Distance from query
    pub distance: f64,
    /// Original document ID (if available)
    pub document_id: Option<String>,
    /// Chunk index in document (if available)
    pub chunk_index: Option<usize>,
    /// Additional metadata
    pub metadata: Option<serde_json::Value>,
}

impl RetrievedContext {
    /// Format context as a single string for LLM consumption
    pub fn format_for_llm(&self) -> String {
        let mut output = String::new();

        for (i, chunk) in self.chunks.iter().enumerate() {
            if i > 0 {
                output.push_str("\n\n---\n\n");
            }
            output.push_str(&format!("[Source {}] ", i + 1));
            if let Some(ref doc_id) = chunk.document_id {
                output.push_str(&format!("(Doc: {}) ", doc_id));
            }
            output.push_str(&format!("(Relevance: {:.2})\n", chunk.score));
            output.push_str(&chunk.content);
        }

        output
    }

    /// Format as JSON for structured output
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "query": self.query,
            "estimated_tokens": self.estimated_tokens,
            "num_chunks": self.chunks.len(),
            "chunks": self.chunks.iter().map(|c| {
                serde_json::json!({
                    "content": c.content,
                    "score": c.score,
                    "document_id": c.document_id,
                    "chunk_index": c.chunk_index,
                })
            }).collect::<Vec<_>>()
        })
    }

    /// Get only the text content
    pub fn text_only(&self) -> String {
        self.chunks
            .iter()
            .map(|c| c.content.as_str())
            .collect::<Vec<_>>()
            .join("\n\n")
    }
}

/// Context retriever for RAG applications
pub struct ContextRetriever<'a> {
    db: &'a Database,
    node_type: String,
    embedding_field: String,
    content_field: String,
    max_tokens: usize,
    min_score: f64,
    metric: DistanceMetric,
}

impl<'a> ContextRetriever<'a> {
    /// Create a new context retriever
    pub fn new(db: &'a Database) -> Self {
        Self {
            db,
            node_type: "chunk".to_string(),
            embedding_field: "embedding".to_string(),
            content_field: "content".to_string(),
            max_tokens: 4096,
            min_score: 0.0,
            metric: DistanceMetric::Cosine,
        }
    }

    /// Set the node type to search
    pub fn node_type(mut self, node_type: &str) -> Self {
        self.node_type = node_type.to_string();
        self
    }

    /// Set the field containing embeddings
    pub fn embedding_field(mut self, field: &str) -> Self {
        self.embedding_field = field.to_string();
        self
    }

    /// Set the field containing text content
    pub fn content_field(mut self, field: &str) -> Self {
        self.content_field = field.to_string();
        self
    }

    /// Set maximum tokens to retrieve
    pub fn max_tokens(mut self, tokens: usize) -> Self {
        self.max_tokens = tokens;
        self
    }

    /// Set minimum similarity score threshold
    pub fn min_score(mut self, score: f64) -> Self {
        self.min_score = score;
        self
    }

    /// Set distance metric
    pub fn metric(mut self, metric: DistanceMetric) -> Self {
        self.metric = metric;
        self
    }

    /// Retrieve context for a query vector
    pub async fn retrieve(
        &self,
        query_vector: &[f32],
        query_text: &str,
    ) -> Result<RetrievedContext> {
        // Get more results than needed, then filter by token limit
        let k = self.max_tokens / 100; // Rough estimate: 100 chars per result
        let k = k.clamp(10, 100);

        let results = self
            .db
            .similarity_search(
                query_vector,
                &self.node_type,
                &self.embedding_field,
                k,
                self.metric,
            )
            .await?;

        let mut chunks = Vec::new();
        let mut total_tokens = 0;

        for result in results {
            // Skip low-score results
            if result.score < self.min_score {
                continue;
            }

            // Get the node to extract content
            if let Some(node) = self.db.get_node(&result.node_id.to_string()).await? {
                let content = self.extract_content(&node);
                let tokens = estimate_tokens(&content);

                // Check token limit
                if total_tokens + tokens > self.max_tokens {
                    break;
                }

                let chunk = ContextChunk {
                    node_id: result.node_id.to_string(),
                    content,
                    score: result.score,
                    distance: result.distance,
                    document_id: node
                        .properties
                        .get("document_id")
                        .and_then(|v| v.as_str().map(|s| s.to_string())),
                    chunk_index: node
                        .properties
                        .get("chunk_index")
                        .and_then(|v| v.as_int().map(|i| i as usize)),
                    metadata: node.properties.get("metadata").map(|v| v.to_json()),
                };

                total_tokens += tokens;
                chunks.push(chunk);
            }
        }

        Ok(RetrievedContext {
            chunks,
            estimated_tokens: total_tokens,
            query: query_text.to_string(),
        })
    }

    /// Extract content from a node
    fn extract_content(&self, node: &Node) -> String {
        node.properties
            .get(&self.content_field)
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_default()
    }

    /// Retrieve with reranking (fetch more, then rerank)
    pub async fn retrieve_with_reranking(
        &self,
        query_vector: &[f32],
        query_text: &str,
        rerank_fn: impl Fn(&str, &str) -> f64,
    ) -> Result<RetrievedContext> {
        // Get more results initially
        let initial_k = (self.max_tokens / 50).clamp(20, 200);

        let results = self
            .db
            .similarity_search(
                query_vector,
                &self.node_type,
                &self.embedding_field,
                initial_k,
                self.metric,
            )
            .await?;

        // Fetch content and compute rerank scores
        let mut scored_chunks: Vec<(f64, ContextChunk)> = Vec::new();

        for result in results {
            if result.score < self.min_score {
                continue;
            }

            if let Some(node) = self.db.get_node(&result.node_id.to_string()).await? {
                let content = self.extract_content(&node);

                // Compute rerank score
                let rerank_score = rerank_fn(query_text, &content);

                // Combine original score with rerank score
                let combined_score = result.score * 0.5 + rerank_score * 0.5;

                let chunk = ContextChunk {
                    node_id: result.node_id.to_string(),
                    content,
                    score: combined_score,
                    distance: result.distance,
                    document_id: node
                        .properties
                        .get("document_id")
                        .and_then(|v| v.as_str().map(|s| s.to_string())),
                    chunk_index: node
                        .properties
                        .get("chunk_index")
                        .and_then(|v| v.as_int().map(|i| i as usize)),
                    metadata: node.properties.get("metadata").map(|v| v.to_json()),
                };

                scored_chunks.push((combined_score, chunk));
            }
        }

        // Sort by combined score descending
        scored_chunks.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        // Take chunks up to token limit
        let mut chunks = Vec::new();
        let mut total_tokens = 0;

        for (_, chunk) in scored_chunks {
            let tokens = estimate_tokens(&chunk.content);
            if total_tokens + tokens > self.max_tokens {
                break;
            }
            total_tokens += tokens;
            chunks.push(chunk);
        }

        Ok(RetrievedContext {
            chunks,
            estimated_tokens: total_tokens,
            query: query_text.to_string(),
        })
    }
}

/// Estimate token count from text
fn estimate_tokens(text: &str) -> usize {
    // Rough approximation: ~4 characters per token for English
    text.chars().count() / 4
}

/// Simple keyword-based reranker
#[allow(dead_code)]
pub fn keyword_reranker(query: &str, content: &str) -> f64 {
    let query_lower = query.to_lowercase();
    let content_lower = content.to_lowercase();

    let query_words: Vec<&str> = query_lower.split_whitespace().collect();
    let matches = query_words
        .iter()
        .filter(|word| content_lower.contains(*word))
        .count();

    if query_words.is_empty() {
        return 0.0;
    }

    matches as f64 / query_words.len() as f64
}

/// BM25-inspired reranker (simplified)
#[allow(dead_code)]
pub fn bm25_reranker(query: &str, content: &str) -> f64 {
    let k1 = 1.5;
    let b = 0.75;
    let avg_doc_len = 500.0; // Assume average document length

    let query_lower = query.to_lowercase();
    let content_lower = content.to_lowercase();

    let doc_len = content.chars().count() as f64;
    let query_terms: Vec<&str> = query_lower.split_whitespace().collect();

    let mut score = 0.0;

    for term in &query_terms {
        let tf = content_lower.matches(term).count() as f64;
        if tf > 0.0 {
            let numerator = tf * (k1 + 1.0);
            let denominator = tf + k1 * (1.0 - b + b * doc_len / avg_doc_len);
            score += numerator / denominator;
        }
    }

    // Normalize to 0-1 range
    (score / query_terms.len() as f64).min(1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyword_reranker() {
        let query = "machine learning algorithms";
        let content = "This article discusses machine learning and various algorithms.";

        let score = keyword_reranker(query, content);
        assert!(score > 0.5); // At least 2/3 words match
    }

    #[test]
    fn test_bm25_reranker() {
        let query = "neural networks";
        let content =
            "Neural networks are a type of machine learning model. Neural networks can be deep.";

        let score = bm25_reranker(query, content);
        assert!(score > 0.0);
    }

    #[test]
    fn test_context_format() {
        let context = RetrievedContext {
            chunks: vec![
                ContextChunk {
                    node_id: "1".to_string(),
                    content: "First chunk content".to_string(),
                    score: 0.95,
                    distance: 0.05,
                    document_id: Some("doc1".to_string()),
                    chunk_index: Some(0),
                    metadata: None,
                },
                ContextChunk {
                    node_id: "2".to_string(),
                    content: "Second chunk content".to_string(),
                    score: 0.85,
                    distance: 0.15,
                    document_id: Some("doc2".to_string()),
                    chunk_index: Some(1),
                    metadata: None,
                },
            ],
            estimated_tokens: 10,
            query: "test query".to_string(),
        };

        let formatted = context.format_for_llm();
        assert!(formatted.contains("[Source 1]"));
        assert!(formatted.contains("[Source 2]"));
        assert!(formatted.contains("First chunk content"));
    }

    #[test]
    fn test_estimate_tokens() {
        let text = "This is a test with about forty characters.";
        let tokens = estimate_tokens(text);
        assert!(tokens > 5 && tokens < 20);
    }
}

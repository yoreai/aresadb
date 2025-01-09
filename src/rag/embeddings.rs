//! Embedding generation for RAG applications
//!
//! Supports multiple embedding providers:
//! - OpenAI (text-embedding-3-small, text-embedding-ada-002)
//! - Local hash-based embeddings (for testing/offline use)

#![allow(dead_code)]
//! - Custom providers via trait implementation

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Embedding provider trait
#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// Generate embedding for a single text
    async fn embed(&self, text: &str) -> Result<Vec<f32>>;

    /// Generate embeddings for multiple texts (batch)
    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        let mut results = Vec::with_capacity(texts.len());
        for text in texts {
            results.push(self.embed(text).await?);
        }
        Ok(results)
    }

    /// Get the dimension of embeddings produced by this provider
    fn dimension(&self) -> usize;

    /// Get provider name
    fn name(&self) -> &str;
}

/// OpenAI embedding models
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenAIModel {
    /// text-embedding-3-small (1536 dimensions, recommended)
    TextEmbedding3Small,
    /// text-embedding-3-large (3072 dimensions)
    TextEmbedding3Large,
    /// text-embedding-ada-002 (1536 dimensions, legacy)
    Ada002,
}

impl OpenAIModel {
    fn as_str(&self) -> &'static str {
        match self {
            OpenAIModel::TextEmbedding3Small => "text-embedding-3-small",
            OpenAIModel::TextEmbedding3Large => "text-embedding-3-large",
            OpenAIModel::Ada002 => "text-embedding-ada-002",
        }
    }

    fn dimension(&self) -> usize {
        match self {
            OpenAIModel::TextEmbedding3Small => 1536,
            OpenAIModel::TextEmbedding3Large => 3072,
            OpenAIModel::Ada002 => 1536,
        }
    }
}

impl std::str::FromStr for OpenAIModel {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "text-embedding-3-small" | "3-small" | "small" => Ok(OpenAIModel::TextEmbedding3Small),
            "text-embedding-3-large" | "3-large" | "large" => Ok(OpenAIModel::TextEmbedding3Large),
            "text-embedding-ada-002" | "ada-002" | "ada" => Ok(OpenAIModel::Ada002),
            _ => anyhow::bail!("Unknown OpenAI model: {}. Use: small, large, or ada", s),
        }
    }
}

/// OpenAI embedding provider
pub struct OpenAIEmbeddings {
    api_key: String,
    model: OpenAIModel,
    client: reqwest::Client,
}

impl OpenAIEmbeddings {
    /// Create new OpenAI embeddings provider
    pub fn new(api_key: String, model: OpenAIModel) -> Self {
        Self {
            api_key,
            model,
            client: reqwest::Client::new(),
        }
    }

    /// Create from environment variable OPENAI_API_KEY
    pub fn from_env(model: OpenAIModel) -> Result<Self> {
        let api_key = std::env::var("OPENAI_API_KEY")
            .context("OPENAI_API_KEY environment variable not set")?;
        Ok(Self::new(api_key, model))
    }

    /// Create with default model (text-embedding-3-small)
    pub fn default_from_env() -> Result<Self> {
        Self::from_env(OpenAIModel::TextEmbedding3Small)
    }
}

#[derive(Serialize)]
struct OpenAIRequest<'a> {
    model: &'a str,
    input: Vec<&'a str>,
}

#[derive(Deserialize)]
struct OpenAIResponse {
    data: Vec<OpenAIEmbeddingData>,
}

#[derive(Deserialize)]
struct OpenAIEmbeddingData {
    embedding: Vec<f32>,
}

#[async_trait]
impl EmbeddingProvider for OpenAIEmbeddings {
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let results = self.embed_batch(&[text]).await?;
        results
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("No embedding returned"))
    }

    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        let request = OpenAIRequest {
            model: self.model.as_str(),
            input: texts.to_vec(),
        };

        let response = self
            .client
            .post("https://api.openai.com/v1/embeddings")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .context("Failed to send request to OpenAI")?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            anyhow::bail!("OpenAI API error: {}", error_text);
        }

        let result: OpenAIResponse = response
            .json()
            .await
            .context("Failed to parse OpenAI response")?;

        Ok(result.data.into_iter().map(|d| d.embedding).collect())
    }

    fn dimension(&self) -> usize {
        self.model.dimension()
    }

    fn name(&self) -> &str {
        "openai"
    }
}

/// Local hash-based embedding provider (for testing/offline use)
///
/// This creates deterministic embeddings based on text hashing.
/// NOT suitable for semantic search - use only for testing!
pub struct LocalHashEmbeddings {
    dimension: usize,
}

impl LocalHashEmbeddings {
    /// Create new local embeddings with specified dimension
    pub fn new(dimension: usize) -> Self {
        Self { dimension }
    }

    /// Create with default dimension (384)
    pub fn with_default_dimension() -> Self {
        Self::new(384)
    }

    /// Generate pseudo-random embedding from text using hashing
    fn hash_to_embedding(&self, text: &str) -> Vec<f32> {
        let mut embedding = Vec::with_capacity(self.dimension);

        // Generate multiple hash values to fill the embedding
        for i in 0..self.dimension {
            let mut hasher = DefaultHasher::new();
            text.hash(&mut hasher);
            i.hash(&mut hasher);
            let hash = hasher.finish();

            // Convert to float in range [-1, 1]
            let value = ((hash as f64 / u64::MAX as f64) * 2.0 - 1.0) as f32;
            embedding.push(value);
        }

        // Normalize the vector
        let magnitude: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        if magnitude > 0.0 {
            for v in &mut embedding {
                *v /= magnitude;
            }
        }

        embedding
    }
}

impl Default for LocalHashEmbeddings {
    fn default() -> Self {
        Self::new(384)
    }
}

#[async_trait]
impl EmbeddingProvider for LocalHashEmbeddings {
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        Ok(self.hash_to_embedding(text))
    }

    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        Ok(texts.iter().map(|t| self.hash_to_embedding(t)).collect())
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    fn name(&self) -> &str {
        "local-hash"
    }
}

/// TF-IDF based local embeddings (better than hash for testing)
pub struct TfIdfEmbeddings {
    dimension: usize,
    vocab: std::collections::HashMap<String, usize>,
}

impl TfIdfEmbeddings {
    /// Create new TF-IDF embeddings with vocabulary built from texts
    pub fn new(dimension: usize) -> Self {
        Self {
            dimension,
            vocab: std::collections::HashMap::new(),
        }
    }

    /// Build vocabulary from a corpus of texts
    pub fn build_vocab(&mut self, texts: &[&str]) {
        let mut word_count: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();

        for text in texts {
            for word in text.split_whitespace() {
                let word = word.to_lowercase();
                *word_count.entry(word).or_insert(0) += 1;
            }
        }

        // Take top N words by frequency
        let mut words: Vec<_> = word_count.into_iter().collect();
        words.sort_by_key(|b| std::cmp::Reverse(b.1));

        self.vocab = words
            .into_iter()
            .take(self.dimension)
            .enumerate()
            .map(|(i, (word, _))| (word, i))
            .collect();
    }

    fn text_to_embedding(&self, text: &str) -> Vec<f32> {
        let mut embedding = vec![0.0f32; self.dimension];
        let words: Vec<_> = text.split_whitespace().collect();
        let total_words = words.len() as f32;

        if total_words == 0.0 {
            return embedding;
        }

        // Count word frequencies
        let mut word_freq: std::collections::HashMap<&str, usize> =
            std::collections::HashMap::new();
        for word in &words {
            *word_freq.entry(*word).or_insert(0) += 1;
        }

        // Build TF vector
        for (word, freq) in word_freq {
            let word_lower = word.to_lowercase();
            if let Some(&idx) = self.vocab.get(&word_lower) {
                if idx < self.dimension {
                    embedding[idx] = freq as f32 / total_words;
                }
            }
        }

        // Normalize
        let magnitude: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        if magnitude > 0.0 {
            for v in &mut embedding {
                *v /= magnitude;
            }
        }

        embedding
    }
}

#[async_trait]
impl EmbeddingProvider for TfIdfEmbeddings {
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        Ok(self.text_to_embedding(text))
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    fn name(&self) -> &str {
        "tfidf"
    }
}

/// Embedding manager that wraps different providers
pub struct EmbeddingManager {
    provider: Box<dyn EmbeddingProvider>,
}

impl EmbeddingManager {
    /// Create with OpenAI provider
    pub fn openai(api_key: String, model: OpenAIModel) -> Self {
        Self {
            provider: Box::new(OpenAIEmbeddings::new(api_key, model)),
        }
    }

    /// Create with OpenAI from environment
    pub fn openai_from_env(model: OpenAIModel) -> Result<Self> {
        Ok(Self {
            provider: Box::new(OpenAIEmbeddings::from_env(model)?),
        })
    }

    /// Create with local hash embeddings (for testing)
    pub fn local(dimension: usize) -> Self {
        Self {
            provider: Box::new(LocalHashEmbeddings::new(dimension)),
        }
    }

    /// Create with default local embeddings
    pub fn local_default() -> Self {
        Self::local(384)
    }

    /// Create from provider name string
    pub fn from_name(name: &str, api_key: Option<&str>) -> Result<Self> {
        match name.to_lowercase().as_str() {
            "openai" | "openai-small" => {
                let key = api_key
                    .map(|k| k.to_string())
                    .or_else(|| std::env::var("OPENAI_API_KEY").ok())
                    .context("OpenAI requires API key (--api-key or OPENAI_API_KEY env var)")?;
                Ok(Self::openai(key, OpenAIModel::TextEmbedding3Small))
            }
            "openai-large" => {
                let key = api_key
                    .map(|k| k.to_string())
                    .or_else(|| std::env::var("OPENAI_API_KEY").ok())
                    .context("OpenAI requires API key")?;
                Ok(Self::openai(key, OpenAIModel::TextEmbedding3Large))
            }
            "local" | "hash" | "local-hash" => Ok(Self::local_default()),
            _ => anyhow::bail!(
                "Unknown embedding provider: {}. Use: openai, openai-large, local",
                name
            ),
        }
    }

    /// Embed text
    pub async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        self.provider.embed(text).await
    }

    /// Embed batch of texts
    pub async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        self.provider.embed_batch(texts).await
    }

    /// Get embedding dimension
    pub fn dimension(&self) -> usize {
        self.provider.dimension()
    }

    /// Get provider name
    pub fn name(&self) -> &str {
        self.provider.name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_hash_embeddings() {
        let provider = LocalHashEmbeddings::new(128);

        let emb1 = provider.embed("hello world").await.unwrap();
        let emb2 = provider.embed("hello world").await.unwrap();
        let emb3 = provider.embed("different text").await.unwrap();

        assert_eq!(emb1.len(), 128);
        assert_eq!(emb1, emb2); // Same text = same embedding
        assert_ne!(emb1, emb3); // Different text = different embedding
    }

    #[tokio::test]
    async fn test_local_hash_normalized() {
        let provider = LocalHashEmbeddings::new(64);
        let emb = provider.embed("test text").await.unwrap();

        let magnitude: f32 = emb.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((magnitude - 1.0).abs() < 0.001); // Should be normalized
    }

    #[tokio::test]
    async fn test_embedding_manager_local() {
        let manager = EmbeddingManager::local_default();

        let emb = manager.embed("test").await.unwrap();
        assert_eq!(emb.len(), 384);
        assert_eq!(manager.name(), "local-hash");
    }

    #[tokio::test]
    async fn test_batch_embeddings() {
        let provider = LocalHashEmbeddings::new(64);

        let texts = vec!["first", "second", "third"];
        let embeddings = provider.embed_batch(&texts).await.unwrap();

        assert_eq!(embeddings.len(), 3);
        for emb in &embeddings {
            assert_eq!(emb.len(), 64);
        }
    }

    #[test]
    fn test_openai_model_parse() {
        assert_eq!(
            "small".parse::<OpenAIModel>().unwrap(),
            OpenAIModel::TextEmbedding3Small
        );
        assert_eq!(
            "large".parse::<OpenAIModel>().unwrap(),
            OpenAIModel::TextEmbedding3Large
        );
        assert_eq!("ada".parse::<OpenAIModel>().unwrap(), OpenAIModel::Ada002);
    }

    #[tokio::test]
    async fn test_tfidf_embeddings() {
        let mut provider = TfIdfEmbeddings::new(100);
        provider.build_vocab(&["hello world", "world peace", "hello friend"]);

        let emb1 = provider.embed("hello world").await.unwrap();
        let emb2 = provider.embed("world peace").await.unwrap();

        assert_eq!(emb1.len(), 100);
        assert_eq!(emb2.len(), 100);
        // "world" appears in both, so they should have some similarity
    }
}

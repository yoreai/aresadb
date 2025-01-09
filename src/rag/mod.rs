//! RAG (Retrieval-Augmented Generation) utilities
//!
//! Provides document chunking, embedding workflows, and context retrieval
//! for building RAG applications with AresaDB.

#![allow(dead_code)]
#![allow(unused_imports)]

mod chunker;
mod context;
mod embeddings;
mod hybrid;

pub use chunker::{ChunkStrategy, Chunker, DocumentChunk};
pub use context::{ContextChunk, ContextRetriever, RetrievedContext};
pub use embeddings::{
    EmbeddingManager, EmbeddingProvider, LocalHashEmbeddings, OpenAIEmbeddings, OpenAIModel,
    TfIdfEmbeddings,
};
pub use hybrid::{keyword_search_sync, HybridSearch, HybridSearchConfig, HybridSearchResult};

/// Default chunk size in characters
#[allow(dead_code)]
pub const DEFAULT_CHUNK_SIZE: usize = 512;

/// Default overlap between chunks
#[allow(dead_code)]
pub const DEFAULT_CHUNK_OVERLAP: usize = 50;

/// Maximum context tokens to retrieve
#[allow(dead_code)]
pub const DEFAULT_MAX_CONTEXT_TOKENS: usize = 4096;

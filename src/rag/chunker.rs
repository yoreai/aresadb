//! Document chunking for RAG applications
//!
//! Provides various strategies for splitting documents into chunks
//! suitable for embedding and retrieval.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};

/// A chunk of a document
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentChunk {
    /// Unique identifier for this chunk
    pub id: String,
    /// Original document ID this chunk came from
    pub document_id: String,
    /// The text content of this chunk
    pub content: String,
    /// Index of this chunk in the document (0-based)
    pub chunk_index: usize,
    /// Total number of chunks in the document
    pub total_chunks: usize,
    /// Character offset in the original document
    pub start_offset: usize,
    /// End character offset in the original document
    pub end_offset: usize,
    /// Optional metadata
    pub metadata: Option<serde_json::Value>,
}

/// Chunking strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkStrategy {
    /// Fixed size chunks with optional overlap
    FixedSize {
        /// Maximum number of characters per chunk.
        chunk_size: usize,
        /// Number of overlapping characters between consecutive chunks.
        overlap: usize,
    },
    /// Split by sentences, grouping up to max_tokens
    Sentence {
        /// Maximum token count per chunk.
        max_tokens: usize,
    },
    /// Split by paragraphs (double newlines)
    Paragraph {
        /// Maximum character count per paragraph chunk.
        max_size: usize,
    },
    /// Split by semantic sections (headers, etc.)
    Semantic {
        /// Maximum character count per semantic section.
        max_size: usize,
    },
}

impl Default for ChunkStrategy {
    fn default() -> Self {
        ChunkStrategy::FixedSize {
            chunk_size: 512,
            overlap: 50,
        }
    }
}

/// Document chunker
pub struct Chunker {
    strategy: ChunkStrategy,
}

impl Chunker {
    /// Create a new chunker with the given strategy
    pub fn new(strategy: ChunkStrategy) -> Self {
        Self { strategy }
    }

    /// Create a chunker with default settings (512 chars, 50 overlap)
    pub fn default_chunker() -> Self {
        Self::new(ChunkStrategy::default())
    }

    /// Create a chunker for sentence-based splitting
    pub fn sentence_chunker(max_tokens: usize) -> Self {
        Self::new(ChunkStrategy::Sentence { max_tokens })
    }

    /// Create a chunker for paragraph-based splitting
    pub fn paragraph_chunker(max_size: usize) -> Self {
        Self::new(ChunkStrategy::Paragraph { max_size })
    }

    /// Chunk a document into pieces
    pub fn chunk(&self, document_id: &str, content: &str) -> Vec<DocumentChunk> {
        match self.strategy {
            ChunkStrategy::FixedSize {
                chunk_size,
                overlap,
            } => self.chunk_fixed_size(document_id, content, chunk_size, overlap),
            ChunkStrategy::Sentence { max_tokens } => {
                self.chunk_by_sentence(document_id, content, max_tokens)
            }
            ChunkStrategy::Paragraph { max_size } => {
                self.chunk_by_paragraph(document_id, content, max_size)
            }
            ChunkStrategy::Semantic { max_size } => {
                self.chunk_semantic(document_id, content, max_size)
            }
        }
    }

    /// Fixed-size chunking with overlap
    fn chunk_fixed_size(
        &self,
        document_id: &str,
        content: &str,
        chunk_size: usize,
        overlap: usize,
    ) -> Vec<DocumentChunk> {
        let mut chunks = Vec::new();
        let chars: Vec<char> = content.chars().collect();
        let content_len = chars.len();

        if content_len == 0 {
            return chunks;
        }

        let step = chunk_size.saturating_sub(overlap).max(1);
        let mut start = 0;
        let mut chunk_index = 0;

        while start < content_len {
            let end = (start + chunk_size).min(content_len);
            let chunk_content: String = chars[start..end].iter().collect();

            // Calculate byte offsets
            let start_offset = chars[..start].iter().collect::<String>().len();
            let end_offset = chars[..end].iter().collect::<String>().len();

            chunks.push(DocumentChunk {
                id: format!("{}_{}", document_id, chunk_index),
                document_id: document_id.to_string(),
                content: chunk_content,
                chunk_index,
                total_chunks: 0, // Will be updated after
                start_offset,
                end_offset,
                metadata: None,
            });

            chunk_index += 1;
            start += step;

            // Avoid infinite loop on tiny content
            if end >= content_len {
                break;
            }
        }

        // Update total_chunks
        let total = chunks.len();
        for chunk in &mut chunks {
            chunk.total_chunks = total;
        }

        chunks
    }

    /// Sentence-based chunking
    fn chunk_by_sentence(
        &self,
        document_id: &str,
        content: &str,
        max_tokens: usize,
    ) -> Vec<DocumentChunk> {
        let sentences = self.split_sentences(content);
        let mut chunks = Vec::new();
        let mut current_chunk = String::new();
        let mut current_start = 0;
        let mut chunk_index = 0;

        for sentence in sentences {
            let sentence_len = sentence.chars().count();
            let current_len = current_chunk.chars().count();

            // Rough token estimate: chars / 4
            let estimated_tokens = (current_len + sentence_len) / 4;

            if estimated_tokens > max_tokens && !current_chunk.is_empty() {
                // Save current chunk and start new one
                let end_offset = current_start + current_chunk.len();
                chunks.push(DocumentChunk {
                    id: format!("{}_{}", document_id, chunk_index),
                    document_id: document_id.to_string(),
                    content: current_chunk.trim().to_string(),
                    chunk_index,
                    total_chunks: 0,
                    start_offset: current_start,
                    end_offset,
                    metadata: None,
                });
                chunk_index += 1;
                current_start = end_offset;
                current_chunk = sentence.to_string();
            } else {
                if !current_chunk.is_empty() {
                    current_chunk.push(' ');
                }
                current_chunk.push_str(sentence);
            }
        }

        // Don't forget the last chunk
        if !current_chunk.trim().is_empty() {
            let end_offset = current_start + current_chunk.len();
            chunks.push(DocumentChunk {
                id: format!("{}_{}", document_id, chunk_index),
                document_id: document_id.to_string(),
                content: current_chunk.trim().to_string(),
                chunk_index,
                total_chunks: 0,
                start_offset: current_start,
                end_offset,
                metadata: None,
            });
        }

        // Update total_chunks
        let total = chunks.len();
        for chunk in &mut chunks {
            chunk.total_chunks = total;
        }

        chunks
    }

    /// Paragraph-based chunking
    fn chunk_by_paragraph(
        &self,
        document_id: &str,
        content: &str,
        max_size: usize,
    ) -> Vec<DocumentChunk> {
        let paragraphs: Vec<&str> = content.split("\n\n").collect();
        let mut chunks = Vec::new();
        let mut current_chunk = String::new();
        let mut current_start = 0;
        let mut chunk_index = 0;

        for para in paragraphs {
            let para = para.trim();
            if para.is_empty() {
                continue;
            }

            if current_chunk.len() + para.len() > max_size && !current_chunk.is_empty() {
                // Save current chunk
                let end_offset = current_start + current_chunk.len();
                chunks.push(DocumentChunk {
                    id: format!("{}_{}", document_id, chunk_index),
                    document_id: document_id.to_string(),
                    content: current_chunk.trim().to_string(),
                    chunk_index,
                    total_chunks: 0,
                    start_offset: current_start,
                    end_offset,
                    metadata: None,
                });
                chunk_index += 1;
                current_start = end_offset;
                current_chunk = para.to_string();
            } else {
                if !current_chunk.is_empty() {
                    current_chunk.push_str("\n\n");
                }
                current_chunk.push_str(para);
            }
        }

        // Last chunk
        if !current_chunk.trim().is_empty() {
            let end_offset = current_start + current_chunk.len();
            chunks.push(DocumentChunk {
                id: format!("{}_{}", document_id, chunk_index),
                document_id: document_id.to_string(),
                content: current_chunk.trim().to_string(),
                chunk_index,
                total_chunks: 0,
                start_offset: current_start,
                end_offset,
                metadata: None,
            });
        }

        let total = chunks.len();
        for chunk in &mut chunks {
            chunk.total_chunks = total;
        }

        chunks
    }

    /// Semantic chunking (headers, sections)
    fn chunk_semantic(
        &self,
        document_id: &str,
        content: &str,
        max_size: usize,
    ) -> Vec<DocumentChunk> {
        // Split on markdown headers or common section markers
        let section_patterns = ["# ", "## ", "### ", "#### ", "---", "***", "==="];

        let mut sections = Vec::new();
        let mut current_section = String::new();
        let mut current_start = 0;

        for line in content.lines() {
            let is_header = section_patterns.iter().any(|p| line.starts_with(p));

            if is_header && !current_section.is_empty() {
                sections.push((current_start, current_section.clone()));
                current_start += current_section.len() + 1; // +1 for newline
                current_section = line.to_string();
            } else {
                if !current_section.is_empty() {
                    current_section.push('\n');
                }
                current_section.push_str(line);
            }
        }

        if !current_section.is_empty() {
            sections.push((current_start, current_section));
        }

        // Now chunk sections that are too large
        let mut chunks = Vec::new();
        let mut chunk_index = 0;

        for (start_offset, section) in sections {
            if section.len() <= max_size {
                chunks.push(DocumentChunk {
                    id: format!("{}_{}", document_id, chunk_index),
                    document_id: document_id.to_string(),
                    content: section.trim().to_string(),
                    chunk_index,
                    total_chunks: 0,
                    start_offset,
                    end_offset: start_offset + section.len(),
                    metadata: None,
                });
                chunk_index += 1;
            } else {
                // Fall back to fixed-size for large sections
                let sub_chunks = self.chunk_fixed_size(
                    &format!("{}_section", document_id),
                    &section,
                    max_size,
                    50,
                );
                for mut sub in sub_chunks {
                    sub.id = format!("{}_{}", document_id, chunk_index);
                    sub.document_id = document_id.to_string();
                    sub.start_offset += start_offset;
                    sub.end_offset += start_offset;
                    sub.chunk_index = chunk_index;
                    chunks.push(sub);
                    chunk_index += 1;
                }
            }
        }

        let total = chunks.len();
        for chunk in &mut chunks {
            chunk.total_chunks = total;
        }

        chunks
    }

    /// Split text into sentences (simple implementation)
    fn split_sentences<'a>(&self, text: &'a str) -> Vec<&'a str> {
        let mut sentences = Vec::new();
        let mut start = 0;

        for (i, c) in text.char_indices() {
            if c == '.' || c == '!' || c == '?' {
                // Check if followed by space or end of text
                let rest = &text[i + c.len_utf8()..];
                if rest.is_empty() || rest.starts_with(char::is_whitespace) {
                    let sentence = &text[start..=i];
                    if !sentence.trim().is_empty() {
                        sentences.push(sentence.trim());
                    }
                    start = i + c.len_utf8();
                    // Skip leading whitespace for next sentence
                    while start < text.len() && text[start..].starts_with(char::is_whitespace) {
                        start += 1;
                    }
                }
            }
        }

        // Add remaining text as last sentence
        if start < text.len() {
            let remaining = &text[start..];
            if !remaining.trim().is_empty() {
                sentences.push(remaining.trim());
            }
        }

        sentences
    }

    /// Estimate token count (rough approximation)
    pub fn estimate_tokens(text: &str) -> usize {
        // Common approximation: ~4 characters per token for English
        text.chars().count() / 4
    }

    /// Get overlapping context around a chunk
    pub fn get_context_window(
        &self,
        content: &str,
        chunk: &DocumentChunk,
        context_chars: usize,
    ) -> String {
        let start = chunk.start_offset.saturating_sub(context_chars);
        let end = (chunk.end_offset + context_chars).min(content.len());

        content[start..end].to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_size_chunking() {
        let chunker = Chunker::new(ChunkStrategy::FixedSize {
            chunk_size: 100,
            overlap: 20,
        });

        let content = "a".repeat(250);
        let chunks = chunker.chunk("doc1", &content);

        assert!(chunks.len() >= 3);
        assert_eq!(chunks[0].chunk_index, 0);
        assert_eq!(chunks[0].document_id, "doc1");
    }

    #[test]
    fn test_sentence_chunking() {
        let chunker = Chunker::sentence_chunker(50); // ~200 chars

        let content = "This is sentence one. This is sentence two. This is sentence three. This is sentence four.";
        let chunks = chunker.chunk("doc2", content);

        assert!(chunks.len() >= 1);
        for chunk in &chunks {
            assert!(!chunk.content.is_empty());
        }
    }

    #[test]
    fn test_paragraph_chunking() {
        let chunker = Chunker::paragraph_chunker(200);

        let content = "First paragraph here.\n\nSecond paragraph here.\n\nThird paragraph here.";
        let chunks = chunker.chunk("doc3", content);

        assert!(chunks.len() >= 1);
    }

    #[test]
    fn test_semantic_chunking() {
        let chunker = Chunker::new(ChunkStrategy::Semantic { max_size: 500 });

        let content = "# Introduction\n\nThis is the intro.\n\n## Methods\n\nThis is methods.\n\n## Results\n\nThis is results.";
        let chunks = chunker.chunk("doc4", content);

        assert!(chunks.len() >= 2);
    }

    #[test]
    fn test_token_estimation() {
        let text = "This is a test sentence with some words.";
        let tokens = Chunker::estimate_tokens(text);
        assert!(tokens > 0);
        assert!(tokens < text.len());
    }

    #[test]
    fn test_empty_content() {
        let chunker = Chunker::default_chunker();
        let chunks = chunker.chunk("empty", "");
        assert!(chunks.is_empty());
    }
}

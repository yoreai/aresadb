//! LZ4 Compression for Storage and Network
//!
//! High-speed compression using LZ4 algorithm for:
//! - Reducing storage footprint on disk and buckets
//! - Faster network transfers

#![allow(dead_code)]
//! - Memory-efficient caching

use anyhow::{Context, Result};
use lz4_flex::{compress_prepend_size, decompress_size_prepended};

/// Compressor for data serialization
///
/// Uses LZ4 for fast compression/decompression with good compression ratios.
#[derive(Clone, Debug)]
pub struct Compressor {
    /// Minimum size to compress (smaller data may expand)
    min_compress_size: usize,
}

impl Default for Compressor {
    fn default() -> Self {
        Self::new()
    }
}

impl Compressor {
    /// Create a new compressor with default settings
    pub fn new() -> Self {
        Self {
            min_compress_size: 64, // Don't compress tiny data
        }
    }

    /// Create with custom minimum compression threshold
    pub fn with_min_size(min_compress_size: usize) -> Self {
        Self { min_compress_size }
    }

    /// Compress data using LZ4
    ///
    /// Returns compressed bytes with prepended uncompressed size.
    /// If data is smaller than threshold, returns original data prefixed with marker.
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.len() < self.min_compress_size {
            // Prefix with 0x00 to indicate uncompressed
            let mut result = Vec::with_capacity(data.len() + 1);
            result.push(0x00);
            result.extend_from_slice(data);
            return Ok(result);
        }

        // Prefix with 0x01 to indicate compressed
        let compressed = compress_prepend_size(data);
        let mut result = Vec::with_capacity(compressed.len() + 1);
        result.push(0x01);
        result.extend_from_slice(&compressed);
        Ok(result)
    }

    /// Decompress LZ4 data
    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        match data[0] {
            0x00 => {
                // Uncompressed
                Ok(data[1..].to_vec())
            }
            0x01 => {
                // Compressed
                decompress_size_prepended(&data[1..]).context("Failed to decompress data")
            }
            _ => anyhow::bail!("Invalid compression marker: {}", data[0]),
        }
    }

    /// Compress with statistics
    pub fn compress_with_stats(&self, data: &[u8]) -> Result<(Vec<u8>, CompressionStats)> {
        let original_size = data.len();
        let compressed = self.compress(data)?;
        let compressed_size = compressed.len();

        let stats = CompressionStats {
            original_size,
            compressed_size,
            ratio: original_size as f64 / compressed_size as f64,
            was_compressed: data.len() >= self.min_compress_size,
        };

        Ok((compressed, stats))
    }

    /// Compress multiple chunks in parallel (using rayon would be ideal here)
    pub fn compress_batch(&self, chunks: &[&[u8]]) -> Result<Vec<Vec<u8>>> {
        chunks.iter().map(|chunk| self.compress(chunk)).collect()
    }

    /// Decompress multiple chunks
    pub fn decompress_batch(&self, chunks: &[&[u8]]) -> Result<Vec<Vec<u8>>> {
        chunks.iter().map(|chunk| self.decompress(chunk)).collect()
    }
}

/// Statistics about compression operation
#[derive(Debug, Clone, Copy)]
pub struct CompressionStats {
    /// Original data size in bytes
    pub original_size: usize,
    /// Compressed size in bytes
    pub compressed_size: usize,
    /// Compression ratio (original/compressed)
    pub ratio: f64,
    /// Whether compression was actually applied
    pub was_compressed: bool,
}

impl CompressionStats {
    /// Calculate space savings as percentage
    pub fn savings_percent(&self) -> f64 {
        if self.original_size == 0 {
            0.0
        } else {
            (1.0 - (self.compressed_size as f64 / self.original_size as f64)) * 100.0
        }
    }
}

impl std::fmt::Display for CompressionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> {} bytes (ratio: {:.2}x, savings: {:.1}%)",
            self.original_size,
            self.compressed_size,
            self.ratio,
            self.savings_percent()
        )
    }
}

/// Streaming compressor for large data
#[allow(dead_code)]
pub struct StreamingCompressor {
    compressor: Compressor,
    chunk_size: usize,
}

#[allow(dead_code)]
impl StreamingCompressor {
    /// Create a new streaming compressor
    pub fn new(chunk_size: usize) -> Self {
        Self {
            compressor: Compressor::new(),
            chunk_size,
        }
    }

    /// Compress a stream of data, returning compressed chunks
    pub fn compress_stream<'a>(
        &'a self,
        data: &'a [u8],
    ) -> impl Iterator<Item = Result<Vec<u8>>> + 'a {
        data.chunks(self.chunk_size)
            .map(|chunk| self.compressor.compress(chunk))
    }

    /// Decompress a stream of chunks
    pub fn decompress_stream<'a>(
        &'a self,
        chunks: impl Iterator<Item = &'a [u8]> + 'a,
    ) -> impl Iterator<Item = Result<Vec<u8>>> + 'a {
        chunks.map(|chunk| self.compressor.decompress(chunk))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress_small() {
        let compressor = Compressor::new();
        let data = b"hello"; // Small data, won't be compressed

        let compressed = compressor.compress(data).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();

        assert_eq!(data.as_slice(), decompressed.as_slice());
        assert_eq!(compressed[0], 0x00); // Not compressed marker
    }

    #[test]
    fn test_compress_decompress_large() {
        let compressor = Compressor::new();

        // Create compressible data (repeated pattern)
        let data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();

        let compressed = compressor.compress(&data).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();

        assert_eq!(data, decompressed);
        assert_eq!(compressed[0], 0x01); // Compressed marker
        assert!(compressed.len() < data.len()); // Should actually compress
    }

    #[test]
    fn test_compression_stats() {
        let compressor = Compressor::new();

        // Highly compressible data
        let data = vec![0u8; 1000];

        let (compressed, stats) = compressor.compress_with_stats(&data).unwrap();

        assert!(stats.was_compressed);
        assert!(stats.ratio > 1.0);
        assert!(stats.savings_percent() > 50.0);

        // Verify we can decompress
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_compress_empty() {
        let compressor = Compressor::new();

        let compressed = compressor.compress(&[]).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();

        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_compress_batch() {
        let compressor = Compressor::new();

        let chunks: Vec<Vec<u8>> = vec![vec![1, 2, 3, 4, 5], (0..100).collect(), vec![0; 500]];

        let chunk_refs: Vec<&[u8]> = chunks.iter().map(|c| c.as_slice()).collect();
        let compressed = compressor.compress_batch(&chunk_refs).unwrap();

        let compressed_refs: Vec<&[u8]> = compressed.iter().map(|c| c.as_slice()).collect();
        let decompressed = compressor.decompress_batch(&compressed_refs).unwrap();

        assert_eq!(chunks, decompressed);
    }

    #[test]
    fn test_streaming_compressor() {
        let streaming = StreamingCompressor::new(100);

        let data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();

        let compressed: Vec<Vec<u8>> = streaming
            .compress_stream(&data)
            .collect::<Result<Vec<_>>>()
            .unwrap();

        let decompressed: Vec<u8> = streaming
            .decompress_stream(compressed.iter().map(|c| c.as_slice()))
            .flat_map(|r| r.unwrap())
            .collect();

        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_compression_with_random_data() {
        use rand::Rng;

        let compressor = Compressor::new();
        let mut rng = rand::thread_rng();

        // Random data is hard to compress
        let data: Vec<u8> = (0..1000).map(|_| rng.gen()).collect();

        let compressed = compressor.compress(&data).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();

        assert_eq!(data, decompressed);
    }
}

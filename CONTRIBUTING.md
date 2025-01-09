# Contributing to AresaDB

Thank you for your interest in contributing to AresaDB! This document provides guidelines and instructions for contributing.

---

## Table of Contents

1. [Code of Conduct](#code-of-conduct)
2. [Getting Started](#getting-started)
3. [Development Setup](#development-setup)
4. [Making Changes](#making-changes)
5. [Testing](#testing)
6. [Submitting Changes](#submitting-changes)
7. [Style Guide](#style-guide)
8. [Architecture Overview](#architecture-overview)

---

## Code of Conduct

We are committed to providing a welcoming and inclusive environment. Please:

- Be respectful and inclusive
- Welcome newcomers and help them learn
- Focus on what is best for the community
- Show empathy towards others

---

## Getting Started

### Prerequisites

- **Rust**: 1.85 or later ([rustup](https://rustup.rs/))
- **Git**: For version control
- **Optional**: AWS/GCP credentials for cloud storage testing

### Fork and Clone

```bash
# Fork the repository on GitHub, then:
git clone https://github.com/YOUR_USERNAME/aresadb.git
cd aresadb

# Add upstream remote
git remote add upstream https://github.com/yoreai/aresadb.git
```

---

## Development Setup

### Build the Project

```bash
# Debug build (faster compilation)
cargo build

# Release build (optimized)
cargo build --release

# Check without building
cargo check
```

### Run Tests

```bash
# All tests
cargo test

# Specific test
cargo test test_insert_and_get_node

# With output
cargo test -- --nocapture

# Run ignored tests
cargo test -- --ignored
```

### Run Lints

```bash
# Clippy (linter)
cargo clippy -- -D warnings

# Format check
cargo fmt -- --check

# Format (fix)
cargo fmt
```

### Run Benchmarks

```bash
cargo bench
```

---

## Making Changes

### Branching Strategy

```bash
# Create a feature branch from main
git checkout main
git pull upstream main
git checkout -b feature/my-new-feature

# Or for bug fixes
git checkout -b fix/bug-description
```

### Branch Naming

| Type | Pattern | Example |
|------|---------|---------|
| Feature | `feature/description` | `feature/vector-embeddings` |
| Bug Fix | `fix/description` | `fix/query-parser-crash` |
| Docs | `docs/description` | `docs/api-documentation` |
| Refactor | `refactor/description` | `refactor/storage-layer` |
| Test | `test/description` | `test/stress-tests` |

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `style`: Formatting, no code change
- `refactor`: Code restructuring
- `test`: Adding tests
- `chore`: Maintenance

**Examples:**

```bash
feat(query): add support for LIKE operator
fix(storage): handle concurrent write conflicts
docs(readme): add cloud storage setup guide
test(integration): add S3 sync tests
refactor(cli): extract command handlers
```

---

## Testing

### Test Categories

1. **Unit Tests**: In each module's `#[cfg(test)]` block
2. **Integration Tests**: In `tests/` directory
3. **Property Tests**: Using `proptest`
4. **Stress Tests**: Concurrency and load testing
5. **Examples**: In `examples/` directory

### Writing Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_feature_name() {
        // Arrange
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "test").await.unwrap();

        // Act
        let result = db.some_operation().await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap().field, expected_value);
    }
}
```

### Test Coverage

We aim for 80%+ code coverage. Check coverage with:

```bash
cargo install cargo-tarpaulin
cargo tarpaulin --out Html
```

---

## Submitting Changes

### Before Submitting

- [ ] Code compiles: `cargo build`
- [ ] Tests pass: `cargo test`
- [ ] Lints pass: `cargo clippy`
- [ ] Formatted: `cargo fmt`
- [ ] Documentation updated if needed
- [ ] CHANGELOG.md updated for user-facing changes

### Pull Request Process

1. **Push your branch:**
   ```bash
   git push origin feature/my-new-feature
   ```

2. **Create Pull Request** on GitHub with:
   - Clear title following commit conventions
   - Description of changes
   - Link to related issues
   - Screenshots for UI changes

3. **PR Template:**
   ```markdown
   ## Description
   Brief description of changes

   ## Type of Change
   - [ ] Bug fix
   - [ ] New feature
   - [ ] Breaking change
   - [ ] Documentation

   ## Testing
   - [ ] Unit tests added/updated
   - [ ] Integration tests added/updated
   - [ ] Manual testing performed

   ## Checklist
   - [ ] Code follows style guidelines
   - [ ] Self-review completed
   - [ ] Documentation updated
   - [ ] CHANGELOG updated
   ```

4. **Review Process:**
   - Maintainer will review within 48 hours
   - Address feedback in new commits
   - Squash commits before merge

---

## Style Guide

### Rust Style

Follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/):

```rust
// Good: Clear, descriptive names
pub async fn get_all_by_type(&self, node_type: &str) -> Result<Vec<Node>>

// Bad: Abbreviated, unclear
pub async fn get_n_t(&self, t: &str) -> Result<Vec<Node>>
```

### Documentation

```rust
/// Brief description of what this does.
///
/// More detailed explanation if needed.
///
/// # Arguments
///
/// * `node_type` - The type of nodes to retrieve
/// * `limit` - Maximum number of results (None for all)
///
/// # Returns
///
/// A vector of nodes matching the type.
///
/// # Errors
///
/// Returns an error if the database is not accessible.
///
/// # Examples
///
/// ```rust
/// let users = db.get_all_by_type("user", Some(100)).await?;
/// ```
pub async fn get_all_by_type(
    &self,
    node_type: &str,
    limit: Option<usize>,
) -> Result<Vec<Node>> {
    // ...
}
```

### Error Handling

```rust
// Use anyhow for application errors
use anyhow::{Result, Context};

pub async fn load_config(path: &Path) -> Result<Config> {
    let content = std::fs::read_to_string(path)
        .context("Failed to read config file")?;

    let config: Config = toml::from_str(&content)
        .context("Failed to parse config")?;

    Ok(config)
}

// Use thiserror for library errors
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Node not found: {0}")]
    NodeNotFound(NodeId),

    #[error("Database locked")]
    DatabaseLocked,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
```

### Code Organization

```
src/
├── lib.rs           # Public API exports
├── main.rs          # CLI entry point
├── module/
│   ├── mod.rs       # Module public interface
│   ├── types.rs     # Data structures
│   ├── impl.rs      # Implementation
│   └── tests.rs     # Unit tests (or inline)
```

---

## Architecture Overview

### Key Modules

| Module | Purpose | Key Types |
|--------|---------|-----------|
| `storage` | Data persistence | `Database`, `Node`, `Edge` |
| `query` | SQL processing | `QueryEngine` |
| `schema` | Schema management | `Schema`, `Migration` |
| `cli` | Command interface | `Cli`, `Commands` |
| `distributed` | V2 features | `WAL`, `BloomFilter`, `Shard` |

### Adding a New Feature

1. **Design**: Write a brief design doc or discussion
2. **Types**: Define data structures in `types.rs`
3. **Implementation**: Add logic in `impl.rs`
4. **Tests**: Add comprehensive tests
5. **CLI**: Expose via CLI if user-facing
6. **Docs**: Update documentation

### Performance Considerations

- Use `#[inline]` for small, hot functions
- Prefer `&str` over `String` in function signatures
- Use `Arc` for shared data across threads
- Batch operations when possible
- Profile with `cargo flamegraph`

---

## Getting Help

- **Issues**: [GitHub Issues](https://github.com/yoreai/aresadb/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yoreai/aresadb/discussions)
- **Email**: yevheniyc@gmail.com

---

## Recognition

Contributors will be:
- Credited in CHANGELOG.md for their contributions
- Thanked in release notes

---

Thank you for contributing to AresaDB! 🎉


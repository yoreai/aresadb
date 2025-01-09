"""Tests for vector embedding / similarity search operations."""

import json
import math


def _rand_vec(dim: int, seed: int = 0) -> list[float]:
    """Deterministic pseudo-random vector for testing."""
    import hashlib
    out = []
    for i in range(dim):
        h = hashlib.md5(f"{seed}:{i}".encode()).hexdigest()
        out.append((int(h[:8], 16) % 10000) / 10000.0)
    return out


def test_insert_and_search(db):
    dim = 32
    for i in range(20):
        db.insert_with_embedding(
            "doc",
            json.dumps({"title": f"doc_{i}"}),
            "embedding",
            _rand_vec(dim, seed=i),
        )

    query = _rand_vec(dim, seed=0)
    results = db.similarity_search(query, "doc", "embedding", k=5)
    assert len(results) == 5
    assert results[0].score >= results[-1].score


def test_similarity_search_radius(db):
    dim = 8
    base = [1.0] * dim
    near = [1.0 + 0.01 * j for j in range(dim)]
    far = [10.0] * dim

    db.insert_with_embedding("vec", json.dumps({"label": "base"}), "emb", base)
    db.insert_with_embedding("vec", json.dumps({"label": "near"}), "emb", near)
    db.insert_with_embedding("vec", json.dumps({"label": "far"}), "emb", far)

    results = db.similarity_search_radius(base, "vec", "emb", max_distance=0.5, metric="euclidean")
    labels_found = {r.node_id for r in results}
    assert len(results) >= 1


def test_get_node_with_embedding(db):
    dim = 4
    vec = [0.1, 0.2, 0.3, 0.4]
    node = db.insert_with_embedding("vec", json.dumps({"x": 1}), "emb", vec)

    result = db.get_node_with_embedding(node.id, "emb")
    assert result is not None
    py_node, embedding = result
    assert py_node.id == node.id
    assert embedding is not None
    assert len(embedding) == dim


def test_rebuild_vector_index(db):
    dim = 8
    for i in range(10):
        db.insert_with_embedding("vec", json.dumps({"i": i}), "emb", _rand_vec(dim, i))

    stats = db.rebuild_vector_index("vec", "emb")
    assert stats.num_vectors == 10
    assert stats.dimension == dim


def test_different_metrics(db):
    dim = 4
    for i in range(5):
        db.insert_with_embedding("vec", json.dumps({"i": i}), "emb", _rand_vec(dim, i))

    query = _rand_vec(dim, seed=0)
    for metric in ("cosine", "euclidean", "dot", "manhattan"):
        results = db.similarity_search(query, "vec", "emb", k=3, metric=metric)
        assert len(results) == 3

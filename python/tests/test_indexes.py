"""Tests for secondary indexes and full-text search."""

import json


def test_create_and_use_secondary_index(db):
    db.insert_dict("user", {"name": "Alice", "age": 30})
    db.insert_dict("user", {"name": "Bob", "age": 25})
    db.insert_dict("user", {"name": "Charlie", "age": 30})

    indexed = db.create_index("user", "age")
    assert indexed >= 0

    indexes = db.list_indexes()
    assert ("user", "age") in indexes

    results = db.index_lookup("user", "age", 30)
    assert results is not None
    assert len(results) == 2


def test_drop_index(db):
    db.insert_dict("user", {"name": "Alice", "age": 30})
    db.create_index("user", "age")
    db.drop_index("user", "age")

    indexes = db.list_indexes()
    assert ("user", "age") not in indexes


def test_fulltext_index_and_search(db):
    db.insert("user", json.dumps({"name": "Alice", "bio": "software engineer who loves Rust"}))
    db.insert("user", json.dumps({"name": "Bob", "bio": "designer specializing in user interfaces"}))
    db.insert("user", json.dumps({"name": "Charlie", "bio": "Rust and Python developer"}))

    indexed = db.create_fulltext_index("user", "bio")
    assert indexed >= 0

    results = db.fulltext_search("user", "bio", "Rust", limit=5)
    assert len(results) >= 1
    assert all(r.score > 0 for r in results)

    ft_indexes = db.list_fulltext_indexes()
    assert ("user", "bio") in ft_indexes

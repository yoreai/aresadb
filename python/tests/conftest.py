"""Shared fixtures for AresaDB Python binding tests."""

import json
import tempfile
from pathlib import Path

import pytest


@pytest.fixture()
def db():
    """Create a fresh ephemeral database for each test."""
    from aresadb_python import Database

    with tempfile.TemporaryDirectory() as tmpdir:
        d = Database.create(str(Path(tmpdir) / "testdb"), "testdb")
        yield d


@pytest.fixture()
def populated_db(db):
    """Database pre-loaded with sample nodes and edges."""
    alice = db.insert("user", json.dumps({"name": "Alice", "age": 30, "bio": "engineer from NYC"}))
    bob = db.insert("user", json.dumps({"name": "Bob", "age": 25, "bio": "designer from LA"}))
    charlie = db.insert("user", json.dumps({"name": "Charlie", "age": 35, "bio": "manager from Chicago"}))

    db.create_edge(alice.id, bob.id, "follows", {"since": "2024"})
    db.create_edge(bob.id, charlie.id, "follows", {"since": "2025"})
    db.create_edge(alice.id, charlie.id, "knows")

    return db, {"alice": alice, "bob": bob, "charlie": charlie}

"""Tests for node CRUD operations."""

import json

import pytest


def test_insert_and_get(db):
    node = db.insert("user", json.dumps({"name": "Alice"}))
    assert node.id
    assert node.node_type == "user"
    assert node.properties["name"] == "Alice"

    fetched = db.get(node.id)
    assert fetched is not None
    assert fetched.id == node.id


def test_insert_dict(db):
    node = db.insert_dict("user", {"name": "Bob", "age": 25})
    assert node.properties["name"] == "Bob"
    assert node.properties["age"] == 25


def test_insert_batch(db):
    items = [
        ("user", {"name": "A"}),
        ("user", {"name": "B"}),
        ("user", {"name": "C"}),
    ]
    nodes = db.insert_batch(items)
    assert len(nodes) == 3
    names = {n.properties["name"] for n in nodes}
    assert names == {"A", "B", "C"}


def test_update(db):
    node = db.insert_dict("user", {"name": "Alice", "age": 30})
    updated = db.update(node.id, {"name": "Alice", "age": 31})
    assert updated.properties["age"] == 31


def test_delete(db):
    node = db.insert_dict("user", {"name": "Temp"})
    db.delete(node.id)
    assert db.get(node.id) is None


def test_get_by_type(db):
    db.insert_dict("user", {"name": "A"})
    db.insert_dict("user", {"name": "B"})
    db.insert_dict("product", {"name": "X"})

    users = db.get_by_type("user")
    assert len(users) == 2

    limited = db.get_by_type("user", limit=1)
    assert len(limited) == 1


def test_get_nonexistent(db):
    assert db.get("00000000-0000-0000-0000-000000000000") is None


def test_get_invalid_id_raises(db):
    with pytest.raises(Exception):
        db.get("not-a-valid-uuid")


def test_insert_invalid_json(db):
    with pytest.raises(Exception):
        db.insert("user", "not valid json")

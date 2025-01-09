"""Tests for edge (relationship) operations."""

import json


def test_create_and_query_edge(db):
    a = db.insert_dict("user", {"name": "Alice"})
    b = db.insert_dict("user", {"name": "Bob"})

    edge = db.create_edge(a.id, b.id, "follows")
    assert edge.id
    assert edge.from_id == a.id
    assert edge.to_id == b.id
    assert edge.edge_type == "follows"


def test_create_edge_with_properties(db):
    a = db.insert_dict("user", {"name": "Alice"})
    b = db.insert_dict("user", {"name": "Bob"})

    edge = db.create_edge(a.id, b.id, "follows", {"since": "2024"})
    assert edge.properties["since"] == "2024"


def test_create_edges_batch(db):
    a = db.insert_dict("user", {"name": "A"})
    b = db.insert_dict("user", {"name": "B"})
    c = db.insert_dict("user", {"name": "C"})

    edges = db.create_edges_batch([
        (a.id, b.id, "knows"),
        (b.id, c.id, "knows"),
        (a.id, c.id, "follows"),
    ])
    assert len(edges) == 3


def test_get_edges_from(db):
    a = db.insert_dict("user", {"name": "Alice"})
    b = db.insert_dict("user", {"name": "Bob"})
    c = db.insert_dict("user", {"name": "Charlie"})

    db.create_edge(a.id, b.id, "follows")
    db.create_edge(a.id, c.id, "knows")

    all_edges = db.get_edges_from(a.id)
    assert len(all_edges) == 2

    follows_only = db.get_edges_from(a.id, edge_type="follows")
    assert len(follows_only) == 1
    assert follows_only[0].edge_type == "follows"


def test_get_edges_to(db):
    a = db.insert_dict("user", {"name": "Alice"})
    b = db.insert_dict("user", {"name": "Bob"})

    db.create_edge(a.id, b.id, "follows")

    incoming = db.get_edges_to(b.id)
    assert len(incoming) == 1
    assert incoming[0].from_id == a.id


def test_delete_edge(db):
    a = db.insert_dict("user", {"name": "Alice"})
    b = db.insert_dict("user", {"name": "Bob"})

    edge = db.create_edge(a.id, b.id, "follows")
    db.delete_edge(edge.id)

    edges = db.get_edges_from(a.id)
    assert len(edges) == 0

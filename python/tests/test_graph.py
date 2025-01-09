"""Tests for graph traversal operations."""


def test_traverse(populated_db):
    db, people = populated_db
    result = db.traverse(people["alice"].id, max_depth=2)

    assert result.root.id == people["alice"].id
    assert result.depth == 2
    assert len(result.nodes) >= 1
    assert isinstance(result.adjacency, dict)


def test_traverse_with_edge_filter(populated_db):
    db, people = populated_db
    result = db.traverse(people["alice"].id, max_depth=2, edge_types=["follows"])

    node_ids = {n.id for n in result.nodes}
    assert people["alice"].id in node_ids
    assert people["bob"].id in node_ids


def test_shortest_path(populated_db):
    db, people = populated_db
    path = db.shortest_path(people["alice"].id, people["charlie"].id)

    assert path is not None
    assert len(path) >= 2
    assert path[0].id == people["alice"].id
    assert path[-1].id == people["charlie"].id


def test_shortest_path_not_found(db):
    a = db.insert_dict("user", {"name": "A"})
    b = db.insert_dict("user", {"name": "B"})
    path = db.shortest_path(a.id, b.id, max_depth=3)
    assert path is None


def test_connected_components(populated_db):
    db, people = populated_db
    components = db.connected_components("user")
    assert len(components) >= 1

    all_ids = {n.id for comp in components for n in comp}
    assert people["alice"].id in all_ids

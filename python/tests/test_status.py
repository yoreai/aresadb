"""Tests for database introspection / status."""


def test_status_fields(db):
    status = db.status()
    assert status.name == "testdb"
    assert status.path
    assert status.node_count == 0
    assert status.edge_count == 0
    assert status.size_bytes >= 0


def test_status_after_inserts(db):
    db.insert_dict("user", {"name": "A"})
    db.insert_dict("user", {"name": "B"})

    a = db.insert_dict("user", {"name": "X"})
    b = db.insert_dict("user", {"name": "Y"})
    db.create_edge(a.id, b.id, "knows")

    status = db.status()
    assert status.node_count == 4
    assert status.edge_count == 1


def test_name_and_path(db):
    assert db.name() == "testdb"
    assert "testdb" in db.path()


def test_repr(db):
    r = repr(db)
    assert "Database" in r
    assert "testdb" in r

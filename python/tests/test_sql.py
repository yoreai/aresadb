"""Tests for the SQL query interface."""

import json


def test_select_all(db):
    db.insert_dict("user", {"name": "Alice", "age": 30})
    db.insert_dict("user", {"name": "Bob", "age": 25})

    result = db.query("SELECT * FROM user")
    assert len(result.columns) > 0
    assert len(result.rows) == 2
    assert result.execution_time_ms >= 0


def test_select_with_where(db):
    db.insert_dict("user", {"name": "Alice", "age": 30})
    db.insert_dict("user", {"name": "Bob", "age": 25})

    result = db.query("SELECT * FROM user WHERE age > 28")
    assert len(result.rows) == 1


def test_select_with_limit(db):
    for i in range(10):
        db.insert_dict("item", {"name": f"item_{i}"})

    result = db.query("SELECT * FROM item", limit=3)
    assert len(result.rows) == 3


def test_insert_via_sql(db):
    result = db.query("INSERT INTO product (name, price) VALUES ('Widget', 9.99)")
    assert result.rows_affected == 1

    result = db.query("SELECT * FROM product")
    assert len(result.rows) == 1


def test_update_via_sql(db):
    db.insert_dict("user", {"name": "Alice", "age": 30})

    db.query("UPDATE user SET age = 31 WHERE name = 'Alice'")
    result = db.query("SELECT * FROM user WHERE name = 'Alice'")
    assert len(result.rows) == 1


def test_delete_via_sql(db):
    db.insert_dict("user", {"name": "Alice", "age": 30})
    db.insert_dict("user", {"name": "Bob", "age": 25})

    db.query("DELETE FROM user WHERE name = 'Alice'")
    result = db.query("SELECT * FROM user")
    assert len(result.rows) == 1

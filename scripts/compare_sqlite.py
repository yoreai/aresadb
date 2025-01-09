#!/usr/bin/env python3
"""
SQLite Comparison Benchmarks for AresaDB

Compares insert, query, and aggregation performance against SQLite.
"""

import sqlite3
import time
import json
import statistics
import sys
from pathlib import Path


def generate_test_data(n: int) -> list:
    """Generate n test records."""
    return [
        {
            "id": f"id-{i}",
            "name": f"User {i}",
            "email": f"user{i}@example.com",
            "age": i % 100,
            "balance": float(i * 10.5),
            "active": i % 2 == 0,
            "tags": json.dumps(["tag1", "tag2", "tag3"]),
            "created_at": "2024-01-01T00:00:00Z"
        }
        for i in range(n)
    ]


def bench_insert(n: int, iterations: int = 5) -> dict:
    """Benchmark insert performance."""
    times = []

    for _ in range(iterations):
        conn = sqlite3.connect(':memory:')
        conn.execute('''
            CREATE TABLE users (
                id TEXT PRIMARY KEY,
                name TEXT,
                email TEXT,
                age INTEGER,
                balance REAL,
                active INTEGER,
                tags TEXT,
                created_at TEXT
            )
        ''')

        data = generate_test_data(n)

        start = time.perf_counter()
        for record in data:
            conn.execute(
                'INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (record['id'], record['name'], record['email'],
                 record['age'], record['balance'], record['active'],
                 record['tags'], record['created_at'])
            )
        conn.commit()
        elapsed = time.perf_counter() - start

        times.append(elapsed)
        conn.close()

    return {
        "operation": "insert",
        "database": "sqlite",
        "records": n,
        "mean_ms": statistics.mean(times) * 1000,
        "std_ms": statistics.stdev(times) * 1000 if len(times) > 1 else 0,
        "min_ms": min(times) * 1000,
        "max_ms": max(times) * 1000,
        "records_per_sec": n / statistics.mean(times)
    }


def bench_point_lookup(n: int, iterations: int = 100) -> dict:
    """Benchmark point lookup by ID."""
    conn = sqlite3.connect(':memory:')
    conn.execute('''
        CREATE TABLE users (
            id TEXT PRIMARY KEY,
            name TEXT,
            email TEXT,
            age INTEGER
        )
    ''')

    for i in range(n):
        conn.execute(
            'INSERT INTO users VALUES (?, ?, ?, ?)',
            (f'id-{i}', f'User {i}', f'user{i}@example.com', i % 100)
        )
    conn.commit()

    times = []
    for i in range(iterations):
        lookup_id = f'id-{i % n}'
        start = time.perf_counter()
        cursor = conn.execute('SELECT * FROM users WHERE id = ?', (lookup_id,))
        result = cursor.fetchone()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    conn.close()

    sorted_times = sorted(times)
    return {
        "operation": "point_lookup",
        "database": "sqlite",
        "records": n,
        "p50_us": sorted_times[len(times)//2] * 1_000_000,
        "p95_us": sorted_times[int(len(times)*0.95)] * 1_000_000,
        "p99_us": sorted_times[int(len(times)*0.99)] * 1_000_000,
        "mean_us": statistics.mean(times) * 1_000_000
    }


def bench_scan(n: int, iterations: int = 5) -> dict:
    """Benchmark full table scan with filter."""
    conn = sqlite3.connect(':memory:')
    conn.execute('''
        CREATE TABLE users (
            id TEXT PRIMARY KEY,
            name TEXT,
            email TEXT,
            age INTEGER
        )
    ''')

    for i in range(n):
        conn.execute(
            'INSERT INTO users VALUES (?, ?, ?, ?)',
            (f'id-{i}', f'User {i}', f'user{i}@example.com', i % 100)
        )
    conn.commit()

    times = []
    result_count = 0
    for _ in range(iterations):
        start = time.perf_counter()
        cursor = conn.execute('SELECT * FROM users WHERE age > 50')
        results = cursor.fetchall()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
        result_count = len(results)

    conn.close()

    return {
        "operation": "scan_filter",
        "database": "sqlite",
        "records": n,
        "result_count": result_count,
        "mean_ms": statistics.mean(times) * 1000,
        "std_ms": statistics.stdev(times) * 1000 if len(times) > 1 else 0
    }


def bench_aggregation(n: int, iterations: int = 5) -> dict:
    """Benchmark aggregation query."""
    conn = sqlite3.connect(':memory:')
    conn.execute('''
        CREATE TABLE sales (
            id INTEGER PRIMARY KEY,
            product TEXT,
            amount REAL,
            category TEXT
        )
    ''')

    for i in range(n):
        conn.execute(
            'INSERT INTO sales VALUES (?, ?, ?, ?)',
            (i, f'Product {i % 100}', float(i * 10), f'Category {i % 10}')
        )
    conn.commit()

    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        cursor = conn.execute('''
            SELECT category, COUNT(*) as count, SUM(amount) as total, AVG(amount) as avg
            FROM sales
            GROUP BY category
            ORDER BY total DESC
        ''')
        results = cursor.fetchall()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    conn.close()

    return {
        "operation": "aggregation",
        "database": "sqlite",
        "records": n,
        "mean_ms": statistics.mean(times) * 1000,
        "std_ms": statistics.stdev(times) * 1000 if len(times) > 1 else 0
    }


def main():
    """Run all benchmarks."""
    print("=" * 60)
    print("SQLite Benchmark Results")
    print("=" * 60)
    print()

    sizes = [1000, 10000, 100000]
    results = []

    # Insert benchmarks
    print("Insert Benchmarks:")
    print("-" * 40)
    for size in sizes:
        result = bench_insert(size)
        results.append(result)
        print(f"  {size:>7} records: {result['mean_ms']:.1f}ms (±{result['std_ms']:.1f}ms) - {result['records_per_sec']:.0f} rec/s")
    print()

    # Point lookup benchmarks
    print("Point Lookup Benchmarks:")
    print("-" * 40)
    for size in sizes:
        result = bench_point_lookup(size)
        results.append(result)
        print(f"  {size:>7} records: p50={result['p50_us']:.1f}µs, p95={result['p95_us']:.1f}µs, p99={result['p99_us']:.1f}µs")
    print()

    # Scan benchmarks
    print("Scan Benchmarks:")
    print("-" * 40)
    for size in sizes:
        result = bench_scan(size)
        results.append(result)
        print(f"  {size:>7} records: {result['mean_ms']:.1f}ms (±{result['std_ms']:.1f}ms) - {result['result_count']} matches")
    print()

    # Aggregation benchmarks
    print("Aggregation Benchmarks:")
    print("-" * 40)
    for size in sizes:
        result = bench_aggregation(size)
        results.append(result)
        print(f"  {size:>7} records: {result['mean_ms']:.1f}ms (±{result['std_ms']:.1f}ms)")
    print()

    # Output JSON for machine parsing
    print(json.dumps({"sqlite": results}, indent=2))

    return 0


if __name__ == '__main__':
    sys.exit(main())


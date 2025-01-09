#!/usr/bin/env python3
"""
DuckDB Comparison Benchmarks for AresaDB

Compares analytical query performance against DuckDB.
DuckDB is optimized for OLAP workloads, so this comparison
focuses on analytics operations.
"""

import time
import json
import statistics
import sys

try:
    import duckdb
except ImportError:
    print("DuckDB not installed. Install with: pip install duckdb")
    sys.exit(1)


def bench_bulk_insert(n: int, iterations: int = 5) -> dict:
    """Benchmark bulk insert performance."""
    times = []

    for _ in range(iterations):
        conn = duckdb.connect(':memory:')
        conn.execute('''
            CREATE TABLE users (
                id VARCHAR,
                name VARCHAR,
                email VARCHAR,
                age INTEGER,
                balance DOUBLE
            )
        ''')

        # Generate data as tuples for bulk insert
        data = [(f'id-{i}', f'User {i}', f'user{i}@example.com', i % 100, float(i * 10.5))
                for i in range(n)]

        start = time.perf_counter()
        conn.executemany('INSERT INTO users VALUES (?, ?, ?, ?, ?)', data)
        elapsed = time.perf_counter() - start

        times.append(elapsed)
        conn.close()

    return {
        "operation": "bulk_insert",
        "database": "duckdb",
        "records": n,
        "mean_ms": statistics.mean(times) * 1000,
        "std_ms": statistics.stdev(times) * 1000 if len(times) > 1 else 0,
        "records_per_sec": n / statistics.mean(times)
    }


def bench_analytics(n: int, iterations: int = 5) -> dict:
    """Benchmark analytical query (GROUP BY, aggregations)."""
    conn = duckdb.connect(':memory:')
    conn.execute('''
        CREATE TABLE sales (
            id INTEGER,
            product VARCHAR,
            amount DOUBLE,
            category VARCHAR,
            region VARCHAR,
            date DATE
        )
    ''')

    # Bulk insert test data
    data = [(i, f'Product {i % 100}', float(i * 10), f'Category {i % 10}',
             f'Region {i % 5}', '2024-01-01') for i in range(n)]
    conn.executemany('INSERT INTO sales VALUES (?, ?, ?, ?, ?, ?)', data)

    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        result = conn.execute('''
            SELECT
                category,
                region,
                COUNT(*) as count,
                SUM(amount) as total,
                AVG(amount) as avg,
                MIN(amount) as min,
                MAX(amount) as max
            FROM sales
            GROUP BY category, region
            ORDER BY total DESC
            LIMIT 20
        ''').fetchall()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    conn.close()

    return {
        "operation": "analytics_groupby",
        "database": "duckdb",
        "records": n,
        "mean_ms": statistics.mean(times) * 1000,
        "std_ms": statistics.stdev(times) * 1000 if len(times) > 1 else 0
    }


def bench_window_function(n: int, iterations: int = 5) -> dict:
    """Benchmark window function performance."""
    conn = duckdb.connect(':memory:')
    conn.execute('''
        CREATE TABLE events (
            id INTEGER,
            user_id INTEGER,
            event_type VARCHAR,
            value DOUBLE,
            timestamp TIMESTAMP
        )
    ''')

    data = [(i, i % 1000, f'event_{i % 10}', float(i), '2024-01-01 00:00:00')
            for i in range(n)]
    conn.executemany('INSERT INTO events VALUES (?, ?, ?, ?, ?)', data)

    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        result = conn.execute('''
            SELECT
                user_id,
                event_type,
                value,
                SUM(value) OVER (PARTITION BY user_id ORDER BY id) as running_total,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY id) as row_num,
                LAG(value) OVER (PARTITION BY user_id ORDER BY id) as prev_value
            FROM events
            LIMIT 10000
        ''').fetchall()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    conn.close()

    return {
        "operation": "window_function",
        "database": "duckdb",
        "records": n,
        "mean_ms": statistics.mean(times) * 1000,
        "std_ms": statistics.stdev(times) * 1000 if len(times) > 1 else 0
    }


def bench_full_scan(n: int, iterations: int = 5) -> dict:
    """Benchmark full table scan."""
    conn = duckdb.connect(':memory:')
    conn.execute('''
        CREATE TABLE logs (
            id INTEGER,
            level VARCHAR,
            message VARCHAR,
            timestamp TIMESTAMP
        )
    ''')

    data = [(i, ['INFO', 'WARN', 'ERROR'][i % 3], f'Log message {i}', '2024-01-01 00:00:00')
            for i in range(n)]
    conn.executemany('INSERT INTO logs VALUES (?, ?, ?, ?)', data)

    times = []
    result_count = 0
    for _ in range(iterations):
        start = time.perf_counter()
        result = conn.execute("SELECT * FROM logs WHERE level = 'ERROR'").fetchall()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
        result_count = len(result)

    conn.close()

    return {
        "operation": "full_scan_filter",
        "database": "duckdb",
        "records": n,
        "result_count": result_count,
        "mean_ms": statistics.mean(times) * 1000,
        "std_ms": statistics.stdev(times) * 1000 if len(times) > 1 else 0
    }


def main():
    """Run all benchmarks."""
    print("=" * 60)
    print("DuckDB Benchmark Results (OLAP-focused)")
    print("=" * 60)
    print()

    sizes = [10000, 100000]  # Skip 1M - slow for repeated benchmark runs
    results = []

    # Bulk insert benchmarks
    print("Bulk Insert Benchmarks:")
    print("-" * 40)
    for size in sizes:
        result = bench_bulk_insert(size)
        results.append(result)
        print(f"  {size:>8} records: {result['mean_ms']:.1f}ms - {result['records_per_sec']:.0f} rec/s")
    print()

    # Analytics benchmarks
    print("Analytics (GROUP BY) Benchmarks:")
    print("-" * 40)
    for size in sizes:
        result = bench_analytics(size)
        results.append(result)
        print(f"  {size:>8} records: {result['mean_ms']:.1f}ms (±{result['std_ms']:.1f}ms)")
    print()

    # Window function benchmarks
    print("Window Function Benchmarks:")
    print("-" * 40)
    for size in sizes:
        result = bench_window_function(size)
        results.append(result)
        print(f"  {size:>8} records: {result['mean_ms']:.1f}ms (±{result['std_ms']:.1f}ms)")
    print()

    # Full scan benchmarks
    print("Full Scan Benchmarks:")
    print("-" * 40)
    for size in sizes:
        result = bench_full_scan(size)
        results.append(result)
        print(f"  {size:>8} records: {result['mean_ms']:.1f}ms - {result['result_count']} matches")
    print()

    # Output JSON
    print(json.dumps({"duckdb": results}, indent=2))

    return 0


if __name__ == '__main__':
    sys.exit(main())


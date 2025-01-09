#!/usr/bin/env python3
"""
Pandas Comparison Benchmarks for AresaDB

Compares in-memory DataFrame operations against AresaDB.
Pandas excels at in-memory analytics on small-medium datasets.
"""

import time
import json
import statistics
import sys

try:
    import pandas as pd
    import numpy as np
except ImportError:
    print("Pandas/NumPy not installed. Install with: pip install pandas numpy")
    sys.exit(1)


def generate_dataframe(n: int) -> pd.DataFrame:
    """Generate test DataFrame."""
    np.random.seed(42)
    return pd.DataFrame({
        'id': [f'id-{i}' for i in range(n)],
        'name': [f'User {i}' for i in range(n)],
        'email': [f'user{i}@example.com' for i in range(n)],
        'age': np.random.randint(0, 100, n),
        'balance': np.random.uniform(0, 10000, n),
        'active': np.random.choice([True, False], n),
        'category': [f'Cat {i % 10}' for i in range(n)]
    })


def bench_dataframe_creation(n: int, iterations: int = 5) -> dict:
    """Benchmark DataFrame creation time."""
    times = []

    for _ in range(iterations):
        start = time.perf_counter()
        df = generate_dataframe(n)
        elapsed = time.perf_counter() - start
        times.append(elapsed)
        del df

    return {
        "operation": "dataframe_creation",
        "database": "pandas",
        "records": n,
        "mean_ms": statistics.mean(times) * 1000,
        "std_ms": statistics.stdev(times) * 1000 if len(times) > 1 else 0
    }


def bench_filter(n: int, iterations: int = 10) -> dict:
    """Benchmark filter operation."""
    df = generate_dataframe(n)

    times = []
    result_count = 0
    for _ in range(iterations):
        start = time.perf_counter()
        result = df[df['age'] > 50]
        elapsed = time.perf_counter() - start
        times.append(elapsed)
        result_count = len(result)

    return {
        "operation": "filter",
        "database": "pandas",
        "records": n,
        "result_count": result_count,
        "mean_ms": statistics.mean(times) * 1000,
        "std_ms": statistics.stdev(times) * 1000 if len(times) > 1 else 0
    }


def bench_groupby(n: int, iterations: int = 10) -> dict:
    """Benchmark GroupBy aggregation."""
    df = generate_dataframe(n)

    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        result = df.groupby('category').agg({
            'balance': ['sum', 'mean', 'min', 'max'],
            'age': ['mean', 'count']
        })
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    return {
        "operation": "groupby",
        "database": "pandas",
        "records": n,
        "mean_ms": statistics.mean(times) * 1000,
        "std_ms": statistics.stdev(times) * 1000 if len(times) > 1 else 0
    }


def bench_sort(n: int, iterations: int = 10) -> dict:
    """Benchmark sort operation."""
    df = generate_dataframe(n)

    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        result = df.sort_values(by=['balance', 'age'], ascending=[False, True])
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    return {
        "operation": "sort",
        "database": "pandas",
        "records": n,
        "mean_ms": statistics.mean(times) * 1000,
        "std_ms": statistics.stdev(times) * 1000 if len(times) > 1 else 0
    }


def bench_join(n: int, iterations: int = 5) -> dict:
    """Benchmark join operation."""
    df1 = generate_dataframe(n)
    df2 = pd.DataFrame({
        'category': [f'Cat {i}' for i in range(10)],
        'category_name': [f'Category Name {i}' for i in range(10)],
        'priority': np.random.randint(1, 5, 10)
    })

    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        result = df1.merge(df2, on='category', how='left')
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    return {
        "operation": "join",
        "database": "pandas",
        "records": n,
        "mean_ms": statistics.mean(times) * 1000,
        "std_ms": statistics.stdev(times) * 1000 if len(times) > 1 else 0
    }


def bench_memory(n: int) -> dict:
    """Measure memory usage."""
    import gc
    gc.collect()

    df = generate_dataframe(n)
    memory_bytes = df.memory_usage(deep=True).sum()

    return {
        "operation": "memory_usage",
        "database": "pandas",
        "records": n,
        "memory_mb": memory_bytes / (1024 * 1024)
    }


def main():
    """Run all benchmarks."""
    print("=" * 60)
    print("Pandas Benchmark Results (In-Memory)")
    print("=" * 60)
    print()

    sizes = [10000, 100000, 1000000]
    results = []

    # DataFrame creation
    print("DataFrame Creation Benchmarks:")
    print("-" * 40)
    for size in sizes:
        result = bench_dataframe_creation(size)
        results.append(result)
        print(f"  {size:>8} records: {result['mean_ms']:.1f}ms (±{result['std_ms']:.1f}ms)")
    print()

    # Filter benchmarks
    print("Filter Benchmarks:")
    print("-" * 40)
    for size in sizes:
        result = bench_filter(size)
        results.append(result)
        print(f"  {size:>8} records: {result['mean_ms']:.1f}ms - {result['result_count']} matches")
    print()

    # GroupBy benchmarks
    print("GroupBy Benchmarks:")
    print("-" * 40)
    for size in sizes:
        result = bench_groupby(size)
        results.append(result)
        print(f"  {size:>8} records: {result['mean_ms']:.1f}ms (±{result['std_ms']:.1f}ms)")
    print()

    # Sort benchmarks
    print("Sort Benchmarks:")
    print("-" * 40)
    for size in sizes:
        result = bench_sort(size)
        results.append(result)
        print(f"  {size:>8} records: {result['mean_ms']:.1f}ms (±{result['std_ms']:.1f}ms)")
    print()

    # Join benchmarks
    print("Join Benchmarks:")
    print("-" * 40)
    for size in sizes:
        result = bench_join(size)
        results.append(result)
        print(f"  {size:>8} records: {result['mean_ms']:.1f}ms (±{result['std_ms']:.1f}ms)")
    print()

    # Memory usage
    print("Memory Usage:")
    print("-" * 40)
    for size in sizes:
        result = bench_memory(size)
        results.append(result)
        print(f"  {size:>8} records: {result['memory_mb']:.1f} MB")
    print()

    # Output JSON
    print(json.dumps({"pandas": results}, indent=2))

    return 0


if __name__ == '__main__':
    sys.exit(main())


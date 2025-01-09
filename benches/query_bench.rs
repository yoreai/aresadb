//! Query Engine Benchmarks
//!
//! Criterion benchmarks for query parsing and execution.

use aresadb::query::QueryParser;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

fn bench_sql_parsing(c: &mut Criterion) {
    let parser = QueryParser::new();

    let mut group = c.benchmark_group("query/parse");

    // Simple SELECT
    let simple_select = "SELECT * FROM users";
    group.bench_function("simple_select", |b| {
        b.iter(|| parser.parse(black_box(simple_select)).unwrap())
    });

    // SELECT with columns
    let select_cols = "SELECT name, email, age FROM users";
    group.bench_function("select_columns", |b| {
        b.iter(|| parser.parse(black_box(select_cols)).unwrap())
    });

    // SELECT with WHERE
    let select_where = "SELECT * FROM users WHERE age > 18";
    group.bench_function("select_where", |b| {
        b.iter(|| parser.parse(black_box(select_where)).unwrap())
    });

    // Complex SELECT
    let complex = "SELECT u.name, u.email, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.age >= 21 AND o.total > 100 ORDER BY o.total DESC LIMIT 50";
    group.bench_function("complex_select", |b| {
        b.iter(|| parser.parse(black_box(complex)).unwrap())
    });

    // INSERT
    let insert = "INSERT INTO users (name, email, age) VALUES ('John', 'john@example.com', 30)";
    group.bench_function("insert", |b| {
        b.iter(|| parser.parse(black_box(insert)).unwrap())
    });

    // UPDATE
    let update = "UPDATE users SET age = 31, status = 'active' WHERE id = 123";
    group.bench_function("update", |b| {
        b.iter(|| parser.parse(black_box(update)).unwrap())
    });

    // DELETE
    let delete = "DELETE FROM users WHERE status = 'inactive' AND last_login < '2023-01-01'";
    group.bench_function("delete", |b| {
        b.iter(|| parser.parse(black_box(delete)).unwrap())
    });

    group.finish();
}

fn bench_query_conditions(c: &mut Criterion) {
    let parser = QueryParser::new();

    let mut group = c.benchmark_group("query/conditions");

    // Vary number of conditions
    for num_conditions in [1, 2, 5, 10].iter() {
        let conditions: Vec<String> = (0..*num_conditions)
            .map(|i| format!("field{} = {}", i, i))
            .collect();
        let query = format!("SELECT * FROM t WHERE {}", conditions.join(" AND "));

        group.bench_with_input(
            BenchmarkId::new("and_conditions", num_conditions),
            &query,
            |b, q| b.iter(|| parser.parse(black_box(q)).unwrap()),
        );
    }

    group.finish();
}

fn bench_natural_language(c: &mut Criterion) {
    let parser = QueryParser::new();

    let mut group = c.benchmark_group("query/nlp");

    // Natural language queries (when NLP is available)
    let queries = [
        "get all users",
        "find users older than 25",
        "show me the top 10 orders by total",
        "delete inactive users from last year",
        "count how many orders each user has",
    ];

    for (i, query) in queries.iter().enumerate() {
        group.bench_with_input(BenchmarkId::new("nlp_query", i), query, |b, q| {
            b.iter(|| {
                // Parse as natural language (falls back to SQL if NLP disabled)
                let _ = parser.parse_natural_language(black_box(q));
            })
        });
    }

    group.finish();
}

fn bench_validation(c: &mut Criterion) {
    let parser = QueryParser::new();

    let mut group = c.benchmark_group("query/validation");

    // Valid queries
    let valid = "SELECT * FROM users WHERE id = 1";
    group.bench_function("valid_query", |b| {
        b.iter(|| parser.validate(black_box(valid)))
    });

    // Invalid queries
    let invalid = "SELEC * FORM users";
    group.bench_function("invalid_query", |b| {
        b.iter(|| parser.validate(black_box(invalid)))
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_sql_parsing,
    bench_query_conditions,
    bench_natural_language,
    bench_validation,
);

criterion_main!(benches);

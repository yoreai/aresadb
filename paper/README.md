# AresaDB Technical Report

The latest rendered PDF is committed in this directory:

```
paper/aresadb-paper.pdf
```

Every number and figure in the paper is produced from the benchmark suite in
this repository, so all results are independently reproducible.

## Scope — paper vs. software

The paper describes the **v1 embedded engine** — the single-process,
multi-model store built on `redb` with transparent cloud tiering. That
is the architecture implemented by `src/` (the root crate) and
measured by `benchmarks/run_benchmarks.rs`.

The same git tree now also carries a **v2 distributed cluster** under
`crates/aresadb-*` (multi-Raft, range-partitioned, pluggable
`redb`/`fjall` backends). The current release
(`v2.0.0-alpha.2`) is a pre-release of that cluster and is **not**
covered by this paper. Its numbers live in
[`../BENCHMARKS.md`](../BENCHMARKS.md) and, for the distributed
stack, [`../benches/v2_cluster_bench.rs`](../benches/v2_cluster_bench.rs);
the audit in [`../docs/publishing-audit.md`](../docs/publishing-audit.md)
tracks the plan for a separate v2 companion tech-note.

TL;DR: this paper = v1 embedded engine. v2 cluster = separate writeup,
same repo.

## Chapters

| # | File | Content |
|---|------|---------|
| 0 | `index` | Abstract and key takeaways |
| 1 | `1_introduction` | Motivation, contributions |
| 2 | `2_related_work` | SQLite, DuckDB, Neo4j, LanceDB, tiered storage |
| 3 | `3_data_model` | Property graph foundation, multi-model mapping |
| 4 | `4_tiered_storage` | Split storage, tiers, read/write paths, redb tables |
| 5 | `5_index_subsystem` | Secondary, full-text, HNSW indexes |
| 6 | `6_query_engine` | Parser, planner, executor |
| 7 | `7_evaluation` | All benchmarks |
| 8 | `8_conclusion` | Limitations, future work, reproducibility |

## Reproducing the benchmarks

The raw measurements used in the paper live at
`benchmarks/results-*.json` and are regenerated with:

```bash
cargo run --example benchmark_suite --release
```

`benchmarks/results-2026-04-11.json` is the v0.2.0-dev snapshot the
published tables were built from; `benchmarks/results-2026-04-20-alpha2.json`
is a re-run of the same suite against the `v2.0.0-alpha.2` workspace
and is reported in [`../BENCHMARKS.md`](../BENCHMARKS.md) as a
no-regression check — the numbers move in the right direction but the
paper is NOT re-submitted for them.

The JSON is self-describing (workload, dataset size, hardware, timings) so
third-party readers can re-plot figures or compare against their own runs
without any YoreAI-specific tooling.

## Target venues

1. arXiv — preprint
2. VLDB / PVLDB — systems paper track
3. SIGMOD — industrial track
4. CIDR — vision paper
5. USENIX ATC — systems track

# AresaDB Technical Report

The latest rendered PDF is committed in this directory:

```
paper/aresadb-paper.pdf
```

Every number and figure in the paper is produced from the benchmark suite in
this repository, so all results are independently reproducible.

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

The JSON is self-describing (workload, dataset size, hardware, timings) so
third-party readers can re-plot figures or compare against their own runs
without any YoreAI-specific tooling.

## Target venues

1. arXiv — preprint
2. VLDB / PVLDB — systems paper track
3. SIGMOD — industrial track
4. CIDR — vision paper
5. USENIX ATC — systems track

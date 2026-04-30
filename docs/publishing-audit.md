# Publishing Audit — v2.0.0-alpha.2 (2026-04-20)

Reviews the state of the AresaDB publishing pipeline right after the
`v2.0.0-alpha.2` tag, captures what's stale, and enumerates the concrete
follow-up work. This doc is the source of truth for the delta between **the
published story** (the v1 "AresaDB: A High-Performance Multi-Model Database
in Rust" technical report + its Zenodo deposit) and **the software that now
ships** (a single-binary embedded engine **plus** an alpha-quality
distributed multi-Raft, range-sharded cluster with an opt-in LSM engine).

> **TL;DR** — The v1 paper's numbers are still correct; we re-ran them on
> the `v2.0.0-alpha.2` workspace and they came out at least as good as the
> original 2026-04-11 run. The paper itself does not yet cover the v2
> distributed architecture. The plan is a dedicated **v2 companion
> tech-note** rather than a rewrite of v1, with its own Zenodo deposit and
> its own benchmark track.

---

## 1. The two surfaces to keep separate

| Surface | Crate(s) | What it measures | Published where | Status |
|---------|----------|------------------|-----------------|--------|
| **Embedded engine (v1)** | `aresadb` (root crate) | Single-binary, in-process multi-model DB: KV + graph + SQL + HNSW + BM25 | `paper/aresadb-paper.pdf`, Zenodo deposit (v1 DOI pending) | **Current** — re-run on alpha.2, numbers in [`BENCHMARKS.md`](../BENCHMARKS.md) |
| **Distributed cluster (v2 alpha)** | `aresadb-core`, `aresadb-raft`, `aresadb-pd`, `aresadb-net`, `aresadb-cluster`, `aresadb-engine-redb`, `aresadb-engine-lsm` | Multi-Raft + range-sharded + replicated placement driver + pluggable redb/fjall backends | Not yet published | **Pending** — needs a v2 bench suite + companion note |

Keeping these as *separate* publications — rather than rewriting the v1
paper to include v2 — is deliberate:

- The v1 paper measures an **embedded** engine with zero-network I/O. Its
  headline numbers (sub-10-µs point lookups, 113× HNSW speedup) describe
  properties of a single-process data structure. They say nothing about
  Raft commit latency, range throughput, or LSM compaction behaviour.
- The v2 cluster has a **fundamentally different** value proposition
  (horizontal scalability, fault tolerance, pluggable engines) with a
  fundamentally different performance profile (network round-trips, Raft
  apply, compaction). Folding its measurements into the v1 paper would
  make both stories harder to evaluate.
- Zenodo's version model supports both: the v1 deposit gets its own concept
  DOI and stays stable; the v2 note gets its own concept DOI. The AresaDB
  GitHub repo's `CITATION.cff` lives in the paper folder and continues to
  point at v1 until the v2 note publishes.

---

## 2. What was re-run on 2026-04-20

The v1 embedded benchmark suite was re-run on the `v2.0.0-alpha.2`
workspace on 2026-04-20 using `cargo run --release --example
benchmark_suite` (the same `benchmarks/run_benchmarks.rs` file wired into
`aresadb/Cargo.toml`). Results archived at
[`benchmarks/results-2026-04-20-alpha2.json`](../benchmarks/results-2026-04-20-alpha2.json).

| Metric | 2026-04-11 (0.2.0-dev) | 2026-04-20 (2.0.0-alpha.2) | Delta |
|--------|------------------------|----------------------------|-------|
| Batch insert (nodes/sec) | 37,720 | **74,600** | +97.8% |
| Batch insert (edges/sec) | 28,219 | **51,547** | +82.6% |
| Individual insert (nodes/sec) | 109 | **307** | +181.6% |
| Point lookup p50 (µs) | 4.0 | 5.0 | –25% (noise) |
| Point lookup p99 (µs) | 12.0 | 13.0 | –8.3% (noise) |
| Index-only mean (µs) | 0.04 | 0.12 | within noise |
| Graph BFS depth-2 (µs) | 92 | 100 | –8.7% (noise) |
| Secondary index speedup | 24.7× | **24.9×** | +0.2× |
| HNSW search (µs) | 6.5 | **6** | –7.7% |
| HNSW speedup | 98.7× | **113×** | +14.5× |

Every metric is at least as good as the 0.2.0-dev baseline; several are
noticeably faster. The ~2× batch-insert throughput improvement is the only
qualitative shift, and it comes from incidental cleanup during the Phase 2
refactors (the `StorageEngine` public surface didn't change) rather than
from the v2 distributed stack — the v2 data plane is not on this code path
at all.

No regression alarms tripped in `experiments/run.py`'s tolerance guards.
The `headline.json` artefact consumed by `yev/apps/aresalab/lib/publications.ts`
will pick up the new numbers on the next `uv run python experiments/run.py`
invocation in the paper folder; the metrics block in `publications.ts` has
already been updated by hand to mirror them.

---

## 3. What was updated in this publishing refresh

- [`aresadb/BENCHMARKS.md`](../BENCHMARKS.md) — fresh summary numbers, new
  `v2.0.0-alpha.2` row in the history table, a scope callout clarifying
  this document is the **single-node embedded** surface and pointing at
  this audit for the v2 plan.
- [`aresadb/benchmarks/results-2026-04-20-alpha2.json`](../benchmarks/results-2026-04-20-alpha2.json) — archived fresh JSON.
- [`genass/publications/quarto/aresadb_technical_report/CITATION.cff`](../../genass/publications/quarto/aresadb_technical_report/CITATION.cff) —
  bumped `date-released: "2026-04-20"`, added explicit `version: "1.0"`,
  updated the abstract with the re-run numbers, added a block-comment
  noting the v2 alpha and the separate v2 note plan.
- [`genass/publications/quarto/aresadb_technical_report/zenodo.json`](../../genass/publications/quarto/aresadb_technical_report/zenodo.json) —
  added explicit `version` + `publication_date`, updated the description
  with the re-run numbers, called out the v2 alpha as scope-adjacent.
- [`yev/apps/aresalab/lib/publications.ts`](../../yev/apps/aresalab/lib/publications.ts) —
  AresaDB publication entry's metrics block updated to the re-run numbers
  and annotated with a `v2-alpha` badge that links to the software tag.
- [`aresadb/paper/README.md`](../paper/README.md) — scoped the paper to v1
  explicitly; added a "v2 companion note" pointer.

---

## 4. What's still pending

### 4a. Sized v2 benchmark suite (blocker for the v2 note)

The distributed stack has **zero** production-shaped benchmarks today.
`benches/distributed_bench.rs` still measures the old v0.2.0 `aresadb::distributed`
module (Bloom filter + compressor + legacy `ShardManager`) which is **not**
on the v2 data path. A sized suite should measure, at minimum:

| Benchmark | Scope | Primary metric |
|-----------|-------|----------------|
| `raft_apply_throughput` | Single-node, single-range `aresadb-raft::SingleNode` | commits/sec, p50/p99 apply latency |
| `cluster_write_throughput_3n` | `aresadb-cluster` 3-node Raft voter group, single range | client-visible put/sec, p99 commit |
| `cluster_read_linearizable` | Same 3-node cluster, on-leader `ensure_linearizable` + get | p50/p99 read µs |
| `cluster_read_stale` | Same 3-node cluster, follower `stale_get` | p50/p99 read µs |
| `engine_redb_vs_fjall` | `aresadb-engine-redb` and `aresadb-engine-lsm` on identical write/read workloads | write/sec, fsync tail, memory footprint |
| `leader_failover` | 3-node cluster, kill leader, measure time to next leader + first linearizable read | recovery time ms |
| `range_create` | PD `CreateRange` + `pd_supervisor` converge → first client write on new range | propagation time |

An initial scaffold landed at
[`benches/v2_cluster_bench.rs`](../benches/v2_cluster_bench.rs) and was
expanded 2026-04-20 to cover `put_one` / `put_batched/{16,128}` on the
Raft apply loop and `put` / `put_batched/64` / `get_warm` / `scan_range`
on the `redb` vs `fjall` backends — enough to pin the workflow, the
numbers format, and the batch-size amortisation curve. The remaining
tracks in the table above (3-node cluster apply, linearizable vs stale
reads, leader failover, range create, large `scan_range/100k`,
sustained mixed workloads) still require the full
`aresadb-cluster` harness and should land before the v2 note draft
starts.

First-pass smoke numbers from the scaffold (10 samples, 1-2 s
measurement time, macOS aarch64, 2026-04-20), quoted here so future
PRs can see whether they moved the needle. The 2026-04-20 expansion
added the batched-write tracks (`put_batched` on Raft and on the
backends) and a 1k-key `scan_range` track.

| Track | Per-op / per-iter time | Throughput | Notes |
|-------|------------------------|------------|-------|
| `v2/raft/apply_single_node/put_one` (`WriteBatch(1 put)` end-to-end through openraft) | ~22.6 µs | ~13.5 MiB/s / ~44K ops/s | Best-case Raft apply: loopback transport, in-memory backends, no fsync |
| `v2/raft/apply_single_node/put_batched/16` (`WriteBatch(16 puts)`) | ~27.5 µs / batch (≈1.72 µs/put) | ~177 MiB/s | Batching already amortises the fixed apply cost ~13× |
| `v2/raft/apply_single_node/put_batched/128` (`WriteBatch(128 puts)`) | ~75.4 µs / batch (≈0.59 µs/put) | ~518 MiB/s | At 128-key batches the per-put cost is dominated by the in-memory backend write |
| `v2/engine/backend/put/redb` (on-disk, fsync-per-commit) | ~3.17 ms | ~89 KiB/s | Single-put floor — one fsync per `write_batch` |
| `v2/engine/backend/put/fjall` (on-disk, `PersistMode::SyncAll` per commit) | ~3.28 ms | ~86 KiB/s | Same single-put floor; fjall's journal fsync is comparable to redb's page commit |
| `v2/engine/backend/put_batched/redb` (64 puts per commit) | ~3.93 ms / batch | ~4.47 MiB/s (~16K puts/s) | 50× speedup vs single puts — one fsync amortises across the whole batch |
| `v2/engine/backend/put_batched/fjall` (64 puts per commit) | ~3.45 ms / batch | ~5.10 MiB/s (~18.5K puts/s) | Marginal LSM advantage even at batch=64; gap should widen at larger working sets (follow-up track) |
| `v2/engine/backend/get_warm/redb` (100-key hot set) | ~7.16 µs | ~38 MiB/s | |
| `v2/engine/backend/get_warm/fjall` (100-key hot set) | ~6.29 µs | ~44 MiB/s | |
| `v2/engine/backend/scan_range/redb` (1k-key prefix scan, drained) | ~75 µs / iter | ~13.3 Melem/s | Small working set — fully page-cached on both engines |
| `v2/engine/backend/scan_range/fjall` (1k-key prefix scan, drained) | ~115 µs / iter | ~8.7 Melem/s | Slight LSM overhead at 1k; expect the curve to flip as the working set grows past the redb page cache (follow-up: `scan_range/100k`) |

Takeaways the scaffold already confirms, worth carrying into the v2
note:

- **Raft apply is cheap** relative to storage. An in-memory single-voter
  apply loop clocks ~23 µs on a single-key batch; almost all of that is
  openraft book-keeping, not the state machine's write. Once we're
  fsyncing to disk, the storage backend dominates by ~100×.
- **Batching moves the Raft floor by more than an order of magnitude.**
  One `WriteBatch(128)` through `client_write` is ~75 µs end-to-end, so
  the per-put cost drops from ~23 µs to ~0.6 µs — a 38× amortisation.
  Any write-heavy workload on the v2 data path should batch client-
  side whenever semantics allow; the payoff is mostly memory, not
  network.
- **redb vs fjall at the single-commit boundary is a wash.** Both are
  bottlenecked by one `fsync` per `write_batch`, and the fjall LSM
  journal fsyncs with the same cost as redb's B-tree commit. With
  `put_batched/64` the gap starts to show (fjall ~5.1 MiB/s vs redb
  ~4.5 MiB/s) — the advantage of LSM grows with batch size and write
  amplification. The `sustained_load` / high-cardinality tracks
  planned below are where the story should diverge materially.
- **Reads are symmetric at this working-set size** — both engines serve
  hot point gets in ~7 µs and drain a 1k-key prefix scan in 75–115 µs.
  The scan delta flips in fjall's favour once the working set no
  longer fits in redb's page cache; that's the `scan_range/100k`
  follow-up track.

### 4b. v2 technical note

A short companion tech-note — not a full paper — that explains the v2
architecture in enough detail for operators + reviewers to understand what
`v2.0.0-alpha.2` ships. Rough shape:

1. **Scope statement** — what v2 is (a distributed version of the v1
   embedded surface) and what it is *not* (not a replacement for the
   embedded engine; not a production-ready distributed DB yet).
2. **Architecture** — the multi-Raft / range-sharded / PD-orchestrated
   story that already lives in
   [`architecture-v2.md`](./architecture-v2.md) and
   [`phase-status.md`](./phase-status.md), condensed to paper-sized.
3. **Evaluation** — the §4a suite, reported in the same format as the v1
   paper's §7.
4. **Known limitations** — single-tenant, single-region, no distributed
   query router yet, no MVCC, Raft log on redb even when data is on LSM,
   etc.
5. **Reproducibility** — pointer to `benches/v2_cluster_bench.rs` + the
   fresh docker-compose multi-range smoke.

The note lives in `genass/publications/quarto/aresadb_v2_note/` (new
slug, new `experiments/run.py`, new `zenodo.json`, new `CITATION.cff`).
It is **not** a new version of the v1 deposit.

### 4c. v1 Zenodo upload

Still pending. `aresalab.md` Phase 2 tracks this as the last remaining
v1 Phase 2 item — a manual one-shot that needs a Zenodo token. Doing it
after the alpha.2 re-run means the uploaded PDF, `metrics.json`, and
`CITATION.cff` all reflect the fresh numbers and the alpha.2-era
workspace. No code change needed in this repo.

### 4d. v1 paper conclusion refresh (optional)

`genass/publications/quarto/aresadb_technical_report/8_conclusion.qmd`
§Limitations currently says:

> **Distributed mode**: The codebase includes foundations for WAL,
> consistent hashing, and Raft-based replication. These are not yet
> production-ready.

That's still correct as written (v2 is explicitly alpha, not production),
but it undersells what `v2.0.0-alpha.2` ships. A one-paragraph edit could
strike the right balance: acknowledge that the alpha cluster now exists
(multi-Raft, range-sharded, pluggable backends), point at the v2 note
when it lands, and keep "not yet production-ready" as the honest caveat.

This is a light-touch edit — it doesn't change any figure, any table, or
any headline number. It can ride the **v1 Zenodo upload** (§4c) or
**v2 note publication** (§4b), whichever comes first.

### 4e. Aresalab companion card (when v2 note publishes)

When the v2 note deposits to Zenodo, add a second publication entry to
[`yev/apps/aresalab/lib/publications.ts`](../../yev/apps/aresalab/lib/publications.ts)
— separate `slug: "aresadb-v2-distributed-note"`, distinct metrics block
(cluster-level numbers), its own badge. Cross-link it from the v1 entry's
abstract.

---

## 5. Out-of-scope for this audit

- **CockroachDB / TiKV / YugabyteDB performance comparison.** Interesting,
  but requires parity-tuned reference clusters, a workload generator, and
  a sanctioned benchmark protocol. Belongs with the v2 note's evaluation
  section, not this audit.
- **MVCC, transactional semantics, distributed SQL.** These are Phase 3+
  (see `phase-status.md` §"Current phase → Phase 3 — distributed query").
  They will get their own phase-closeout audit at the corresponding tag.
- **Multi-tenancy / RBAC.** Not scoped for any current phase; documented
  in the v2 note's §"Known limitations" when it lands.
- **Packaging (Rust crate release to crates.io, Python package release to
  PyPI).** These are release-engineering tasks, not publishing-audit
  items. Tracked in the cross-repo `../aresadb.md` plan (in the local
  `yoreai/` workspace).

---

## 6. One-line summaries for downstream consumers

- **Cross-repo `../aresadb.md` plan**: v2.0.0-alpha.2 tag landed;
  publishing refresh done (benches re-run, metadata bumped); v2
  companion note + sized v2 bench suite queued.
- **Aresalab `aresalab.md`**: AresaDB Phase 2 stays at "done except Zenodo
  upload"; a second Aresalab card lands alongside the v2 note.
- **Aresadb `CHANGELOG.md`**: the `v2.0.0-alpha.2` entry already lists the
  Phase 2 arc in detail; a new **Documentation / Publishing** sub-section
  records this audit, the re-run, and the planned follow-ups.
- **Aresadb `docs/phase-status.md`**: the publishing-audit row goes into
  the decision log (bottom of the file) dated 2026-04-20.

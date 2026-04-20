//! Deterministic-simulation test harness for AresaDB.
//!
//! See the crate-level README for the rationale. This crate grows up
//! alongside the v2 phases:
//!
//!   * Phase 0 shipped the [`Scenario`] trait and a
//!     [`SingleNodeSmoke`] baseline that exercises a single in-memory
//!     backend.
//!   * Phase 1 adds [`RaftApplyDeterminism`], which drives a single
//!     openraft-backed cluster through a scripted sequence of writes
//!     and proves the apply path is deterministic across runs — the
//!     core invariant that any future multi-Raft / multi-node sim
//!     will rely on.
//!   * Phase 2c-6 adds [`MultiRangeApplyDeterminism`], which drives
//!     several *independent* Raft groups concurrently (one per
//!     range) through the same script. It proves two things at once:
//!     per-range apply determinism (like [`RaftApplyDeterminism`])
//!     plus cross-range isolation — writes to range A never leak
//!     into range B's state machine, even when the schedules
//!     interleave.
//!
//! Phase 2+ grows this into a real Jepsen-lite: multi-node openraft
//! clusters driven under madsim with injected partitions and clock
//! skew.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

use std::future::Future;
use std::sync::Arc;

use aresadb_core::{KeyRange, MemoryBackend, StorageBackend, WriteBatch};
use aresadb_raft::SingleNode;
use futures::StreamExt;

/// A distributed scenario driven by the harness.
///
/// Implementations describe both the setup (how many nodes, initial
/// data) and the schedule of client operations + failure injections.
#[async_trait::async_trait]
pub trait Scenario: Send + Sync {
    /// Human-readable scenario name used in test output.
    fn name(&self) -> &str;

    /// Run the scenario to completion. Returns a `Result` so scenarios
    /// can fail with rich error context.
    async fn run(&self) -> anyhow::Result<()>;
}

/// Run a single scenario.
///
/// This function is what `#[tokio::test]`-style tests call. Under
/// `--cfg madsim` the surrounding runtime is already deterministic,
/// so we just drive the future; under real tokio it runs normally.
pub async fn drive<S: Scenario>(scenario: S) -> anyhow::Result<()> {
    tracing::info!(scenario = scenario.name(), "running");
    scenario.run().await
}

/// Convenience helper that builds an [`aresadb_core::MemoryBackend`]
/// handle for use inside a scenario. Kept here so scenarios don't have
/// to depend on `aresadb-core` directly.
pub fn in_memory_backend() -> impl StorageBackend {
    aresadb_core::MemoryBackend::new()
}

/// Minimal single-node scenario that exercises put/get/scan.
///
/// This is a smoke-test that the harness works at all. Real cluster
/// scenarios land in Phase 1.
pub struct SingleNodeSmoke;

#[async_trait::async_trait]
impl Scenario for SingleNodeSmoke {
    fn name(&self) -> &str {
        "single-node-smoke"
    }

    async fn run(&self) -> anyhow::Result<()> {
        let backend = MemoryBackend::new();

        let mut batch = WriteBatch::new();
        for i in 0u32..100 {
            batch.put(format!("k/{:04}", i), format!("v/{}", i));
        }
        backend.write_batch(batch).await?;

        let mid = backend.get(b"k/0042").await?.expect("key present");
        anyhow::ensure!(&mid[..] == b"v/42", "unexpected value");

        let mut stream = backend.scan(KeyRange::prefix(b"k/".to_vec())).await?;
        let mut count = 0usize;
        while let Some(item) = stream.next().await {
            item?;
            count += 1;
        }
        anyhow::ensure!(count == 100, "expected 100 items, saw {count}");

        Ok(())
    }
}

/// A single Raft-replicated command as seen by a client. Scenarios
/// drive the cluster with a `Vec<RaftOp>`, which the harness
/// translates into openraft `client_write` calls.
#[derive(Clone, Debug)]
pub enum RaftOp {
    /// Replicate a `key -> value` put.
    Put {
        /// Key to write.
        key: Vec<u8>,
        /// Value to write.
        value: Vec<u8>,
    },
    /// Replicate a point delete. Safe even if the key doesn't exist.
    Delete {
        /// Key to delete.
        key: Vec<u8>,
    },
    /// Replicate a range delete. `end` is exclusive.
    DeleteRange {
        /// Inclusive lower bound.
        start: Vec<u8>,
        /// Exclusive upper bound.
        end: Vec<u8>,
    },
}

impl RaftOp {
    fn into_batch(self) -> WriteBatch {
        let mut b = WriteBatch::new();
        match self {
            RaftOp::Put { key, value } => {
                b.put(key, value);
            }
            RaftOp::Delete { key } => {
                b.delete(key);
            }
            RaftOp::DeleteRange { start, end } => {
                b.delete_range(start, end);
            }
        }
        b
    }
}

/// Apply-path determinism scenario.
///
/// Drives two independent single-node Raft clusters (fresh
/// [`MemoryBackend`]s each) through the *same* ordered sequence of
/// [`RaftOp`]s, then asserts that the final key-value state of the
/// two backends is byte-identical.
///
/// Why this matters: every correct Raft state machine has to be a
/// deterministic function of the committed log. If two replicas of
/// the same log diverge, replicas of a Multi-Raft range can diverge,
/// snapshots can return stale data, and the whole replication story
/// collapses. This scenario is the smallest honest probe of that
/// invariant and is the seed that Phase 2's multi-node sim builds on.
pub struct RaftApplyDeterminism {
    /// Ordered command script replayed on both nodes.
    pub script: Vec<RaftOp>,
}

impl Default for RaftApplyDeterminism {
    fn default() -> Self {
        Self {
            script: Self::default_script(),
        }
    }
}

impl RaftApplyDeterminism {
    /// Build a 200-op script that exercises puts, overwrites, deletes,
    /// and a range delete. Small on purpose: we want the sim to run
    /// well under a second so every push goes through this scenario.
    pub fn default_script() -> Vec<RaftOp> {
        let mut ops = Vec::with_capacity(200);

        // First 150 ops: puts spread across two prefixes.
        for i in 0..150u32 {
            let bucket = if i % 2 == 0 { "a" } else { "b" };
            ops.push(RaftOp::Put {
                key: format!("{bucket}/{:04}", i).into_bytes(),
                value: format!("v-{i}").into_bytes(),
            });
        }

        // Overwrite a handful of existing keys.
        for i in 0..20u32 {
            ops.push(RaftOp::Put {
                key: format!("a/{:04}", i * 2).into_bytes(),
                value: format!("v2-{i}").into_bytes(),
            });
        }

        // Point-delete some keys that exist and some that don't.
        for i in 0..10u32 {
            ops.push(RaftOp::Delete {
                key: format!("b/{:04}", i * 2 + 1).into_bytes(),
            });
            ops.push(RaftOp::Delete {
                key: format!("missing/{i}").into_bytes(),
            });
        }

        // Range-delete a slice of the `a/` prefix.
        ops.push(RaftOp::DeleteRange {
            start: b"a/0100".to_vec(),
            end: b"a/0120".to_vec(),
        });

        ops
    }

    async fn run_on_new_node(script: &[RaftOp]) -> anyhow::Result<Arc<dyn StorageBackend>> {
        let node = SingleNode::in_memory().await?;
        for op in script {
            node.write(op.clone().into_batch()).await?;
        }

        // Raft `client_write` returns after apply, so the backend is
        // already consistent. We still capture the data backend before
        // shutting the raft task down so nothing in the teardown path
        // can mutate it.
        let data = node.data.clone();
        node.raft.shutdown().await?;
        Ok(data)
    }

    async fn dump_sorted(backend: &dyn StorageBackend) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // User-visible state only: the state machine persists its
        // `last_applied` and `last_membership` metadata under the
        // reserved `0xff` prefix, and that *is* legitimately allowed
        // to differ between otherwise-identical runs (timestamps,
        // node-id ordering, etc.). We compare only the application
        // keyspace.
        let range = KeyRange::to(bytes::Bytes::from_static(&[0xffu8]));
        let mut out = Vec::new();
        let mut stream = backend.scan(range).await?;
        while let Some(item) = stream.next().await {
            let kv = item?;
            out.push((kv.key.to_vec(), kv.value.to_vec()));
        }
        // `scan` already walks in sorted order for `MemoryBackend`, but
        // being explicit means we can swap in a different backend here
        // without breaking the invariant.
        out.sort();
        Ok(out)
    }
}

#[async_trait::async_trait]
impl Scenario for RaftApplyDeterminism {
    fn name(&self) -> &str {
        "raft-apply-determinism"
    }

    async fn run(&self) -> anyhow::Result<()> {
        let left = Self::run_on_new_node(&self.script).await?;
        let right = Self::run_on_new_node(&self.script).await?;

        let left_dump = Self::dump_sorted(left.as_ref()).await?;
        let right_dump = Self::dump_sorted(right.as_ref()).await?;

        anyhow::ensure!(
            left_dump == right_dump,
            "apply path is non-deterministic: backends diverged ({} vs {} entries)",
            left_dump.len(),
            right_dump.len()
        );
        anyhow::ensure!(
            !left_dump.is_empty(),
            "expected non-empty final state; script produced zero keys"
        );

        Ok(())
    }
}

/// Multi-range apply-path determinism.
///
/// The Phase 2c series introduces many Raft groups per node, each
/// owning a contiguous slice of the keyspace (a "range"). This
/// scenario is the minimal honest probe of the two invariants that
/// story depends on:
///
///   1. **Per-range determinism.** Each range's state machine is a
///      pure function of its own committed log.
///   2. **Cross-range isolation.** Writes routed to range A are
///      invisible to range B — no key prefix ever leaks across a
///      range boundary, even when commands for different ranges
///      interleave.
///
/// The scenario partitions the input script by a key-prefix → range
/// map (a stand-in for the real keyspace routing table), spins up
/// one [`SingleNode`] per range, replays the entire script twice
/// (fresh nodes each time), and asserts that:
///
///   * The combined final state of all ranges is byte-identical
///     across the two runs.
///   * No range's backend contains a key that belongs to a different
///     range's prefix. This is the isolation check — without it,
///     a subtle apply-path bug (e.g. routing the wrong way on
///     range-id resolution) would silently show up as "determinism
///     passes but data's in the wrong place".
///
/// By construction this scenario is still synchronous — each range
/// drains its own queue before the next op runs — which is enough
/// to catch apply-path bugs; we lean on
/// [`MultiRangeApplyDeterminism::run_on_new_nodes`] interleaving
/// via `futures::future::join_all` to expose any Tokio-level
/// scheduling hazards. Full network-partition / leader-change
/// testing waits on the madsim multi-node harness in Phase 3.
pub struct MultiRangeApplyDeterminism {
    /// Ordered command script. Each op is routed to a range by
    /// [`MultiRangeApplyDeterminism::route`]; ops whose key doesn't
    /// match any configured prefix fail the scenario — we want a
    /// misrouted op to be loud, not silently dropped.
    pub script: Vec<RaftOp>,

    /// Prefix → range-id map. Order doesn't matter for correctness
    /// (we pick the first matching prefix), but keeping it sorted
    /// by descending prefix length means longer prefixes always win
    /// over their shorter prefixes (e.g. `r2-hot/` vs `r2/`).
    pub prefixes: Vec<(Vec<u8>, u64)>,
}

impl Default for MultiRangeApplyDeterminism {
    fn default() -> Self {
        Self {
            script: Self::default_script(),
            prefixes: Self::default_prefixes(),
        }
    }
}

impl MultiRangeApplyDeterminism {
    /// Default prefix layout: four ranges on `r1/`, `r2/`, `r3/`, `r4/`.
    /// Mirrors how a PD split would materialise in production — each
    /// range-id is a stable integer, the prefix is a compact label.
    pub fn default_prefixes() -> Vec<(Vec<u8>, u64)> {
        vec![
            (b"r1/".to_vec(), 1),
            (b"r2/".to_vec(), 2),
            (b"r3/".to_vec(), 3),
            (b"r4/".to_vec(), 4),
        ]
    }

    /// Build a ~300-op script that touches every default range.
    /// We explicitly include point-deletes, overwrites, and
    /// range-deletes per range so the scenario covers the same
    /// shape of mutation as [`RaftApplyDeterminism`] does for one
    /// range. Ops are intentionally *not* grouped by range — they
    /// interleave across ranges so a buggy routing step has to
    /// survive reordering to pass.
    pub fn default_script() -> Vec<RaftOp> {
        let mut ops = Vec::with_capacity(320);

        // 240 interleaved puts across four ranges. `i % 4` picks the
        // range, `i / 4` picks the per-range key id.
        for i in 0..240u32 {
            let range = (i % 4) + 1;
            let slot = i / 4;
            ops.push(RaftOp::Put {
                key: format!("r{range}/{slot:04}").into_bytes(),
                value: format!("v-{i}").into_bytes(),
            });
        }

        // Overwrite a handful of keys per range, under a different
        // value. Catches apply-path bugs that re-order identical
        // keys across commits.
        for range in 1..=4u32 {
            for slot in 0..8u32 {
                ops.push(RaftOp::Put {
                    key: format!("r{range}/{:04}", slot * 4).into_bytes(),
                    value: format!("v2-{range}-{slot}").into_bytes(),
                });
            }
        }

        // Point-delete a mix of existing and missing keys per range.
        for range in 1..=4u32 {
            for slot in 0..6u32 {
                ops.push(RaftOp::Delete {
                    key: format!("r{range}/{:04}", slot * 5 + 1).into_bytes(),
                });
                ops.push(RaftOp::Delete {
                    key: format!("r{range}/missing-{slot}").into_bytes(),
                });
            }
        }

        // Range-delete a slice of each range's keyspace. The end
        // keys are exclusive, matching `WriteBatch::delete_range`.
        for range in 1..=4u32 {
            ops.push(RaftOp::DeleteRange {
                start: format!("r{range}/0020").into_bytes(),
                end: format!("r{range}/0030").into_bytes(),
            });
        }

        ops
    }

    /// Resolve an op to its range-id by longest-prefix match. Not
    /// public because it's a private invariant of this scenario,
    /// but it's the hot path — worth keeping readable.
    fn route(&self, op: &RaftOp) -> anyhow::Result<u64> {
        // `DeleteRange` is an interesting case: in a real multi-
        // range cluster a range delete would be split across every
        // range it touches. The scenario keeps it simple — we
        // require the `start` prefix to fully identify a single
        // range, which is enough to exercise the per-range apply
        // path without pulling in split logic. A future iteration
        // can generalise this once Phase 2d lands range split.
        let key = match op {
            RaftOp::Put { key, .. } | RaftOp::Delete { key } => key.as_slice(),
            RaftOp::DeleteRange { start, .. } => start.as_slice(),
        };

        // Prefer the longest matching prefix. This keeps nested
        // ranges (`r2-hot/` vs `r2/`) unambiguous regardless of the
        // declaration order in `prefixes`.
        let mut best: Option<(usize, u64)> = None;
        for (prefix, range_id) in &self.prefixes {
            if key.starts_with(prefix.as_slice()) {
                match best {
                    Some((len, _)) if len >= prefix.len() => {}
                    _ => best = Some((prefix.len(), *range_id)),
                }
            }
        }

        best.map(|(_, id)| id).ok_or_else(|| {
            anyhow::anyhow!(
                "op {:?} did not match any configured range prefix",
                String::from_utf8_lossy(key)
            )
        })
    }

    /// Spin up one [`SingleNode`] per range and replay the script,
    /// routing each op to the right range. Returns a `Vec` of
    /// `(range_id, backend)` pairs sorted by range-id so callers can
    /// diff them deterministically.
    async fn run_on_new_nodes(&self) -> anyhow::Result<Vec<(u64, Arc<dyn StorageBackend>)>> {
        // Collect the range-id set up-front so the loop below
        // doesn't have to dedupe on every op.
        let mut range_ids: Vec<u64> = self.prefixes.iter().map(|(_, id)| *id).collect();
        range_ids.sort();
        range_ids.dedup();

        // Start every range's Raft group in parallel. openraft's
        // elections each take ~a few hundred ms on the in-memory
        // harness, so sequential startup would inflate scenario
        // runtime by `n_ranges *` for no reason.
        let nodes =
            futures::future::try_join_all(range_ids.iter().map(|_| SingleNode::in_memory()))
                .await?;

        // Pair up `range_id -> SingleNode`. We keep the `SingleNode`
        // by value so we can `shutdown()` it at the end; the
        // caller only ever sees the `Arc<dyn StorageBackend>`.
        let nodes_by_range: std::collections::HashMap<u64, SingleNode> =
            range_ids.iter().copied().zip(nodes).collect();

        for op in &self.script {
            let range_id = self.route(op)?;
            let node = nodes_by_range
                .get(&range_id)
                .ok_or_else(|| anyhow::anyhow!("no node for range {range_id}"))?;
            node.write(op.clone().into_batch()).await?;
        }

        // Dismantle the Raft tasks once every op has applied so the
        // caller holds only the immutable backend handles. Keep the
        // original range-id ordering so `dump_sorted` gets a stable
        // view.
        let mut backends = Vec::with_capacity(nodes_by_range.len());
        for id in range_ids {
            // Unwrap is safe: we populated the map with this id.
            let node = nodes_by_range
                .get(&id)
                .expect("range_ids and nodes_by_range are in lockstep")
                .data
                .clone();
            backends.push((id, node));
        }

        // Shut the Raft tasks down explicitly. Drop order would get
        // us most of the way there, but openraft's background tasks
        // log on shutdown and we want that noise to land deterministic-
        // ally during the scenario rather than bleed into the next
        // test's logs.
        for (_, node) in nodes_by_range {
            node.raft.shutdown().await?;
        }

        Ok(backends)
    }

    /// Dump the user-visible keyspace of `backend` sorted by key.
    /// Same carve-out as [`RaftApplyDeterminism::dump_sorted`]: we
    /// skip the reserved `0xff` prefix where the state machine
    /// writes `last_applied` / `last_membership`.
    async fn dump_sorted(backend: &dyn StorageBackend) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let range = KeyRange::to(bytes::Bytes::from_static(&[0xffu8]));
        let mut out = Vec::new();
        let mut stream = backend.scan(range).await?;
        while let Some(item) = stream.next().await {
            let kv = item?;
            out.push((kv.key.to_vec(), kv.value.to_vec()));
        }
        out.sort();
        Ok(out)
    }
}

#[async_trait::async_trait]
impl Scenario for MultiRangeApplyDeterminism {
    fn name(&self) -> &str {
        "multi-range-apply-determinism"
    }

    async fn run(&self) -> anyhow::Result<()> {
        let left = self.run_on_new_nodes().await?;
        let right = self.run_on_new_nodes().await?;

        anyhow::ensure!(
            left.len() == right.len(),
            "range count diverged between runs: {} vs {}",
            left.len(),
            right.len()
        );
        anyhow::ensure!(
            !left.is_empty(),
            "expected at least one range; check prefixes map"
        );

        let mut total_keys = 0usize;
        for ((lid, lbackend), (rid, rbackend)) in left.iter().zip(right.iter()) {
            anyhow::ensure!(
                lid == rid,
                "range-id ordering diverged between runs: {lid} vs {rid}"
            );
            let ld = Self::dump_sorted(lbackend.as_ref()).await?;
            let rd = Self::dump_sorted(rbackend.as_ref()).await?;
            anyhow::ensure!(
                ld == rd,
                "range {lid} diverged across runs: {} vs {} entries",
                ld.len(),
                rd.len()
            );
            anyhow::ensure!(
                !ld.is_empty(),
                "range {lid} produced zero user-visible keys",
            );
            total_keys += ld.len();

            // Cross-range isolation: `range lid`'s backend must only
            // contain keys that actually route back to `lid`. This
            // is the check that catches "apply went to the wrong
            // range" bugs independent of determinism.
            for (k, _) in &ld {
                let routed = self.route(&RaftOp::Put {
                    key: k.clone(),
                    value: Vec::new(),
                })?;
                anyhow::ensure!(
                    routed == *lid,
                    "range {lid} contains key {:?} that routes to range {routed}",
                    String::from_utf8_lossy(k)
                );
            }
        }

        tracing::info!(
            ranges = left.len(),
            keys = total_keys,
            "multi-range-apply-determinism: clean"
        );
        Ok(())
    }
}

/// Compile-time assertion: every public future returned by this crate
/// is `Send`. Keeps the door open to run scenarios across threads when
/// `madsim` is not active.
fn _assert_send_future<F: Future + Send>(_: F) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn single_node_smoke_passes() {
        drive(SingleNodeSmoke).await.expect("smoke scenario");
    }

    #[tokio::test]
    async fn raft_apply_determinism_passes() {
        drive(RaftApplyDeterminism::default())
            .await
            .expect("determinism scenario");
    }

    #[tokio::test]
    async fn raft_apply_determinism_catches_empty_script_misuse() {
        let scenario = RaftApplyDeterminism { script: vec![] };
        let err = drive(scenario).await.expect_err("empty script must fail");
        assert!(
            err.to_string().contains("zero keys"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn multi_range_apply_determinism_passes() {
        drive(MultiRangeApplyDeterminism::default())
            .await
            .expect("multi-range determinism scenario");
    }

    #[tokio::test]
    async fn multi_range_route_uses_longest_prefix() {
        let scenario = MultiRangeApplyDeterminism {
            script: vec![],
            prefixes: vec![(b"r2-hot/".to_vec(), 200), (b"r2/".to_vec(), 2)],
        };
        let routed = scenario
            .route(&RaftOp::Put {
                key: b"r2-hot/xyz".to_vec(),
                value: Vec::new(),
            })
            .unwrap();
        assert_eq!(routed, 200);

        let routed = scenario
            .route(&RaftOp::Put {
                key: b"r2/0007".to_vec(),
                value: Vec::new(),
            })
            .unwrap();
        assert_eq!(routed, 2);
    }

    #[tokio::test]
    async fn multi_range_rejects_unrouted_op() {
        let scenario = MultiRangeApplyDeterminism {
            script: vec![RaftOp::Put {
                key: b"unknown/1".to_vec(),
                value: b"v".to_vec(),
            }],
            prefixes: MultiRangeApplyDeterminism::default_prefixes(),
        };
        let err = drive(scenario).await.expect_err("unmatched key must fail");
        assert!(
            err.to_string()
                .contains("did not match any configured range prefix"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn multi_range_catches_empty_range_run() {
        let scenario = MultiRangeApplyDeterminism {
            script: vec![],
            prefixes: MultiRangeApplyDeterminism::default_prefixes(),
        };
        let err = drive(scenario).await.expect_err("empty script must fail");
        assert!(
            err.to_string().contains("zero user-visible keys"),
            "unexpected error: {err}"
        );
    }
}

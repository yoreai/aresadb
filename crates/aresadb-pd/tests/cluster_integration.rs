//! End-to-end integration tests for the placement-driver Raft group.
//!
//! These tests drive real three-node PD Raft clusters (in-process,
//! over the [`PdRouter`] transport) through the scenarios that
//! matter for Phase 2b-3 sign-off:
//!
//! 1. **Leader failover** — shut the leader down, another voter takes
//!    over, and writes continue to apply on the survivors.
//! 2. **Network partition + heal** — isolate the leader from the
//!    other two; the quorum on the far side elects a new leader,
//!    the stale leader steps down cleanly when the partition is
//!    healed, and the catalog on every member converges.
//! 3. **Full cluster process restart** — shut every member down,
//!    reopen the same redb-backed backends, and verify the catalog
//!    rehydrates into a working cluster.
//! 4. **Follower churn under load** — restart each follower in turn
//!    while the leader is applying commands; every member ends up
//!    with the same range count.
//! 5. **Many splits stress** — hundreds of catalog mutations,
//!    cluster-wide convergence asserted at the end.
//!
//! The `#[tokio::test]` bodies use small-ish election timeouts
//! (500-1000ms) because the in-process transport is so fast that
//! bigger timeouts just make the suite slow. They're still wide
//! enough to ride out GC pauses on CI.

use std::sync::Arc;
use std::time::Duration;

use aresadb_core::{MemoryBackend, StorageBackend};
use aresadb_engine_redb::RedbBackend;
use aresadb_pd::{
    MemberBackends, PdCluster, PdCommand, PdResponse, PdRouter, RangeDescriptor, ReplicaPlacement,
};
use tempfile::TempDir;

/// How long a test is willing to wait for a leader before the
/// harness considers something broken. The in-process transport
/// typically converges in <50ms, but we keep this generous to ride
/// out the occasional GC / CI noise.
const LEADER_TIMEOUT: Duration = Duration::from_secs(5);

/// Same idea for catalog replication — how long a follower has to
/// apply a command after the leader accepted it.
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(5);

fn voters(ids: &[u64]) -> Vec<ReplicaPlacement> {
    ids.iter().map(|n| ReplicaPlacement::voter(*n, 1)).collect()
}

fn genesis_range() -> RangeDescriptor {
    RangeDescriptor::new(1, Vec::<u8>::new(), Vec::<u8>::new(), voters(&[1, 2, 3]))
}

// ---------------------------------------------------------------
// 1) Leader failover
// ---------------------------------------------------------------

#[tokio::test]
async fn leader_failover_elects_new_leader_and_continues_applying() {
    let cluster = PdCluster::in_memory(3).await.unwrap();
    let leader = cluster.wait_for_leader(LEADER_TIMEOUT).await.unwrap();

    // Write something first so there's catalog state that survives
    // the failover.
    cluster
        .apply(PdCommand::CreateRange(genesis_range()))
        .await
        .unwrap();
    cluster
        .wait_for_replication(1, CONVERGENCE_TIMEOUT)
        .await
        .unwrap();

    // Kill the leader. We bypass `cluster.restart` because we want
    // the node gone for the whole test, not re-opened.
    let killed = cluster
        .member(leader)
        .expect("leader attached")
        .raft
        .clone();
    cluster.router.unregister(leader);
    killed.shutdown().await.unwrap();

    // The remaining two voters must elect one of themselves. We
    // can't use `cluster.leader()` here because the dead leader's
    // frozen metrics would keep naming itself as leader forever —
    // we want to poll only the live members.
    let live: Vec<u64> = cluster
        .ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    let deadline = std::time::Instant::now() + LEADER_TIMEOUT;
    let new_leader = loop {
        if std::time::Instant::now() >= deadline {
            panic!("no new leader after killing {leader}");
        }
        let mut found = None;
        for id in &live {
            let m = cluster.member(*id).unwrap();
            if m.raft.metrics().borrow().current_leader == Some(*id) {
                found = Some(*id);
                break;
            }
        }
        if let Some(id) = found {
            break id;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_ne!(new_leader, leader);

    // Drive a write through the new leader directly, since
    // `cluster.apply` would also be fooled by the dead leader's
    // stale metrics.
    let new_leader_raft = cluster.member(new_leader).unwrap().raft.clone();
    new_leader_raft
        .client_write(PdCommand::SplitRange {
            parent_range_id: 1,
            split_key: b"m".to_vec(),
        })
        .await
        .unwrap();

    // Both live members see the new range.
    for id in &live {
        let m = cluster.member(*id).unwrap();
        for _ in 0..200 {
            if m.state_machine.read(|c| c.range_count()) == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        m.state_machine.read(|c| {
            assert_eq!(c.range_count(), 2, "follower {id} stuck");
        });
    }

    // `cluster.shutdown` calls `raft.shutdown` on every member it
    // still tracks, including the already-dead one. openraft's
    // shutdown is idempotent, so this is safe.
    cluster.shutdown().await.unwrap();
}

// ---------------------------------------------------------------
// 2) Partition + heal
// ---------------------------------------------------------------

#[tokio::test]
async fn partition_isolates_leader_and_heals_cleanly() {
    let cluster = PdCluster::in_memory(3).await.unwrap();
    let leader = cluster.wait_for_leader(LEADER_TIMEOUT).await.unwrap();

    cluster
        .apply(PdCommand::CreateRange(genesis_range()))
        .await
        .unwrap();
    cluster
        .wait_for_replication(1, CONVERGENCE_TIMEOUT)
        .await
        .unwrap();

    // Isolate the leader from both followers (both directions).
    let followers: Vec<u64> = cluster
        .ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    for &f in &followers {
        cluster.partition(leader, f);
    }

    // The remaining two voters now form a quorum without the
    // original leader. They should elect a new one and keep going.
    let timeout = LEADER_TIMEOUT;
    let deadline = std::time::Instant::now() + timeout;
    let new_leader = loop {
        if std::time::Instant::now() >= deadline {
            panic!(
                "no new leader elected after partition (metrics: {:?})",
                cluster.catalog_snapshot()
            );
        }
        // A node can only "be leader" if it hears heartbeats from
        // quorum. Ask each follower directly.
        let mut found = None;
        for id in &followers {
            let m = cluster.member(*id).unwrap();
            if m.raft.metrics().borrow().current_leader == Some(*id) {
                found = Some(*id);
                break;
            }
        }
        if let Some(id) = found {
            break id;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_ne!(new_leader, leader);

    // The new leader should accept writes. `apply` routes through
    // `cluster.leader()` which reads metrics across all members —
    // the old leader still thinks it's leader because it doesn't
    // hear anybody saying otherwise, so we call `client_write`
    // directly on the new leader's handle.
    let new_leader_raft = cluster.member(new_leader).unwrap().raft.clone();
    new_leader_raft
        .client_write(PdCommand::SplitRange {
            parent_range_id: 1,
            split_key: b"m".to_vec(),
        })
        .await
        .unwrap();

    // Both live members see the split.
    let live = followers.clone();
    for id in &live {
        let m = cluster.member(*id).unwrap();
        for _ in 0..200 {
            if m.state_machine.read(|c| c.range_count()) == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        m.state_machine.read(|c| {
            assert_eq!(
                c.range_count(),
                2,
                "follower {id} did not see split during partition"
            );
        });
    }

    // Heal the partition. The old leader learns it's stale the
    // moment it gets a heartbeat from the new leader with a higher
    // term, and its catalog catches up via log replication.
    for &f in &followers {
        cluster.heal(leader, f);
    }

    cluster
        .wait_for_replication(2, CONVERGENCE_TIMEOUT)
        .await
        .unwrap();

    cluster.shutdown().await.unwrap();
}

// ---------------------------------------------------------------
// 3) Full cluster process restart
// ---------------------------------------------------------------

#[tokio::test]
async fn full_cluster_restart_rehydrates_catalog_from_redb() {
    // Use redb backends so the data actually lives on disk and we
    // can close/reopen across a simulated process restart.
    let tmp = TempDir::new().unwrap();
    let node_ids: [u64; 3] = [1, 2, 3];

    async fn open_backends(
        tmp: &TempDir,
        id: u64,
    ) -> (Arc<dyn StorageBackend>, Arc<dyn StorageBackend>) {
        let base = tmp.path().join(format!("n{id}"));
        std::fs::create_dir_all(&base).unwrap();
        let log: Arc<dyn StorageBackend> = RedbBackend::open(base.join("log.redb")).await.unwrap();
        let data: Arc<dyn StorageBackend> =
            RedbBackend::open(base.join("data.redb")).await.unwrap();
        (log, data)
    }

    // ---- first boot: fresh cluster, apply some commands ----
    let first_members: Vec<MemberBackends> = {
        let mut v = Vec::new();
        for id in node_ids {
            let (log, data) = open_backends(&tmp, id).await;
            v.push((id, log, data));
        }
        v
    };
    let router = PdRouter::new();
    let cluster =
        PdCluster::with_config(first_members, router.clone(), PdCluster::default_config())
            .await
            .unwrap();
    cluster.wait_for_leader(LEADER_TIMEOUT).await.unwrap();

    cluster
        .apply(PdCommand::CreateRange(genesis_range()))
        .await
        .unwrap();
    let mut parent = 1u64;
    for key in [b"e" as &[u8], b"j", b"o"] {
        let resp = cluster
            .apply(PdCommand::SplitRange {
                parent_range_id: parent,
                split_key: key.to_vec(),
            })
            .await
            .unwrap();
        let rhs = match resp {
            PdResponse::Range(r) => r,
            other => panic!("expected Range, got {other:?}"),
        };
        parent = rhs.range_id;
    }
    cluster
        .wait_for_replication(4, CONVERGENCE_TIMEOUT)
        .await
        .unwrap();

    cluster.shutdown().await.unwrap();
    // Drop the router — this simulates a full process exit (all
    // in-memory state is gone). The redb files on disk survive.
    drop(router);

    // ---- second boot: reopen the redb backends, skip initialize ----
    let second_members: Vec<MemberBackends> = {
        let mut v = Vec::new();
        for id in node_ids {
            let (log, data) = open_backends(&tmp, id).await;
            v.push((id, log, data));
        }
        v
    };
    let router2 = PdRouter::new();
    let cluster2 =
        PdCluster::open_existing(second_members, router2.clone(), PdCluster::default_config())
            .await
            .unwrap();
    cluster2.wait_for_leader(LEADER_TIMEOUT).await.unwrap();

    // Every member's rehydrated catalog should match what we applied
    // in the first life.
    cluster2
        .wait_for_replication(4, CONVERGENCE_TIMEOUT)
        .await
        .unwrap();
    for id in cluster2.ids() {
        let m = cluster2.member(id).unwrap();
        m.state_machine.read(|c| {
            assert_eq!(c.range_count(), 4);
            // The original genesis range.
            let r1 = c.get_range(1).unwrap();
            assert_eq!(r1.range_id, 1);
            // The last-split RHS.
            let last = c.get_range(parent).unwrap();
            assert_eq!(last.range_id, parent);
        });
    }

    // And new writes still work against the reconstituted cluster.
    cluster2
        .apply(PdCommand::SplitRange {
            parent_range_id: parent,
            split_key: b"t".to_vec(),
        })
        .await
        .unwrap();
    cluster2
        .wait_for_replication(5, CONVERGENCE_TIMEOUT)
        .await
        .unwrap();

    cluster2.shutdown().await.unwrap();
}

// ---------------------------------------------------------------
// 4) Follower churn under load
// ---------------------------------------------------------------

#[tokio::test]
async fn follower_churn_under_load_converges() {
    let mut cluster = PdCluster::in_memory(3).await.unwrap();
    cluster.wait_for_leader(LEADER_TIMEOUT).await.unwrap();

    cluster
        .apply(PdCommand::CreateRange(genesis_range()))
        .await
        .unwrap();

    // Walk-right split: parent is always the most recent RHS.
    let mut parent = 1u64;
    let keys: &[&[u8]] = &[b"c", b"f", b"i", b"l", b"o", b"r", b"u", b"x"];
    for (i, key) in keys.iter().enumerate() {
        // Bounce a follower roughly every third iteration to
        // exercise restart-while-applying.
        if i % 3 == 2 {
            let leader = cluster.leader().expect("leader stable enough to read");
            let follower = cluster.ids().into_iter().find(|id| *id != leader).unwrap();
            cluster.restart(follower).await.unwrap();
        }
        let resp = cluster
            .apply(PdCommand::SplitRange {
                parent_range_id: parent,
                split_key: key.to_vec(),
            })
            .await
            .unwrap();
        let rhs = match resp {
            PdResponse::Range(r) => r,
            other => panic!("expected Range, got {other:?}"),
        };
        parent = rhs.range_id;
    }

    // Final range count = 1 (genesis) + len(keys) splits = 9.
    let expected = 1 + keys.len();
    cluster
        .wait_for_replication(expected, CONVERGENCE_TIMEOUT)
        .await
        .unwrap();

    for id in cluster.ids() {
        let m = cluster.member(id).unwrap();
        m.state_machine.read(|c| {
            assert_eq!(c.range_count(), expected);
        });
    }

    cluster.shutdown().await.unwrap();
}

// ---------------------------------------------------------------
// 5) Many-splits stress
// ---------------------------------------------------------------

#[tokio::test]
async fn many_splits_converge_across_followers() {
    let cluster = PdCluster::in_memory(3).await.unwrap();
    cluster.wait_for_leader(LEADER_TIMEOUT).await.unwrap();

    cluster
        .apply(PdCommand::CreateRange(genesis_range()))
        .await
        .unwrap();

    // 50 splits. Use lexicographically-increasing keys so each
    // split is valid against the previous RHS.
    let mut parent = 1u64;
    let mut expected_count = 1usize;
    for i in 0..50u32 {
        // Keys: "a00", "a01", ..., "a49" — lex-sorted, all valid.
        let key = format!("a{:02}", i).into_bytes();
        let resp = cluster
            .apply(PdCommand::SplitRange {
                parent_range_id: parent,
                split_key: key,
            })
            .await
            .unwrap();
        let rhs = match resp {
            PdResponse::Range(r) => r,
            other => panic!("expected Range, got {other:?}"),
        };
        parent = rhs.range_id;
        expected_count += 1;
    }

    cluster
        .wait_for_replication(expected_count, CONVERGENCE_TIMEOUT)
        .await
        .unwrap();

    // Stronger assertion: every member sees the same range table,
    // range id for range id.
    let ids = cluster.ids();
    let reference = cluster
        .member(ids[0])
        .unwrap()
        .state_machine
        .read(|c| c.iter_ranges().map(|r| r.range_id).collect::<Vec<_>>());

    for id in &ids[1..] {
        let got = cluster
            .member(*id)
            .unwrap()
            .state_machine
            .read(|c| c.iter_ranges().map(|r| r.range_id).collect::<Vec<_>>());
        assert_eq!(got, reference, "follower {id} diverged");
    }

    cluster.shutdown().await.unwrap();
}

// ---------------------------------------------------------------
// 6) Plain memory-backend restart path (smoke test)
// ---------------------------------------------------------------

#[tokio::test]
async fn open_existing_rejects_fresh_backends() {
    // `open_existing` assumes the backends already contain a
    // membership entry from a prior `initialize`. Calling it on
    // fresh backends produces a cluster that never elects a leader
    // — `wait_for_leader` should time out rather than hang forever.
    let router = PdRouter::new();
    let mut members: Vec<MemberBackends> = Vec::new();
    for id in [1u64, 2, 3] {
        let log: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let data: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        members.push((id, log, data));
    }
    let cluster = PdCluster::open_existing(members, router, PdCluster::default_config())
        .await
        .unwrap();
    // No leader ever — the timeout must fire.
    let got = cluster.wait_for_leader(Duration::from_millis(500)).await;
    assert!(
        got.is_err(),
        "open_existing on fresh backends must not produce a leader (got {:?})",
        got
    );

    cluster.shutdown().await.unwrap();
}

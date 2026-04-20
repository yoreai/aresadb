//! End-to-end smoke test for the `aresadb-cluster pd …` subcommand.
//!
//! Brings up a one-voter PD Raft cluster in-process, attaches a real
//! tonic admin server on a localhost port, then shells out to the
//! compiled `aresadb-cluster` binary (via `CARGO_BIN_EXE_aresadb-cluster`)
//! and verifies the CLI round-trips common catalog flows end-to-end:
//!
//! 1. `pd status` returns valid JSON and reports this node as leader
//!    with an empty catalog.
//! 2. `pd create-range --range-id 1 --replicas 1:1` creates a genesis
//!    range. `pd list-ranges` dumps it back.
//! 3. `pd register` + `pd heartbeat` advance the catalog's node
//!    inventory.
//! 4. `pd split-range` splits the genesis range and `pd list-ranges`
//!    now reports two entries in keyspace order.
//!
//! The point of this test is **not** to re-validate the admin surface
//! — `aresadb-pd/tests/admin_integration.rs` already does that
//! directly against `PdAdminClient`. The point here is to catch CLI
//! regressions: a broken clap definition, a missing dispatch arm, a
//! type-conversion mismatch between the CLI and the typed client.

use std::net::SocketAddr;
use std::process::Output;
use std::time::Duration;

use aresadb_pd::{PdAdminService, PlacementDriverAdminServer, SinglePdNode};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Server;

/// Path to the compiled `aresadb-cluster` binary. Cargo injects this
/// env var for integration tests in the same crate as the bin target.
const CLI_BIN: &str = env!("CARGO_BIN_EXE_aresadb-cluster");

/// Admin server bound to a `SinglePdNode`. Owns the shutdown channel
/// so the test can tear it down cleanly on the way out.
struct AdminServer {
    endpoint: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl AdminServer {
    async fn spawn(node: &SinglePdNode) -> Self {
        // Pick a free localhost port by asking the kernel for one,
        // then releasing it so tonic can rebind. Matches the pattern
        // used across our other integration tests.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        drop(listener);

        let service = PdAdminService::new(
            node.raft.clone(),
            node.state_machine.clone(),
            node.data_backend.clone(),
        );

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            if let Err(e) = Server::builder()
                .add_service(PlacementDriverAdminServer::new(service))
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
            {
                eprintln!("pd cli smoke: admin server at {addr} exited: {e}");
            }
        });

        Self {
            endpoint: format!("http://{addr}"),
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
        }
    }

    async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

/// Run `aresadb-cluster pd <args>` and return its stdout + exit code.
/// Panics with stderr on a non-zero exit so the test output is
/// actually helpful when the CLI misbehaves.
fn run_cli(args: &[&str]) -> Output {
    let output = std::process::Command::new(CLI_BIN)
        .args(args)
        .output()
        .expect("spawn aresadb-cluster binary");
    if !output.status.success() {
        panic!(
            "aresadb-cluster {:?} exited {:?}\n--- stdout ---\n{}\n--- stderr ---\n{}",
            args,
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    output
}

/// Poll the admin surface until the leader has taken — otherwise
/// the first CLI call will race the election and surface as a
/// `NotLeader(None)` error.
async fn wait_for_leader(endpoint: &str) {
    // Shell out to `pd status` in a short loop. A settled leader
    // produces a non-null `current_leader` field.
    for _ in 0..50 {
        let output = std::process::Command::new(CLI_BIN)
            .args(["pd", "status", "--addr", endpoint])
            .output();
        if let Ok(output) = output {
            if output.status.success() {
                if let Ok(stdout) = std::str::from_utf8(&output.stdout) {
                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(stdout) {
                        if json
                            .get("current_leader")
                            .map(|v| !v.is_null())
                            .unwrap_or(false)
                        {
                            return;
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("pd cli smoke: no leader after 5 seconds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pd_cli_round_trips_catalog_flows_end_to_end() {
    let node = SinglePdNode::in_memory().await.expect("start pd node");
    let server = AdminServer::spawn(&node).await;
    wait_for_leader(&server.endpoint).await;

    // ------- status -------
    let out = run_cli(&["pd", "status", "--addr", &server.endpoint]);
    let json: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("pd status emits JSON");
    assert_eq!(json["current_leader"], serde_json::json!(1));
    assert_eq!(json["catalog"]["range_count"], serde_json::json!(0));

    // ------- create-range -------
    let out = run_cli(&[
        "pd",
        "create-range",
        "--addr",
        &server.endpoint,
        "--range-id",
        "1",
        "--replicas",
        "1:1",
    ]);
    let range: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("create-range emits JSON");
    assert_eq!(range["range_id"], serde_json::json!(1));
    assert_eq!(range["raft_group_id"], serde_json::json!(1));

    // ------- list-ranges -------
    let out = run_cli(&["pd", "list-ranges", "--addr", &server.endpoint]);
    let ranges: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("list-ranges emits JSON");
    assert!(ranges.is_array());
    assert_eq!(ranges.as_array().unwrap().len(), 1);
    assert_eq!(ranges[0]["range_id"], serde_json::json!(1));

    // ------- register + heartbeat -------
    run_cli(&[
        "pd",
        "register",
        "--addr",
        &server.endpoint,
        "--node-id",
        "1",
        "--address",
        "127.0.0.1:9000",
        "--stores",
        "1,2",
    ]);
    run_cli(&[
        "pd",
        "heartbeat",
        "--addr",
        &server.endpoint,
        "--node-id",
        "1",
    ]);

    let out = run_cli(&[
        "pd",
        "get-node",
        "--addr",
        &server.endpoint,
        "--node-id",
        "1",
    ]);
    let node_json: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("get-node emits JSON");
    assert_eq!(node_json["node_id"], serde_json::json!(1));
    assert_eq!(node_json["address"], serde_json::json!("127.0.0.1:9000"));
    assert_eq!(node_json["stores"], serde_json::json!([1, 2]));
    assert!(node_json["last_heartbeat_millis"].as_u64().unwrap() > 0);

    // ------- split-range -------
    run_cli(&[
        "pd",
        "split-range",
        "--addr",
        &server.endpoint,
        "--parent-id",
        "1",
        "--split-key",
        "m",
    ]);

    let out = run_cli(&["pd", "list-ranges", "--addr", &server.endpoint]);
    let ranges: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("list-ranges emits JSON");
    let arr = ranges.as_array().unwrap();
    assert_eq!(arr.len(), 2, "expected 2 ranges after split, got {arr:?}");
    // First range now ends at "m".
    assert_eq!(arr[0]["range_id"], serde_json::json!(1));
    assert_eq!(arr[0]["end_key"], serde_json::json!([b'm']));
    // Second range covers [m, +∞).
    assert_eq!(arr[1]["start_key"], serde_json::json!([b'm']));

    // ------- get-range-by-key confirms the new split -------
    let out = run_cli(&[
        "pd",
        "get-range-by-key",
        "--addr",
        &server.endpoint,
        "--key",
        "z",
    ]);
    let hit: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("get-range-by-key emits JSON");
    // The right-hand-side range owns "z".
    assert_ne!(hit["range_id"], serde_json::json!(1));

    server.shutdown().await;
    node.shutdown().await.unwrap();
}

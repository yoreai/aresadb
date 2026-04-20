//! `aresadb-cluster` — operator CLI for AresaDB v2 clusters.
//!
//! Three categories of subcommand:
//!
//!   * **Lifecycle** — `bootstrap` and `join` actually start a node
//!     in the foreground. These stay running until SIGINT / Ctrl-C.
//!   * **Admin** — `add-voter`, `remove-voter`, `status`, `write`,
//!     `read`. These are one-shot RPC clients that talk to a
//!     *running* node and exit.
//!   * **Placement Driver (`pd`)** — a nested subcommand group that
//!     talks to a PD admin server (one of the three members of the
//!     PD Raft group). Covers the full catalog lifecycle: register /
//!     heartbeat nodes, create / split / merge ranges, install or
//!     clear leases, update membership, and a long-running
//!     `heartbeat-loop` that follows leader hints. All one-shot
//!     commands exit after a single RPC; `heartbeat-loop` stays up
//!     until SIGINT.
//!
//! The CLI is deliberately thin: every command maps to one (or two)
//! method calls on `ClusterNode`, `ClusterAdminClient`, or
//! [`PdAdminClient`]. Anything more elaborate belongs in a library
//! helper.

use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use aresadb_cluster::admin::pb;
use aresadb_cluster::{ClusterAdminClient, ClusterNode, NodeConfig};
use aresadb_core::WriteBatch;
use aresadb_pd::admin::HeartbeatConfig;
use aresadb_pd::{
    HeartbeatLoop, LeaseInfo, NodeInfo as PdNodeInfo, PdAdminClient, PdAdminClientError,
    RangeDescriptor, ReplicaPlacement, ReplicaRole,
};
use aresadb_raft::{NodeId, SerializableWriteBatch};
use clap::{Parser, Subcommand, ValueEnum};
use tonic::transport::Endpoint;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Parser)]
#[command(
    name = "aresadb-cluster",
    version,
    about = "AresaDB v2 cluster operator CLI"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Consistency level flag for the `read` subcommand.
///
/// Mirrors `pb::ReadConsistency` but lives here so the CLI can
/// derive a clean `clap::ValueEnum` (`--consistency linearizable`)
/// without having to implement `FromStr` on the generated protobuf
/// enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ReadConsistencyArg {
    /// Phase 1c back-compat — raw state-machine lookup on the
    /// default range, no leadership guard. Ignored when
    /// `--range-id` targets a non-default range, where it becomes
    /// a stale read.
    Unspecified,
    /// Linearizable (leader-lease) read. Hits
    /// `RangeRuntime::linearizable_get`, which runs openraft's
    /// ReadIndex and then reads the state machine. Requires the
    /// CLI to be pointed at the range's Raft leader.
    Linearizable,
    /// Bounded-staleness read. Hits
    /// `RangeRuntime::stale_get`; skips the leadership guard and
    /// reads directly from the local state machine. Safe on any
    /// member.
    Stale,
}

impl std::fmt::Display for ReadConsistencyArg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ReadConsistencyArg::Unspecified => "unspecified",
            ReadConsistencyArg::Linearizable => "linearizable",
            ReadConsistencyArg::Stale => "stale",
        })
    }
}

impl From<ReadConsistencyArg> for pb::ReadConsistency {
    fn from(arg: ReadConsistencyArg) -> Self {
        match arg {
            ReadConsistencyArg::Unspecified => pb::ReadConsistency::Unspecified,
            ReadConsistencyArg::Linearizable => pb::ReadConsistency::Linearizable,
            ReadConsistencyArg::Stale => pb::ReadConsistency::Stale,
        }
    }
}

#[derive(Subcommand)]
enum Command {
    /// Start a single-voter cluster containing only this node.
    ///
    /// Use this for the very first node of a new cluster, or for
    /// single-node deployments. Subsequent nodes use `join`.
    Bootstrap {
        /// Stable, cluster-unique id for this node.
        #[arg(long, env = "ARESADB_NODE_ID")]
        node_id: NodeId,

        /// Local socket to bind the gRPC server on.
        #[arg(long, env = "ARESADB_LISTEN")]
        listen: SocketAddr,

        /// Address other nodes should use to reach this one.
        /// Defaults to `http://<listen>`.
        #[arg(long, env = "ARESADB_ADVERTISE")]
        advertise: Option<String>,

        /// Data directory (contains raft-log/ and state-machine/).
        #[arg(long, env = "ARESADB_DATA_DIR")]
        data_dir: PathBuf,
    },

    /// Start a fresh node and wait for an existing cluster to add it
    /// as a learner. The operator must subsequently issue
    /// `add-voter --addr <leader>` from another terminal.
    Join {
        #[arg(long, env = "ARESADB_NODE_ID")]
        node_id: NodeId,

        #[arg(long, env = "ARESADB_LISTEN")]
        listen: SocketAddr,

        #[arg(long, env = "ARESADB_ADVERTISE")]
        advertise: Option<String>,

        #[arg(long, env = "ARESADB_DATA_DIR")]
        data_dir: PathBuf,
    },

    /// Ask a running leader to add a new node as a voting member.
    ///
    /// Runs an `AddLearner` then `ChangeMembership` — i.e. the
    /// learner is promoted to voter in one shot. If the target node
    /// is not a learner yet, the operator should start it with
    /// `join` first.
    AddVoter {
        /// gRPC endpoint of the current leader, e.g. `http://127.0.0.1:7001`.
        #[arg(long)]
        leader: String,

        /// Id of the node being added.
        #[arg(long)]
        node_id: NodeId,

        /// Advertise address of the node being added.
        #[arg(long)]
        addr: String,
    },

    /// Replace the voter set. `voters` must contain every node id
    /// that should stay a voter after the change.
    ChangeMembership {
        #[arg(long)]
        leader: String,

        /// Comma-separated list of node ids.
        #[arg(long, value_delimiter = ',')]
        voters: Vec<NodeId>,

        /// Keep existing learners after the change.
        #[arg(long)]
        retain_learners: bool,
    },

    /// Apply a single key-value write via Raft on the leader.
    /// Useful for smoke-testing a freshly bootstrapped cluster.
    Write {
        #[arg(long)]
        leader: String,

        #[arg(long)]
        key: String,

        #[arg(long)]
        value: String,

        /// Target range. `0` (the default) resolves to the node's
        /// default range (`DEFAULT_RANGE_ID = 1`) — the back-compat
        /// Phase 1c path. Non-zero values route the batch through
        /// the range's own Raft group; the call must land on that
        /// range's leader (`FAILED_PRECONDITION` otherwise, with
        /// `x-aresa-leader-id` metadata for re-routing).
        #[arg(long, default_value_t = 0)]
        range_id: u64,
    },

    /// Read a single key from the node this CLI connects to.
    Read {
        #[arg(long)]
        addr: String,

        #[arg(long)]
        key: String,

        /// Range to read from. `0` (the default) resolves to the
        /// node's default range (`DEFAULT_RANGE_ID = 1`).
        #[arg(long, default_value_t = 0)]
        range_id: u64,

        /// Consistency level: `linearizable` for leader-lease
        /// reads (uses openraft ReadIndex under the hood),
        /// `stale` for bounded-staleness follower reads, or
        /// `unspecified` to preserve the Phase 1c raw backend
        /// read (back-compat default).
        #[arg(long, default_value_t = ReadConsistencyArg::Unspecified, value_enum)]
        consistency: ReadConsistencyArg,
    },

    /// Dump cluster metrics in JSON format.
    Status {
        #[arg(long)]
        addr: String,
    },

    /// Open a new range on the node serving the admin RPC, creating
    /// its per-range backends at `<data-dir>/ranges/<range_id>/{log,data}`.
    ///
    /// Used by the multi-range Docker smoke test to spin up ranges
    /// outside of the Phase 2c-4 PD supervisor (which is stubbed in
    /// the single-node-PD Docker compose). In production the PD
    /// converges ranges automatically via `pd_supervisor`; this CLI
    /// command is for manual wiring and tests.
    AddRange {
        /// Admin endpoint to hit (one of the cluster nodes). Does
        /// not need to be a leader — each node owns its own range
        /// directory.
        #[arg(long)]
        leader: String,

        /// Stable range id. Must be non-zero. `DEFAULT_RANGE_ID = 1`
        /// is reserved for the Phase 1c back-compat range.
        #[arg(long)]
        range_id: u64,

        /// UTF-8 start key. Empty means start of keyspace.
        #[arg(long, default_value = "")]
        start_key: String,

        /// UTF-8 end key. Empty means +∞ (top of keyspace).
        #[arg(long, default_value = "")]
        end_key: String,

        /// Same format as `pd create-range --replicas`:
        /// `node_id:store_id[:role]`, comma-separated.
        #[arg(long)]
        replicas: String,

        /// Raft group id. Defaults to `range_id`.
        #[arg(long)]
        raft_group_id: Option<u64>,

        #[arg(long, default_value_t = 0)]
        epoch: u64,

        #[arg(long, default_value_t = 0)]
        generation: u64,

        /// Bootstrap this node as the single voter of the new
        /// range. Required on the first node of a brand-new range;
        /// all other members should be added as learners via
        /// `add-voter` / `change-membership`.
        #[arg(long)]
        bootstrap_as_voter: bool,
    },

    /// Close a range on the node serving the admin RPC. On-disk
    /// state is retained so the range can be re-opened later.
    RemoveRange {
        #[arg(long)]
        addr: String,

        #[arg(long)]
        range_id: u64,

        /// Tear the runtime down even if outstanding references
        /// are held. Default (false) fails with
        /// `FAILED_PRECONDITION` when the runtime can't be consumed
        /// cleanly.
        #[arg(long)]
        force: bool,
    },

    /// Dump every range registered on this node as JSON. Combine
    /// with `pd list-ranges` to diff catalog-vs-data-plane truth.
    ListRanges {
        #[arg(long)]
        addr: String,
    },

    /// Placement-driver admin subcommands.
    ///
    /// Every subcommand takes `--addr <PD endpoint>` and talks to the
    /// [`PdAdminClient`]. Mutations go through the PD Raft group;
    /// reads are served from whichever member you hit.
    Pd {
        #[command(subcommand)]
        command: PdCommand,
    },
}

/// Placement-driver admin subcommands.
///
/// The command tree is flat because every action maps to one RPC.
/// All mutation commands accept `--addr <PD endpoint>` — typically
/// one of the three PD gRPC addresses. Point it at any member; if
/// the member isn't the leader, the command fails with a clear
/// `not leader` error that includes the suggested leader id so the
/// operator can retry against the right endpoint in one hop.
///
/// Keys are accepted as UTF-8 strings. An empty `--end-key` on
/// `create-range` denotes +∞ (top of keyspace) — matching the
/// catalog's convention.
#[derive(Subcommand)]
enum PdCommand {
    /// Dump the PD status JSON (leader, term, range/node count,
    /// voter / learner sets).
    Status {
        #[arg(long)]
        addr: String,
    },

    /// List every range in keyspace order.
    ListRanges {
        #[arg(long)]
        addr: String,
    },

    /// List every registered node, ordered by node id.
    ListNodes {
        #[arg(long)]
        addr: String,
    },

    /// Look up a single range by id.
    GetRange {
        #[arg(long)]
        addr: String,

        #[arg(long)]
        range_id: u64,
    },

    /// Look up the range that currently owns `key`.
    GetRangeByKey {
        #[arg(long)]
        addr: String,

        /// UTF-8 key. Use "" to query the very start of the keyspace.
        #[arg(long)]
        key: String,
    },

    /// Look up a single node by id.
    GetNode {
        #[arg(long)]
        addr: String,

        #[arg(long)]
        node_id: NodeId,
    },

    /// Register (or refresh) a node in the cluster inventory.
    /// Must be called before any [`PdCommand::Heartbeat`] for that
    /// node id.
    Register {
        #[arg(long)]
        addr: String,

        #[arg(long)]
        node_id: NodeId,

        /// Advertise address this node will be reachable at.
        #[arg(long)]
        address: String,

        /// Comma-separated list of store ids this node hosts.
        #[arg(long, value_delimiter = ',', default_value = "1")]
        stores: Vec<u64>,
    },

    /// Send a single heartbeat RPC for `node_id`. Uses the current
    /// wall-clock as the `last_seen_millis` timestamp.
    Heartbeat {
        #[arg(long)]
        addr: String,

        #[arg(long)]
        node_id: NodeId,
    },

    /// Spawn a long-running heartbeat loop for `node_id` that stays
    /// up until Ctrl-C. Follows leader hints when `--peer` entries
    /// are supplied.
    HeartbeatLoop {
        /// Initial endpoint to dial. The loop will rotate if it
        /// receives a leader hint and `--peer` covers that id.
        #[arg(long)]
        addr: String,

        #[arg(long)]
        node_id: NodeId,

        /// Heartbeat cadence in milliseconds.
        #[arg(long, default_value_t = 2_000)]
        interval_ms: u64,

        /// Additional `ID=URL` pairs letting the loop follow leader
        /// hints. The initial `--addr` is always included. Repeat the
        /// flag for each member.
        #[arg(long = "peer")]
        peers: Vec<String>,
    },

    /// Insert a brand-new range descriptor. Used for genesis ranges
    /// and for operator-driven repair.
    CreateRange {
        #[arg(long)]
        addr: String,

        #[arg(long)]
        range_id: u64,

        /// UTF-8 start key. Empty means start of keyspace.
        #[arg(long, default_value = "")]
        start_key: String,

        /// UTF-8 end key. Empty means +∞ (top of keyspace).
        #[arg(long, default_value = "")]
        end_key: String,

        /// Comma-separated replica specs: `node_id:store_id[:role]`.
        /// Role defaults to `voter`; use `learner` (or `l`) to flag
        /// one explicitly. e.g. `1:1,2:1,3:1:learner`.
        #[arg(long)]
        replicas: String,

        /// Raft group id. Defaults to `range_id` which is the
        /// convention used throughout the catalog tests.
        #[arg(long)]
        raft_group_id: Option<u64>,

        #[arg(long, default_value_t = 0)]
        epoch: u64,

        #[arg(long, default_value_t = 0)]
        generation: u64,
    },

    /// Split a range at `split_key`. Parent shrinks to
    /// `[start, split_key)`; a new range covering `[split_key, end)`
    /// is created.
    SplitRange {
        #[arg(long)]
        addr: String,

        #[arg(long)]
        parent_id: u64,

        #[arg(long)]
        split_key: String,
    },

    /// Merge two adjacent ranges. `left.end_key` must equal
    /// `right.start_key` and both must share the same replica set.
    MergeRanges {
        #[arg(long)]
        addr: String,

        #[arg(long)]
        left_id: u64,

        #[arg(long)]
        right_id: u64,
    },

    /// Install (or renew) the leader lease on a range.
    InstallLease {
        #[arg(long)]
        addr: String,

        #[arg(long)]
        range_id: u64,

        #[arg(long)]
        holder: NodeId,

        #[arg(long)]
        expires_at_millis: u64,
    },

    /// Clear the leader lease on a range.
    ClearLease {
        #[arg(long)]
        addr: String,

        #[arg(long)]
        range_id: u64,
    },

    /// Replace the replica set of a range atomically. `epoch` must be
    /// strictly greater than the current epoch.
    UpdateMembership {
        #[arg(long)]
        addr: String,

        #[arg(long)]
        range_id: u64,

        /// Same format as [`PdCommand::CreateRange::replicas`].
        #[arg(long)]
        replicas: String,

        #[arg(long)]
        epoch: u64,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .try_init();

    let cli = Cli::parse();
    match cli.command {
        Command::Bootstrap {
            node_id,
            listen,
            advertise,
            data_dir,
        } => run_bootstrap(node_id, listen, advertise, data_dir).await,
        Command::Join {
            node_id,
            listen,
            advertise,
            data_dir,
        } => run_join(node_id, listen, advertise, data_dir).await,
        Command::AddVoter {
            leader,
            node_id,
            addr,
        } => run_add_voter(leader, node_id, addr).await,
        Command::ChangeMembership {
            leader,
            voters,
            retain_learners,
        } => run_change_membership(leader, voters, retain_learners).await,
        Command::Write {
            leader,
            key,
            value,
            range_id,
        } => run_write(leader, key, value, range_id).await,
        Command::AddRange {
            leader,
            range_id,
            start_key,
            end_key,
            replicas,
            raft_group_id,
            epoch,
            generation,
            bootstrap_as_voter,
        } => {
            run_add_range(
                leader,
                range_id,
                start_key,
                end_key,
                replicas,
                raft_group_id,
                epoch,
                generation,
                bootstrap_as_voter,
            )
            .await
        }
        Command::RemoveRange {
            addr,
            range_id,
            force,
        } => run_remove_range(addr, range_id, force).await,
        Command::ListRanges { addr } => run_list_ranges(addr).await,
        Command::Read {
            addr,
            key,
            range_id,
            consistency,
        } => run_read(addr, key, range_id, consistency).await,
        Command::Status { addr } => run_status(addr).await,
        Command::Pd { command } => run_pd(command).await,
    }
}

fn build_config(
    node_id: NodeId,
    listen: SocketAddr,
    advertise: Option<String>,
    data_dir: PathBuf,
) -> NodeConfig {
    let mut cfg = NodeConfig::new(node_id, listen, data_dir);
    if let Some(a) = advertise {
        cfg = cfg.with_advertise_addr(a);
    }
    cfg
}

async fn run_bootstrap(
    node_id: NodeId,
    listen: SocketAddr,
    advertise: Option<String>,
    data_dir: PathBuf,
) -> anyhow::Result<()> {
    let cfg = build_config(node_id, listen, advertise, data_dir);
    let node = ClusterNode::bootstrap_single(cfg).await?;
    println!(
        "aresadb-cluster: node {} listening on {} — single-voter cluster initialised",
        node.node_id(),
        node.raft().metrics().borrow().id
    );
    wait_for_ctrl_c_then_shutdown(node).await
}

async fn run_join(
    node_id: NodeId,
    listen: SocketAddr,
    advertise: Option<String>,
    data_dir: PathBuf,
) -> anyhow::Result<()> {
    let cfg = build_config(node_id, listen, advertise, data_dir);
    let node = ClusterNode::start(cfg).await?;
    println!(
        "aresadb-cluster: node {} listening on {} — waiting to be added to a cluster",
        node.node_id(),
        node.raft().metrics().borrow().id
    );
    wait_for_ctrl_c_then_shutdown(node).await
}

async fn wait_for_ctrl_c_then_shutdown(node: ClusterNode) -> anyhow::Result<()> {
    tokio::signal::ctrl_c().await?;
    println!("aresadb-cluster: ctrl-c received, shutting down");
    node.shutdown().await?;
    Ok(())
}

async fn admin_client(addr: &str) -> anyhow::Result<ClusterAdminClient<tonic::transport::Channel>> {
    let endpoint = Endpoint::from_shared(addr.to_string())?.connect_timeout(Duration::from_secs(5));
    let channel = endpoint.connect().await?;
    Ok(ClusterAdminClient::new(channel))
}

async fn run_add_voter(leader: String, node_id: NodeId, addr: String) -> anyhow::Result<()> {
    let mut client = admin_client(&leader).await?;

    client
        .add_learner(pb::AddLearnerRequest {
            node: Some(pb::NodeDescriptor {
                node_id,
                rpc_addr: addr.clone(),
            }),
            blocking: true,
        })
        .await?;

    // Promote by issuing a membership change that includes the new
    // id plus whatever the current voters are. We derive the existing
    // voter set from the leader's status payload.
    let status: serde_json::Value = fetch_status(&mut client).await?;
    let mut voters: BTreeSet<NodeId> = status["membership"]["voters"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_u64()).collect())
        .unwrap_or_default();
    voters.insert(node_id);

    client
        .change_membership(pb::ChangeMembershipRequest {
            voter_ids: voters.into_iter().collect(),
            retain_learners: false,
        })
        .await?;

    println!(
        "aresadb-cluster: node {} promoted to voter in cluster at {}",
        node_id, leader
    );
    Ok(())
}

async fn run_change_membership(
    leader: String,
    voters: Vec<NodeId>,
    retain_learners: bool,
) -> anyhow::Result<()> {
    let mut client = admin_client(&leader).await?;
    client
        .change_membership(pb::ChangeMembershipRequest {
            voter_ids: voters.clone(),
            retain_learners,
        })
        .await?;
    println!("aresadb-cluster: membership changed to voters {:?}", voters);
    Ok(())
}

async fn run_write(
    leader: String,
    key: String,
    value: String,
    range_id: u64,
) -> anyhow::Result<()> {
    let mut batch = WriteBatch::new();
    batch.put(key.clone(), value);
    let serialisable: SerializableWriteBatch = batch.into();
    let encoded = bincode::serialize(&serialisable)?;

    let mut client = admin_client(&leader).await?;
    let resp = client
        .write(pb::WriteRequest {
            batch: encoded,
            range_id,
        })
        .await?
        .into_inner();
    println!(
        "aresadb-cluster: write committed on range {} at index {} (ops_applied={})",
        resp.range_id, resp.log_index, resp.ops_applied
    );
    Ok(())
}

// Every parameter corresponds 1:1 to a flag on `add-range`, so
// matching the CLI layout with a targeted allow keeps the signature
// honest rather than inventing a single-use wrapper struct.
#[allow(clippy::too_many_arguments)]
async fn run_add_range(
    leader: String,
    range_id: u64,
    start_key: String,
    end_key: String,
    replicas: String,
    raft_group_id: Option<u64>,
    epoch: u64,
    generation: u64,
    bootstrap_as_voter: bool,
) -> anyhow::Result<()> {
    if range_id == 0 {
        bail!("--range-id must be non-zero");
    }
    let placements = parse_replica_specs(&replicas)?;
    let pb_descriptor = pb::RangeDescriptor {
        range_id,
        start_key: start_key.into_bytes(),
        end_key: end_key.into_bytes(),
        replicas: placements
            .iter()
            .map(|r| pb::ReplicaPlacement {
                node_id: r.node_id,
                store_id: r.store_id,
                role: match r.role {
                    ReplicaRole::Voter => pb::ReplicaRole::Voter as i32,
                    ReplicaRole::Learner => pb::ReplicaRole::Learner as i32,
                },
            })
            .collect(),
        raft_group_id: raft_group_id.unwrap_or(range_id),
        epoch,
        generation,
        lease: None,
    };

    let mut client = admin_client(&leader).await?;
    let resp = client
        .add_range(pb::AddRangeRequest {
            descriptor: Some(pb_descriptor),
            bootstrap_as_voter,
        })
        .await?
        .into_inner();

    let d = resp
        .descriptor
        .ok_or_else(|| anyhow!("add-range: server returned no descriptor"))?;
    println!(
        "aresadb-cluster: range {} opened (raft_group_id={}, replicas={}, bootstrap_as_voter={})",
        d.range_id,
        d.raft_group_id,
        d.replicas.len(),
        bootstrap_as_voter
    );
    Ok(())
}

async fn run_remove_range(addr: String, range_id: u64, force: bool) -> anyhow::Result<()> {
    if range_id == 0 {
        bail!("--range-id must be non-zero");
    }
    let mut client = admin_client(&addr).await?;
    client
        .remove_range(pb::RemoveRangeRequest { range_id, force })
        .await?;
    println!("aresadb-cluster: range {} removed", range_id);
    Ok(())
}

async fn run_list_ranges(addr: String) -> anyhow::Result<()> {
    let mut client = admin_client(&addr).await?;
    let resp = client
        .list_ranges(pb::ListRangesRequest {})
        .await?
        .into_inner();
    let view: Vec<serde_json::Value> = resp
        .ranges
        .iter()
        .map(|d| {
            serde_json::json!({
                "range_id": d.range_id,
                "raft_group_id": d.raft_group_id,
                "start_key": String::from_utf8_lossy(&d.start_key),
                "end_key": String::from_utf8_lossy(&d.end_key),
                "epoch": d.epoch,
                "generation": d.generation,
                "replicas": d
                    .replicas
                    .iter()
                    .map(|r| serde_json::json!({
                        "node_id": r.node_id,
                        "store_id": r.store_id,
                        "role": match pb::ReplicaRole::try_from(r.role).unwrap_or(pb::ReplicaRole::Unspecified) {
                            pb::ReplicaRole::Unspecified => "unspecified",
                            pb::ReplicaRole::Voter => "voter",
                            pb::ReplicaRole::Learner => "learner",
                        },
                    }))
                    .collect::<Vec<_>>(),
                "lease": d.lease.as_ref().map(|l| serde_json::json!({
                    "holder": l.holder,
                    "expires_at_millis": l.expires_at_millis,
                })),
            })
        })
        .collect();
    println!("{}", serde_json::to_string_pretty(&view)?);
    Ok(())
}

async fn run_read(
    addr: String,
    key: String,
    range_id: u64,
    consistency: ReadConsistencyArg,
) -> anyhow::Result<()> {
    let mut client = admin_client(&addr).await?;
    let resp = client
        .read(pb::ReadRequest {
            key: key.into_bytes(),
            range_id,
            consistency: pb::ReadConsistency::from(consistency) as i32,
        })
        .await?
        .into_inner();
    if !resp.found {
        println!("<not found>");
    } else {
        match std::str::from_utf8(&resp.value) {
            Ok(s) => println!("{}", s),
            Err(_) => println!("{:?}", resp.value),
        }
    }
    if resp.read_log_index > 0 {
        eprintln!(
            "aresadb-cluster: served linearizable read from range {} @ applied index {}",
            resp.range_id, resp.read_log_index
        );
    }
    Ok(())
}

async fn run_status(addr: String) -> anyhow::Result<()> {
    let mut client = admin_client(&addr).await?;
    let value = fetch_status(&mut client).await?;
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

async fn fetch_status(
    client: &mut ClusterAdminClient<tonic::transport::Channel>,
) -> anyhow::Result<serde_json::Value> {
    let resp = client.status(pb::StatusRequest {}).await?.into_inner();
    Ok(serde_json::from_slice(&resp.json)?)
}

// ---------------------------------------------------------------------
// Placement-driver subcommand plumbing
// ---------------------------------------------------------------------

/// Connect an admin client to a PD endpoint with a short connect
/// timeout. Kept private so callers always pay the same timeout.
async fn pd_client(addr: &str) -> anyhow::Result<PdAdminClient> {
    PdAdminClient::connect(addr.to_string())
        .await
        .with_context(|| format!("connect to PD admin at {addr}"))
}

/// Parse a `--replicas "1:1,2:1:learner,3:1"` string into a list of
/// [`ReplicaPlacement`]. Role defaults to `voter` when omitted.
/// Empty / all-whitespace specs surface a clear error rather than
/// silently producing an empty replica set.
fn parse_replica_specs(s: &str) -> anyhow::Result<Vec<ReplicaPlacement>> {
    let replicas: Vec<ReplicaPlacement> = s
        .split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(parse_one_replica_spec)
        .collect::<anyhow::Result<_>>()?;
    if replicas.is_empty() {
        bail!("--replicas must contain at least one spec of the form node_id:store_id[:role]");
    }
    Ok(replicas)
}

fn parse_one_replica_spec(spec: &str) -> anyhow::Result<ReplicaPlacement> {
    let parts: Vec<&str> = spec.split(':').collect();
    match parts.as_slice() {
        [node, store] => Ok(ReplicaPlacement {
            node_id: parse_u64(node, "replica node_id")?,
            store_id: parse_u64(store, "replica store_id")?,
            role: ReplicaRole::Voter,
        }),
        [node, store, role] => Ok(ReplicaPlacement {
            node_id: parse_u64(node, "replica node_id")?,
            store_id: parse_u64(store, "replica store_id")?,
            role: parse_replica_role(role)?,
        }),
        _ => bail!(
            "bad replica spec {spec:?}; expected node_id:store_id[:role], e.g. `1:1` or `2:1:learner`"
        ),
    }
}

fn parse_u64(s: &str, field: &str) -> anyhow::Result<u64> {
    s.parse::<u64>()
        .map_err(|e| anyhow!("{field}: failed to parse {s:?} as u64: {e}"))
}

fn parse_replica_role(s: &str) -> anyhow::Result<ReplicaRole> {
    match s.to_ascii_lowercase().as_str() {
        "voter" | "v" => Ok(ReplicaRole::Voter),
        "learner" | "l" => Ok(ReplicaRole::Learner),
        other => bail!("unknown replica role {other:?}; expected `voter` or `learner`"),
    }
}

/// Parse `--peer` entries like `2=http://127.0.0.1:7002` into a
/// `{id -> endpoint}` map. The initial `--addr` is injected by the
/// caller so the map is never empty.
fn parse_peer_map(
    peers: &[String],
    initial: (NodeId, String),
) -> anyhow::Result<HashMap<NodeId, String>> {
    let mut map = HashMap::new();
    map.insert(initial.0, initial.1);
    for raw in peers {
        let (id, endpoint) = raw
            .split_once('=')
            .ok_or_else(|| anyhow!("bad --peer {raw:?}; expected ID=URL"))?;
        let id: NodeId = id
            .trim()
            .parse()
            .map_err(|e| anyhow!("--peer {raw:?}: node id: {e}"))?;
        map.insert(id, endpoint.trim().to_string());
    }
    Ok(map)
}

/// Current wall-clock in Unix millis. Saturates at 0 if the system
/// clock is somehow before the epoch — matches the convention in
/// [`aresadb_pd::admin::heartbeat::wall_clock`].
fn now_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Pretty-print any serde-serialisable value as JSON to stdout.
fn print_json<T: serde::Serialize>(value: &T) -> anyhow::Result<()> {
    let out = serde_json::to_string_pretty(value)?;
    println!("{out}");
    Ok(())
}

/// Render `err` so the operator can see leader hints up-front
/// without having to grep the stderr logs. Other error classes
/// render with their `Display` impl.
fn report_pd_error(context: &str, err: PdAdminClientError) -> anyhow::Error {
    match &err {
        PdAdminClientError::NotLeader(Some(id)) => {
            anyhow!("{context}: not leader; retry against node {id} (supply its --addr)")
        }
        PdAdminClientError::NotLeader(None) => {
            anyhow!("{context}: no leader elected yet; retry in a moment")
        }
        _ => anyhow!("{context}: {err}"),
    }
}

async fn run_pd(command: PdCommand) -> anyhow::Result<()> {
    match command {
        PdCommand::Status { addr } => run_pd_status(addr).await,
        PdCommand::ListRanges { addr } => run_pd_list_ranges(addr).await,
        PdCommand::ListNodes { addr } => run_pd_list_nodes(addr).await,
        PdCommand::GetRange { addr, range_id } => run_pd_get_range(addr, range_id).await,
        PdCommand::GetRangeByKey { addr, key } => run_pd_get_range_by_key(addr, key).await,
        PdCommand::GetNode { addr, node_id } => run_pd_get_node(addr, node_id).await,
        PdCommand::Register {
            addr,
            node_id,
            address,
            stores,
        } => run_pd_register(addr, node_id, address, stores).await,
        PdCommand::Heartbeat { addr, node_id } => run_pd_heartbeat(addr, node_id).await,
        PdCommand::HeartbeatLoop {
            addr,
            node_id,
            interval_ms,
            peers,
        } => run_pd_heartbeat_loop(addr, node_id, interval_ms, peers).await,
        PdCommand::CreateRange {
            addr,
            range_id,
            start_key,
            end_key,
            replicas,
            raft_group_id,
            epoch,
            generation,
        } => {
            run_pd_create_range(
                addr,
                range_id,
                start_key,
                end_key,
                replicas,
                raft_group_id,
                epoch,
                generation,
            )
            .await
        }
        PdCommand::SplitRange {
            addr,
            parent_id,
            split_key,
        } => run_pd_split_range(addr, parent_id, split_key).await,
        PdCommand::MergeRanges {
            addr,
            left_id,
            right_id,
        } => run_pd_merge_ranges(addr, left_id, right_id).await,
        PdCommand::InstallLease {
            addr,
            range_id,
            holder,
            expires_at_millis,
        } => run_pd_install_lease(addr, range_id, holder, expires_at_millis).await,
        PdCommand::ClearLease { addr, range_id } => run_pd_clear_lease(addr, range_id).await,
        PdCommand::UpdateMembership {
            addr,
            range_id,
            replicas,
            epoch,
        } => run_pd_update_membership(addr, range_id, replicas, epoch).await,
    }
}

async fn run_pd_status(addr: String) -> anyhow::Result<()> {
    let mut client = pd_client(&addr).await?;
    let value = client
        .status()
        .await
        .map_err(|e| report_pd_error("pd status", e))?;
    print_json(&value)
}

async fn run_pd_list_ranges(addr: String) -> anyhow::Result<()> {
    let mut client = pd_client(&addr).await?;
    let ranges = client
        .list_ranges()
        .await
        .map_err(|e| report_pd_error("pd list-ranges", e))?;
    print_json(&ranges)
}

async fn run_pd_list_nodes(addr: String) -> anyhow::Result<()> {
    let mut client = pd_client(&addr).await?;
    let nodes = client
        .list_nodes()
        .await
        .map_err(|e| report_pd_error("pd list-nodes", e))?;
    print_json(&nodes)
}

async fn run_pd_get_range(addr: String, range_id: u64) -> anyhow::Result<()> {
    let mut client = pd_client(&addr).await?;
    let found = client
        .get_range(range_id)
        .await
        .map_err(|e| report_pd_error("pd get-range", e))?;
    match found {
        Some(r) => print_json(&r),
        None => {
            println!("<not found: range {range_id}>");
            Ok(())
        }
    }
}

async fn run_pd_get_range_by_key(addr: String, key: String) -> anyhow::Result<()> {
    let mut client = pd_client(&addr).await?;
    let found = client
        .get_range_by_key(key.clone().into_bytes())
        .await
        .map_err(|e| report_pd_error("pd get-range-by-key", e))?;
    match found {
        Some(r) => print_json(&r),
        None => {
            println!("<not found: key {key:?}>");
            Ok(())
        }
    }
}

async fn run_pd_get_node(addr: String, node_id: NodeId) -> anyhow::Result<()> {
    let mut client = pd_client(&addr).await?;
    let found = client
        .get_node(node_id)
        .await
        .map_err(|e| report_pd_error("pd get-node", e))?;
    match found {
        Some(n) => print_json(&n),
        None => {
            println!("<not found: node {node_id}>");
            Ok(())
        }
    }
}

async fn run_pd_register(
    addr: String,
    node_id: NodeId,
    address: String,
    stores: Vec<u64>,
) -> anyhow::Result<()> {
    let mut client = pd_client(&addr).await?;
    let info = PdNodeInfo {
        node_id,
        address,
        stores,
        last_heartbeat_millis: 0,
    };
    client
        .register_node(info)
        .await
        .map_err(|e| report_pd_error("pd register", e))?;
    println!("pd: registered node {node_id}");
    Ok(())
}

async fn run_pd_heartbeat(addr: String, node_id: NodeId) -> anyhow::Result<()> {
    let mut client = pd_client(&addr).await?;
    let now = now_millis();
    client
        .heartbeat_node(node_id, now)
        .await
        .map_err(|e| report_pd_error("pd heartbeat", e))?;
    println!("pd: heartbeat for node {node_id} at {now} ms");
    Ok(())
}

async fn run_pd_heartbeat_loop(
    addr: String,
    node_id: NodeId,
    interval_ms: u64,
    peers: Vec<String>,
) -> anyhow::Result<()> {
    // Seed the peer map with the initial endpoint so `NotLeader`
    // hints resolve even without explicit --peer args pointing at it.
    let peer_map = parse_peer_map(&peers, (node_id, addr.clone()))?;
    let resolver_map = peer_map.clone();
    let resolver: aresadb_pd::admin::EndpointResolver =
        Arc::new(move |id| resolver_map.get(&id).cloned());

    let cfg = HeartbeatConfig::new(node_id, addr.clone(), Duration::from_millis(interval_ms))
        .with_endpoint_resolver(resolver);

    let handle = HeartbeatLoop::spawn(cfg);
    println!(
        "pd heartbeat-loop: node {node_id} -> {addr} every {interval_ms}ms ({} peer(s))",
        peer_map.len()
    );

    tokio::signal::ctrl_c().await?;
    println!("pd heartbeat-loop: ctrl-c received, shutting down");
    handle.stop().await;
    Ok(())
}

// Every parameter corresponds 1:1 to a flag on `pd create-range`, so
// shrinking the arg list means inventing a wrapper struct that only
// exists to placate clippy. Targeted allow keeps the signature
// readable and mirrors the CLI flag layout.
#[allow(clippy::too_many_arguments)]
async fn run_pd_create_range(
    addr: String,
    range_id: u64,
    start_key: String,
    end_key: String,
    replicas: String,
    raft_group_id: Option<u64>,
    epoch: u64,
    generation: u64,
) -> anyhow::Result<()> {
    let replicas = parse_replica_specs(&replicas)?;
    let desc = RangeDescriptor {
        range_id,
        start_key: start_key.into_bytes(),
        end_key: end_key.into_bytes(),
        replicas,
        raft_group_id: raft_group_id.unwrap_or(range_id),
        epoch,
        generation,
        lease: None,
    };
    let mut client = pd_client(&addr).await?;
    let stored = client
        .create_range(desc)
        .await
        .map_err(|e| report_pd_error("pd create-range", e))?;
    print_json(&stored)
}

async fn run_pd_split_range(addr: String, parent_id: u64, split_key: String) -> anyhow::Result<()> {
    if split_key.is_empty() {
        bail!("--split-key must be non-empty");
    }
    let mut client = pd_client(&addr).await?;
    let rhs = client
        .split_range(parent_id, split_key.into_bytes())
        .await
        .map_err(|e| report_pd_error("pd split-range", e))?;
    print_json(&rhs)
}

async fn run_pd_merge_ranges(addr: String, left_id: u64, right_id: u64) -> anyhow::Result<()> {
    let mut client = pd_client(&addr).await?;
    client
        .merge_ranges(left_id, right_id)
        .await
        .map_err(|e| report_pd_error("pd merge-ranges", e))?;
    println!("pd: merged ranges {left_id} + {right_id}");
    Ok(())
}

async fn run_pd_install_lease(
    addr: String,
    range_id: u64,
    holder: NodeId,
    expires_at_millis: u64,
) -> anyhow::Result<()> {
    let mut client = pd_client(&addr).await?;
    client
        .install_lease(
            range_id,
            LeaseInfo {
                holder,
                expires_at_millis,
            },
        )
        .await
        .map_err(|e| report_pd_error("pd install-lease", e))?;
    println!(
        "pd: installed lease on range {range_id} (holder {holder}, expires {expires_at_millis})"
    );
    Ok(())
}

async fn run_pd_clear_lease(addr: String, range_id: u64) -> anyhow::Result<()> {
    let mut client = pd_client(&addr).await?;
    client
        .clear_lease(range_id)
        .await
        .map_err(|e| report_pd_error("pd clear-lease", e))?;
    println!("pd: cleared lease on range {range_id}");
    Ok(())
}

async fn run_pd_update_membership(
    addr: String,
    range_id: u64,
    replicas: String,
    epoch: u64,
) -> anyhow::Result<()> {
    let replicas = parse_replica_specs(&replicas)?;
    let mut client = pd_client(&addr).await?;
    client
        .update_membership(range_id, replicas, epoch)
        .await
        .map_err(|e| report_pd_error("pd update-membership", e))?;
    println!("pd: updated range {range_id} membership (epoch {epoch})");
    Ok(())
}

// ---------------------------------------------------------------------
// Unit tests for the pure parsers
// ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_replica_specs_defaults_to_voter() {
        let v = parse_replica_specs("1:1,2:1,3:1").unwrap();
        assert_eq!(v.len(), 3);
        for r in &v {
            assert_eq!(r.role, ReplicaRole::Voter);
            assert_eq!(r.store_id, 1);
        }
        assert_eq!(v[0].node_id, 1);
        assert_eq!(v[1].node_id, 2);
        assert_eq!(v[2].node_id, 3);
    }

    #[test]
    fn parse_replica_specs_mixes_roles() {
        let v = parse_replica_specs("1:1:voter, 2:1:learner , 3:2:L").unwrap();
        assert_eq!(v.len(), 3);
        assert_eq!(v[0].role, ReplicaRole::Voter);
        assert_eq!(v[1].role, ReplicaRole::Learner);
        assert_eq!(v[2].role, ReplicaRole::Learner);
        assert_eq!(v[2].store_id, 2);
    }

    #[test]
    fn parse_replica_specs_rejects_empty() {
        let err = parse_replica_specs("").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("at least one spec"), "got: {msg}");
    }

    #[test]
    fn parse_replica_specs_rejects_bad_format() {
        assert!(parse_replica_specs("just-a-word").is_err());
        assert!(parse_replica_specs("1").is_err());
        assert!(parse_replica_specs("1:2:3:4").is_err());
        assert!(parse_replica_specs("1:x").is_err());
        assert!(parse_replica_specs("1:1:bogus").is_err());
    }

    #[test]
    fn parse_peer_map_includes_initial_and_extras() {
        let peers = vec![
            "2=http://127.0.0.1:7002".to_string(),
            "3=http://127.0.0.1:7003".to_string(),
        ];
        let map = parse_peer_map(&peers, (1, "http://127.0.0.1:7001".to_string())).unwrap();
        assert_eq!(map.len(), 3);
        assert_eq!(map[&1], "http://127.0.0.1:7001");
        assert_eq!(map[&2], "http://127.0.0.1:7002");
        assert_eq!(map[&3], "http://127.0.0.1:7003");
    }

    #[test]
    fn parse_peer_map_rejects_missing_equals() {
        let peers = vec!["just-a-url".to_string()];
        let err = parse_peer_map(&peers, (1, "http://x".to_string())).unwrap_err();
        assert!(format!("{err}").contains("expected ID=URL"));
    }

    #[test]
    fn parse_peer_map_rejects_non_numeric_id() {
        let peers = vec!["abc=http://x".to_string()];
        let err = parse_peer_map(&peers, (1, "http://x".to_string())).unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("node id"));
    }

    #[test]
    fn report_pd_error_highlights_leader_hint() {
        let err = PdAdminClientError::NotLeader(Some(7));
        let rendered = format!("{}", report_pd_error("pd split-range", err));
        assert!(rendered.contains("node 7"), "rendered: {rendered}");
    }

    #[test]
    fn report_pd_error_handles_no_leader() {
        let err = PdAdminClientError::NotLeader(None);
        let rendered = format!("{}", report_pd_error("pd status", err));
        assert!(rendered.contains("no leader"), "rendered: {rendered}");
    }

    #[test]
    fn report_pd_error_passes_other_classes_through() {
        let err = PdAdminClientError::CatalogRejected("bad".to_string());
        let rendered = format!("{}", report_pd_error("pd create-range", err));
        assert!(rendered.contains("bad"), "rendered: {rendered}");
    }

    #[test]
    fn now_millis_is_within_expected_bounds() {
        use std::time::{SystemTime, UNIX_EPOCH};
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let ours = now_millis();
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert!(
            ours >= before.saturating_sub(10) && ours <= after + 10,
            "now_millis {ours} not within [{before}, {after}]"
        );
    }
}

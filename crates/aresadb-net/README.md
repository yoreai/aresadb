# aresadb-net

gRPC transport layer for AresaDB v2. Phase 1b ships a tonic-backed
implementation of `openraft::RaftNetwork` so a multi-node cluster can
exchange `append_entries`, `vote`, and `install_snapshot` RPCs over a
real network. Phase 2c extends it to multi-Raft: every RPC carries a
`raft_group_id` field and the server dispatches each incoming request
through a [`RaftDirectory`] so one listener can host many Raft groups
on the same port.

## Why bincode-in-protobuf

The openraft request/response types (`AppendEntriesRequest`,
`VoteRequest`, …) are generic over our `TypeConfig` and contain
deeply nested Rust-specific bits (`LogId`, `CommittedLeaderId`,
`Membership`, per-entry `EntryPayload::Normal(AresaCommand)`). Writing
a faithful protobuf schema for every field would be:

1. A lot of hand-written conversion code that has to track every
   upstream openraft change.
2. Mostly wasted effort — the wire consumers are always going to be
   other AresaDB nodes, not a polyglot zoo of language clients.

So instead the `.proto` schema wraps each RPC in a single `bytes`
payload. The payload is produced by `bincode::serialize` on the Rust
struct, matching the on-disk format we already use in `aresadb-raft`.
The tradeoff is honest: we lose cross-language wire interop in
exchange for a tiny, self-documenting transport. When we eventually
need a client protocol for SQL / gRPC clients, that layer gets its
own fully-typed protobuf schema (in a separate `.proto` file) — this
file is *only* the node-to-node path.

## Layout

- `proto/raft.proto` — schema for the three Raft RPCs.
- `src/pb.rs` — generated module (via `tonic_build` at compile time).
- `src/codec.rs` — bincode (de)serialization for openraft request /
  response types.
- `src/server.rs` — `RaftGrpcServer` adapter between the tonic trait
  and `openraft::Raft`.
- `src/client.rs` — `GrpcRaftNetwork` implementing
  `RaftNetworkFactory` + `RaftNetwork` for outbound replication.

## Usage

Phase 1 (single Raft group — existing behavior):

```rust,ignore
use aresadb_net::{RaftGrpcServer, GrpcRaftNetwork};

let server = RaftGrpcServer::new(raft.clone()); // wraps a `SingletonRaftDirectory`
tonic::transport::Server::builder()
    .add_service(server.into_service())
    .serve("0.0.0.0:7020".parse().unwrap())
    .await?;

// Elsewhere, point the raft instance at a `GrpcRaftNetwork`:
let network = GrpcRaftNetwork::new_singleton(peer_lookup.clone());
// …pass `network` to `openraft::Raft::new`.
```

Phase 2c (many Raft groups per node — one factory per group):

```rust,ignore
use aresadb_net::{RaftGrpcServer, GrpcRaftNetwork, RaftDirectory};

// `RangeDirectory` lives in `aresadb-cluster` and implements
// `RaftDirectory` directly (a `HashMap<RangeId, Raft<TypeConfig>>`).
let server = RaftGrpcServer::from_directory(range_directory.clone());
tonic::transport::Server::builder()
    .add_service(server.into_service())
    .serve("0.0.0.0:7020".parse().unwrap())
    .await?;

// Each range constructs its own network factory tagged with its
// `raft_group_id`:
let network = GrpcRaftNetwork::new(peer_lookup.clone(), descriptor.raft_group_id);
// …pass this `network` to the range's `openraft::Raft::new`.
```

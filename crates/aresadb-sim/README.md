# aresadb-sim

Deterministic-simulation test harness for AresaDB distributed scenarios.

Built on [madsim](https://github.com/madsim-rs/madsim). In simulation
mode every source of non-determinism (time, task scheduling, random
numbers, network, disk) is controlled, which lets us reproduce
cluster-scale scenarios — partitions, message reordering, clock skew,
replica failures — in a single process, single thread, in milliseconds.

This is the same approach FoundationDB famously used, and it's how
RisingWave tests their distributed stream engine today.

## Status

Phase 0 — skeleton plus a trivial single-node scenario. Phase 1+ will
grow this crate into a full-blown Jepsen-lite: we drive an openraft
cluster, inject partitions and delays, and check linearizability of
observed histories.

## Running simulations

```bash
RUSTFLAGS='--cfg madsim' cargo test -p aresadb-sim
```

The `--cfg madsim` flag swaps `tokio` internals for their deterministic
replacements at compile time. Without it the same tests still run but
against the real tokio runtime.

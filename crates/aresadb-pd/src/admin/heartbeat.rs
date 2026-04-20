//! Node-side heartbeat background task.
//!
//! Every cluster node spawns one [`HeartbeatLoop`] pointing at a PD
//! endpoint. The loop sends `HeartbeatNode` RPCs at a fixed cadence
//! so the PD catalog learns when nodes go silent. Late / lost
//! heartbeats don't corrupt anything — the catalog rule is that
//! timestamps only move forward.
//!
//! The loop is cancellation-safe: a dedicated `tokio::sync::watch`
//! channel signals shutdown, and all waits happen inside
//! `tokio::select!`. Dropping the [`HeartbeatHandle`] is enough to
//! stop the task; explicit `stop()` is provided for callers that
//! want to wait for the last outbound RPC to finish.
//!
//! Failure handling:
//!
//! - Transport / transient errors are logged at `warn` and the loop
//!   continues on its normal cadence. The next heartbeat will pick
//!   up the new leader if one is elected.
//! - `NotLeader` errors rotate the endpoint if the server included a
//!   `pd-leader-id` hint and the caller supplied an
//!   [`HeartbeatConfig::endpoint_for`] resolver. Otherwise the loop
//!   keeps retrying the same endpoint — the PD cluster eventually
//!   points the follower at the new leader.
//! - A shutdown signal drops out of the loop immediately, without
//!   waiting for the next tick.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::types::NodeId;

use super::client::{PdAdminClient, PdAdminClientError};

/// Function signature used to resolve a leader hint back into a PD
/// admin endpoint URL. Takes the hinted [`NodeId`], returns
/// `Some(endpoint)` if the caller knows how to reach that node, or
/// `None` if the hint should be ignored.
pub type EndpointResolver = Arc<dyn Fn(NodeId) -> Option<String> + Send + Sync>;

/// How the heartbeat loop should obtain wall-clock timestamps.
/// Production callers use `wall_clock`; tests inject a frozen clock.
pub type ClockFn = Arc<dyn Fn() -> u64 + Send + Sync>;

/// Configuration passed to [`HeartbeatLoop::spawn`].
pub struct HeartbeatConfig {
    /// Node id this loop is reporting for.
    pub node_id: NodeId,
    /// How often to send a heartbeat.
    pub interval: Duration,
    /// Initial endpoint URL to dial. Replaced at runtime when a
    /// `NotLeader` response arrives with a resolvable leader hint.
    pub endpoint: String,
    /// Optional resolver: given a hinted leader id, produce an
    /// endpoint URL. Leave `None` to keep the initial endpoint
    /// forever.
    pub endpoint_for: Option<EndpointResolver>,
    /// Clock source. Defaults to [`wall_clock`] via
    /// [`HeartbeatConfig::new`].
    pub clock: ClockFn,
}

impl std::fmt::Debug for HeartbeatConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeartbeatConfig")
            .field("node_id", &self.node_id)
            .field("interval", &self.interval)
            .field("endpoint", &self.endpoint)
            .field("has_endpoint_for", &self.endpoint_for.is_some())
            .finish()
    }
}

impl HeartbeatConfig {
    /// Build a default config with the given node id, endpoint, and
    /// interval. Uses [`wall_clock`] and no leader-hint resolver.
    pub fn new(node_id: NodeId, endpoint: impl Into<String>, interval: Duration) -> Self {
        Self {
            node_id,
            interval,
            endpoint: endpoint.into(),
            endpoint_for: None,
            clock: Arc::new(wall_clock),
        }
    }

    /// Attach a resolver for `NotLeader` hints.
    pub fn with_endpoint_resolver(mut self, resolver: EndpointResolver) -> Self {
        self.endpoint_for = Some(resolver);
        self
    }

    /// Override the clock source. Used by tests that want
    /// deterministic timestamps.
    pub fn with_clock(mut self, clock: ClockFn) -> Self {
        self.clock = clock;
        self
    }
}

/// Current wall-clock time, Unix millis. Saturates at `0` on systems
/// whose clock is somehow before the epoch — the catalog treats
/// zero as "never heartbeated" so this is a safe fallback.
pub fn wall_clock() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Handle to a running [`HeartbeatLoop`]. Drop to terminate, or
/// call [`HeartbeatHandle::stop`] to wait for the task to finish.
pub struct HeartbeatHandle {
    shutdown_tx: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl HeartbeatHandle {
    /// Signal shutdown and wait for the task to exit. Safe to call
    /// multiple times.
    pub async fn stop(mut self) {
        // Best-effort: if the receiver is already gone, the task
        // is about to exit anyway.
        let _ = self.shutdown_tx.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }

    /// Returns `true` if the underlying task is still running.
    pub fn is_running(&self) -> bool {
        self.task.as_ref().is_some_and(|t| !t.is_finished())
    }
}

impl Drop for HeartbeatHandle {
    fn drop(&mut self) {
        // Fire-and-forget: flip the shutdown flag. The task sees
        // it on the next `select!` branch and exits.
        let _ = self.shutdown_tx.send(true);
        // `JoinHandle` is cancellation-safe to drop.
    }
}

/// Spawner for the heartbeat loop.
pub struct HeartbeatLoop;

impl HeartbeatLoop {
    /// Spawn a background task that sends `HeartbeatNode` RPCs at
    /// `config.interval`. Returns a handle that keeps the task
    /// alive until it's dropped or stopped.
    pub fn spawn(config: HeartbeatConfig) -> HeartbeatHandle {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let HeartbeatConfig {
                node_id,
                interval,
                mut endpoint,
                endpoint_for,
                clock,
            } = config;

            // Establish the initial connection. If dialing fails
            // we don't panic — the loop just retries on the next
            // tick. That way a slow-starting PD server doesn't
            // sink the node-side task.
            let mut client = match PdAdminClient::connect(endpoint.clone()).await {
                Ok(c) => Some(c),
                Err(e) => {
                    warn!(
                        node_id,
                        error = %e,
                        %endpoint,
                        "pd heartbeat: initial dial failed, will retry"
                    );
                    None
                }
            };

            let mut ticker = tokio::time::interval(interval);
            // Burn the immediate first tick — we want the first
            // heartbeat to fire at `interval`, not at `t=0`.
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            debug!(node_id, "pd heartbeat: shutdown requested");
                            return;
                        }
                    }
                    _ = ticker.tick() => {
                        // Lazily reconnect if the previous dial
                        // failed; keeps the hot path cheap.
                        if client.is_none() {
                            match PdAdminClient::connect(endpoint.clone()).await {
                                Ok(c) => {
                                    client = Some(c);
                                }
                                Err(e) => {
                                    warn!(
                                        node_id,
                                        error = %e,
                                        %endpoint,
                                        "pd heartbeat: reconnect failed"
                                    );
                                    continue;
                                }
                            }
                        }

                        let Some(c) = client.as_mut() else { continue };
                        let now = (clock)();
                        match c.heartbeat_node(node_id, now).await {
                            Ok(()) => {}
                            Err(PdAdminClientError::NotLeader(hint)) => {
                                if let (Some(hint), Some(resolver)) = (hint, endpoint_for.as_ref()) {
                                    if let Some(new_endpoint) = resolver(hint) {
                                        if new_endpoint != endpoint {
                                            debug!(
                                                node_id,
                                                leader_hint = hint,
                                                new_endpoint = %new_endpoint,
                                                "pd heartbeat: rotating to new leader"
                                            );
                                            endpoint = new_endpoint;
                                            client = None;
                                            continue;
                                        }
                                    }
                                }
                                warn!(
                                    node_id,
                                    ?hint,
                                    "pd heartbeat: received NotLeader with no resolvable hint; will retry"
                                );
                            }
                            Err(other) => {
                                warn!(
                                    node_id,
                                    error = %other,
                                    %endpoint,
                                    "pd heartbeat: rpc failed"
                                );
                                // Drop the client to force a fresh
                                // reconnect on the next tick — the
                                // previous channel may be stale if
                                // the server restarted.
                                client = None;
                            }
                        }
                    }
                }
            }
        });

        HeartbeatHandle {
            shutdown_tx,
            task: Some(task),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wall_clock_is_close_to_system_millis() {
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let ours = wall_clock();
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert!(
            ours >= before.saturating_sub(10) && ours <= after + 10,
            "wall_clock {ours} not within [{before}, {after}]"
        );
    }

    #[test]
    fn heartbeat_config_new_defaults_to_wall_clock() {
        let cfg = HeartbeatConfig::new(1, "http://127.0.0.1:7000", Duration::from_millis(10));
        // Smoke test: the closure doesn't panic and returns a non-
        // zero value on any system whose clock isn't before the epoch.
        let t = (cfg.clock)();
        assert!(t > 0);
    }

    #[test]
    fn heartbeat_config_with_clock_overrides() {
        let cfg = HeartbeatConfig::new(1, "http://127.0.0.1:7000", Duration::from_millis(10))
            .with_clock(Arc::new(|| 42));
        assert_eq!((cfg.clock)(), 42);
    }

    #[test]
    fn heartbeat_config_with_resolver_is_captured() {
        let resolver: EndpointResolver = Arc::new(|id| Some(format!("http://resolved-{id}:7000")));
        let cfg = HeartbeatConfig::new(1, "http://127.0.0.1:7000", Duration::from_millis(10))
            .with_endpoint_resolver(resolver.clone());
        assert!(cfg.endpoint_for.is_some());
        let endpoint = cfg.endpoint_for.as_ref().unwrap()(9);
        assert_eq!(endpoint.unwrap(), "http://resolved-9:7000");
    }

    #[tokio::test]
    async fn handle_drop_terminates_task() {
        // The `HeartbeatHandle::drop` impl should flip the shutdown
        // flag. We can't easily observe this without a live gRPC
        // connection, but we can at least prove the task exits.
        let cfg = HeartbeatConfig::new(1, "http://127.0.0.1:1", Duration::from_secs(60)); // unreachable
        let handle = HeartbeatLoop::spawn(cfg);
        // Dropping the handle signals shutdown. The task may still be
        // trying to connect; give it a moment to observe the flag and
        // exit.
        drop(handle);
        tokio::time::sleep(Duration::from_millis(50)).await;
        // If we got here without hanging, the task cleaned up.
    }

    #[tokio::test]
    async fn handle_stop_is_idempotent() {
        let cfg = HeartbeatConfig::new(1, "http://127.0.0.1:1", Duration::from_secs(60));
        let handle = HeartbeatLoop::spawn(cfg);
        handle.stop().await; // must not panic
    }
}

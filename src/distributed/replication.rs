//! Replication for Fault Tolerance
//!
//! Implements leader election and data replication across multiple nodes.
//! Uses a Raft-like consensus protocol for consistency.

#![allow(dead_code)]

use anyhow::{bail, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Replica state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaState {
    /// Node is a follower, receiving updates from leader
    Follower,
    /// Node is a candidate, attempting to become leader
    Candidate,
    /// Node is the leader, coordinating writes
    Leader,
}

/// Configuration for a replica
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaConfig {
    /// Unique node ID
    pub node_id: String,
    /// List of peer addresses
    pub peers: Vec<String>,
    /// Election timeout range in milliseconds
    pub election_timeout_ms: (u64, u64),
    /// Heartbeat interval in milliseconds
    pub heartbeat_interval_ms: u64,
}

impl Default for ReplicaConfig {
    fn default() -> Self {
        Self {
            node_id: uuid::Uuid::new_v4().to_string(),
            peers: Vec::new(),
            election_timeout_ms: (150, 300),
            heartbeat_interval_ms: 50,
        }
    }
}

/// Log entry for replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Term when entry was created
    pub term: u64,
    /// Index in the log
    pub index: u64,
    /// Command to execute
    pub command: ReplicationCommand,
}

/// Commands that can be replicated
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationCommand {
    /// No-op (used for leader confirmation)
    Nop,
    /// Insert a node
    InsertNode(Vec<u8>),
    /// Update a node
    UpdateNode(Vec<u8>),
    /// Delete a node
    DeleteNode(Vec<u8>),
    /// Insert an edge
    InsertEdge(Vec<u8>),
    /// Delete an edge
    DeleteEdge(Vec<u8>),
}

/// Message types for consensus protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusMessage {
    /// Request vote from peers
    RequestVote {
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    },
    /// Response to vote request
    VoteResponse { term: u64, vote_granted: bool },
    /// Append entries (heartbeat or replication)
    AppendEntries {
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    /// Response to append entries
    AppendResponse {
        term: u64,
        success: bool,
        match_index: u64,
    },
}

/// Replica set for managing replication
pub struct ReplicaSet {
    /// Configuration
    config: ReplicaConfig,
    /// Current state
    state: RwLock<ReplicaState>,
    /// Current term
    current_term: RwLock<u64>,
    /// Who we voted for in current term
    voted_for: RwLock<Option<String>>,
    /// Replicated log
    log: RwLock<Vec<LogEntry>>,
    /// Index of highest committed entry
    commit_index: RwLock<u64>,
    /// Index of highest applied entry
    last_applied: RwLock<u64>,
    /// For leader: next index to send to each peer
    next_index: RwLock<HashMap<String, u64>>,
    /// For leader: highest index known to be replicated on each peer
    match_index: RwLock<HashMap<String, u64>>,
    /// Last heartbeat received (for followers)
    last_heartbeat: RwLock<Instant>,
    /// Current leader ID
    leader_id: RwLock<Option<String>>,
}

impl ReplicaSet {
    /// Create a new replica set
    pub fn new(config: ReplicaConfig) -> Self {
        Self {
            config,
            state: RwLock::new(ReplicaState::Follower),
            current_term: RwLock::new(0),
            voted_for: RwLock::new(None),
            log: RwLock::new(Vec::new()),
            commit_index: RwLock::new(0),
            last_applied: RwLock::new(0),
            next_index: RwLock::new(HashMap::new()),
            match_index: RwLock::new(HashMap::new()),
            last_heartbeat: RwLock::new(Instant::now()),
            leader_id: RwLock::new(None),
        }
    }

    /// Get current state
    pub fn state(&self) -> ReplicaState {
        *self.state.read()
    }

    /// Get current term
    pub fn term(&self) -> u64 {
        *self.current_term.read()
    }

    /// Check if this node is the leader
    pub fn is_leader(&self) -> bool {
        *self.state.read() == ReplicaState::Leader
    }

    /// Get the current leader ID
    pub fn leader(&self) -> Option<String> {
        self.leader_id.read().clone()
    }

    /// Get the node ID
    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    /// Append a command to the log (leader only)
    pub fn append_command(&self, command: ReplicationCommand) -> Result<u64> {
        if !self.is_leader() {
            bail!("Not the leader");
        }

        let term = *self.current_term.read();
        let mut log = self.log.write();
        let index = log.len() as u64 + 1;

        log.push(LogEntry {
            term,
            index,
            command,
        });

        Ok(index)
    }

    /// Process a consensus message
    pub fn process_message(&self, msg: ConsensusMessage) -> Option<ConsensusMessage> {
        match msg {
            ConsensusMessage::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => Some(self.handle_request_vote(term, candidate_id, last_log_index, last_log_term)),

            ConsensusMessage::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => Some(self.handle_append_entries(
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            )),

            ConsensusMessage::VoteResponse { term, vote_granted } => {
                self.handle_vote_response(term, vote_granted);
                None
            }

            ConsensusMessage::AppendResponse {
                term,
                success,
                match_index,
            } => {
                self.handle_append_response(term, success, match_index);
                None
            }
        }
    }

    /// Handle a vote request
    fn handle_request_vote(
        &self,
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    ) -> ConsensusMessage {
        let mut current_term = self.current_term.write();
        let mut voted_for = self.voted_for.write();

        // Update term if necessary
        if term > *current_term {
            *current_term = term;
            *voted_for = None;
            *self.state.write() = ReplicaState::Follower;
        }

        let vote_granted = if term < *current_term
            || (voted_for.is_some() && voted_for.as_ref() != Some(&candidate_id))
        {
            false
        } else {
            // Check if candidate's log is at least as up-to-date
            let log = self.log.read();
            let our_last_term = log.last().map(|e| e.term).unwrap_or(0);
            let our_last_index = log.len() as u64;

            let log_ok = last_log_term > our_last_term
                || (last_log_term == our_last_term && last_log_index >= our_last_index);

            if log_ok {
                *voted_for = Some(candidate_id);
                *self.last_heartbeat.write() = Instant::now();
                true
            } else {
                false
            }
        };

        ConsensusMessage::VoteResponse {
            term: *current_term,
            vote_granted,
        }
    }

    /// Handle append entries (heartbeat/replication)
    fn handle_append_entries(
        &self,
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> ConsensusMessage {
        let mut current_term = self.current_term.write();

        // Reply false if term < currentTerm
        if term < *current_term {
            return ConsensusMessage::AppendResponse {
                term: *current_term,
                success: false,
                match_index: 0,
            };
        }

        // Update term and convert to follower if necessary
        if term > *current_term {
            *current_term = term;
            *self.voted_for.write() = None;
        }
        *self.state.write() = ReplicaState::Follower;
        *self.leader_id.write() = Some(leader_id);
        *self.last_heartbeat.write() = Instant::now();

        let mut log = self.log.write();

        // Check if log contains entry at prevLogIndex with matching term
        if prev_log_index > 0 {
            if let Some(entry) = log.get(prev_log_index as usize - 1) {
                if entry.term != prev_log_term {
                    // Conflict - remove this and all following entries
                    log.truncate(prev_log_index as usize - 1);
                    return ConsensusMessage::AppendResponse {
                        term: *current_term,
                        success: false,
                        match_index: log.len() as u64,
                    };
                }
            } else {
                // Missing entries
                return ConsensusMessage::AppendResponse {
                    term: *current_term,
                    success: false,
                    match_index: log.len() as u64,
                };
            }
        }

        // Append new entries
        for entry in entries {
            if entry.index as usize > log.len() {
                log.push(entry);
            }
        }

        // Update commit index
        if leader_commit > *self.commit_index.read() {
            let new_commit = leader_commit.min(log.len() as u64);
            *self.commit_index.write() = new_commit;
        }

        ConsensusMessage::AppendResponse {
            term: *current_term,
            success: true,
            match_index: log.len() as u64,
        }
    }

    /// Handle vote response
    fn handle_vote_response(&self, term: u64, vote_granted: bool) {
        let current_term = *self.current_term.read();

        if term > current_term {
            *self.current_term.write() = term;
            *self.voted_for.write() = None;
            *self.state.write() = ReplicaState::Follower;
            return;
        }

        if *self.state.read() == ReplicaState::Candidate && vote_granted {
            // Count votes (simplified - in real implementation would track votes)
            // If majority, become leader
        }
    }

    /// Handle append response
    fn handle_append_response(&self, term: u64, success: bool, _match_index: u64) {
        let current_term = *self.current_term.read();

        if term > current_term {
            *self.current_term.write() = term;
            *self.voted_for.write() = None;
            *self.state.write() = ReplicaState::Follower;
            return;
        }

        if success && *self.state.read() == ReplicaState::Leader {
            // Update match_index for the peer
            // Check if we can advance commit_index
        }
    }

    /// Start an election
    pub fn start_election(&self) -> ConsensusMessage {
        let mut current_term = self.current_term.write();
        *current_term += 1;
        *self.state.write() = ReplicaState::Candidate;
        *self.voted_for.write() = Some(self.config.node_id.clone());

        let log = self.log.read();
        let last_log_index = log.len() as u64;
        let last_log_term = log.last().map(|e| e.term).unwrap_or(0);

        ConsensusMessage::RequestVote {
            term: *current_term,
            candidate_id: self.config.node_id.clone(),
            last_log_index,
            last_log_term,
        }
    }

    /// Become leader
    pub fn become_leader(&self) {
        *self.state.write() = ReplicaState::Leader;
        *self.leader_id.write() = Some(self.config.node_id.clone());

        // Initialize next_index and match_index for peers
        let log_len = self.log.read().len() as u64 + 1;
        let mut next_index = self.next_index.write();
        let mut match_index = self.match_index.write();

        for peer in &self.config.peers {
            next_index.insert(peer.clone(), log_len);
            match_index.insert(peer.clone(), 0);
        }
    }

    /// Create heartbeat message
    pub fn create_heartbeat(&self) -> ConsensusMessage {
        let log = self.log.read();
        let prev_log_index = log.len() as u64;
        let prev_log_term = log.last().map(|e| e.term).unwrap_or(0);

        ConsensusMessage::AppendEntries {
            term: *self.current_term.read(),
            leader_id: self.config.node_id.clone(),
            prev_log_index,
            prev_log_term,
            entries: Vec::new(),
            leader_commit: *self.commit_index.read(),
        }
    }

    /// Check if election timeout has passed
    pub fn election_timeout_elapsed(&self) -> bool {
        let last = *self.last_heartbeat.read();
        let timeout = Duration::from_millis(self.config.election_timeout_ms.0);
        last.elapsed() > timeout
    }

    /// Get committed entries that haven't been applied
    pub fn get_unapplied_entries(&self) -> Vec<LogEntry> {
        let log = self.log.read();
        let commit_index = *self.commit_index.read();
        let last_applied = *self.last_applied.read();

        log.iter()
            .skip(last_applied as usize)
            .take((commit_index - last_applied) as usize)
            .cloned()
            .collect()
    }

    /// Mark entries as applied
    pub fn mark_applied(&self, up_to: u64) {
        let mut last_applied = self.last_applied.write();
        if up_to > *last_applied {
            *last_applied = up_to;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replica_creation() {
        let config = ReplicaConfig::default();
        let replica = ReplicaSet::new(config);

        assert_eq!(replica.state(), ReplicaState::Follower);
        assert_eq!(replica.term(), 0);
        assert!(!replica.is_leader());
    }

    #[test]
    fn test_start_election() {
        let config = ReplicaConfig::default();
        let replica = ReplicaSet::new(config);

        let msg = replica.start_election();

        assert_eq!(replica.state(), ReplicaState::Candidate);
        assert_eq!(replica.term(), 1);

        match msg {
            ConsensusMessage::RequestVote { term, .. } => {
                assert_eq!(term, 1);
            }
            _ => panic!("Expected RequestVote message"),
        }
    }

    #[test]
    fn test_become_leader() {
        let config = ReplicaConfig {
            peers: vec!["peer1".to_string(), "peer2".to_string()],
            ..Default::default()
        };
        let replica = ReplicaSet::new(config);

        replica.become_leader();

        assert_eq!(replica.state(), ReplicaState::Leader);
        assert!(replica.is_leader());
        assert_eq!(replica.leader(), Some(replica.node_id().to_string()));
    }

    #[test]
    fn test_append_command() {
        let config = ReplicaConfig::default();
        let replica = ReplicaSet::new(config);

        // Can't append if not leader
        assert!(replica.append_command(ReplicationCommand::Nop).is_err());

        // Become leader and append
        replica.become_leader();
        let index = replica.append_command(ReplicationCommand::Nop).unwrap();
        assert_eq!(index, 1);
    }

    #[test]
    fn test_vote_request() {
        let config = ReplicaConfig::default();
        let replica = ReplicaSet::new(config);

        let response = replica.process_message(ConsensusMessage::RequestVote {
            term: 1,
            candidate_id: "other".to_string(),
            last_log_index: 0,
            last_log_term: 0,
        });

        match response {
            Some(ConsensusMessage::VoteResponse { term, vote_granted }) => {
                assert_eq!(term, 1);
                assert!(vote_granted);
            }
            _ => panic!("Expected VoteResponse"),
        }
    }

    #[test]
    fn test_heartbeat() {
        let config = ReplicaConfig::default();
        let replica = ReplicaSet::new(config);
        replica.become_leader();

        let heartbeat = replica.create_heartbeat();

        match heartbeat {
            ConsensusMessage::AppendEntries { entries, .. } => {
                assert!(entries.is_empty());
            }
            _ => panic!("Expected AppendEntries"),
        }
    }
}

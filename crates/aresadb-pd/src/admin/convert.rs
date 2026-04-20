//! Total conversions between the generated protobuf wire types and
//! the Rust catalog types.
//!
//! Every mapping is total in at least one direction — `From<Rust>`
//! for the pb side always succeeds. The opposite direction is
//! fallible because the wire may carry fields we don't accept
//! (unspecified replica roles, both `lease` and `clear` set on an
//! `UpdateLease`, etc.); those failures are turned into tonic
//! `Status`es at the server boundary.
//
// Every fallible conversion here returns `Result<_, tonic::Status>`
// because they're called directly from the tonic service trait
// impls. `Status` is ~176 bytes which trips `result_large_err`;
// boxing it would lose the `?`-through-`TryFrom` ergonomics. A
// module-wide allow is the right call — mirrors the targeted
// allows used elsewhere in the crate for trait-adjacent APIs
// (see `aresadb-pd::raft::state_machine` and `router`).
#![allow(clippy::result_large_err)]

use tonic::Status;

use crate::types::{LeaseInfo, NodeInfo, RangeDescriptor, ReplicaPlacement, ReplicaRole};

use super::pb;

// ----- ReplicaRole -----

impl From<ReplicaRole> for pb::ReplicaRolePb {
    fn from(r: ReplicaRole) -> Self {
        match r {
            ReplicaRole::Voter => pb::ReplicaRolePb::ReplicaRoleVoter,
            ReplicaRole::Learner => pb::ReplicaRolePb::ReplicaRoleLearner,
        }
    }
}

/// Map a wire `ReplicaRolePb` value back to a Rust [`ReplicaRole`].
/// Rejects the zero-valued `Unspecified` variant (a client that
/// left the field defaulted hasn't filled in a valid role yet).
pub fn replica_role_from_i32(value: i32) -> Result<ReplicaRole, Status> {
    match pb::ReplicaRolePb::try_from(value) {
        Ok(pb::ReplicaRolePb::ReplicaRoleVoter) => Ok(ReplicaRole::Voter),
        Ok(pb::ReplicaRolePb::ReplicaRoleLearner) => Ok(ReplicaRole::Learner),
        Ok(pb::ReplicaRolePb::ReplicaRoleUnspecified) => Err(Status::invalid_argument(
            "replica role must be set to voter or learner",
        )),
        Err(_) => Err(Status::invalid_argument(format!(
            "unknown replica role value {value}"
        ))),
    }
}

// ----- ReplicaPlacement -----

impl From<ReplicaPlacement> for pb::ReplicaPlacementPb {
    fn from(r: ReplicaPlacement) -> Self {
        let role: pb::ReplicaRolePb = r.role.into();
        Self {
            node_id: r.node_id,
            store_id: r.store_id,
            role: role as i32,
        }
    }
}

impl TryFrom<pb::ReplicaPlacementPb> for ReplicaPlacement {
    type Error = Status;
    fn try_from(p: pb::ReplicaPlacementPb) -> Result<Self, Self::Error> {
        Ok(Self {
            node_id: p.node_id,
            store_id: p.store_id,
            role: replica_role_from_i32(p.role)?,
        })
    }
}

// ----- LeaseInfo -----

impl From<LeaseInfo> for pb::LeaseInfoPb {
    fn from(l: LeaseInfo) -> Self {
        Self {
            holder: l.holder,
            expires_at_millis: l.expires_at_millis,
        }
    }
}

impl From<pb::LeaseInfoPb> for LeaseInfo {
    fn from(p: pb::LeaseInfoPb) -> Self {
        Self {
            holder: p.holder,
            expires_at_millis: p.expires_at_millis,
        }
    }
}

// ----- RangeDescriptor -----

impl From<RangeDescriptor> for pb::RangeDescriptorPb {
    fn from(r: RangeDescriptor) -> Self {
        Self {
            range_id: r.range_id,
            start_key: r.start_key,
            end_key: r.end_key,
            replicas: r.replicas.into_iter().map(Into::into).collect(),
            raft_group_id: r.raft_group_id,
            epoch: r.epoch,
            generation: r.generation,
            lease: r.lease.map(Into::into),
        }
    }
}

impl TryFrom<pb::RangeDescriptorPb> for RangeDescriptor {
    type Error = Status;
    fn try_from(p: pb::RangeDescriptorPb) -> Result<Self, Self::Error> {
        let replicas = p
            .replicas
            .into_iter()
            .map(ReplicaPlacement::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            range_id: p.range_id,
            start_key: p.start_key,
            end_key: p.end_key,
            replicas,
            raft_group_id: p.raft_group_id,
            epoch: p.epoch,
            generation: p.generation,
            lease: p.lease.map(Into::into),
        })
    }
}

// ----- NodeInfo -----

impl From<NodeInfo> for pb::NodeInfoPb {
    fn from(n: NodeInfo) -> Self {
        Self {
            node_id: n.node_id,
            address: n.address,
            stores: n.stores,
            last_heartbeat_millis: n.last_heartbeat_millis,
        }
    }
}

impl From<pb::NodeInfoPb> for NodeInfo {
    fn from(p: pb::NodeInfoPb) -> Self {
        Self {
            node_id: p.node_id,
            address: p.address,
            stores: p.stores,
            last_heartbeat_millis: p.last_heartbeat_millis,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replica_role_round_trip() {
        for role in [ReplicaRole::Voter, ReplicaRole::Learner] {
            let pb: pb::ReplicaRolePb = role.into();
            let back = replica_role_from_i32(pb as i32).unwrap();
            assert_eq!(back, role);
        }
    }

    #[test]
    fn replica_role_rejects_unspecified_and_unknown() {
        let err =
            replica_role_from_i32(pb::ReplicaRolePb::ReplicaRoleUnspecified as i32).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        let err = replica_role_from_i32(9999).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn replica_placement_round_trip() {
        let r = ReplicaPlacement::voter(7, 3);
        let pb: pb::ReplicaPlacementPb = r.clone().into();
        assert_eq!(ReplicaPlacement::try_from(pb).unwrap(), r);
    }

    #[test]
    fn lease_info_round_trip() {
        let l = LeaseInfo {
            holder: 4,
            expires_at_millis: 1_700_000_001_234,
        };
        let pb: pb::LeaseInfoPb = l.clone().into();
        assert_eq!(LeaseInfo::from(pb), l);
    }

    #[test]
    fn range_descriptor_round_trip_with_and_without_lease() {
        let r_with = RangeDescriptor {
            range_id: 9,
            start_key: b"a".to_vec(),
            end_key: b"z".to_vec(),
            replicas: vec![
                ReplicaPlacement::voter(1, 1),
                ReplicaPlacement::voter(2, 1),
                ReplicaPlacement::learner(3, 1),
            ],
            raft_group_id: 9,
            epoch: 4,
            generation: 2,
            lease: Some(LeaseInfo {
                holder: 2,
                expires_at_millis: 5_000,
            }),
        };
        let pb: pb::RangeDescriptorPb = r_with.clone().into();
        assert_eq!(RangeDescriptor::try_from(pb).unwrap(), r_with);

        let r_without = RangeDescriptor {
            lease: None,
            ..r_with
        };
        let pb: pb::RangeDescriptorPb = r_without.clone().into();
        assert_eq!(RangeDescriptor::try_from(pb).unwrap(), r_without);
    }

    #[test]
    fn node_info_round_trip() {
        let n = NodeInfo {
            node_id: 2,
            address: "10.0.0.2:7001".to_string(),
            stores: vec![1, 2, 3],
            last_heartbeat_millis: 12_345,
        };
        let pb: pb::NodeInfoPb = n.clone().into();
        assert_eq!(NodeInfo::from(pb), n);
    }
}

//! Conformance tests against openraft's official `testing::Suite`.
//!
//! Openraft ships a minimal test suite that exercises a few of the
//! harder-to-get-right corners of the storage API — re-applying
//! committed logs on startup and transferring a snapshot between
//! nodes. Anything that passes both of these, combined with the unit
//! tests in the crate itself, is safe to wire into a real cluster.
//!
//! These tests live in `tests/` (i.e. run as a separate crate) so
//! they link against only the public API. That keeps them honest:
//! if they compile, downstream consumers will too.

use std::sync::Arc;

use aresadb_core::{MemoryBackend, StorageBackend};
use openraft::testing::{StoreBuilder, Suite};
use openraft::StorageError;

use aresadb_raft::{LogStore, NodeId, StateMachineStore, TypeConfig};

/// Builder that the openraft suite uses to spin up a fresh pair of
/// stores for every scenario.
struct AresaStoreBuilder;

impl StoreBuilder<TypeConfig, LogStore, Arc<StateMachineStore>> for AresaStoreBuilder {
    async fn build(&self) -> Result<((), LogStore, Arc<StateMachineStore>), StorageError<NodeId>> {
        let log_backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let data_backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let log = LogStore::new(log_backend);
        let sm = StateMachineStore::new(data_backend);
        Ok(((), log, sm))
    }
}

#[tokio::test]
async fn openraft_suite_re_apply_committed() {
    let ((), log, sm) =
        <AresaStoreBuilder as StoreBuilder<TypeConfig, LogStore, Arc<StateMachineStore>>>::build(
            &AresaStoreBuilder,
        )
        .await
        .unwrap();
    Suite::<TypeConfig, LogStore, Arc<StateMachineStore>, AresaStoreBuilder, ()>::get_initial_state_re_apply_committed(log, sm)
        .await
        .expect("openraft Suite::get_initial_state_re_apply_committed");
}

#[tokio::test]
async fn openraft_suite_transfer_snapshot() {
    Suite::<TypeConfig, LogStore, Arc<StateMachineStore>, AresaStoreBuilder, ()>::transfer_snapshot(&AresaStoreBuilder)
        .await
        .expect("openraft Suite::transfer_snapshot");
}

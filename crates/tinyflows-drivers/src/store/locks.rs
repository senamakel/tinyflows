//! The lease behind [`WorkflowStore::lock_proposal_decision`].
//!
//! A `tsd.leases` lease (owner `flows`, key `workflow_decision/<workflow id>`)
//! held by a random per-call holder token. That is exclusion across every
//! process and store instance sharing the database, which the file store got
//! from an advisory file lock held across the decision. A holder that crashes
//! stops mattering when its lease lapses; the next acquisition then bumps the
//! lease's epoch.
//!
//! [`WorkflowStore::lock_proposal_decision`]: tinyflows::store::WorkflowStore::lock_proposal_decision

use std::time::{Duration, Instant};

use tinyflows::store::ProposalDecisionGuard;
use tinyflows::store::types::WorkflowError;
use tinyflows_storage::{Key, Leases};

use super::{DriverWorkflowStore, Inner, doc_id, storage};

/// How long a claimed lease is good for. A decision that outlives it loses its
/// exclusion, which is what lets a crashed holder's lease be taken over.
pub const LOCK_LEASE: Duration = Duration::from_secs(120);

/// How long to wait for another holder before giving up.
const WAIT: Duration = Duration::from_secs(150);

/// Pause between attempts on a lease someone else holds.
const POLL: Duration = Duration::from_millis(15);

/// The `tsd.leases` owner of every lease the store takes.
pub(crate) const LEASE_OWNER: &str = "flows";

/// The lease key of `workflow_id`'s proposal decisions.
pub(crate) fn lease_key(workflow_id: &str) -> String {
    format!("workflow_decision/{workflow_id}")
}

/// Proof of holding a decision lease: who holds it and at which epoch.
#[derive(Debug, Clone)]
pub(super) struct Held {
    holder: String,
    epoch: u64,
}

impl Inner {
    /// Claims the decision lease of `workflow_id`, waiting while another holder
    /// has it.
    pub(super) async fn acquire_decision_lease(
        &self,
        workflow_id: &str,
    ) -> Result<Held, WorkflowError> {
        self.ensure().await?;
        // Refuses an unusable id (empty, control characters) like every other
        // operation on a workflow.
        doc_id(workflow_id)?;
        let key = lease_key(workflow_id);
        let holder = Key::ulid().as_str().to_owned();
        let ttl_ms = i64::try_from(LOCK_LEASE.as_millis()).unwrap_or(i64::MAX);
        let leases = Leases::new(&self.docs);
        let started = Instant::now();
        loop {
            if let Some(lease) = leases
                .acquire(LEASE_OWNER, &key, &holder, ttl_ms)
                .await
                .map_err(storage)?
            {
                tracing::debug!(target: "workflows", workflow_id, epoch = lease.epoch, "[workflows] took the proposal decision lease");
                return Ok(Held {
                    holder,
                    epoch: lease.epoch,
                });
            }
            if started.elapsed() >= WAIT {
                return Err(WorkflowError::Storage(format!(
                    "timed out waiting for the decision lease on '{workflow_id}'"
                )));
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Releases `held`, if the lease is still ours.
    pub(super) async fn release_decision_lease(&self, workflow_id: &str, held: &Held) {
        let released = Leases::new(&self.docs)
            .release(
                LEASE_OWNER,
                &lease_key(workflow_id),
                &held.holder,
                held.epoch,
            )
            .await;
        if let Err(error) = released {
            tracing::warn!(target: "workflows", workflow_id, %error, "[workflows] failed to release a decision lease; it expires on its own");
        }
    }
}

/// Holding the lease is the operation; dropping releases it.
pub(super) struct DecisionLease {
    store: DriverWorkflowStore,
    workflow_id: String,
    held: Held,
}

impl DecisionLease {
    pub(super) fn new(store: DriverWorkflowStore, workflow_id: String, held: Held) -> Self {
        Self {
            store,
            workflow_id,
            held,
        }
    }
}

impl ProposalDecisionGuard for DecisionLease {}

impl Drop for DecisionLease {
    fn drop(&mut self) {
        let inner = std::sync::Arc::clone(&self.store.inner);
        let (workflow_id, held) = (self.workflow_id.clone(), self.held.clone());
        let released = self.store.block(async move {
            inner.release_decision_lease(&workflow_id, &held).await;
            Ok(())
        });
        if let Err(error) = released {
            tracing::warn!(target: "workflows", workflow_id = %self.workflow_id, %error, "[workflows] could not run a decision lease release; it expires on its own");
        }
    }
}

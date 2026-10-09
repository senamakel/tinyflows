//! What every catalog store shares, whichever backend holds it: the error a
//! guarded graph update reports and the retention caps.
//!
//! `tinyflows-sqlite` and `tinyflows-drivers` both implement the catalog; a
//! host matches one [`FlowUpdateError`] whichever it was handed.

use crate::Flow;

/// How many revision snapshots a store keeps per flow (audit F6). Older ones
/// are pruned on each new capture.
pub const MAX_REVISIONS_PER_FLOW: usize = 20;

/// Default per-flow run-history retention cap: how many of the most-recent runs
/// a single flow keeps before older *terminal* runs are pruned on the next
/// insert (and by the manual `flows_prune_runs` sweep). Bounds unbounded
/// `flow_runs` growth for a hot, frequently-triggered flow while keeping enough
/// history for the run-history inspector.
///
/// Non-terminal runs (`running`, `pending_approval`) are **never** pruned — a
/// parked `pending_approval` run must survive so a later `flows_resume` can find
/// it — so the effective count for a flow may briefly exceed this cap by the
/// number of live/parked runs.
pub const MAX_FLOW_RUNS_PER_FLOW: usize = 100;

/// Failure modes of a store's `update_flow_graph` that the caller must
/// distinguish: a genuine not-found, an optimistic-concurrency conflict
/// (carrying the current server flow so the UI can diff/reload), or a store
/// error.
#[derive(Debug)]
pub enum FlowUpdateError {
    /// No flow with that id exists.
    NotFound,
    /// The flow changed since `expected_updated_at` was observed — the write
    /// was refused to avoid clobbering. Carries the current server flow.
    Conflict(Box<Flow>),
    /// An underlying store failure.
    Store(anyhow::Error),
}

impl std::fmt::Display for FlowUpdateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "flow not found"),
            Self::Conflict(_) => write!(f, "flow changed since it was loaded"),
            Self::Store(e) => write!(f, "{e}"),
        }
    }
}

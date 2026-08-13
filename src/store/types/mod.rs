//! The data model for stored workflows, their runs, and what a host learns
//! about them.
//!
//! A *workflow* is a [`crate::model::WorkflowGraph`] — the engine's own
//! portable JSON shape — plus the bookkeeping a host needs to find it, list
//! it, and say where it came from. The graph itself is deliberately not
//! re-modelled here: it is the contract shared with the engine and with the
//! sibling hosts that embed it, and a parallel host-side copy would only
//! drift.
//!
//! Runs are recorded rather than merely streamed, so a workflow that paused for
//! approval or died with the process can be found again by id.
//!
//! The submodules split the model by lifetime rather than by shape, because
//! that is what decides where each type is stored:
//!
//! - [`workflow`] — the versioned document an operator edits.
//! - [`run`] — one execution's durable record, written once and never revised.
//! - [`note`] — what the host has learned about a workflow across runs.
//! - [`proposal`] — a graph change suggested but not yet made.
//! - [`error`] — the failure vocabulary every surface reports through.
//! - [`diagnosis`] — why a failed run failed, in terms an author can act on.
//! - [`transcript`] — one line of what an agent did inside a step.

pub mod diagnosis;
mod error;
mod note;
mod proposal;
mod run;
mod transcript;
mod workflow;

#[cfg(test)]
mod tests;

pub use diagnosis::Diagnosis;
pub use error::WorkflowError;
pub use note::{NoteId, NoteKind, NoteSource, WorkflowNote};
pub use proposal::{
    ProposalId, ProposalStatus, ProposalVerification, WorkflowProposal, fingerprint,
};

pub use run::{RunId, RunOrigin, RunRecord, RunStatus, RunStep, bounded_evidence, bounded_within};
pub use transcript::TranscriptEntry;
pub use workflow::{
    WorkflowDefaults, WorkflowId, WorkflowRecord, WorkflowRevision, WorkflowSummary,
    record_fingerprint,
};

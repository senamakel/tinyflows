//! Cron jobs and their run history on a `tinystoragedrivers` [`DocumentStore`].
//!
//! The document-port twin of `tinyflows_sqlite::schedule`: the same functions,
//! as `async` methods on [`CronDocuments`] instead of free functions over a
//! [`CronStoreOptions`](https://docs.rs/tinyflows-sqlite), with the same
//! arguments, results and error messages. A host that opened a storage backend
//! (SQLite on a desktop, MongoDB in the cloud, memory in tests) hands it a
//! document handle, usually one already bound to a tenant scope.
//!
//! # Layout
//!
//! | Collection | Document id | Holds |
//! | --- | --- | --- |
//! | `cron_jobs` | job id (`flow:<flow_id>` for a flow-schedule job) | one job |
//! | `cron_runs` | zero-padded run number | one run of one job |
//! | `cron_counters` | `runs` | the next run number |
//!
//! A job's `schedule`, `delivery` and `origin` are JSON strings, so a MongoDB
//! backend never meets keys a caller chose. Optional fields are left out
//! rather than stored as `null`. Instants are kept twice: RFC 3339 text for
//! reading back, and epoch integers for ordering and range filters:
//! `created_ms`, `next_run_ms` plus `next_run_ns`, and `started_ns`
//! (nanoseconds, so jobs due and runs started within one millisecond still
//! order by their real instant).
//!
//! # Concurrency
//!
//! What is atomic across processes sharing one database:
//!
//! - Every job update ([`CronDocuments::update_job`], the run bookkeeping) is
//!   a compare-and-swap on the job document, retried on conflict, so two
//!   writers never lose each other's fields.
//! - [`CronDocuments::reschedule_after_run`] advances `next_run` only from the
//!   occurrence the caller fired: if the stored `next_run` no longer matches
//!   the job it was handed, another process already rescheduled it (or the
//!   schedule was edited), and only the run outcome is recorded.
//! - [`CronDocuments::add_flow_schedule_job`] is idempotent: a flow's job has
//!   a deterministic id, written only if absent.
//! - Run numbers come from a compare-and-swap counter, so they are unique.
//!
//! What is not:
//!
//! - [`CronDocuments::due_jobs`] is a read. Two schedulers polling one
//!   database can both pick up the same due job and both run it, as two
//!   processes on one SQLite file can; the guarded reschedule keeps the
//!   schedule itself from advancing twice.
//! - Recording a run and pruning the job's history are separate writes. A
//!   crash in between leaves a few extra runs, removed by the next record.
//! - Removing a job and then its runs are separate writes; a crash in between
//!   leaves orphan runs that no job lists. [`CronDocuments::sweep_orphan_runs`]
//!   collects them; [`CronDocuments::clear_all_jobs`] runs it.
//! - A run recorded after its job was removed and re-created under the same
//!   id (a flow's schedule job) is checked against the job's current
//!   incarnation when it is recorded, but dispatch does not carry the
//!   incarnation it fired, so a run that started under the old job and
//!   finished after the new one exists attaches to the new one. Closing that
//!   needs the incarnation passed from dispatch to `record_run`, a change to
//!   both stores' API.

use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use tinystoragedrivers_core::{
    CollectionSpec, DocumentStore, ErrorKind, IndexSpec, StorageError, Versioned,
};

mod codec;
mod jobs;
mod patch;
mod removal;
mod runs;
#[cfg(test)]
mod test_support;

pub use tinyflows_schedule::{
    AgentJobSpec, MAX_CRON_OUTPUT_BYTES, TRUNCATED_OUTPUT_MARKER, truncate_cron_output,
};

/// Collection holding one document per job.
pub const JOBS: &str = "cron_jobs";
/// Collection holding one document per run.
pub const RUNS: &str = "cron_runs";
/// Collection holding the run-number counter.
pub const COUNTERS: &str = "cron_counters";

/// Compare-and-swap attempts before a contended write gives up.
const CAS_ATTEMPTS: usize = 32;

/// The cron job store over one (usually tenant-scoped) document handle.
#[derive(Debug, Clone)]
pub struct CronDocuments {
    docs: Arc<dyn DocumentStore>,
    max_run_history: usize,
    max_tasks: usize,
}

impl CronDocuments {
    /// A store over `docs` keeping 50 runs per job and returning at most 64
    /// due jobs per poll, as `CronStoreOptions::new` does.
    pub fn new(docs: Arc<dyn DocumentStore>) -> Self {
        Self {
            docs,
            max_run_history: 50,
            max_tasks: 64,
        }
    }

    /// The same store with a run-history cap and due-job batch size (each at
    /// least 1).
    #[must_use]
    pub fn with_limits(self, max_run_history: usize, max_tasks: usize) -> Self {
        Self {
            max_run_history: max_run_history.max(1),
            max_tasks: max_tasks.max(1),
            ..self
        }
    }

    /// The collections this store writes, with the indexes its queries use.
    pub fn collections() -> Vec<CollectionSpec> {
        vec![
            CollectionSpec::new(JOBS)
                .index(IndexSpec::new("by_next_run", ["next_run_ms"]))
                .index(IndexSpec::new("by_due", ["enabled", "next_run_ms"]))
                // A new name, not a widened `by_due`: MongoDB refuses to
                // redefine an existing index's keys under the same name.
                .index(IndexSpec::new("by_due_ns", ["enabled", "next_run_ns"]))
                .index(IndexSpec::new("by_name", ["name"]))
                .index(IndexSpec::new("by_flow", ["job_type", "command"])),
            CollectionSpec::new(RUNS)
                .index(IndexSpec::new("by_job", ["job_id", "started_ns", "seq"])),
            CollectionSpec::new(COUNTERS),
        ]
    }

    /// Declares [`Self::collections`] (idempotent). Every method calls it.
    pub async fn ensure(&self) -> Result<()> {
        for spec in Self::collections() {
            self.docs
                .ensure_collection(&spec)
                .await
                .map_err(storage_error)?;
        }
        Ok(())
    }
}

/// A storage failure as this store's error.
fn storage_error(error: StorageError) -> anyhow::Error {
    anyhow!("cron store: {error}")
}

/// The job-not-found error, worded as the SQLite store words it.
fn job_not_found(job_id: &str) -> anyhow::Error {
    anyhow!("Cron job '{job_id}' not found")
}

/// Applies `change` to document `id` of `collection` under compare-and-swap.
/// Returns what was stored, or `None` when the document is missing or
/// `change` returns `Ok(None)` (nothing to write).
async fn compare_and_swap(
    docs: &Arc<dyn DocumentStore>,
    collection: &str,
    id: &str,
    change: impl Fn(&Value) -> Result<Option<Value>>,
) -> Result<Option<Versioned<Value>>> {
    for _ in 0..CAS_ATTEMPTS {
        let Some(stored) = docs.get(collection, id).await.map_err(storage_error)? else {
            return Ok(None);
        };
        let Some(next) = change(&stored.doc)? else {
            return Ok(None);
        };
        match docs
            .put(collection, id, next.clone(), stored.unchanged())
            .await
        {
            Ok(version) => {
                return Ok(Some(Versioned {
                    id: id.to_string(),
                    version,
                    doc: next,
                }));
            }
            Err(error) if error.kind() == ErrorKind::Conflict => {}
            Err(error) => return Err(storage_error(error)),
        }
    }
    Err(anyhow!(
        "cron store: {collection}/{id} kept changing under {CAS_ATTEMPTS} attempts"
    ))
    .context("compare-and-swap gave up")
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

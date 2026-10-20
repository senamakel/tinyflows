//! Id-preserving, write-if-absent import of cron jobs and runs from an older
//! store.
//!
//! A host that kept its jobs in `jobs.db` (the retired `tinyflows-sqlite`)
//! reads them back as [`CronJob`]s and [`CronRun`]s and hands them to
//! [`CronDocuments::import_job`] and [`CronDocuments::import_run`]. Nothing
//! already stored is replaced, so an import can be repeated, or run against a
//! scope that has been written to since, without clobbering anything.
//!
//! Two things differ from a plain copy, both forced by the document layout:
//!
//! - A flow-schedule job is filed under the deterministic id the store derives
//!   from the flow (`flow:<flow_id>`), not the random id the old store gave it,
//!   because [`CronDocuments::find_flow_schedule_job`] and the idempotent
//!   registration both address it by that id. [`ImportedJob::id`] says where it
//!   went, and a job's runs must be imported under that id.
//! - Run numbers keep the old run's number where that slot is free; when it
//!   is taken by a different run the next number from the counter is used, so
//!   an import into a scope with history never overwrites it. The counter is
//!   always left beyond every number used.
//!
//! Hosts own any file discovery; these methods only write through the
//! caller-provided storage port.

use anyhow::{Context, Result};
use tinyflows_schedule::{CronJob, CronRun, JobType};
use tinyflows_storage::{Counters, ErrorKind, Filter, Precondition, Query, WriteOptions};

use super::codec::{INCARNATION, run_id, run_to_doc};
use super::jobs::{flow_job_id, new_job_doc, runs_of};
use super::{
    CAS_ATTEMPTS, CronDocuments, JOBS, OWNER, RUN_COUNTER, RUNS, SCHEMA_VERSION, storage_error,
};

/// What [`CronDocuments::import_job`] did with a job (the bus wire record).
pub use tinyflows_schedule::ImportedJob;

impl CronDocuments {
    /// Imports `job`, keeping its schedule, delivery, origin, policy, claim
    /// generation, timestamps and last-run summary, and giving it a fresh
    /// incarnation.
    ///
    /// # Errors
    ///
    /// When the job cannot be encoded, the scope's active-job cap would be
    /// exceeded, or the backend fails.
    pub async fn import_job(&self, job: &CronJob) -> Result<ImportedJob> {
        self.ensure().await?;
        let id = match job.job_type {
            JobType::Flow => flow_job_id(&job.command),
            _ => job.id.clone(),
        };
        let doc = new_job_doc(job)?;
        for _ in 0..CAS_ATTEMPTS {
            if self
                .docs
                .get(JOBS, &id)
                .await
                .map_err(storage_error)?
                .is_some()
            {
                tracing::debug!(target: "cron", job_id = %id, "[cron] import: job already present");
                return Ok(ImportedJob { id, written: false });
            }
            // Written with the job's own creation time, which the atomic batch
            // cannot carry; an importer is the one writer allowed to set it.
            match self
                .insert_with_created(&id, doc.clone(), job.created_at.timestamp_millis())
                .await
            {
                Ok(()) => {
                    tracing::debug!(target: "cron", job_id = %id, source_id = %job.id, "[cron] import: job");
                    return Ok(ImportedJob { id, written: true });
                }
                // Lost a race to another writer: look again, and either find
                // the job there now or try the write once more.
                Err(error) if super::admission::conflict(&error) || is_taken(&error) => {}
                Err(error) => return Err(error).context("Failed to import cron job"),
            }
        }
        anyhow::bail!("cron store: job import kept changing")
    }

    /// Imports one run of `run.job_id` (a job already in the store, under the
    /// id [`ImportedJob::id`] reported), keeping its times, status, output,
    /// duration and delivery status. Returns whether this call wrote it; a run
    /// the job already has at the same start time with the same status is
    /// skipped.
    ///
    /// # Errors
    ///
    /// When the job is not in the store or the backend fails.
    pub async fn import_run(&self, run: &CronRun) -> Result<bool> {
        self.ensure().await?;
        let job = self.job_doc(&run.job_id).await?.doc;
        let same = runs_of(&run.job_id, &job)
            .and(Filter::eq("started_ms", run.started_at.timestamp_millis()))
            .and(Filter::eq("status", run.status.as_str()));
        let known = self
            .docs
            .query(RUNS, &Query::filter(same).limit(1))
            .await
            .map_err(storage_error)?;
        if !known.items.is_empty() {
            return Ok(false);
        }
        let fired = job
            .get(INCARNATION)
            .and_then(|v| v.as_str())
            .map(str::to_string);
        // The old number when its slot is free, else the next from the counter.
        let mut seq = run.id.max(1);
        loop {
            let doc = run_to_doc(
                seq,
                &run.job_id,
                run.run_id.as_deref(),
                fired.as_deref(),
                run.started_at,
                run.finished_at,
                &run.status,
                run.output.clone(),
                run.duration_ms.unwrap_or_default(),
                run.delivery_status.as_ref(),
            );
            match self
                .docs
                .put_with(
                    RUNS,
                    &run_id(seq),
                    doc,
                    Precondition::Absent,
                    WriteOptions::new().schema(SCHEMA_VERSION),
                )
                .await
            {
                Ok(_) => break,
                Err(error)
                    if matches!(error.kind(), ErrorKind::Conflict | ErrorKind::AlreadyExists) =>
                {
                    seq = self.next_run_number().await?;
                }
                Err(error) => {
                    return Err(storage_error(error)).context("Failed to import cron run");
                }
            }
        }
        self.raise_run_counter(seq + 1).await?;
        tracing::debug!(target: "cron", job_id = %run.job_id, seq, "[cron] import: run");
        Ok(true)
    }

    /// Raises the run counter to at least `floor`, never lowering it.
    async fn raise_run_counter(&self, floor: i64) -> Result<()> {
        // The counter holds the last number handed out; `floor` is the next.
        let last = u64::try_from(floor.saturating_sub(1)).unwrap_or_default();
        Counters::new(&self.docs)
            .set_at_least(OWNER, RUN_COUNTER, last)
            .await
            .map_err(storage_error)?;
        Ok(())
    }
}

/// Whether `error` is the storage refusing to write an id that is taken.
fn is_taken(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<tinyflows_storage::StorageError>()
        .is_some_and(|error| matches!(error.kind(), ErrorKind::Conflict | ErrorKind::AlreadyExists))
}

#[cfg(test)]
#[path = "import_tests.rs"]
mod tests;

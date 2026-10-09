//! Run bookkeeping: a run's outcome on its job, the job's next run, and the
//! bounded run history.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tinyflows_schedule::{
    CronJob, CronRun, DeliveryStatus, Schedule, next_run_for_schedule, truncate_cron_output,
};
use tinystoragedrivers_core::{DocumentStoreExt, ErrorKind, Filter, Precondition, Query, Sort};

use super::codec::{doc_to_run, run_id, run_to_doc, set_last_run};
use super::jobs::reschedule;
use super::{CAS_ATTEMPTS, COUNTERS, CronDocuments, JOBS, RUNS, compare_and_swap, storage_error};

/// The counter document run numbers are drawn from.
const RUN_COUNTER: &str = "runs";

impl CronDocuments {
    /// Records a run's outcome on the job (`last_run`, `last_status`, bounded
    /// `last_output`) without touching `next_run` or `enabled`. A missing job
    /// is not an error.
    pub async fn record_last_run(
        &self,
        job_id: &str,
        finished_at: DateTime<Utc>,
        success: bool,
        output: &str,
    ) -> Result<()> {
        self.ensure().await?;
        let output = truncate_cron_output(output);
        compare_and_swap(&self.docs, JOBS, job_id, |doc| {
            let mut next = doc.as_object().cloned().unwrap_or_default();
            set_last_run(&mut next, finished_at, success, output.clone());
            Ok(Some(Value::Object(next)))
        })
        .await
        .context("Failed to update cron last run fields")?;
        Ok(())
    }

    /// Records a run's outcome on the job and advances `next_run` from now.
    ///
    /// The advance applies only while the stored `next_run` is still the one
    /// `job` carries, i.e. the occurrence the caller fired. If another
    /// process already rescheduled it, or the schedule was edited meanwhile,
    /// the stored `next_run` stands and only the outcome is recorded.
    ///
    /// A `Schedule::At` job has no later occurrence, so it is disabled when
    /// advanced: it runs once, and the job and its history are kept.
    pub async fn reschedule_after_run(
        &self,
        job: &CronJob,
        success: bool,
        output: &str,
    ) -> Result<()> {
        self.ensure().await?;
        let now = Utc::now();
        let next_run = next_run_for_schedule(&job.schedule, now)?;
        let one_shot = matches!(job.schedule, Schedule::At { .. });
        let fired_ms = job.next_run.timestamp_millis();
        let output = truncate_cron_output(output);
        compare_and_swap(&self.docs, JOBS, &job.id, |doc| {
            let mut next = doc.as_object().cloned().unwrap_or_default();
            set_last_run(&mut next, now, success, output.clone());
            if doc.get("next_run_ms").and_then(Value::as_i64) == Some(fired_ms) {
                reschedule(&mut next, next_run, one_shot);
            } else {
                tracing::debug!(
                    target: "cron",
                    job_id = %job.id,
                    "[cron] reschedule_after_run: next run already moved — recording the outcome only"
                );
            }
            Ok(Some(Value::Object(next)))
        })
        .await
        .context("Failed to update cron job run state")?;
        Ok(())
    }

    /// Appends one run to the job's history (bounded output) and prunes the
    /// history to the newest runs. Records no delivery status; see
    /// [`Self::record_run_with_delivery`].
    pub async fn record_run(
        &self,
        job_id: &str,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        status: &str,
        output: Option<&str>,
        duration_ms: i64,
    ) -> Result<()> {
        self.record_run_with_delivery(
            job_id,
            started_at,
            finished_at,
            status,
            output,
            duration_ms,
            None,
        )
        .await
    }

    /// [`Self::record_run`] that also stores what happened to the run's
    /// result. The insert and the prune are separate writes; see the module
    /// docs.
    #[allow(clippy::too_many_arguments)]
    pub async fn record_run_with_delivery(
        &self,
        job_id: &str,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        status: &str,
        output: Option<&str>,
        duration_ms: i64,
        delivery_status: Option<DeliveryStatus>,
    ) -> Result<()> {
        tracing::debug!(
            target: "cron",
            %job_id,
            status,
            delivery_status = delivery_status.as_ref().map(DeliveryStatus::as_str),
            "[cron] record_run_with_delivery"
        );
        self.ensure().await?;
        let seq = self.next_run_number().await?;
        let doc = run_to_doc(
            seq,
            job_id,
            started_at,
            finished_at,
            status,
            output.map(truncate_cron_output),
            duration_ms,
            delivery_status.as_ref(),
        );
        self.docs
            .put(RUNS, &run_id(seq), doc, Precondition::Absent)
            .await
            .map_err(storage_error)
            .context("Failed to insert cron run")?;
        self.prune_runs(job_id)
            .await
            .context("Failed to prune cron run history")
    }

    /// Draws the next run number from the counter, under compare-and-swap.
    async fn next_run_number(&self) -> Result<i64> {
        for _ in 0..CAS_ATTEMPTS {
            let stored = self
                .docs
                .get(COUNTERS, RUN_COUNTER)
                .await
                .map_err(storage_error)?;
            let next = stored
                .as_ref()
                .and_then(|stored| stored.doc.get("next"))
                .and_then(Value::as_i64)
                .unwrap_or(1);
            let precondition = stored
                .as_ref()
                .map_or(Precondition::Absent, |stored| stored.unchanged());
            match self
                .docs
                .put(
                    COUNTERS,
                    RUN_COUNTER,
                    json!({ "next": next + 1 }),
                    precondition,
                )
                .await
            {
                Ok(_) => return Ok(next),
                Err(error) if error.kind() == ErrorKind::Conflict => {}
                Err(error) => return Err(storage_error(error)),
            }
        }
        anyhow::bail!("cron store: the run counter kept changing under {CAS_ATTEMPTS} attempts")
    }

    /// Deletes all but the newest `max_run_history` runs of `job_id`.
    async fn prune_runs(&self, job_id: &str) -> Result<()> {
        let stale = self
            .docs
            .query_all(RUNS, &newest_first(job_id))
            .await
            .map_err(storage_error)?;
        for run in stale.iter().skip(self.max_run_history) {
            self.docs
                .delete(RUNS, &run.id, Precondition::None)
                .await
                .map_err(storage_error)?;
        }
        Ok(())
    }

    /// Removes every "queued" placeholder run of `job_id`, so only the real
    /// result remains. Returns how many were removed.
    pub async fn delete_queued_runs(&self, job_id: &str) -> Result<usize> {
        self.ensure().await?;
        let removed = self
            .docs
            .delete_where(
                RUNS,
                &Filter::eq("job_id", job_id).and(Filter::eq("status", "queued")),
            )
            .await
            .map_err(storage_error)?;
        Ok(usize::try_from(removed).unwrap_or(usize::MAX))
    }

    /// The job's newest runs, most recent first, at most `limit` (min 1).
    pub async fn list_runs(&self, job_id: &str, limit: usize) -> Result<Vec<CronRun>> {
        self.ensure().await?;
        let page = self
            .docs
            .query(RUNS, &newest_first(job_id).limit(limit.max(1)))
            .await
            .map_err(storage_error)?;
        page.items.iter().map(doc_to_run).collect()
    }
}

/// `job_id`'s runs, newest first (ties to the later run number).
fn newest_first(job_id: &str) -> Query {
    Query::filter(Filter::eq("job_id", job_id))
        .sort(Sort::desc("started_ms"))
        .sort(Sort::desc("seq"))
}

#[cfg(test)]
#[path = "runs_tests.rs"]
mod tests;

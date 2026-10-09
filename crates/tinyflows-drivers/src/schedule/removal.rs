//! Removing jobs: one job, every job, and the conditional delete both use.

use anyhow::{Context, Result};
use serde_json::Value;
use tinystoragedrivers_core::{DocumentStoreExt, ErrorKind, Filter, Query, Versioned};

use super::codec::{INCARNATION, incarnation};
use super::jobs::runs_of;
use super::{CAS_ATTEMPTS, CronDocuments, JOBS, RUNS, job_not_found, storage_error};

impl CronDocuments {
    /// Deletes the job with `id` and its run history; errors when there is
    /// none.
    pub async fn remove_job(&self, id: &str) -> Result<()> {
        self.ensure().await?;
        for _ in 0..CAS_ATTEMPTS {
            let stored = self
                .docs
                .get(JOBS, id)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| job_not_found(id))?;
            if self.remove_stored(&stored).await? {
                return Ok(());
            }
        }
        anyhow::bail!("cron store: job {id} kept changing under {CAS_ATTEMPTS} attempts")
    }

    /// Deletes the job `stored` holds, then the runs of that incarnation
    /// only, so a job re-created under the same id meanwhile (a flow's
    /// schedule job) keeps its own runs. `false` when that incarnation is
    /// already gone.
    ///
    /// The delete is conditioned on the incarnation, not the document
    /// version: a re-created document starts again at the first version, so
    /// a version check could match it and delete the new job. A document
    /// written before incarnations existed falls back to the version check.
    pub(super) async fn remove_stored(&self, stored: &Versioned<Value>) -> Result<bool> {
        let removed = match incarnation(&stored.doc) {
            Some(meant) => {
                let this =
                    Filter::eq("_id", stored.id.as_str()).and(Filter::eq(INCARNATION, meant));
                self.docs
                    .delete_where(JOBS, &this)
                    .await
                    .map_err(storage_error)
                    .context("Failed to delete cron job")?
                    > 0
            }
            None => match self.docs.delete(JOBS, &stored.id, stored.unchanged()).await {
                Ok(deleted) => deleted,
                Err(error) if error.kind() == ErrorKind::Conflict => false,
                Err(error) => {
                    return Err(storage_error(error)).context("Failed to delete cron job");
                }
            },
        };
        if !removed {
            return Ok(false);
        }
        self.docs
            .delete_where(RUNS, &runs_of(&stored.id, &stored.doc))
            .await
            .map_err(storage_error)?;
        Ok(true)
    }

    /// Deletes every job and the runs of the jobs it deleted, then sweeps
    /// runs left behind by jobs already gone. Returns the number of jobs
    /// removed.
    ///
    /// Each job goes with its own incarnation's runs, so a job created while
    /// this runs (and the runs it records) survives the clear.
    pub async fn clear_all_jobs(&self) -> Result<usize> {
        self.ensure().await?;
        let jobs = self
            .docs
            .query_all(JOBS, &Query::all())
            .await
            .map_err(storage_error)
            .context("Failed to clear cron jobs")?;
        let mut removed = 0usize;
        for stored in &jobs {
            if self.remove_current(stored).await? {
                removed += 1;
            }
        }
        let swept = self.sweep_orphan_runs().await?;
        tracing::info!("[cron] cleared all cron jobs (removed {removed} jobs, swept {swept} runs)");
        Ok(removed)
    }

    /// Removes the job `stored` names, retrying while a document written
    /// before incarnations existed keeps changing. `false` when it is already
    /// gone, or has since been removed and created again (a new incarnation,
    /// e.g. a flow's schedule job registered again): that is not the job the
    /// caller meant to remove.
    pub(super) async fn remove_current(&self, stored: &Versioned<Value>) -> Result<bool> {
        if self.remove_stored(stored).await? {
            return Ok(true);
        }
        let meant = incarnation(&stored.doc);
        for _ in 0..CAS_ATTEMPTS {
            let Some(current) = self
                .docs
                .get(JOBS, &stored.id)
                .await
                .map_err(storage_error)?
            else {
                return Ok(false);
            };
            if incarnation(&current.doc) != meant {
                return Ok(false);
            }
            if self.remove_stored(&current).await? {
                return Ok(true);
            }
        }
        anyhow::bail!(
            "cron store: job {} kept changing under {CAS_ATTEMPTS} attempts",
            stored.id
        )
    }
}

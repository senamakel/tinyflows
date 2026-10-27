//! Removing jobs: one job, every job, and the conditional delete both use.

use anyhow::{Context, Result};
use serde_json::Value;
use tinyflows_storage::{Capability, DocumentStoreExt, Filter, Query, Versioned};

use super::codec::incarnation;
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
        let removed = if !self.docs.capabilities().contains(Capability::Transactions)
            && incarnation(&stored.doc).is_some()
        {
            self.require_unlimited_backend().await?;
            // Preserve legacy incarnation-conditioned removal even on backends
            // without atomic batches. Re-created documents can reuse versions.
            let filter = Filter::eq("_id", stored.id.as_str()).and(Filter::eq(
                super::codec::INCARNATION,
                incarnation(&stored.doc).unwrap_or_default(),
            ));
            self.docs
                .delete_where(JOBS, &filter)
                .await
                .map_err(storage_error)?
                > 0
        } else {
            match self
                .commit_job(&stored.id, Some(stored), None, vec![])
                .await
            {
                Ok(results) => matches!(
                    results.first(),
                    Some(tinyflows_storage::WriteResult::Delete { removed: true })
                ),
                Err(error) if super::admission::conflict(&error) => false,
                Err(error) => return Err(error).context("Failed to delete cron job"),
            }
        };
        if !removed {
            return Ok(false);
        }
        // Claims taken on the removed job die with it, so a job re-created
        // under the same id can claim the same occurrence.
        self.fence_claims(&stored.id, &stored.doc).await;
        self.delete_runs_of(stored).await?;
        Ok(true)
    }

    /// Deletes the job `stored` holds only if it is still exactly that
    /// document: same incarnation and same version, so a duplicate edited
    /// since (renamed, re-enabled) is kept. Then its incarnation's runs.
    /// `false` when it changed or is gone.
    ///
    /// The incarnation check guards the one case a version cannot: a job
    /// removed and re-created under the same id starts again at the first
    /// version.
    pub(super) async fn remove_unchanged(&self, stored: &Versioned<Value>) -> Result<bool> {
        let Some(current) = self
            .docs
            .get(JOBS, &stored.id)
            .await
            .map_err(storage_error)?
        else {
            return Ok(false);
        };
        if current.version != stored.version
            || incarnation(&current.doc) != incarnation(&stored.doc)
        {
            return Ok(false);
        }
        if !self.docs.capabilities().contains(Capability::Transactions) {
            match self
                .commit_job(&stored.id, Some(stored), None, vec![])
                .await
            {
                Ok(results)
                    if matches!(
                        results.first(),
                        Some(tinyflows_storage::WriteResult::Delete { removed: true })
                    ) =>
                {
                    self.delete_runs_of(stored).await?;
                    return Ok(true);
                }
                Ok(_) => return Ok(false),
                Err(error) if super::admission::conflict(&error) => return Ok(false),
                Err(error) => return Err(error),
            }
        }
        self.remove_stored(stored).await
    }

    /// Deletes the runs of the job incarnation `stored` holds.
    async fn delete_runs_of(&self, stored: &Versioned<Value>) -> Result<()> {
        self.docs
            .delete_where(RUNS, &runs_of(&stored.id, &stored.doc))
            .await
            .map_err(storage_error)?;
        Ok(())
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

#[cfg(test)]
#[path = "removal_claims_tests.rs"]
mod tests;

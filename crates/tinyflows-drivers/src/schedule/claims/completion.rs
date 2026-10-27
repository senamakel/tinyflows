//! Fenced terminal history and schedule mutation for occurrence leases.
use super::super::codec::{doc_to_job, run_id, run_to_doc, set_last_run};
use super::super::jobs::reschedule;
use super::super::{CAS_ATTEMPTS, CronDocuments, OWNER, RUNS, storage_error};
use super::{
    CLAIM_EPOCH, CLAIM_OWNER, COMPLETED, ClaimedOccurrence, JOBS, QUEUED, Receipt,
    job_occurrence_ms, lease_key, queued_run_cleanup,
};
use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use tinyflows_schedule::{DeliveryStatus, Schedule, next_run_for_schedule, truncate_cron_output};
use tinyflows_storage::{Capability, Precondition, WriteOp};

/// The outcome and invocation identity to persist for a claimed run.
#[derive(Debug, Clone)]
pub struct RunCompletion {
    pub finished_at: DateTime<Utc>,
    pub success: bool,
    pub output: String,
    pub delivery_status: Option<DeliveryStatus>,
    pub run_id: Option<String>,
}

impl CronDocuments {
    /// Atomically records a run and advances its still-owned occurrence.
    /// Stale, expired, edited, canceled or re-created jobs return false
    /// without any outcome write. A one-shot is disabled, retaining history.
    /// The host applies delete_after_run with [`Self::remove_completed_claim`]
    /// separately when it wants removal. Never follow completion with
    /// `remove_job(job_id)`: that id may already belong to a replacement.
    /// The claim's queued placeholder run, if any, is replaced by the
    /// terminal run.
    ///
    /// # Errors
    /// Completion predates acquisition, state/schedule is malformed, transactions
    /// are unavailable, retries are exhausted, or the backend fails. Once the
    /// run and advance have committed the call is `Ok(true)`: a failed lease
    /// release or history prune is logged, never returned.
    pub async fn complete_claim(
        &self,
        claim: &ClaimedOccurrence,
        finished_at: DateTime<Utc>,
        success: bool,
        output: &str,
        delivery_status: Option<DeliveryStatus>,
    ) -> Result<bool> {
        self.complete_claim_mode(
            claim,
            RunCompletion {
                finished_at,
                success,
                output: output.to_string(),
                delivery_status,
                run_id: None,
            },
            true,
        )
        .await
    }

    /// As [`Self::complete_claim`], also persisting the invocation identity.
    pub async fn complete_claim_with_run(
        &self,
        claim: &ClaimedOccurrence,
        completion: RunCompletion,
    ) -> Result<bool> {
        self.complete_claim_mode(claim, completion, true).await
    }

    /// Atomically records a manual run without advancing or mutating its saved
    /// schedule, enabled state, or delete-after-run policy.
    ///
    /// # Errors
    /// As [`Self::complete_claim`].
    pub async fn complete_manual_claim(
        &self,
        claim: &ClaimedOccurrence,
        finished_at: DateTime<Utc>,
        success: bool,
        output: &str,
        delivery_status: Option<DeliveryStatus>,
    ) -> Result<bool> {
        self.complete_claim_mode(
            claim,
            RunCompletion {
                finished_at,
                success,
                output: output.to_string(),
                delivery_status,
                run_id: None,
            },
            false,
        )
        .await
    }

    /// As [`Self::complete_manual_claim`], also persisting the invocation identity.
    pub async fn complete_manual_claim_with_run(
        &self,
        claim: &ClaimedOccurrence,
        completion: RunCompletion,
    ) -> Result<bool> {
        self.complete_claim_mode(claim, completion, false).await
    }

    async fn complete_claim_mode(
        &self,
        claim: &ClaimedOccurrence,
        completion: RunCompletion,
        advance_schedule: bool,
    ) -> Result<bool> {
        let RunCompletion {
            finished_at,
            success,
            output,
            delivery_status,
            run_id: invocation_id,
        } = completion;
        self.docs
            .capabilities()
            .require(Capability::Transactions)
            .map_err(storage_error)?;
        self.ensure().await?;
        if finished_at < claim.lease.started_at {
            bail!("cron store: completion precedes acquisition");
        }
        for _ in 0..CAS_ATTEMPTS {
            let Some(stored) = self
                .docs
                .get(JOBS, &claim.job.id)
                .await
                .map_err(storage_error)?
            else {
                return Ok(false);
            };
            if !self.owns(&stored, claim, finished_at).await? {
                return Ok(false);
            }
            let job = doc_to_job(&stored)?;
            let mut doc = stored.doc.clone();
            let body = doc
                .as_object_mut()
                .ok_or_else(|| anyhow::anyhow!("cron store: invalid job"))?;
            body.remove(QUEUED);
            body.remove(CLAIM_EPOCH);
            body.remove(CLAIM_OWNER);
            body.insert(
                COMPLETED.into(),
                serde_json::to_value(Receipt {
                    owner: claim.lease.owner.clone(),
                    token: claim.lease.token.clone(),
                    epoch: claim.lease.epoch,
                    incarnation: claim.lease.incarnation.clone(),
                    occurrence_ms: claim.lease.occurrence.timestamp_millis(),
                })?,
            );
            set_last_run(body, finished_at, success, truncate_cron_output(&output));
            if advance_schedule {
                reschedule(
                    body,
                    next_run_for_schedule(&job.schedule, finished_at)?,
                    matches!(job.schedule, Schedule::At { .. }),
                );
            }
            let seq = self.next_run_number().await?;
            let run = run_to_doc(
                seq,
                &job.id,
                invocation_id.as_deref(),
                Some(&claim.lease.incarnation),
                claim.lease.started_at,
                finished_at,
                if success { "ok" } else { "error" },
                Some(truncate_cron_output(&output)),
                (finished_at - claim.lease.started_at).num_milliseconds(),
                delivery_status.as_ref(),
            );
            let mut extra = vec![WriteOp::Put {
                collection: RUNS.into(),
                id: run_id(seq),
                doc: run,
                precondition: Precondition::Absent,
            }];
            extra.extend(queued_run_cleanup(&stored.doc)?);
            let next_ms = job_occurrence_ms(&doc).ok();
            match self
                .commit_job(&job.id, Some(&stored), Some(doc), extra)
                .await
            {
                Ok(_) => {
                    // The run, receipt and advance are durable, so nothing
                    // below may fail the completion: a caller that saw an
                    // error would skip `delete_after_run` removal and report
                    // a finished run as failed. Ending the lease and pruning
                    // history are liveness only (`claim_epoch` already fences
                    // a stale holder and the lease expires on its own).
                    match lease_key(&job.id, claim.lease.occurrence.timestamp_millis()) {
                        Ok(key) => {
                            let leases = self.leases_at(finished_at);
                            // Once the job's next run is past this occurrence
                            // nothing can claim it again, so its lease record
                            // is dead weight: drop it, conditional on the
                            // token and epoch this claim holds (tsd keeps
                            // expired leases only so a takeover continues the
                            // epoch, and none can follow). A manual claim or a
                            // one-shot leaves the schedule where it was, so
                            // its occurrence is still ahead and the record
                            // stays, released.
                            let moved_past = next_ms.is_some_and(|next| {
                                next > claim.lease.occurrence.timestamp_millis()
                            });
                            let ended = if advance_schedule && moved_past {
                                tracing::debug!(
                                    job_id = %job.id,
                                    epoch = claim.lease.epoch,
                                    "[cron-claims] compacting a completed occurrence lease"
                                );
                                leases
                                    .compact(OWNER, &key, &claim.lease.token, claim.lease.epoch)
                                    .await
                                    .map(|_| ())
                            } else {
                                leases
                                    .release(OWNER, &key, &claim.lease.token, claim.lease.epoch)
                                    .await
                                    .map(|_| ())
                            };
                            if let Err(error) = ended {
                                tracing::warn!(
                                    job_id = %job.id,
                                    epoch = claim.lease.epoch,
                                    %error,
                                    "[cron-claims] ending a completed claim's lease failed; it expires on its own"
                                );
                            }
                        }
                        Err(error) => tracing::warn!(
                            job_id = %job.id,
                            epoch = claim.lease.epoch,
                            %error,
                            "[cron-claims] no lease key for a completed claim"
                        ),
                    }
                    if let Err(error) = self.prune_runs(&job.id, &stored.doc).await {
                        tracing::warn!(
                            job_id = %job.id,
                            %error,
                            "[cron-claims] pruning run history after completion failed"
                        );
                    }
                    return Ok(true);
                }
                Err(error) if super::super::admission::conflict(&error) => {}
                Err(error) => return Err(error),
            }
        }
        bail!("cron store: occurrence kept changing during completion")
    }
}

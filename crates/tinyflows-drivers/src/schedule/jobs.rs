//! Cron job CRUD: creating shell/agent/flow-schedule jobs, listing,
//! patching, deduplicating, and selecting due jobs.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tinyflows_schedule::{
    AgentJobSpec, CronJob, CronJobPatch, DeliveryConfig, JobOrigin, JobType, Schedule,
    SessionTarget, next_run_for_schedule, schedule_cron_expression, validate_agent_schedule,
    validate_schedule,
};
use tinystoragedrivers_core::{
    DocumentStoreExt, ErrorKind, Filter, Precondition, Query, Sort, Versioned,
};
use uuid::Uuid;

use super::codec::{
    INCARNATION, created_at, doc_to_job, incarnation, job_to_doc, nanos, set_next_run, text,
};
use super::patch::apply_patch;
use super::{CAS_ATTEMPTS, CronDocuments, JOBS, RUNS, job_not_found, storage_error};

/// The document id of `flow_id`'s schedule job. Deterministic, so writing it
/// only if absent makes registration idempotent across processes.
fn flow_job_id(flow_id: &str) -> String {
    format!("flow:{flow_id}")
}

/// The document for a newly created `job`, with a fresh incarnation.
fn new_job_doc(job: &CronJob) -> Result<Value> {
    let mut doc = job_to_doc(job)?;
    doc[INCARNATION] = json!(Uuid::new_v4().to_string());
    Ok(doc)
}

/// The document for an updated `job`, keeping `previous`'s incarnation.
fn updated_job_doc(job: &CronJob, previous: &Value) -> Result<Value> {
    let mut doc = job_to_doc(job)?;
    if let Some(incarnation) = incarnation(previous) {
        doc[INCARNATION] = json!(incarnation);
    }
    Ok(doc)
}

/// A fresh job of `job_type` on `schedule`, due at its next occurrence.
fn new_job(job_type: JobType, schedule: Schedule, now: DateTime<Utc>) -> Result<CronJob> {
    Ok(CronJob {
        id: Uuid::new_v4().to_string(),
        expression: schedule_cron_expression(&schedule).unwrap_or_default(),
        next_run: next_run_for_schedule(&schedule, now)?,
        schedule,
        command: String::new(),
        prompt: None,
        name: None,
        job_type,
        session_target: SessionTarget::Isolated,
        model: None,
        agent_id: None,
        enabled: true,
        delivery: DeliveryConfig::default(),
        delete_after_run: false,
        created_at: now,
        last_run: None,
        last_status: None,
        last_output: None,
        origin: None,
    })
}

impl CronDocuments {
    /// Writes a new job and reads it back.
    async fn insert(&self, job: &CronJob, what: &str) -> Result<CronJob> {
        self.ensure().await?;
        self.docs
            .put(JOBS, &job.id, new_job_doc(job)?, Precondition::Absent)
            .await
            .map_err(storage_error)
            .with_context(|| format!("Failed to insert cron {what}"))?;
        self.get_job(&job.id).await
    }

    /// Adds an enabled shell job on a plain cron `expression` (see
    /// [`Self::add_shell_job`]).
    pub async fn add_job(&self, expression: &str, command: &str) -> Result<CronJob> {
        let schedule = Schedule::Cron {
            expr: expression.to_string(),
            tz: None,
            active_hours: None,
        };
        self.add_shell_job(None, schedule, command).await
    }

    /// Adds an enabled, optionally named shell job running `command` on
    /// `schedule`.
    pub async fn add_shell_job(
        &self,
        name: Option<String>,
        schedule: Schedule,
        command: &str,
    ) -> Result<CronJob> {
        let now = Utc::now();
        validate_schedule(&schedule, now)?;
        let mut job = new_job(JobType::Shell, schedule, now)?;
        job.name = name;
        job.command = command.to_string();
        self.insert(&job, "shell job").await
    }

    /// Adds an agent job with no agent definition (see
    /// [`Self::add_agent_job_with_definition`]).
    #[allow(clippy::too_many_arguments)]
    pub async fn add_agent_job(
        &self,
        name: Option<String>,
        schedule: Schedule,
        prompt: &str,
        session_target: SessionTarget,
        model: Option<String>,
        delivery: Option<DeliveryConfig>,
        delete_after_run: bool,
    ) -> Result<CronJob> {
        self.add_agent_job_with_definition(
            name,
            schedule,
            prompt,
            session_target,
            model,
            delivery,
            delete_after_run,
            None,
            true,
        )
        .await
    }

    /// Like [`Self::add_agent_job`] but with an optional built-in agent
    /// definition id and the initial enabled state. Stores no origin; see
    /// [`Self::add_agent_job_from_spec`] for that.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_agent_job_with_definition(
        &self,
        name: Option<String>,
        schedule: Schedule,
        prompt: &str,
        session_target: SessionTarget,
        model: Option<String>,
        delivery: Option<DeliveryConfig>,
        delete_after_run: bool,
        agent_id: Option<String>,
        enabled: bool,
    ) -> Result<CronJob> {
        self.add_agent_job_from_spec(AgentJobSpec {
            name,
            schedule,
            prompt: prompt.to_string(),
            session_target,
            model,
            delivery,
            delete_after_run,
            agent_id,
            enabled,
            origin: None,
        })
        .await
    }

    /// Adds an agent job described by `spec`, including its origin
    /// conversation and session target. The job is written in its final
    /// enabled state, so an opt-in job is never briefly enabled.
    pub async fn add_agent_job_from_spec(&self, spec: AgentJobSpec) -> Result<CronJob> {
        let now = Utc::now();
        // Agent runs are inference turns: refuse a schedule tighter than the
        // agent cadence floor.
        validate_agent_schedule(&spec.schedule, now)?;
        tracing::debug!(
            target: "cron",
            session_target = spec.session_target.as_str(),
            origin_kind = spec.origin.as_ref().map(JobOrigin::kind_str),
            "[cron] add_agent_job_from_spec: inserting agent job"
        );
        let mut job = new_job(JobType::Agent, spec.schedule, now)?;
        job.prompt = Some(spec.prompt);
        job.name = spec.name;
        job.session_target = spec.session_target;
        job.model = spec.model;
        job.delivery = spec.delivery.unwrap_or_default();
        job.delete_after_run = spec.delete_after_run;
        job.agent_id = spec.agent_id;
        job.enabled = spec.enabled;
        job.origin = spec.origin;
        self.insert(&job, "agent job").await
    }

    /// Registers the job that fires `flow_id`'s `schedule` trigger. The flow
    /// id is the job's `command`.
    ///
    /// Idempotent and race-safe: the job's id is derived from `flow_id` and
    /// written only if absent, so two concurrent registrations get back the
    /// same single job and neither errors.
    pub async fn add_flow_schedule_job(
        &self,
        flow_id: &str,
        schedule: Schedule,
    ) -> Result<CronJob> {
        let now = Utc::now();
        validate_schedule(&schedule, now)?;
        let mut job = new_job(JobType::Flow, schedule, now)?;
        job.id = flow_job_id(flow_id);
        job.name = Some(format!("flow:{flow_id}"));
        job.command = flow_id.to_string();
        self.ensure().await?;
        match self
            .docs
            .put(JOBS, &job.id, new_job_doc(&job)?, Precondition::Absent)
            .await
        {
            Ok(_) => self.get_job(&job.id).await,
            Err(error) if error.kind() == ErrorKind::Conflict => {
                tracing::debug!(
                    target: "cron",
                    %flow_id,
                    "[cron] add_flow_schedule_job: the flow already has a job — returning it"
                );
                self.find_flow_schedule_job(flow_id)
                    .await?
                    .with_context(|| {
                        format!(
                            "add_flow_schedule_job: insert for flow '{flow_id}' conflicted but no \
                         existing flow-schedule job was found"
                        )
                    })
            }
            Err(error) => {
                Err(storage_error(error)).context("Failed to insert cron flow-schedule job")
            }
        }
    }

    /// The job registered for `flow_id`'s `schedule` trigger, if any.
    pub async fn find_flow_schedule_job(&self, flow_id: &str) -> Result<Option<CronJob>> {
        self.ensure().await?;
        let found = self
            .docs
            .get(JOBS, &flow_job_id(flow_id))
            .await
            .map_err(storage_error)?;
        found.as_ref().map(doc_to_job).transpose()
    }

    /// Every job, ordered by next run.
    pub async fn list_jobs(&self) -> Result<Vec<CronJob>> {
        self.ensure().await?;
        let query = Query::all()
            .sort(Sort::asc("next_run_ms"))
            .sort(Sort::asc("next_run_ns"))
            .sort(Sort::asc("_id"));
        self.docs
            .query_all(JOBS, &query)
            .await
            .map_err(storage_error)?
            .iter()
            .map(doc_to_job)
            .collect()
    }

    /// The job with `job_id`, or an error when there is none.
    pub async fn get_job(&self, job_id: &str) -> Result<CronJob> {
        self.ensure().await?;
        let stored = self
            .docs
            .get(JOBS, job_id)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| job_not_found(job_id))?;
        doc_to_job(&stored)
    }

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

    /// Deletes `stored` if it is unchanged, then the runs of that
    /// incarnation only, so a job re-created under the same id meanwhile (a
    /// flow's schedule job) keeps its own runs. `false` when the job changed
    /// or was already removed.
    pub(super) async fn remove_stored(&self, stored: &Versioned<Value>) -> Result<bool> {
        match self.docs.delete(JOBS, &stored.id, stored.unchanged()).await {
            Ok(true) => {}
            Ok(false) => return Ok(false),
            Err(error) if error.kind() == ErrorKind::Conflict => return Ok(false),
            Err(error) => return Err(storage_error(error)).context("Failed to delete cron job"),
        }
        self.docs
            .delete_where(RUNS, &runs_of(&stored.id, &stored.doc))
            .await
            .map_err(storage_error)?;
        Ok(true)
    }

    /// Deletes every job (and every run). Returns the number of jobs removed.
    pub async fn clear_all_jobs(&self) -> Result<usize> {
        self.ensure().await?;
        let removed = self
            .docs
            .delete_where(JOBS, &Filter::All)
            .await
            .map_err(storage_error)
            .context("Failed to clear cron jobs")?;
        self.docs
            .delete_where(RUNS, &Filter::All)
            .await
            .map_err(storage_error)?;
        tracing::info!("[cron] cleared all cron jobs (removed {removed} rows)");
        Ok(usize::try_from(removed).unwrap_or(usize::MAX))
    }

    /// Removes duplicate jobs sharing a `name`: per name, keeps the job with
    /// the most run history (ties to the earliest created, then the smallest
    /// id) and deletes the rest with their runs. Returns how many were
    /// removed; idempotent.
    pub async fn dedup_named_jobs(&self) -> Result<usize> {
        self.ensure().await?;
        let named = self
            .docs
            .query_all(JOBS, &Query::filter(Filter::exists("name", true)))
            .await
            .map_err(storage_error)?;
        // Ranked by creation at full precision: jobs created within one
        // millisecond (the concurrent-seeding case) still keep the earliest.
        let mut by_name: BTreeMap<String, Vec<&Versioned<Value>>> = BTreeMap::new();
        for stored in &named {
            if let Some(name) = text(&stored.doc, "name") {
                by_name.entry(name.to_string()).or_default().push(stored);
            }
        }
        let mut total_removed = 0usize;
        for (name, jobs) in by_name.into_iter().filter(|(_, jobs)| jobs.len() > 1) {
            let mut ranked = Vec::with_capacity(jobs.len());
            for stored in jobs {
                let runs = self
                    .docs
                    .count(RUNS, &runs_of(&stored.id, &stored.doc))
                    .await
                    .map_err(storage_error)?;
                let created = created_at(&stored.doc).unwrap_or(DateTime::<Utc>::MAX_UTC);
                ranked.push((std::cmp::Reverse(runs), created, stored.id.clone(), stored));
            }
            ranked.sort_by(|a, b| (&a.0, &a.1, &a.2).cmp(&(&b.0, &b.1, &b.2)));
            let keep = ranked[0].2.clone();
            let mut deleted = 0usize;
            for (_, _, _, stored) in ranked.into_iter().skip(1) {
                if self.remove_stored(stored).await? {
                    deleted += 1;
                }
            }
            tracing::info!(
                "[cron] dedup_named_jobs: removed {deleted} duplicate(s) of '{name}' \
                 (keeping id={keep})"
            );
            total_removed += deleted;
        }
        Ok(total_removed)
    }

    /// Enabled jobs due at `now`, earliest first, at most the store's batch
    /// size. A read: see the module docs on two schedulers sharing a database.
    pub async fn due_jobs(&self, now: DateTime<Utc>) -> Result<Vec<CronJob>> {
        self.ensure().await?;
        // Due at nanosecond precision; a document written before
        // `next_run_ns` existed falls back to its millisecond field.
        let due = Filter::lte("next_run_ns", nanos(now)).or(Filter::exists("next_run_ns", false)
            .and(Filter::lte("next_run_ms", now.timestamp_millis())));
        let query = Query::filter(Filter::eq("enabled", true).and(due))
            .sort(Sort::asc("next_run_ms"))
            .sort(Sort::asc("next_run_ns"))
            .sort(Sort::asc("_id"))
            .limit(self.max_tasks);
        let page = self.docs.query(JOBS, &query).await.map_err(storage_error)?;
        page.items.iter().map(doc_to_job).collect()
    }

    /// Applies `patch` to the job with `job_id` and returns the updated job.
    /// The read, patch and write are one compare-and-swap, retried when
    /// another writer got there first.
    pub async fn update_job(&self, job_id: &str, patch: CronJobPatch) -> Result<CronJob> {
        self.ensure().await?;
        for _ in 0..CAS_ATTEMPTS {
            let stored = self
                .docs
                .get(JOBS, job_id)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| job_not_found(job_id))?;
            let job = apply_patch(doc_to_job(&stored)?, patch.clone())?;
            let doc = updated_job_doc(&job, &stored.doc)?;
            match self
                .docs
                .put(JOBS, job_id, doc.clone(), stored.unchanged())
                .await
            {
                Ok(version) => {
                    return doc_to_job(&Versioned {
                        id: job_id.to_string(),
                        version,
                        doc,
                    });
                }
                Err(error) if error.kind() == ErrorKind::Conflict => {}
                Err(error) => {
                    return Err(storage_error(error)).context("Failed to update cron job");
                }
            }
        }
        anyhow::bail!("cron store: job {job_id} kept changing under {CAS_ATTEMPTS} attempts")
    }
}

/// The filter for the runs of the job stored as `job_id` with `doc`: its
/// own incarnation's runs only.
pub(super) fn runs_of(job_id: &str, doc: &Value) -> Filter {
    let of_job = Filter::eq("job_id", job_id);
    match incarnation(doc) {
        Some(incarnation) => of_job.and(Filter::eq(INCARNATION, incarnation)),
        None => of_job,
    }
}

/// Marks `doc` as rescheduled to `next_run` (used by the run bookkeeping).
pub(super) fn reschedule(
    doc: &mut serde_json::Map<String, Value>,
    next_run: DateTime<Utc>,
    one_shot: bool,
) {
    set_next_run(doc, next_run);
    if one_shot {
        doc.insert("enabled".into(), json!(false));
    }
}

#[cfg(test)]
#[path = "jobs_tests.rs"]
mod tests;

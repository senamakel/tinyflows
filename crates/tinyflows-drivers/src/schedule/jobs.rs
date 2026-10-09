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

use super::codec::{doc_to_job, job_to_doc, set_next_run, text};
use super::{CAS_ATTEMPTS, CronDocuments, JOBS, RUNS, job_not_found, storage_error};

/// The document id of `flow_id`'s schedule job. Deterministic, so writing it
/// only if absent makes registration idempotent across processes.
fn flow_job_id(flow_id: &str) -> String {
    format!("flow:{flow_id}")
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
            .put(JOBS, &job.id, job_to_doc(job)?, Precondition::Absent)
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
            .put(JOBS, &job.id, job_to_doc(&job)?, Precondition::Absent)
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
        let removed = self
            .docs
            .delete(JOBS, id, Precondition::None)
            .await
            .map_err(storage_error)
            .context("Failed to delete cron job")?;
        if !removed {
            return Err(job_not_found(id));
        }
        self.remove_runs_of(id).await
    }

    /// Deletes every run of `job_id`.
    async fn remove_runs_of(&self, job_id: &str) -> Result<()> {
        self.docs
            .delete_where(RUNS, &Filter::eq("job_id", job_id))
            .await
            .map_err(storage_error)
            .map(|_| ())
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
        let mut by_name: BTreeMap<String, Vec<(String, i64)>> = BTreeMap::new();
        for stored in &named {
            if let Some(name) = text(&stored.doc, "name") {
                let created = stored
                    .doc
                    .get("created_ms")
                    .and_then(Value::as_i64)
                    .unwrap_or(i64::MAX);
                by_name
                    .entry(name.to_string())
                    .or_default()
                    .push((stored.id.clone(), created));
            }
        }
        let mut total_removed = 0usize;
        for (name, jobs) in by_name.into_iter().filter(|(_, jobs)| jobs.len() > 1) {
            let mut ranked = Vec::with_capacity(jobs.len());
            for (id, created) in jobs {
                let runs = self
                    .docs
                    .count(RUNS, &Filter::eq("job_id", id.as_str()))
                    .await
                    .map_err(storage_error)?;
                ranked.push((std::cmp::Reverse(runs), created, id));
            }
            ranked.sort();
            let keep = ranked[0].2.clone();
            let mut deleted = 0usize;
            for (_, _, id) in ranked.into_iter().skip(1) {
                if self
                    .docs
                    .delete(JOBS, &id, Precondition::None)
                    .await
                    .map_err(storage_error)?
                {
                    self.remove_runs_of(&id).await?;
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
        let query = Query::filter(
            Filter::eq("enabled", true).and(Filter::lte("next_run_ms", now.timestamp_millis())),
        )
        .sort(Sort::asc("next_run_ms"))
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
            match self
                .docs
                .put(JOBS, job_id, job_to_doc(&job)?, stored.unchanged())
                .await
            {
                Ok(version) => {
                    return doc_to_job(&Versioned {
                        id: job_id.to_string(),
                        version,
                        doc: job_to_doc(&job)?,
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

/// `job` with `patch` applied, exactly as the SQLite store applies it.
fn apply_patch(mut job: CronJob, patch: CronJobPatch) -> Result<CronJob> {
    let was_enabled = job.enabled;
    let mut schedule_changed = false;
    if let Some(schedule) = patch.schedule {
        // The agent-only floor applies whenever the schedule is (re)set.
        match job.job_type {
            JobType::Agent => validate_agent_schedule(&schedule, Utc::now())?,
            JobType::Shell | JobType::Flow => validate_schedule(&schedule, Utc::now())?,
        }
        job.schedule = schedule;
        job.expression = schedule_cron_expression(&job.schedule).unwrap_or_default();
        schedule_changed = true;
    }
    if let Some(command) = patch.command {
        job.command = command;
    }
    if let Some(prompt) = patch.prompt {
        job.prompt = Some(prompt);
    }
    if let Some(name) = patch.name {
        job.name = Some(name);
    }
    if let Some(enabled) = patch.enabled {
        job.enabled = enabled;
    }
    if let Some(delivery) = patch.delivery {
        job.delivery = delivery;
    }
    if let Some(model) = patch.model {
        job.model = Some(model);
    }
    if let Some(target) = patch.session_target {
        job.session_target = target;
    }
    if let Some(delete_after_run) = patch.delete_after_run {
        job.delete_after_run = delete_after_run;
    }
    if let Some(agent_id) = patch.agent_id {
        job.agent_id = agent_id;
    }
    if let Some(origin) = patch.origin {
        job.origin = origin;
    }
    if schedule_changed {
        job.next_run = next_run_for_schedule(&job.schedule, Utc::now())?;
    } else if job.enabled && !was_enabled {
        // A job that sat disabled past its next run would fire the instant
        // it is enabled; move a stale next run to the next occurrence.
        let now = Utc::now();
        if job.next_run <= now {
            job.next_run = next_run_for_schedule(&job.schedule, now)?;
        }
    }
    Ok(job)
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

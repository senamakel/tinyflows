//! Agent-job creation: the [`AgentJobSpec`] a job is created from and the
//! insert that persists it, including its origin conversation.

use super::CronStoreOptions;
use super::jobs::get_job;
use super::schema::{encode_origin, with_connection};
use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::params;
use tinyflows_schedule::{
    CronJob, DeliveryConfig, JobOrigin, Schedule, SessionTarget, next_run_for_schedule,
    schedule_cron_expression, validate_agent_schedule,
};
use uuid::Uuid;

/// Everything an agent job is created with. [`AgentJobSpec::new`] gives an
/// enabled, isolated job with no name, model, definition or origin.
#[derive(Debug, Clone)]
pub struct AgentJobSpec {
    pub name: Option<String>,
    pub schedule: Schedule,
    pub prompt: String,
    pub session_target: SessionTarget,
    pub model: Option<String>,
    /// `None` stores [`DeliveryConfig::default`].
    pub delivery: Option<DeliveryConfig>,
    pub delete_after_run: bool,
    /// Built-in agent definition to run with (see [`add_agent_job_with_definition`]).
    pub agent_id: Option<String>,
    /// Inserted in this state in one statement, so an opt-in job is never
    /// briefly enabled.
    pub enabled: bool,
    /// The conversation the job was created from.
    pub origin: Option<JobOrigin>,
}

impl AgentJobSpec {
    pub fn new(schedule: Schedule, prompt: impl Into<String>) -> Self {
        Self {
            name: None,
            schedule,
            prompt: prompt.into(),
            session_target: SessionTarget::default(),
            model: None,
            delivery: None,
            delete_after_run: false,
            agent_id: None,
            enabled: true,
            origin: None,
        }
    }
}

/// Adds an agent job described by `spec`, including its origin conversation
/// and session target.
pub fn add_agent_job_from_spec(opts: &CronStoreOptions, spec: AgentJobSpec) -> Result<CronJob> {
    let AgentJobSpec {
        name,
        schedule,
        prompt,
        session_target,
        model,
        delivery,
        delete_after_run,
        agent_id,
        enabled,
        origin,
    } = spec;
    let now = Utc::now();
    // Agent runs are inference turns: on top of the generic checks, refuse a
    // schedule tighter than `MIN_AGENT_JOB_INTERVAL` (#6158).
    validate_agent_schedule(&schedule, now)?;
    let next_run = next_run_for_schedule(&schedule, now)?;
    let id = Uuid::new_v4().to_string();
    let expression = schedule_cron_expression(&schedule).unwrap_or_default();
    let schedule_json = serde_json::to_string(&schedule)?;
    let delivery = delivery.unwrap_or_default();
    let origin_json = encode_origin(origin.as_ref())?;
    tracing::debug!(
        target: "cron",
        session_target = session_target.as_str(),
        origin_kind = origin.as_ref().map(JobOrigin::kind_str),
        "[cron] add_agent_job_from_spec: inserting agent job"
    );

    with_connection(opts, |conn| {
        // `enabled` is bound (?13) rather than hard-coded so callers can insert a
        // job in its final disabled state in one statement — important for opt-in
        // jobs (e.g. the autopilot) where a create-then-disable sequence could
        // leave the row enabled if the process died between the two writes.
        conn.execute(
            "INSERT INTO cron_jobs (
                id, expression, command, schedule, job_type, prompt, name, session_target, model,
                enabled, delivery, delete_after_run, created_at, next_run, agent_id, origin
             ) VALUES (?1, ?2, '', ?3, 'agent', ?4, ?5, ?6, ?7, ?13, ?8, ?9, ?10, ?11, ?12, ?14)",
            params![
                id,
                expression,
                schedule_json,
                prompt.as_str(),
                name,
                session_target.as_str(),
                model,
                serde_json::to_string(&delivery)?,
                if delete_after_run { 1 } else { 0 },
                now.to_rfc3339(),
                next_run.to_rfc3339(),
                agent_id,
                if enabled { 1 } else { 0 },
                origin_json,
            ],
        )
        .context("Failed to insert cron agent job")?;
        Ok(())
    })?;

    get_job(opts, &id)
}

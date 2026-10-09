//! Agent-job creation: the [`AgentJobSpec`] a job is created from and the
//! insert that persists it, including its origin conversation.

use super::CronStoreOptions;
use super::jobs::get_job;
use super::schema::{encode_origin, with_connection};
use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::params;
use tinyflows_schedule::{
    CronJob, JobOrigin, next_run_for_schedule, schedule_cron_expression, validate_agent_schedule,
};
use uuid::Uuid;

pub use tinyflows_schedule::AgentJobSpec;

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

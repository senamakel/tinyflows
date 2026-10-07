//! Origin capture and delivery status: the columns this store added for
//! routing a scheduled run's result back to the conversation that created the
//! job, including how a database written before them migrates.

use super::*;
use chrono::{Duration as ChronoDuration, Utc};
use rusqlite::Connection;
use tempfile::TempDir;
use tinyflows_schedule::{
    CronJobPatch, DeliveryConfig, DeliveryStatus, JobOrigin, Schedule, SessionTarget, delivery_mode,
};

fn opts(tmp: &TempDir) -> CronStoreOptions {
    CronStoreOptions::new(tmp.path().join("cron").join("jobs.db"))
}

fn daily() -> Schedule {
    Schedule::Cron {
        expr: "0 9 * * *".into(),
        tz: None,
        active_hours: None,
    }
}

fn web_origin() -> JobOrigin {
    JobOrigin::Web {
        thread_id: "thread-1".into(),
        agent_id: Some("orchestrator".into()),
    }
}

fn channel_origin() -> JobOrigin {
    JobOrigin::Channel {
        channel: "telegram".into(),
        reply_target: "chat-42".into(),
        history_key: "telegram:chat-42".into(),
        sender: Some("alice".into()),
        thread_id: None,
    }
}

/// The layout this store wrote just before origin capture: every current
/// `cron_jobs` column except `origin`, and `cron_runs` without
/// `delivery_status`.
const PRE_ORIGIN_LAYOUT: &str = "
    CREATE TABLE cron_jobs (
        id               TEXT PRIMARY KEY,
        expression       TEXT NOT NULL,
        command          TEXT NOT NULL,
        schedule         TEXT,
        job_type         TEXT NOT NULL DEFAULT 'shell',
        prompt           TEXT,
        name             TEXT,
        session_target   TEXT NOT NULL DEFAULT 'isolated',
        model            TEXT,
        enabled          INTEGER NOT NULL DEFAULT 1,
        delivery         TEXT,
        delete_after_run INTEGER NOT NULL DEFAULT 0,
        created_at       TEXT NOT NULL,
        next_run         TEXT NOT NULL,
        last_run         TEXT,
        last_status      TEXT,
        last_output      TEXT,
        agent_id         TEXT
    );
    CREATE TABLE cron_runs (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id      TEXT NOT NULL,
        started_at  TEXT NOT NULL,
        finished_at TEXT NOT NULL,
        status      TEXT NOT NULL,
        output      TEXT,
        duration_ms INTEGER,
        FOREIGN KEY (job_id) REFERENCES cron_jobs(id) ON DELETE CASCADE
    );
    INSERT INTO cron_jobs (id, expression, command, schedule, job_type, prompt, session_target,
        created_at, next_run)
    VALUES ('old', '0 9 * * *', '', '{\"kind\":\"cron\",\"expr\":\"0 9 * * *\"}', 'agent',
        'brief me', 'isolated', '2025-01-01T00:00:00+00:00', '2025-01-01T09:00:00+00:00');
    INSERT INTO cron_runs (job_id, started_at, finished_at, status, output, duration_ms)
    VALUES ('old', '2025-01-01T09:00:00+00:00', '2025-01-01T09:00:01+00:00', 'ok', 'hi', 1000);";

fn table_columns(conn: &Connection, table: &str) -> Vec<String> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .unwrap();
    stmt.query_map([], |r| r.get::<_, String>(1))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[test]
fn pre_origin_database_migrates_and_loads_with_no_origin() {
    let tmp = TempDir::new().unwrap();
    let opts = opts(&tmp);
    std::fs::create_dir_all(opts.db_path.parent().unwrap()).unwrap();
    Connection::open(&opts.db_path)
        .unwrap()
        .execute_batch(PRE_ORIGIN_LAYOUT)
        .unwrap();

    let job = get_job(&opts, "old").unwrap();
    assert_eq!(job.origin, None);
    assert_eq!(job.prompt.as_deref(), Some("brief me"));
    assert_eq!(list_jobs(&opts).unwrap()[0].origin, None);

    let runs = list_runs(&opts, "old", 10).unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].delivery_status, None);

    with_connection(&opts, |conn| {
        assert!(table_columns(conn, "cron_jobs").contains(&"origin".to_string()));
        assert!(table_columns(conn, "cron_runs").contains(&"delivery_status".to_string()));
        Ok(())
    })
    .unwrap();
}

#[test]
fn agent_job_with_origin_and_current_target_roundtrips_through_every_read() {
    let tmp = TempDir::new().unwrap();
    let opts = opts(&tmp);
    let mut spec = AgentJobSpec::new(daily(), "summarise my inbox");
    spec.name = Some("inbox".into());
    spec.session_target = SessionTarget::Current;
    spec.delivery = Some(DeliveryConfig {
        mode: delivery_mode::ORIGIN.into(),
        ..DeliveryConfig::default()
    });
    spec.origin = Some(web_origin());

    let created = add_agent_job_from_spec(&opts, spec).unwrap();
    assert_eq!(created.session_target, SessionTarget::Current);
    assert_eq!(created.origin, Some(web_origin()));
    assert_eq!(created.delivery.mode, "origin");
    assert_eq!(created.prompt.as_deref(), Some("summarise my inbox"));
    assert!(created.enabled);

    let fetched = get_job(&opts, &created.id).unwrap();
    assert_eq!(fetched.origin, Some(web_origin()));
    assert_eq!(fetched.session_target, SessionTarget::Current);

    let listed = list_jobs(&opts).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].origin, Some(web_origin()));

    let due = due_jobs(&opts, Utc::now() + ChronoDuration::days(2)).unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].origin, Some(web_origin()));
    assert_eq!(due[0].session_target, SessionTarget::Current);
}

#[test]
fn spec_honours_disabled_definition_and_channel_origin() {
    let tmp = TempDir::new().unwrap();
    let opts = opts(&tmp);
    let mut spec = AgentJobSpec::new(daily(), "p");
    spec.agent_id = Some("morning_briefing".into());
    spec.enabled = false;
    spec.delete_after_run = true;
    spec.model = Some("m1".into());
    spec.origin = Some(channel_origin());

    let job = add_agent_job_from_spec(&opts, spec).unwrap();
    assert!(!job.enabled);
    assert!(job.delete_after_run);
    assert_eq!(job.agent_id.as_deref(), Some("morning_briefing"));
    assert_eq!(job.model.as_deref(), Some("m1"));
    assert_eq!(job.session_target, SessionTarget::Isolated);
    assert_eq!(
        get_job(&opts, &job.id).unwrap().origin,
        Some(channel_origin())
    );
}

#[test]
fn legacy_creation_functions_store_no_origin() {
    let tmp = TempDir::new().unwrap();
    let opts = opts(&tmp);
    let agent = add_agent_job_with_definition(
        &opts,
        None,
        daily(),
        "p",
        SessionTarget::Main,
        None,
        None,
        false,
        None,
        true,
    )
    .unwrap();
    assert_eq!(agent.origin, None);
    assert_eq!(agent.session_target, SessionTarget::Main);
    let shell = add_job(&opts, "*/5 * * * *", "echo ok").unwrap();
    assert_eq!(shell.origin, None);
    let flow = add_flow_schedule_job(&opts, "flow-1", daily()).unwrap();
    assert_eq!(flow.origin, None);
}

#[test]
fn update_job_sets_keeps_and_clears_origin() {
    let tmp = TempDir::new().unwrap();
    let opts = opts(&tmp);
    let job = add_agent_job_from_spec(&opts, AgentJobSpec::new(daily(), "p")).unwrap();
    assert_eq!(job.origin, None);

    let set = update_job(
        &opts,
        &job.id,
        CronJobPatch {
            origin: Some(Some(channel_origin())),
            session_target: Some(SessionTarget::Current),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(set.origin, Some(channel_origin()));
    assert_eq!(set.session_target, SessionTarget::Current);

    let untouched = update_job(
        &opts,
        &job.id,
        CronJobPatch {
            name: Some("renamed".into()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(untouched.origin, Some(channel_origin()));

    let cleared = update_job(
        &opts,
        &job.id,
        CronJobPatch {
            origin: Some(None),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(cleared.origin, None);
    assert_eq!(get_job(&opts, &job.id).unwrap().origin, None);
}

#[test]
fn record_run_with_delivery_persists_and_lists_the_status() {
    let tmp = TempDir::new().unwrap();
    let opts = opts(&tmp);
    let job = add_job(&opts, "*/5 * * * *", "echo ok").unwrap();
    let t0 = Utc::now();

    record_run(&opts, &job.id, t0, t0, "ok", Some("plain"), 1).unwrap();
    record_run_with_delivery(
        &opts,
        &job.id,
        t0 + ChronoDuration::seconds(1),
        t0 + ChronoDuration::seconds(2),
        "ok",
        Some("sent"),
        1000,
        Some(DeliveryStatus::Delivered),
    )
    .unwrap();
    record_run_with_delivery(
        &opts,
        &job.id,
        t0 + ChronoDuration::seconds(3),
        t0 + ChronoDuration::seconds(4),
        "error",
        None,
        1000,
        Some(DeliveryStatus::Failed),
    )
    .unwrap();

    let runs = list_runs(&opts, &job.id, 10).unwrap();
    let statuses: Vec<_> = runs.iter().map(|r| r.delivery_status.clone()).collect();
    assert_eq!(
        statuses,
        [
            Some(DeliveryStatus::Failed),
            Some(DeliveryStatus::Delivered),
            None
        ]
    );
}

#[test]
fn an_unknown_stored_delivery_status_reads_as_none() {
    let tmp = TempDir::new().unwrap();
    let opts = opts(&tmp);
    let job = add_job(&opts, "*/5 * * * *", "echo ok").unwrap();
    with_connection(&opts, |conn| {
        conn.execute(
            "INSERT INTO cron_runs (job_id, started_at, finished_at, status, delivery_status)
             VALUES (?1, '2025-01-01T00:00:00+00:00', '2025-01-01T00:00:01+00:00', 'ok', 'teleported')",
            [&job.id],
        )?;
        Ok(())
    })
    .unwrap();
    assert_eq!(
        list_runs(&opts, &job.id, 10).unwrap()[0].delivery_status,
        None
    );
}

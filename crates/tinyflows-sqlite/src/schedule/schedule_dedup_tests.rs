//! Duplicate handling in the cron store: `dedup_named_jobs`, the flow-command
//! unique index (including reconciling duplicates a legacy database already
//! holds), and one-shot `At` jobs after a run.

use super::schedule_tests::test_config;
use super::*;
use chrono::{Duration as ChronoDuration, Utc};
use rusqlite::params;
use tempfile::TempDir;
use tinyflows_schedule::{JobType, Schedule};

// ── dedup_named_jobs ─────────────────────────────────────────────

#[test]
fn dedup_named_jobs_no_op_on_empty_db() {
    let tmp = TempDir::new().unwrap();
    let config = test_config(&tmp);
    let removed = dedup_named_jobs(&config).unwrap();
    assert_eq!(removed, 0);
}

#[test]
fn dedup_named_jobs_no_op_when_no_duplicates() {
    let tmp = TempDir::new().unwrap();
    let config = test_config(&tmp);
    add_shell_job(
        &config,
        Some("job_a".into()),
        Schedule::Cron {
            expr: "*/5 * * * *".into(),
            tz: None,
            active_hours: None,
        },
        "echo a",
    )
    .unwrap();
    add_shell_job(
        &config,
        Some("job_b".into()),
        Schedule::Cron {
            expr: "*/10 * * * *".into(),
            tz: None,
            active_hours: None,
        },
        "echo b",
    )
    .unwrap();
    let removed = dedup_named_jobs(&config).unwrap();
    assert_eq!(removed, 0);
    assert_eq!(list_jobs(&config).unwrap().len(), 2);
}

#[test]
fn dedup_named_jobs_removes_duplicates_keeping_history() {
    let tmp = TempDir::new().unwrap();
    let config = test_config(&tmp);

    // Insert two jobs with the same name directly — simulating the old double-seed bug.
    let job_a = add_shell_job(
        &config,
        Some("morning_briefing".into()),
        Schedule::Cron {
            expr: "0 7 * * *".into(),
            tz: None,
            active_hours: None,
        },
        "echo briefing",
    )
    .unwrap();
    let job_b = add_shell_job(
        &config,
        Some("morning_briefing".into()),
        Schedule::Cron {
            expr: "0 7 * * *".into(),
            tz: None,
            active_hours: None,
        },
        "echo briefing",
    )
    .unwrap();

    // Add run history to job_a — it should survive.
    let now = Utc::now();
    record_run(
        &config,
        &job_a.id,
        now,
        now + ChronoDuration::seconds(1),
        "ok",
        Some("output"),
        1000,
    )
    .unwrap();

    let removed = dedup_named_jobs(&config).unwrap();
    assert_eq!(removed, 1);

    let remaining = list_jobs(&config).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(
        remaining[0].id, job_a.id,
        "job with run history should be kept"
    );
    assert!(
        get_job(&config, &job_b.id).is_err(),
        "duplicate without history should be removed"
    );
}

#[test]
fn dedup_named_jobs_keeps_earliest_when_history_tied() {
    let tmp = TempDir::new().unwrap();
    let config = test_config(&tmp);

    // Both jobs have no run history — tie broken by earliest created_at.
    let job_a = add_shell_job(
        &config,
        Some("routine".into()),
        Schedule::Cron {
            expr: "0 8 * * *".into(),
            tz: None,
            active_hours: None,
        },
        "echo first",
    )
    .unwrap();
    let job_b = add_shell_job(
        &config,
        Some("routine".into()),
        Schedule::Cron {
            expr: "0 8 * * *".into(),
            tz: None,
            active_hours: None,
        },
        "echo second",
    )
    .unwrap();

    let removed = dedup_named_jobs(&config).unwrap();
    assert_eq!(removed, 1);

    let remaining = list_jobs(&config).unwrap();
    assert_eq!(remaining.len(), 1);
    // job_a was created first — it should win the tie.
    assert_eq!(remaining[0].id, job_a.id, "earliest job should be kept");
    assert!(get_job(&config, &job_b.id).is_err());
}

// ── add_flow_schedule_job race-safety (CodeRabbit finding A) ────────

#[test]
fn add_flow_schedule_job_twice_yields_a_single_row() {
    let tmp = TempDir::new().unwrap();
    let config = test_config(&tmp);
    let schedule = Schedule::Cron {
        expr: "0 9 * * *".into(),
        tz: None,
        active_hours: None,
    };

    let first = add_flow_schedule_job(&config, "flow-1", schedule.clone()).unwrap();
    let second = add_flow_schedule_job(&config, "flow-1", schedule).unwrap();

    // Calling it twice for the same flow must not create a duplicate — the
    // second call returns the same row the first one created.
    assert_eq!(first.id, second.id);

    let flow_jobs: Vec<_> = list_jobs(&config)
        .unwrap()
        .into_iter()
        .filter(|j| j.job_type == JobType::Flow && j.command == "flow-1")
        .collect();
    assert_eq!(
        flow_jobs.len(),
        1,
        "exactly one job_type='flow' row should exist for flow-1"
    );
}

#[test]
fn add_flow_schedule_job_unique_index_does_not_affect_shell_jobs() {
    let tmp = TempDir::new().unwrap();
    let config = test_config(&tmp);

    // Two shell jobs sharing the same command must both persist — the new
    // partial unique index is scoped to job_type = 'flow' and must not
    // constrain shell/agent jobs, which may legitimately share a command.
    let shell_a = add_job(&config, "*/5 * * * *", "echo shared").unwrap();
    let shell_b = add_job(&config, "*/10 * * * *", "echo shared").unwrap();

    assert!(get_job(&config, &shell_a.id).is_ok());
    assert!(get_job(&config, &shell_b.id).is_ok());
    assert_eq!(list_jobs(&config).unwrap().len(), 2);
}

#[test]
fn dedup_named_jobs_ignores_unnamed_jobs() {
    let tmp = TempDir::new().unwrap();
    let config = test_config(&tmp);

    // Unnamed jobs (name = NULL) — dedup should not touch them.
    add_job(&config, "*/5 * * * *", "echo unnamed-1").unwrap();
    add_job(&config, "*/5 * * * *", "echo unnamed-2").unwrap();

    let removed = dedup_named_jobs(&config).unwrap();
    assert_eq!(removed, 0);
    assert_eq!(list_jobs(&config).unwrap().len(), 2);
}

#[test]
fn legacy_duplicate_flow_jobs_are_reconciled_before_the_unique_index() {
    let tmp = TempDir::new().unwrap();
    let config = test_config(&tmp);
    // Open once to create the schema, then drop the index and write the
    // duplicates an older, index-less build could leave behind.
    with_connection(&config, |conn| {
        conn.execute_batch("DROP INDEX idx_cron_jobs_flow_command;")?;
        for (id, created) in [
            ("late", "2024-01-02T00:00:00+00:00"),
            ("early", "2024-01-01T00:00:00+00:00"),
        ] {
            conn.execute(
                "INSERT INTO cron_jobs (id, expression, command, job_type, created_at, next_run)
                 VALUES (?1, '0 9 * * *', 'flow-1', 'flow', ?2, '2024-01-03T09:00:00+00:00')",
                params![id, created],
            )?;
        }
        Ok(())
    })
    .unwrap();

    // The next connection migrates instead of failing on the unique index.
    let flow_jobs: Vec<_> = list_jobs(&config)
        .unwrap()
        .into_iter()
        .filter(|j| j.job_type == JobType::Flow)
        .collect();
    assert_eq!(flow_jobs.len(), 1);
    assert_eq!(flow_jobs[0].id, "early");
    let has_index: bool = with_connection(&config, |conn| {
        Ok(conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'idx_cron_jobs_flow_command')",
            [],
            |r| r.get(0),
        )?)
    })
    .unwrap();
    assert!(has_index);
}

#[test]
fn reschedule_after_run_disables_a_one_shot_at_job() {
    let tmp = TempDir::new().unwrap();
    let config = test_config(&tmp);
    let at = Utc::now() + ChronoDuration::minutes(5);
    let job = add_shell_job(&config, None, Schedule::At { at }, "echo once").unwrap();
    assert!(job.enabled);

    reschedule_after_run(&config, &job, true, "done").unwrap();

    let stored = get_job(&config, &job.id).unwrap();
    assert!(!stored.enabled, "a one-shot job must not stay due");
    assert_eq!(stored.last_status.as_deref(), Some("ok"));
    assert!(
        due_jobs(&config, at + ChronoDuration::hours(1))
            .unwrap()
            .is_empty()
    );
}

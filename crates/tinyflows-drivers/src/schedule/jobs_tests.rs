use super::*;

use super::super::test_support::{channel_origin, daily, store};
use chrono::Duration;
use tinyflows_schedule::{ActiveHours, JobOrigin};

fn every(minutes: u64) -> Schedule {
    Schedule::Every {
        every_ms: minutes * 60_000,
    }
}

#[tokio::test]
async fn add_job_accepts_a_five_field_expression() {
    let job = store().add_job("*/5 * * * *", "echo ok").await.unwrap();
    assert_eq!(job.expression, "*/5 * * * *");
    assert_eq!(job.command, "echo ok");
    assert_eq!(job.job_type, JobType::Shell);
    assert!(job.enabled && job.next_run > Utc::now());
    assert!(matches!(job.schedule, Schedule::Cron { .. }));
}

#[tokio::test]
async fn a_shell_job_keeps_its_name_and_active_hours() {
    let schedule = Schedule::Cron {
        expr: "0 9 * * *".into(),
        tz: Some("UTC".into()),
        active_hours: Some(ActiveHours {
            start: "09:00".into(),
            end: "17:00".into(),
        }),
    };
    let store = store();
    let job = store
        .add_shell_job(Some("business".into()), schedule.clone(), "ls")
        .await
        .unwrap();
    let read = store.get_job(&job.id).await.unwrap();
    assert_eq!(read.name.as_deref(), Some("business"));
    assert_eq!(read.schedule, schedule);
    assert_eq!(read.created_at, job.created_at);
}

#[tokio::test]
async fn an_invalid_schedule_is_refused() {
    let error = store().add_job("not cron", "x").await.unwrap_err();
    assert!(!error.to_string().is_empty());
}

#[tokio::test]
async fn agent_jobs_carry_their_spec_and_origin() {
    let store = store();
    let job = store
        .add_agent_job(
            Some("brief".into()),
            daily(),
            "summarize",
            SessionTarget::Main,
            Some("m1".into()),
            None,
            true,
        )
        .await
        .unwrap();
    assert_eq!(job.job_type, JobType::Agent);
    assert_eq!(job.prompt.as_deref(), Some("summarize"));
    assert_eq!(job.session_target, SessionTarget::Main);
    assert!(job.delete_after_run && job.origin.is_none());

    let defined = store
        .add_agent_job_with_definition(
            None,
            daily(),
            "p",
            SessionTarget::Isolated,
            None,
            None,
            false,
            Some("welcome".into()),
            false,
        )
        .await
        .unwrap();
    assert_eq!(defined.agent_id.as_deref(), Some("welcome"));
    assert!(!defined.enabled, "written disabled, never briefly enabled");

    let mut spec = AgentJobSpec::new(daily(), "p");
    spec.session_target = SessionTarget::Current;
    spec.origin = Some(channel_origin());
    let from_spec = store.add_agent_job_from_spec(spec).await.unwrap();
    let read = store.get_job(&from_spec.id).await.unwrap();
    assert_eq!(read.origin, Some(channel_origin()));
    assert_eq!(read.session_target, SessionTarget::Current);
}

#[tokio::test]
async fn an_agent_job_tighter_than_the_floor_is_refused() {
    let error = store()
        .add_agent_job(
            None,
            every(1),
            "p",
            SessionTarget::Isolated,
            None,
            None,
            false,
        )
        .await;
    assert!(error.is_err());
}

#[tokio::test]
async fn flow_schedule_jobs_are_one_per_flow() {
    let store = store();
    assert!(store.find_flow_schedule_job("f1").await.unwrap().is_none());
    let first = store.add_flow_schedule_job("f1", daily()).await.unwrap();
    let second = store.add_flow_schedule_job("f1", every(60)).await.unwrap();
    assert_eq!(first.id, second.id);
    assert_eq!(first.command, "f1");
    assert_eq!(first.name.as_deref(), Some("flow:f1"));
    assert_eq!(first.job_type, JobType::Flow);
    assert_eq!(store.list_jobs().await.unwrap().len(), 1);
    assert_eq!(
        store
            .find_flow_schedule_job("f1")
            .await
            .unwrap()
            .map(|j| j.id),
        Some(first.id.clone())
    );
    store.add_job("0 9 * * *", "f1").await.unwrap();
    assert_eq!(
        store.list_jobs().await.unwrap().len(),
        2,
        "shell jobs may share a command"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_flow_registrations_return_one_job() {
    let store = store();
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let store = store.clone();
            tokio::spawn(async move { store.add_flow_schedule_job("f", daily()).await })
        })
        .collect();
    let mut ids = Vec::new();
    for handle in handles {
        ids.push(handle.await.unwrap().unwrap().id);
    }
    ids.dedup();
    assert_eq!(ids.len(), 1);
    assert_eq!(store.list_jobs().await.unwrap().len(), 1);
}

#[tokio::test]
async fn add_list_remove_round_trip() {
    let store = store();
    let late = store.add_job("0 23 * * *", "late").await.unwrap();
    let job = store.add_shell_job(None, every(5), "soon").await.unwrap();
    let listed: Vec<String> = store
        .list_jobs()
        .await
        .unwrap()
        .into_iter()
        .map(|j| j.id)
        .collect();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0], job.id, "ordered by next run");
    store.remove_job(&late.id).await.unwrap();
    let error = store.remove_job(&late.id).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        format!("Cron job '{}' not found", late.id)
    );
    assert!(store.get_job(&late.id).await.is_err());
    assert_eq!(store.clear_all_jobs().await.unwrap(), 1);
    assert!(store.list_jobs().await.unwrap().is_empty());
}

#[tokio::test]
async fn due_jobs_filter_by_time_and_enabled_and_respect_the_batch() {
    let store = store().with_limits(50, 2);
    let a = store.add_job("* * * * *", "a").await.unwrap();
    let b = store.add_job("* * * * *", "b").await.unwrap();
    let c = store.add_job("* * * * *", "c").await.unwrap();
    assert!(store.due_jobs(Utc::now()).await.unwrap().is_empty());
    let later = Utc::now() + Duration::minutes(2);
    assert_eq!(store.due_jobs(later).await.unwrap().len(), 2, "capped");
    store
        .update_job(
            &a.id,
            CronJobPatch {
                enabled: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let due: Vec<String> = store
        .due_jobs(later)
        .await
        .unwrap()
        .into_iter()
        .map(|j| j.id)
        .collect();
    assert_eq!(due.len(), 2);
    assert!(due.contains(&b.id) && due.contains(&c.id));
}

#[tokio::test]
async fn update_job_applies_the_patch() {
    let store = store();
    let job = store.add_job("0 9 * * *", "old").await.unwrap();
    let updated = store
        .update_job(
            &job.id,
            CronJobPatch {
                schedule: Some(every(30)),
                command: Some("new".into()),
                prompt: Some("p".into()),
                name: Some("n".into()),
                model: Some("m".into()),
                delete_after_run: Some(true),
                agent_id: Some(Some("a".into())),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.command, "new");
    assert_eq!(updated.schedule, every(30));
    assert_eq!(updated.expression, "");
    assert_eq!(updated.agent_id.as_deref(), Some("a"));
    assert!(updated.delete_after_run);
    assert_eq!(
        store.get_job(&job.id).await.unwrap().name.as_deref(),
        Some("n")
    );
    assert!(
        store
            .update_job("missing", CronJobPatch::default())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn update_job_sets_keeps_and_clears_origin() {
    let store = store();
    let job = store
        .add_agent_job_from_spec(AgentJobSpec::new(daily(), "p"))
        .await
        .unwrap();
    let set = store
        .update_job(
            &job.id,
            CronJobPatch {
                origin: Some(Some(channel_origin())),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(set.origin, Some(channel_origin()));
    let kept = store
        .update_job(
            &job.id,
            CronJobPatch {
                name: Some("renamed".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(kept.origin, Some(channel_origin()));
    let cleared = store
        .update_job(
            &job.id,
            CronJobPatch {
                origin: Some(None::<JobOrigin>),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(cleared.origin, None);
    assert_eq!(store.get_job(&job.id).await.unwrap().origin, None);
}

#[tokio::test]
async fn enabling_a_stale_disabled_job_moves_its_next_run_forward() {
    let store = store();
    let job = store
        .add_agent_job_with_definition(
            None,
            daily(),
            "p",
            SessionTarget::Isolated,
            None,
            None,
            false,
            None,
            false,
        )
        .await
        .unwrap();
    // Age the stored next run into the past, as if the job sat disabled.
    let past = Utc::now() - Duration::days(2);
    let stored = store.docs.get(JOBS, &job.id).await.unwrap().unwrap();
    let mut doc = stored.doc.as_object().cloned().unwrap();
    set_next_run(&mut doc, past);
    store
        .docs
        .put(JOBS, &job.id, Value::Object(doc), stored.unchanged())
        .await
        .unwrap();
    let enabled = store
        .update_job(
            &job.id,
            CronJobPatch {
                enabled: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(enabled.next_run > Utc::now());

    // A future next run survives enabling.
    let disabled = store
        .update_job(
            &job.id,
            CronJobPatch {
                enabled: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let again = store
        .update_job(
            &job.id,
            CronJobPatch {
                enabled: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(again.next_run, disabled.next_run);
}

#[tokio::test]
async fn the_agent_floor_applies_when_an_agent_schedule_changes_only() {
    let store = store();
    let agent = store
        .add_agent_job_from_spec(AgentJobSpec::new(daily(), "p"))
        .await
        .unwrap();
    let tight = CronJobPatch {
        schedule: Some(every(1)),
        ..Default::default()
    };
    assert!(store.update_job(&agent.id, tight.clone()).await.is_err());
    let shell = store.add_job("0 9 * * *", "x").await.unwrap();
    assert!(store.update_job(&shell.id, tight).await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_patches_never_lose_a_field() {
    let store = store();
    let job = store.add_job("0 9 * * *", "x").await.unwrap();
    let handles: Vec<_> = (0..8)
        .map(|i| {
            let (store, id) = (store.clone(), job.id.clone());
            tokio::spawn(async move {
                let patch = if i % 2 == 0 {
                    CronJobPatch {
                        name: Some(format!("n{i}")),
                        ..Default::default()
                    }
                } else {
                    CronJobPatch {
                        model: Some(format!("m{i}")),
                        ..Default::default()
                    }
                };
                store.update_job(&id, patch).await.unwrap();
            })
        })
        .collect();
    for handle in handles {
        handle.await.unwrap();
    }
    let read = store.get_job(&job.id).await.unwrap();
    assert!(read.name.is_some() && read.model.is_some());
}

#[tokio::test]
async fn a_flow_jobs_command_cannot_be_retargeted() {
    let store = store();
    let job = store
        .add_flow_schedule_job("flow-a", daily())
        .await
        .unwrap();
    let patch = CronJobPatch {
        command: Some("flow-b".into()),
        ..CronJobPatch::default()
    };
    let error = store.update_job(&job.id, patch).await.unwrap_err();
    assert!(error.to_string().contains("cannot be changed"), "{error}");
    assert_eq!(store.get_job(&job.id).await.unwrap().command, "flow-a");
    // The same command, or any other field, still patches.
    let same = CronJobPatch {
        command: Some("flow-a".into()),
        enabled: Some(false),
        ..CronJobPatch::default()
    };
    assert!(!store.update_job(&job.id, same).await.unwrap().enabled);
    assert!(
        store
            .find_flow_schedule_job("flow-b")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn due_jobs_order_within_a_millisecond_and_read_legacy_documents() {
    let store = store();
    let first = store.add_job("0 9 * * *", "first").await.unwrap();
    let second = store.add_job("0 9 * * *", "second").await.unwrap();
    let legacy = store.add_job("0 9 * * *", "legacy").await.unwrap();
    let base = Utc::now() - Duration::minutes(1);
    // `second` is due 1µs before `first`, within the same millisecond.
    let ms_floor =
        base - Duration::nanoseconds(i64::from(base.timestamp_subsec_nanos() % 1_000_000));
    set_due_at(&store, &first.id, ms_floor + Duration::microseconds(2)).await;
    set_due_at(&store, &second.id, ms_floor + Duration::microseconds(1)).await;
    // A document written before `next_run_ns` existed is still due.
    set_due_at(&store, &legacy.id, ms_floor - Duration::seconds(1)).await;
    let stored = store.docs.get(JOBS, &legacy.id).await.unwrap().unwrap();
    let mut doc = stored.doc.clone();
    doc.as_object_mut().unwrap().remove("next_run_ns");
    store
        .docs
        .put(JOBS, &legacy.id, doc, stored.unchanged())
        .await
        .unwrap();

    let due: Vec<String> = store
        .due_jobs(Utc::now())
        .await
        .unwrap()
        .into_iter()
        .map(|job| job.command)
        .collect();
    assert_eq!(due, ["legacy", "second", "first"]);
    let capped = store
        .clone()
        .with_limits(50, 2)
        .due_jobs(Utc::now())
        .await
        .unwrap();
    assert_eq!(capped.len(), 2);
}

async fn set_due_at(store: &CronDocuments, id: &str, at: DateTime<Utc>) {
    let stored = store.docs.get(JOBS, id).await.unwrap().unwrap();
    let mut doc = stored.doc.as_object().cloned().unwrap();
    set_next_run(&mut doc, at);
    store
        .docs
        .put(JOBS, id, Value::Object(doc), stored.unchanged())
        .await
        .unwrap();
}

#[tokio::test]
async fn clearing_spares_a_job_registered_again_after_the_snapshot() {
    let store = store();
    let first = store.add_flow_schedule_job("f", daily()).await.unwrap();
    let snapshot = store.docs.get(JOBS, &first.id).await.unwrap().unwrap();
    // The flow is disabled and re-enabled before the clear reaches it.
    store.remove_job(&first.id).await.unwrap();
    let again = store.add_flow_schedule_job("f", daily()).await.unwrap();
    assert_eq!(again.id, first.id, "the same deterministic id");

    assert!(!store.remove_current(&snapshot).await.unwrap());
    assert!(store.find_flow_schedule_job("f").await.unwrap().is_some());
}

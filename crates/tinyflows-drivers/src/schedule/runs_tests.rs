use super::*;

use super::super::codec::incarnation;
use super::super::test_support::{daily, store};
use chrono::Duration;
use tinyflows_schedule::{MAX_CRON_OUTPUT_BYTES, TRUNCATED_OUTPUT_MARKER};

async fn run(store: &CronDocuments, job_id: &str, status: &str, secs_ago: i64) {
    let at = Utc::now() - Duration::seconds(secs_ago);
    store
        .record_run(job_id, at, at, status, Some(status), 5)
        .await
        .unwrap();
}

#[tokio::test]
async fn runs_are_listed_newest_first_and_pruned_to_the_cap() {
    let store = store().with_limits(3, 64);
    let job = store.add_job("0 9 * * *", "x").await.unwrap();
    for (i, status) in ["a", "b", "c", "d", "e"].into_iter().enumerate() {
        run(&store, &job.id, status, 100 - i as i64).await;
    }
    let runs = store.list_runs(&job.id, 10).await.unwrap();
    let statuses: Vec<&str> = runs.iter().map(|r| r.status.as_str()).collect();
    assert_eq!(statuses, ["e", "d", "c"]);
    assert!(runs[0].id > runs[1].id, "run numbers increase");
    assert_eq!(runs[0].duration_ms, Some(5));
    assert_eq!(store.list_runs(&job.id, 0).await.unwrap().len(), 1, "min 1");
}

#[tokio::test]
async fn removing_a_job_removes_its_runs() {
    let store = store();
    let job = store.add_job("0 9 * * *", "x").await.unwrap();
    let other = store.add_job("0 9 * * *", "y").await.unwrap();
    run(&store, &job.id, "ok", 1).await;
    run(&store, &other.id, "ok", 1).await;
    store.remove_job(&job.id).await.unwrap();
    assert!(store.list_runs(&job.id, 10).await.unwrap().is_empty());
    assert_eq!(store.list_runs(&other.id, 10).await.unwrap().len(), 1);
    store.clear_all_jobs().await.unwrap();
    assert!(store.list_runs(&other.id, 10).await.unwrap().is_empty());
}

#[tokio::test]
async fn queued_placeholders_are_removed() {
    let store = store();
    let job = store.add_job("0 9 * * *", "x").await.unwrap();
    run(&store, &job.id, "queued", 3).await;
    run(&store, &job.id, "queued", 2).await;
    run(&store, &job.id, "ok", 1).await;
    assert_eq!(store.delete_queued_runs(&job.id).await.unwrap(), 2);
    let runs = store.list_runs(&job.id, 10).await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].status, "ok");
}

#[tokio::test]
async fn delivery_status_is_stored_and_read_back() {
    let store = store();
    let job = store.add_job("0 9 * * *", "x").await.unwrap();
    let now = Utc::now();
    store
        .record_run_with_delivery(
            &job.id,
            now,
            now,
            "ok",
            None,
            1,
            Some(DeliveryStatus::Delivered),
        )
        .await
        .unwrap();
    store
        .record_run(&job.id, now, now, "ok", None, 1)
        .await
        .unwrap();
    let runs = store.list_runs(&job.id, 10).await.unwrap();
    assert_eq!(runs[0].delivery_status, None);
    assert_eq!(runs[0].output, None);
    assert_eq!(runs[1].delivery_status, Some(DeliveryStatus::Delivered));
}

#[tokio::test]
async fn an_unknown_delivery_status_reads_as_none() {
    let store = store();
    let job = store.add_job("0 9 * * *", "x").await.unwrap();
    let stored = store.docs.get(JOBS, &job.id).await.unwrap().unwrap();
    let mut doc = run_to_doc(
        1,
        &job.id,
        incarnation(&stored.doc),
        Utc::now(),
        Utc::now(),
        "ok",
        None,
        1,
        None,
    );
    doc["delivery_status"] = json!("teleported");
    store.ensure().await.unwrap();
    store
        .docs
        .put(RUNS, &run_id(1), doc, Precondition::Absent)
        .await
        .unwrap();
    assert_eq!(
        store.list_runs(&job.id, 10).await.unwrap()[0].delivery_status,
        None
    );
}

#[tokio::test]
async fn long_output_is_truncated_on_runs_and_on_the_job() {
    let store = store();
    let job = store.add_job("0 9 * * *", "x").await.unwrap();
    let long = "x".repeat(MAX_CRON_OUTPUT_BYTES + 100);
    let now = Utc::now();
    store
        .record_run(&job.id, now, now, "ok", Some(&long), 1)
        .await
        .unwrap();
    let output = store.list_runs(&job.id, 1).await.unwrap()[0]
        .output
        .clone()
        .unwrap();
    assert!(output.len() <= MAX_CRON_OUTPUT_BYTES && output.ends_with(TRUNCATED_OUTPUT_MARKER));
    store.reschedule_after_run(&job, true, &long).await.unwrap();
    let last = store.get_job(&job.id).await.unwrap().last_output.unwrap();
    assert!(last.ends_with(TRUNCATED_OUTPUT_MARKER));
}

#[tokio::test]
async fn record_last_run_leaves_the_schedule_alone() {
    let store = store();
    let job = store.add_job("0 9 * * *", "x").await.unwrap();
    let at = Utc::now();
    store
        .record_last_run(&job.id, at, false, "boom")
        .await
        .unwrap();
    store
        .record_last_run("missing", at, true, "")
        .await
        .unwrap();
    let read = store.get_job(&job.id).await.unwrap();
    assert_eq!(read.last_status.as_deref(), Some("error"));
    assert_eq!(read.last_output.as_deref(), Some("boom"));
    assert_eq!(read.last_run, Some(at));
    assert_eq!(read.next_run, job.next_run);
}

#[tokio::test]
async fn reschedule_records_the_outcome_and_advances_once() {
    let store = store();
    let job = store
        .add_shell_job(None, Schedule::Every { every_ms: 60_000 }, "x")
        .await
        .unwrap();
    // Make the job due now, as the scheduler would see it.
    let due_at = Utc::now() - Duration::seconds(5);
    let fired = store
        .update_job(&job.id, tinyflows_schedule::CronJobPatch::default())
        .await
        .unwrap();
    let mut fired = fired;
    set_due(&store, &fired.id, due_at).await;
    fired.next_run = store.get_job(&job.id).await.unwrap().next_run;

    store
        .reschedule_after_run(&fired, true, "done")
        .await
        .unwrap();
    let after = store.get_job(&job.id).await.unwrap();
    assert!(after.next_run > Utc::now());
    assert_eq!(after.last_status.as_deref(), Some("ok"));

    // A second reschedule of the same fired occurrence only records.
    store
        .reschedule_after_run(&fired, false, "again")
        .await
        .unwrap();
    let twice = store.get_job(&job.id).await.unwrap();
    assert_eq!(twice.next_run, after.next_run);
    assert_eq!(twice.last_status.as_deref(), Some("error"));
}

async fn set_due(store: &CronDocuments, id: &str, at: DateTime<Utc>) {
    let stored = store.docs.get(JOBS, id).await.unwrap().unwrap();
    let mut doc = stored.doc.as_object().cloned().unwrap();
    super::super::codec::set_next_run(&mut doc, at);
    store
        .docs
        .put(JOBS, id, Value::Object(doc), stored.unchanged())
        .await
        .unwrap();
}

#[tokio::test]
async fn a_one_shot_job_is_disabled_after_its_run() {
    let store = store();
    let at = Utc::now() + Duration::hours(1);
    let job = store
        .add_shell_job(None, Schedule::At { at }, "once")
        .await
        .unwrap();
    store.reschedule_after_run(&job, true, "").await.unwrap();
    let read = store.get_job(&job.id).await.unwrap();
    assert!(!read.enabled);
    assert!(
        store
            .due_jobs(at + Duration::minutes(1))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_recording_keeps_every_run_and_advances_once() {
    let store = store();
    let job = store
        .add_shell_job(None, Schedule::Every { every_ms: 60_000 }, "x")
        .await
        .unwrap();
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let (store, job) = (store.clone(), job.clone());
            tokio::spawn(async move {
                let now = Utc::now();
                store
                    .record_run(&job.id, now, now, "ok", None, 1)
                    .await
                    .unwrap();
                store.reschedule_after_run(&job, true, "").await.unwrap();
            })
        })
        .collect();
    for handle in handles {
        handle.await.unwrap();
    }
    let runs = store.list_runs(&job.id, 100).await.unwrap();
    let mut ids: Vec<i64> = runs.iter().map(|r| r.id).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 8, "every run kept, each with its own number");
    let advanced = store.get_job(&job.id).await.unwrap().next_run;
    assert_ne!(
        advanced, job.next_run,
        "advanced exactly from the fired run"
    );
}

#[tokio::test]
async fn dedup_keeps_the_job_with_the_most_history() {
    let store = store();
    assert_eq!(store.dedup_named_jobs().await.unwrap(), 0, "empty store");
    let quiet = store
        .add_shell_job(Some("brief".into()), daily(), "a")
        .await
        .unwrap();
    let busy = store
        .add_shell_job(Some("brief".into()), daily(), "b")
        .await
        .unwrap();
    store
        .add_shell_job(Some("other".into()), daily(), "c")
        .await
        .unwrap();
    store.add_shell_job(None, daily(), "d").await.unwrap();
    store.add_shell_job(None, daily(), "e").await.unwrap();
    run(&store, &busy.id, "ok", 2).await;
    run(&store, &quiet.id, "ok", 1).await;
    run(&store, &busy.id, "ok", 1).await;
    assert_eq!(store.dedup_named_jobs().await.unwrap(), 1);
    assert!(store.get_job(&quiet.id).await.is_err());
    assert!(store.list_runs(&quiet.id, 10).await.unwrap().is_empty());
    assert!(store.get_job(&busy.id).await.is_ok());
    assert_eq!(
        store.list_jobs().await.unwrap().len(),
        4,
        "unnamed jobs untouched"
    );
    assert_eq!(store.dedup_named_jobs().await.unwrap(), 0, "idempotent");
}

#[tokio::test]
async fn dedup_ties_go_to_the_earliest_created() {
    let store = store();
    let first = store
        .add_shell_job(Some("dup".into()), daily(), "a")
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    store
        .add_shell_job(Some("dup".into()), daily(), "b")
        .await
        .unwrap();
    assert_eq!(store.dedup_named_jobs().await.unwrap(), 1);
    let left = store.list_jobs().await.unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].id, first.id);
}

#[tokio::test]
async fn a_run_for_a_missing_job_is_refused() {
    let store = store();
    let now = Utc::now();
    let error = store
        .record_run("gone", now, now, "ok", None, 1)
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "Cron job 'gone' not found");
    assert!(store.list_runs("gone", 10).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_reused_flow_job_id_does_not_inherit_old_runs() {
    let store = store();
    let first = store.add_flow_schedule_job("f", daily()).await.unwrap();
    let now = Utc::now();
    store
        .record_run(&first.id, now, now, "ok", None, 1)
        .await
        .unwrap();
    // A run written for the first incarnation that lands after the job was
    // removed and re-created, as a late write would.
    let old = store.docs.get(JOBS, &first.id).await.unwrap().unwrap();
    store.remove_job(&first.id).await.unwrap();
    let again = store.add_flow_schedule_job("f", daily()).await.unwrap();
    assert_eq!(again.id, first.id, "the flow's job id is reused");
    store
        .docs
        .put(
            RUNS,
            &run_id(999),
            run_to_doc(
                999,
                &first.id,
                incarnation(&old.doc),
                now,
                now,
                "late",
                None,
                1,
                None,
            ),
            Precondition::Absent,
        )
        .await
        .unwrap();
    assert!(store.list_runs(&again.id, 10).await.unwrap().is_empty());
}

#[tokio::test]
async fn an_edited_schedule_with_the_same_next_run_is_not_advanced() {
    let store = store();
    let job = store.add_job("0 9 * * *", "x").await.unwrap();
    // Same next occurrence, different schedule.
    let edited = store
        .update_job(
            &job.id,
            tinyflows_schedule::CronJobPatch {
                schedule: Some(Schedule::Cron {
                    expr: "0 9 */1 * *".into(),
                    tz: None,
                    active_hours: None,
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(edited.next_run, job.next_run);
    store.reschedule_after_run(&job, true, "").await.unwrap();
    let read = store.get_job(&job.id).await.unwrap();
    assert_eq!(
        read.next_run, edited.next_run,
        "not advanced from the stale schedule"
    );
    assert_eq!(
        read.last_status.as_deref(),
        Some("ok"),
        "outcome still recorded"
    );
}

#[tokio::test]
async fn runs_started_within_a_millisecond_order_by_their_real_start() {
    let store = store().with_limits(1, 64);
    let job = store.add_job("0 9 * * *", "x").await.unwrap();
    let earlier: DateTime<Utc> = "2026-01-01T00:00:00.000100Z".parse().unwrap();
    let later: DateTime<Utc> = "2026-01-01T00:00:00.000900Z".parse().unwrap();
    // The later start is recorded first, as a run finishing sooner would be.
    store
        .record_run(&job.id, later, later, "later", None, 1)
        .await
        .unwrap();
    store
        .record_run(&job.id, earlier, earlier, "earlier", None, 1)
        .await
        .unwrap();
    let runs = store.list_runs(&job.id, 10).await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].status, "later", "pruning keeps the latest start");
}

#[tokio::test]
async fn removing_a_stale_incarnation_keeps_the_new_jobs_runs() {
    let store = store();
    let first = store.add_flow_schedule_job("f", daily()).await.unwrap();
    let stale = store.docs.get(JOBS, &first.id).await.unwrap().unwrap();
    store.remove_job(&first.id).await.unwrap();
    let again = store.add_flow_schedule_job("f", daily()).await.unwrap();
    let now = Utc::now();
    store
        .record_run(&again.id, now, now, "ok", None, 1)
        .await
        .unwrap();
    // A remover still holding the old incarnation cannot take the new job
    // or its runs with it.
    assert!(!store.remove_stored(&stale).await.unwrap());
    assert!(store.get_job(&again.id).await.is_ok());
    assert_eq!(store.list_runs(&again.id, 10).await.unwrap().len(), 1);
}

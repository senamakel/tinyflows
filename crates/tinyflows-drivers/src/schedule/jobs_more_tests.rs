use super::*;

use super::super::test_support::{daily, store};
use chrono::Duration;

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

#[tokio::test]
async fn dedup_keeps_a_duplicate_edited_after_the_snapshot() {
    let store = store();
    let job = store.add_job("0 9 * * *", "x").await.unwrap();
    let snapshot = store.docs.get(JOBS, &job.id).await.unwrap().unwrap();
    let rename = CronJobPatch {
        name: Some("renamed".into()),
        ..CronJobPatch::default()
    };
    store.update_job(&job.id, rename).await.unwrap();
    assert!(!store.remove_unchanged(&snapshot).await.unwrap());
    assert!(store.get_job(&job.id).await.is_ok());
    let current = store.docs.get(JOBS, &job.id).await.unwrap().unwrap();
    assert!(store.remove_unchanged(&current).await.unwrap());
}

#[tokio::test]
async fn a_far_future_job_is_not_due_past_the_nanosecond_range() {
    let store = store();
    let job = store.add_job("0 9 * * *", "far").await.unwrap();
    let year = |y: &str| {
        DateTime::parse_from_rfc3339(&format!("{y}-01-01T00:00:00Z"))
            .unwrap()
            .with_timezone(&Utc)
    };
    set_due_at(&store, &job.id, year("2300")).await;
    assert!(store.due_jobs(year("2263")).await.unwrap().is_empty());
    assert_eq!(store.due_jobs(year("2301")).await.unwrap().len(), 1);
}

use super::*;

use super::super::test_support::{daily, store};
use chrono::Duration;

async fn run(store: &CronDocuments, job_id: &str) {
    let now = Utc::now();
    store
        .record_run(job_id, now - Duration::seconds(1), now, "ok", None, 1)
        .await
        .unwrap();
}

/// Writes a run directly, as a crash between a job's removal and its runs'
/// removal would leave it.
async fn orphan(store: &CronDocuments, seq: i64, job_id: &str, incarnation: Option<&str>) {
    let now = Utc::now();
    let doc = run_to_doc(seq, job_id, incarnation, now, now, "ok", None, 1, None);
    store
        .docs
        .put(RUNS, &run_id(seq), doc, Precondition::Absent)
        .await
        .unwrap();
}

#[tokio::test]
async fn the_sweep_removes_runs_of_gone_or_replaced_jobs_only() {
    let store = store();
    let live = store.add_job("0 9 * * *", "live").await.unwrap();
    run(&store, &live.id).await;
    let flow = store.add_flow_schedule_job("f", daily()).await.unwrap();
    run(&store, &flow.id).await;
    orphan(&store, 9_001, "gone-job", Some("old")).await;
    // A run of the flow job's earlier incarnation.
    orphan(&store, 9_002, &flow.id, Some("earlier-incarnation")).await;

    assert_eq!(store.sweep_orphan_runs().await.unwrap(), 2);
    assert_eq!(store.list_runs(&live.id, 10).await.unwrap().len(), 1);
    assert_eq!(store.list_runs(&flow.id, 10).await.unwrap().len(), 1);
    assert_eq!(store.sweep_orphan_runs().await.unwrap(), 0, "idempotent");
}

#[tokio::test]
async fn clearing_removes_each_jobs_runs_and_sweeps_orphans() {
    let store = store();
    let a = store.add_job("0 9 * * *", "a").await.unwrap();
    let b = store.add_job("0 9 * * *", "b").await.unwrap();
    run(&store, &a.id).await;
    run(&store, &b.id).await;
    orphan(&store, 9_003, "gone-job", None).await;

    assert_eq!(store.clear_all_jobs().await.unwrap(), 2);
    let left = store.docs.query_all(RUNS, &Query::all()).await.unwrap();
    assert!(left.is_empty(), "{left:?}");
    assert!(store.list_jobs().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_malformed_run_counter_is_an_error() {
    let store = store();
    let job = store.add_job("0 9 * * *", "x").await.unwrap();
    store.ensure().await.unwrap();
    store
        .docs
        .put(
            COUNTERS,
            "runs",
            json!({ "next": "seven" }),
            Precondition::None,
        )
        .await
        .unwrap();
    let error = store
        .record_run(&job.id, Utc::now(), Utc::now(), "ok", None, 1)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("next"), "{error}");
}

#[tokio::test]
async fn removing_a_legacy_job_spares_a_replacements_runs() {
    let store = store();
    let job = store.add_job("0 9 * * *", "x").await.unwrap();
    // The job as a document written before incarnations existed.
    let legacy = store.docs.get(JOBS, &job.id).await.unwrap().unwrap();
    let mut doc = legacy.doc.clone();
    doc.as_object_mut()
        .unwrap()
        .remove(super::super::codec::INCARNATION);
    let version = store
        .docs
        .put(JOBS, &job.id, doc.clone(), legacy.unchanged())
        .await
        .unwrap();
    let legacy = tinystoragedrivers_core::Versioned {
        id: job.id.clone(),
        version,
        doc,
    };
    orphan(&store, 9_101, &job.id, None).await;
    // A replacement's run, recorded under the same id with an incarnation.
    orphan(&store, 9_102, &job.id, Some("replacement")).await;

    assert!(store.remove_stored(&legacy).await.unwrap());
    let left: Vec<String> = store
        .docs
        .query_all(RUNS, &Query::all())
        .await
        .unwrap()
        .into_iter()
        .map(|run| run.id)
        .collect();
    assert_eq!(left, [run_id(9_102)]);
}

#[tokio::test]
async fn the_sweep_keeps_a_run_with_no_job_id() {
    let store = store();
    store.ensure().await.unwrap();
    store
        .docs
        .put(
            RUNS,
            &run_id(9_201),
            json!({ "seq": 9_201, "status": "ok" }),
            Precondition::Absent,
        )
        .await
        .unwrap();
    assert_eq!(store.sweep_orphan_runs().await.unwrap(), 0);
    assert!(
        store
            .docs
            .get(RUNS, &run_id(9_201))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn reschedule_tells_apart_two_occurrences_past_2262() {
    let store = store();
    let job = store.add_job("0 9 * * *", "far").await.unwrap();
    let at = |y: &str| {
        DateTime::parse_from_rfc3339(&format!("{y}-01-01T09:00:00Z"))
            .unwrap()
            .with_timezone(&Utc)
    };
    // Both saturate to the same nanosecond value; only milliseconds differ.
    let (fired, moved) = (at("2300"), at("2301"));
    let stored = store.docs.get(JOBS, &job.id).await.unwrap().unwrap();
    let mut doc = stored.doc.as_object().cloned().unwrap();
    super::super::codec::set_next_run(&mut doc, moved);
    store
        .docs
        .put(JOBS, &job.id, Value::Object(doc), stored.unchanged())
        .await
        .unwrap();
    let mut seen = store.get_job(&job.id).await.unwrap();
    seen.next_run = fired;

    store.reschedule_after_run(&seen, true, "ok").await.unwrap();
    let after = store.get_job(&job.id).await.unwrap();
    assert_eq!(after.next_run, moved, "another occurrence: not advanced");
    assert_eq!(after.last_status.as_deref(), Some("ok"));
}

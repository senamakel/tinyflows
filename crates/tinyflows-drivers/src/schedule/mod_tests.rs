use super::*;

use super::test_support::{daily, docs_in, store};
use chrono::Utc;
use serde_json::json;
use tinystoragedrivers_core::{MemoryStorage, Precondition};

#[test]
fn limits_default_like_the_sqlite_store_and_clamp_to_one() {
    let store = store();
    assert_eq!((store.max_run_history, store.max_tasks), (50, 64));
    let clamped = store.with_limits(0, 0);
    assert_eq!((clamped.max_run_history, clamped.max_tasks), (1, 1));
}

#[tokio::test]
async fn ensure_is_idempotent_and_declares_every_collection() {
    let store = store();
    store.ensure().await.unwrap();
    store.ensure().await.unwrap();
    let names: Vec<String> = CronDocuments::collections()
        .into_iter()
        .map(|spec| spec.name)
        .collect();
    assert_eq!(names, [JOBS, RUNS, COUNTERS]);
}

#[tokio::test]
async fn scopes_keep_jobs_and_runs_apart() {
    let storage = MemoryStorage::new();
    let alice = CronDocuments::new(docs_in(&storage, "alice"));
    let bob = CronDocuments::new(docs_in(&storage, "bob"));
    let job = alice.add_shell_job(None, daily(), "echo a").await.unwrap();
    let now = Utc::now();
    alice
        .record_run(&job.id, now, now, "ok", None, 1)
        .await
        .unwrap();
    assert!(bob.list_jobs().await.unwrap().is_empty());
    assert!(bob.get_job(&job.id).await.is_err());
    assert!(bob.list_runs(&job.id, 10).await.unwrap().is_empty());
    assert!(bob.remove_job(&job.id).await.is_err());
    assert_eq!(alice.list_jobs().await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_corrupt_job_document_is_an_error_naming_the_field() {
    let store = store();
    store.ensure().await.unwrap();
    store
        .docs
        .put(
            JOBS,
            "bad",
            json!({ "job_type": "shell" }),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let error = store.get_job("bad").await.unwrap_err().to_string();
    assert!(error.contains("schedule"), "{error}");
    assert!(store.list_jobs().await.is_err());
}

#[tokio::test]
async fn compare_and_swap_skips_missing_documents_and_declined_changes() {
    let store = store();
    store.ensure().await.unwrap();
    assert!(
        compare_and_swap(&store.docs, JOBS, "missing", |_| Ok(Some(json!({}))))
            .await
            .unwrap()
            .is_none()
    );
    store
        .docs
        .put(JOBS, "j", json!({ "n": 1 }), Precondition::Absent)
        .await
        .unwrap();
    assert!(
        compare_and_swap(&store.docs, JOBS, "j", |_| Ok(None))
            .await
            .unwrap()
            .is_none()
    );
    let error = compare_and_swap(&store.docs, JOBS, "j", |_| anyhow::bail!("refused"))
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "refused");
}

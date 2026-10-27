use super::callbacks;
use chrono::{Duration, Utc};
use serde_json::{Value, json};
use std::sync::Arc;
use tinybus::Proxy;
use tinyflows_bus::schedule::{CallbackAddress, StartScheduler, StoreConfig};
use tinyflows_drivers::schedule::CronDocuments;
use tinyflows_schedule::Schedule;
use tinyflows_storage::DocumentStore;

/// Recovers an expired persisted lease through the loaded cdylib, then proves
/// the former owner cannot renew, release or complete the recovered run.
pub async fn verify(
    proxy: &Proxy,
    docs: Arc<dyn DocumentStore>,
    storage: Value,
    callback: CallbackAddress,
    state: Arc<callbacks::State>,
) {
    let store = CronDocuments::new(docs.clone());
    let job = store
        .add_shell_job(
            None,
            Schedule::Every {
                every_ms: 24 * 60 * 60 * 1_000,
            },
            "echo recover",
        )
        .await
        .unwrap();
    let due_at = Utc::now() - Duration::hours(4);
    let stored = docs.get("cron.jobs", &job.id).await.unwrap().unwrap();
    let mut body = stored.doc.clone();
    body["next_run_ms"] = json!(due_at.timestamp_millis());
    docs.put("cron.jobs", &job.id, body, stored.unchanged())
        .await
        .unwrap();
    let job = store.get_job(&job.id).await.unwrap();
    // Acquired three hours ago for two hours: the lease expired an hour ago.
    let acquired_at = Utc::now() - Duration::hours(3);
    let old_owner = store
        .claim_due_job(&job.id, "pre-crash-owner", acquired_at, Duration::hours(2))
        .await
        .unwrap()
        .unwrap();

    let storage: StoreConfig = serde_json::from_value(storage).unwrap();
    let request = StartScheduler {
        scope: "artifact-recovery-scope".into(),
        callback,
        config: json!({}),
        manual_ticks: true,
        storage: Some(storage),
    };
    let executions_before = state.executions.lock().unwrap().len();
    let handle: String = proxy.call("StartScheduler", (request,)).await.unwrap();
    assert_eq!(
        proxy
            .call::<usize>("TickScheduler", (&handle,))
            .await
            .unwrap(),
        1
    );
    proxy
        .call::<()>("DrainScheduler", (&handle,))
        .await
        .unwrap();
    proxy.call::<()>("StopScheduler", (&handle,)).await.unwrap();

    let now = Utc::now();
    assert!(
        !store
            .renew_claim(&old_owner, now, Duration::hours(2))
            .await
            .unwrap()
    );
    assert!(!store.release_claim(&old_owner, now).await.unwrap());
    assert!(
        !store
            .complete_claim(&old_owner, now, true, "stale result", None)
            .await
            .unwrap()
    );
    let history = store.list_runs(&job.id, 10).await.unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].status, "ok");
    assert_eq!(
        state.executions.lock().unwrap().len(),
        executions_before + 1
    );
    let projection = state.projections.lock().unwrap();
    assert_eq!(projection.last().unwrap()["success"], true);
    assert_eq!(
        projection.last().unwrap()["attempt"]["raw_detail"],
        "opaque callback classification"
    );
}

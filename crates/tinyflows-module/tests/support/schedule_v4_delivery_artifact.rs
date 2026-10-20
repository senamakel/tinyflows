use super::callbacks;
use chrono::{Duration, Utc};
use serde_json::{Value, json};
use std::sync::Arc;
use tinybus::Proxy;
use tinyflows_bus::schedule::{
    CallbackAddress, DeliveryConfig, StartScheduler, StoreConfig, delivery_mode,
};
use tinyflows_drivers::schedule::CronDocuments;
use tinyflows_schedule::{CronJobPatch, DeliveryStatus, Schedule};
use tinyflows_storage::DocumentStore;

/// Verifies one actual send and truthful terminal projection through the cdylib.
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
            "echo delivery",
        )
        .await
        .unwrap();
    let job = store
        .update_job(
            &job.id,
            CronJobPatch {
                delivery: Some(DeliveryConfig {
                    mode: delivery_mode::ANNOUNCE.into(),
                    channel: Some("test-channel".into()),
                    to: Some("test-recipient".into()),
                    best_effort: false,
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let due_at = Utc::now() - Duration::minutes(1);
    let stored = docs.get("cron.jobs", &job.id).await.unwrap().unwrap();
    let mut body = stored.doc.clone();
    body["next_run_ms"] = json!(due_at.timestamp_millis());
    docs.put("cron.jobs", &job.id, body, stored.unchanged())
        .await
        .unwrap();

    state
        .fail_send
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let sends_before = state.sends.load(std::sync::atomic::Ordering::SeqCst);
    let projections_before = state.projections.lock().unwrap().len();
    let storage: StoreConfig = serde_json::from_value(storage).unwrap();
    let request = StartScheduler {
        scope: "artifact-delivery-truth-scope".into(),
        callback,
        config: json!({}),
        manual_ticks: true,
        storage: Some(storage),
    };
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

    assert_eq!(
        state.sends.load(std::sync::atomic::Ordering::SeqCst),
        sends_before + 1,
        "the terminal projection must not send a second time"
    );
    {
        let projections = state.projections.lock().unwrap();
        let terminal = &projections[projections_before];
        assert_eq!(terminal["success"], false);
        assert_eq!(terminal["delivery_status"], "failed");
        assert_eq!(terminal["attempt"]["success"], true);
        assert_eq!(
            terminal["attempt"]["raw_detail"],
            "opaque callback classification"
        );
    }
    let after = store.get_job(&job.id).await.unwrap();
    assert_eq!(after.last_status.as_deref(), Some("error"));
    let history = store.list_runs(&job.id, 10).await.unwrap();
    assert_eq!(history[0].status, "error");
    assert_eq!(history[0].delivery_status, Some(DeliveryStatus::Failed));
}

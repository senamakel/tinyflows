use super::callbacks;
use chrono::{Duration, Utc};
use serde_json::Value;
use std::sync::Arc;
use tinybus::Proxy;
use tinyflows_bus::schedule::{
    CallbackAddress, ManualDelivery, RunJobNow, RunJobRefusal, RunJobReply, StoreConfig,
};
use tinyflows_drivers::schedule::CronDocuments;
use tinyflows_schedule::{CronJobPatch, Schedule};
use tinyflows_storage::DocumentStore;

/// Exercises module-owned manual queueing and lease refusal through the loaded cdylib.
pub async fn verify(
    proxy: &Proxy,
    docs: Arc<dyn DocumentStore>,
    storage: Value,
    callback: CallbackAddress,
    state: Arc<callbacks::State>,
) {
    let store = CronDocuments::new(docs);
    let now = Utc::now();
    let saved = store
        .add_shell_job(
            Some("artifact-manual".into()),
            Schedule::At {
                at: now + Duration::hours(1),
            },
            "echo manual",
        )
        .await
        .unwrap();
    let saved = store
        .update_job(
            &saved.id,
            CronJobPatch {
                enabled: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let storage: StoreConfig = serde_json::from_value(storage.clone()).unwrap();
    let reply: RunJobReply = proxy
        .call(
            "RunJobNow",
            &(RunJobNow {
                scope: "artifact-manual-scope".into(),
                callback: callback.clone(),
                config: serde_json::json!({}),
                storage: storage.clone(),
                job: saved.clone(),
                run_id: "artifact-manual-run".into(),
                delivery: ManualDelivery::NotRequested,
                wait: true,
            },),
        )
        .await
        .unwrap();
    let RunJobReply::Completed {
        run_id,
        success,
        delivery_status,
        ..
    } = reply
    else {
        panic!("loaded module must finish a blocking manual run")
    };
    assert_eq!(run_id, "artifact-manual-run");
    assert!(success);
    assert_eq!(delivery_status, None);
    let after = store.get_job(&saved.id).await.unwrap();
    assert!(!after.enabled);
    assert_eq!(after.next_run, saved.next_run);
    assert_eq!(after.delete_after_run, saved.delete_after_run);
    let history = store.list_runs(&saved.id, 10).await.unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].status, "ok");
    assert_eq!(history[0].delivery_status, None);
    assert!(
        state
            .executions
            .lock()
            .unwrap()
            .contains(&"artifact-manual-run".to_owned())
    );
    assert_eq!(
        state.projections.lock().unwrap().last().unwrap()["attempt"]["raw_detail"],
        "opaque callback classification"
    );

    let held_job = store
        .add_shell_job(
            None,
            Schedule::At {
                at: Utc::now() + Duration::hours(1),
            },
            "echo held",
        )
        .await
        .unwrap();
    let held = store
        .claim_job_now(&held_job, "scheduled-owner", Utc::now(), Duration::hours(2))
        .await
        .unwrap()
        .unwrap();
    let refusal: RunJobReply = proxy
        .call(
            "RunJobNow",
            &(RunJobNow {
                scope: "artifact-manual-scope".into(),
                callback: callback.clone(),
                config: serde_json::json!({}),
                storage: storage.clone(),
                job: held_job.clone(),
                run_id: "artifact-refused-run".into(),
                delivery: ManualDelivery::NotRequested,
                wait: true,
            },),
        )
        .await
        .unwrap();
    assert!(matches!(
        refusal,
        RunJobReply::Refused {
            reason: RunJobRefusal::AlreadyRunning
        }
    ));
    assert!(store.list_runs(&held_job.id, 10).await.unwrap().is_empty());
    assert!(store.release_claim(&held, Utc::now()).await.unwrap());

    let detached_job = store
        .add_shell_job(
            None,
            Schedule::At {
                at: Utc::now() + Duration::hours(1),
            },
            "echo detached",
        )
        .await
        .unwrap();
    let projection_count = state.projections.lock().unwrap().len();
    state
        .defer_execute
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let accepted: RunJobReply = proxy
        .call(
            "RunJobNow",
            &(RunJobNow {
                scope: "artifact-manual-scope".into(),
                callback: callback.clone(),
                config: serde_json::json!({}),
                storage: storage.clone(),
                job: detached_job.clone(),
                run_id: "artifact-detached-run".into(),
                delivery: ManualDelivery::NotRequested,
                wait: false,
            },),
        )
        .await
        .unwrap();
    assert!(matches!(
        accepted,
        RunJobReply::Accepted { ref run_id } if run_id == "artifact-detached-run"
    ));
    let queued = store.list_runs(&detached_job.id, 10).await.unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!(
        queued[0].status, "queued",
        "queue is visible before RPC ack"
    );
    state.execute_started.notified().await;
    let duplicate: RunJobReply = proxy
        .call(
            "RunJobNow",
            &(RunJobNow {
                scope: "artifact-manual-scope".into(),
                callback,
                config: serde_json::json!({}),
                storage,
                job: detached_job.clone(),
                run_id: "artifact-detached-duplicate".into(),
                delivery: ManualDelivery::NotRequested,
                wait: true,
            },),
        )
        .await
        .unwrap();
    assert!(matches!(
        duplicate,
        RunJobReply::Refused {
            reason: RunJobRefusal::AlreadyRunning
        }
    ));
    assert_eq!(
        store.list_runs(&detached_job.id, 10).await.unwrap().len(),
        1
    );
    state.execute_release.notify_one();
    let mut projected = false;
    for _ in 0..1_000 {
        if state.projections.lock().unwrap().len() > projection_count {
            projected = true;
            break;
        }
        let projection = state.terminal_projection.notified();
        if state.projections.lock().unwrap().len() > projection_count {
            projected = true;
            break;
        }
        projection.await;
    }
    assert!(
        projected,
        "detached module worker reached terminal projection"
    );
    let mut finished = false;
    for _ in 0..1_000 {
        let history = store.list_runs(&detached_job.id, 10).await.unwrap();
        if history.first().is_some_and(|run| run.status == "ok") {
            finished = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        finished,
        "queued placeholder is replaced by terminal history"
    );
    state
        .defer_execute
        .store(false, std::sync::atomic::Ordering::SeqCst);
}

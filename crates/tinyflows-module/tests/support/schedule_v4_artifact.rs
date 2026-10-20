use super::{
    callbacks, schedule_v4_delivery_artifact, schedule_v4_manual_artifact,
    schedule_v4_recovery_artifact,
};
use serde_json::Value;
use std::sync::Arc;
use tinybus::Proxy;
use tinyflows_bus::schedule::CallbackAddress;
use tinyflows_storage::DocumentStore;

pub fn verify_captured_triggers(state: &callbacks::State, stored_job_id: &str) {
    let jobs = state.trigger_jobs.lock().unwrap();
    assert_eq!(jobs[0]["id"], "artifact-flow-job");
    assert_eq!(jobs[0]["command"], "artifact-flow");
    assert_eq!(jobs[1]["id"], stored_job_id);
    assert_eq!(jobs[1]["command"], "stored-flow");
}

pub async fn verify(
    proxy: &Proxy,
    docs: Arc<dyn DocumentStore>,
    storage: Value,
    state: Arc<callbacks::State>,
) {
    let callback = CallbackAddress {
        destination: "test.ScheduleCallbacks".into(),
        object_path: "/test/ScheduleCallbacks".into(),
    };
    schedule_v4_manual_artifact::verify(
        proxy,
        docs.clone(),
        storage.clone(),
        callback.clone(),
        state.clone(),
    )
    .await;
    schedule_v4_recovery_artifact::verify(
        proxy,
        docs.clone(),
        storage.clone(),
        callback.clone(),
        state.clone(),
    )
    .await;
    schedule_v4_delivery_artifact::verify(proxy, docs, storage, callback, state).await;
}

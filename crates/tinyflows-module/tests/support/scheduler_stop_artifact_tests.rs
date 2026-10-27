use super::callbacks;
use chrono::Duration;
use std::sync::Arc;
use tinybus::{
    Connection, Error, broker::Broker, module::ModuleHost, transport::memory::MemoryTransport,
};
use tinyflows_bus::schedule::{CallbackAddress, StartScheduler};
use tinyflows_drivers::schedule::CronDocuments;
use tinyflows_schedule::Schedule;
use tinyflows_storage::{Scope, StorageBackend};
use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;

#[tokio::test]
#[ignore = "run with TINYFLOWS_MODULE_ARTIFACT set to a compiled cdylib"]
/// Verify timed-out and concurrent Stop calls through the compiled TinyBus module.
async fn compiled_module_stop_scheduler_retries_after_tinybus_timeout() {
    let artifact = std::env::var_os("TINYFLOWS_MODULE_ARTIFACT")
        .expect("set TINYFLOWS_MODULE_ARTIFACT to a compiled cdylib");
    let broker = Broker::new();
    let host = ModuleHost::new(broker.clone());
    host.load_file(std::path::PathBuf::from(artifact)).unwrap();
    let (client, transport) = MemoryTransport::pair();
    broker.attach(Arc::new(transport));
    let connection = Connection::connect(Box::new(client)).await.unwrap();

    let callback_state = Arc::new(callbacks::State::default());
    callback_state
        .defer_execute
        .store(true, std::sync::atomic::Ordering::SeqCst);
    connection
        .serve_at(
            "/test/ScheduleCallbacks".try_into().unwrap(),
            callbacks::Callbacks(callback_state.clone()),
        )
        .await
        .unwrap();
    connection
        .request_name("test.ScheduleCallbacks")
        .await
        .unwrap();

    let storage = MemoryStorage::new()
        .for_scope(&Scope::new("artifact-stop-scope").unwrap())
        .unwrap();
    let documents = CronDocuments::new(storage.documents().clone());
    let job = documents
        .add_shell_job(
            Some("artifact-stop-job".into()),
            Schedule::Every { every_ms: 60_000 },
            "echo stop",
        )
        .await
        .unwrap();
    *callback_state.due_job.lock().unwrap() = Some(serde_json::to_value(&job).unwrap());
    let scheduler_now = job.next_run + Duration::seconds(1);
    let schedule = connection
        .proxy(
            tinyflows_bus::schedule::BUS_NAME,
            tinyflows_bus::schedule::OBJECT_PATH,
            tinyflows_bus::schedule::INTERFACE,
        )
        .unwrap();
    let callback = CallbackAddress {
        destination: "test.ScheduleCallbacks".into(),
        object_path: "/test/ScheduleCallbacks".into(),
    };
    let scope = "artifact-stop-scope".to_string();
    let request = StartScheduler {
        scope: scope.clone(),
        callback: callback.clone(),
        config: serde_json::json!({"now": scheduler_now}),
        manual_ticks: true,
        storage: None,
    };
    let handle: String = schedule
        .call("StartScheduler", &(request.clone(),))
        .await
        .unwrap();
    let ticked: usize = schedule.call("TickScheduler", &(&handle,)).await.unwrap();
    assert_eq!(ticked, 1);
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        callback_state.execute_started.notified(),
    )
    .await
    .expect("held Execute callback must start within the test failure cap");

    let first = schedule
        .clone()
        .with_timeout(std::time::Duration::from_millis(10))
        .call::<()>("StopScheduler", &(&handle,))
        .await;
    assert!(matches!(first, Err(Error::Timeout { .. })), "{first:?}");
    let stop_args = (&handle,);
    let retry_proxy = schedule
        .clone()
        .with_timeout(std::time::Duration::from_millis(10));
    let duplicate_proxy = schedule
        .clone()
        .with_timeout(std::time::Duration::from_millis(10));
    let retry = retry_proxy.call::<()>("StopScheduler", &stop_args);
    let duplicate = duplicate_proxy.call::<()>("StopScheduler", &stop_args);
    let (retry, duplicate) = tokio::join!(retry, duplicate);
    assert!(matches!(retry, Err(Error::Timeout { .. })), "{retry:?}");
    assert!(
        matches!(duplicate, Err(Error::Timeout { .. })),
        "{duplicate:?}"
    );

    callback_state.execute_release.notify_one();
    let released_args = (&handle,);
    let retry_after_release_proxy = schedule.clone();
    let duplicate_after_release_proxy = schedule.clone();
    let retry_after_release = retry_after_release_proxy.call::<()>("StopScheduler", &released_args);
    let duplicate_after_release =
        duplicate_after_release_proxy.call::<()>("StopScheduler", &released_args);
    let (retry_after_release, duplicate_after_release) =
        tokio::join!(retry_after_release, duplicate_after_release);
    retry_after_release.unwrap();
    duplicate_after_release.unwrap();
    assert!(callback_state.leases.lock().unwrap().is_empty());
    assert_eq!(callback_state.executions.lock().unwrap().len(), 1);
    schedule
        .call::<()>("StopScheduler", &(&handle,))
        .await
        .unwrap();
    schedule
        .call::<()>("StopScheduler", &("never-created",))
        .await
        .unwrap();

    let restarted: String = schedule.call("StartScheduler", &(request,)).await.unwrap();
    schedule
        .call::<()>("StopScheduler", &(&restarted,))
        .await
        .unwrap();
}

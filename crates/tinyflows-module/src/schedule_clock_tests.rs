use super::*;

// Manual passes use one supplied calendar instant across dispatch and records.
use chrono::{Duration, Utc};
use serde_json::json;

#[tokio::test(start_paused = true)]
async fn retry_sleep_uses_tokio_time_without_changing_the_calendar_instant() {
    let now = Utc::now();
    let clock = clock::HostClock::fixed(now);
    let began = tokio::time::Instant::now();
    clock.sleep(std::time::Duration::from_secs(10)).await;
    assert_eq!(began.elapsed(), std::time::Duration::from_secs(10));
    assert_eq!(clock.now(), now);
}

#[tokio::test]
async fn a_manual_pass_uses_the_supplied_time_for_due_jobs_and_run_records() {
    use tinyflows_bus::schedule::{Schedule, StoreConfig};
    use tinyflows_storage::{ Scope, StorageBackend};

    let (_server, host, _state) = tests::connections().await;
    let proxy = host
        .proxy(
            schedule::BUS_NAME,
            schedule::OBJECT_PATH,
            schedule::INTERFACE,
        )
        .unwrap();
    // Stored instants have millisecond precision.
    let now = chrono::DateTime::from_timestamp_millis(
        (Utc::now() + Duration::days(1)).timestamp_millis(),
    )
    .unwrap();
    let scoped = MemoryStorage::new()
        .for_scope(&Scope::new("fixed-clock").unwrap())
        .unwrap();
    let documents = tinyflows_drivers::schedule::CronDocuments::new(scoped.documents().clone());
    let mut once = documents
        .add_shell_job(None, Schedule::At { at: now }, "echo once")
        .await
        .unwrap();
    once.next_run = now;
    documents.remove_job(&once.id).await.unwrap();
    documents.import_job(&once).await.unwrap();
    let mut repeating = documents
        .add_shell_job(None, Schedule::Every { every_ms: 60_000 }, "echo repeat")
        .await
        .unwrap();
    repeating.next_run = now;
    documents.remove_job(&repeating.id).await.unwrap();
    documents.import_job(&repeating).await.unwrap();
    host.serve_at(
        "/test/Documents".try_into().unwrap(),
        tests::document_callbacks::Documents {
            docs: scoped.documents().clone(),
            conflict_once: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            atomic_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        },
    )
    .await
    .unwrap();
    let storage: StoreConfig = serde_json::from_value(json!({
        "backend": {"kind":"documents", "callback": {
            "destination":"test.ScheduleHost", "object_path":"/test/Documents"
        }}
    }))
    .unwrap();

    // The exact same future jobs are not due before the supplied instant.
    for (at, expected) in [(now - Duration::seconds(1), 0), (now, 2)] {
        let request = json!({
            "scope": "fixed-clock", "callback": tests::callback(),
            "manual_ticks": true, "storage": storage, "config": {"now": at},
        });
        let handle: String = proxy
            .call(schedule::members::START_SCHEDULER, &(request,))
            .await
            .unwrap();
        let count: usize = proxy
            .call(schedule::members::TICK_SCHEDULER, &(&handle,))
            .await
            .unwrap();
        assert_eq!(count, expected);
        proxy
            .call::<()>(schedule::members::DRAIN_SCHEDULER, &(&handle,))
            .await
            .unwrap();
        proxy
            .call::<()>(schedule::members::STOP_SCHEDULER, &(&handle,))
            .await
            .unwrap();
    }
    for job in [&once, &repeating] {
        let history = documents.list_runs(&job.id, 10).await.unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].started_at, now);
        assert_eq!(history[0].finished_at, now);
    }
    let completed_repeat = documents.get_job(&repeating.id).await.unwrap();
    assert_eq!(completed_repeat.next_run, now + Duration::seconds(60));
    assert!(!documents.get_job(&once.id).await.unwrap().enabled);
}

#[tokio::test]
async fn a_fixed_calendar_time_is_refused_for_an_automatic_loop() {
    let (_server, host, _) = tests::connections().await;
    let proxy = host
        .proxy(
            schedule::BUS_NAME,
            schedule::OBJECT_PATH,
            schedule::INTERFACE,
        )
        .unwrap();
    let request = json!({
        "scope": "fixed-loop", "callback": tests::callback(),
        "config": {"now": Utc::now()},
    });
    let error = proxy
        .call::<String>(schedule::members::START_SCHEDULER, &(request,))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("manual ticks"), "{error}");
}

#[tokio::test]
async fn an_invalid_calendar_time_is_refused() {
    let (_server, host, _) = tests::connections().await;
    let proxy = host
        .proxy(
            schedule::BUS_NAME,
            schedule::OBJECT_PATH,
            schedule::INTERFACE,
        )
        .unwrap();
    for now in [json!("not a date"), json!(42), Value::Null] {
        let request = json!({
            "scope": "invalid-clock", "callback": tests::callback(),
            "manual_ticks": true, "config": {"now": now},
        });
        assert!(
            proxy
                .call::<String>(schedule::members::START_SCHEDULER, &(request,))
                .await
                .is_err()
        );
    }
}

#[test]
fn a_host_clock_reads_system_time_until_the_host_sets_an_instant() {
    let clock = clock::HostClock::system();
    let before = Utc::now();
    let read = clock.now();
    assert!(read >= before && read <= Utc::now());
    let later = Utc::now() + Duration::days(30);
    clock.set(later);
    assert_eq!(clock.now(), later);
}

#[tokio::test]
async fn ticking_at_a_later_instant_runs_a_future_job_without_waiting_for_it() {
    use tinyflows_bus::schedule::{Schedule, StoreConfig};
    use tinyflows_storage::{ Scope, StorageBackend};

    let (_server, host, _state) = tests::connections().await;
    let proxy = host
        .proxy(
            schedule::BUS_NAME,
            schedule::OBJECT_PATH,
            schedule::INTERFACE,
        )
        .unwrap();
    // An hour from now, at the millisecond precision instants are stored at.
    let at = chrono::DateTime::from_timestamp_millis(
        (Utc::now() + Duration::hours(1)).timestamp_millis(),
    )
    .unwrap();
    let scoped = MemoryStorage::new()
        .for_scope(&Scope::new("stepped-clock").unwrap())
        .unwrap();
    let documents = tinyflows_drivers::schedule::CronDocuments::new(scoped.documents().clone());
    let job = documents
        .add_shell_job(None, Schedule::At { at }, "echo later")
        .await
        .unwrap();
    host.serve_at(
        "/test/Documents".try_into().unwrap(),
        tests::document_callbacks::Documents {
            docs: scoped.documents().clone(),
            conflict_once: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            atomic_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        },
    )
    .await
    .unwrap();
    let storage: StoreConfig = serde_json::from_value(json!({
        "backend": {"kind":"documents", "callback": {
            "destination":"test.ScheduleHost", "object_path":"/test/Documents"
        }}
    }))
    .unwrap();
    // No `config.now`: the scheduler starts on system time.
    let request = json!({
        "scope": "stepped-clock", "callback": tests::callback(),
        "manual_ticks": true, "storage": storage, "config": {},
    });
    let handle: String = proxy
        .call(schedule::members::START_SCHEDULER, &(request,))
        .await
        .unwrap();

    let on_system_time: usize = proxy
        .call(schedule::members::TICK_SCHEDULER, &(&handle,))
        .await
        .unwrap();
    assert_eq!(on_system_time, 0, "the job is an hour away on system time");
    let stepped: usize = proxy
        .call(schedule::members::TICK_SCHEDULER_AT, &(&handle, at))
        .await
        .unwrap();
    assert_eq!(
        stepped, 1,
        "stepping the clock to the job's instant runs it"
    );
    proxy
        .call::<()>(schedule::members::DRAIN_SCHEDULER, &(&handle,))
        .await
        .unwrap();
    proxy
        .call::<()>(schedule::members::STOP_SCHEDULER, &(&handle,))
        .await
        .unwrap();

    let history = documents.list_runs(&job.id, 10).await.unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(
        history[0].started_at, at,
        "the run is recorded at the host's instant"
    );
    assert!(!documents.get_job(&job.id).await.unwrap().enabled);
}

#[tokio::test]
async fn ticking_at_an_instant_is_refused_for_an_automatic_loop() {
    let (_server, host, _) = tests::connections().await;
    let proxy = host
        .proxy(
            schedule::BUS_NAME,
            schedule::OBJECT_PATH,
            schedule::INTERFACE,
        )
        .unwrap();
    let request = json!({
        "scope": "stepped-loop", "callback": tests::callback(), "config": {},
    });
    let handle: String = proxy
        .call(schedule::members::START_SCHEDULER, &(request,))
        .await
        .unwrap();
    let error = proxy
        .call::<usize>(schedule::members::TICK_SCHEDULER_AT, &(&handle, Utc::now()))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("poll loop"), "{error}");
    proxy
        .call::<()>(schedule::members::STOP_SCHEDULER, &(&handle,))
        .await
        .unwrap();
}

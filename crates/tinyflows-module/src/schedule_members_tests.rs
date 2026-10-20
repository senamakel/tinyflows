// Every contract-v3 pure member, called through the bus like a host would.
use super::tests::{connections, document_callbacks};
use super::*;
use serde_json::json;
use tinyflows_bus::schedule::{self, CronJob, members};

async fn proxy() -> (tinybus::Proxy, Connection, Connection) {
    let (server, host, _) = connections().await;
    let proxy = host
        .proxy(
            schedule::BUS_NAME,
            schedule::OBJECT_PATH,
            schedule::INTERFACE,
        )
        .unwrap();
    (proxy, host, server)
}

#[tokio::test]
async fn schedule_maths_members_match_the_in_process_functions() {
    let (proxy, _host, _server) = proxy().await;
    let every = json!({"kind":"every","every_ms":600000});
    let next: chrono::DateTime<chrono::Utc> = proxy
        .call(
            members::NEXT_RUN,
            &(NextRunRequest {
                schedule: serde_json::from_value(every.clone()).unwrap(),
                from: "2026-01-01T00:00:00Z".parse().unwrap(),
            },),
        )
        .await
        .unwrap();
    assert_eq!(next.to_rfc3339(), "2026-01-01T00:10:00+00:00");

    for at in ["2026-01-01T00:00:00Z", "2025-12-31T00:00:00Z"] {
        let elapsed = proxy
            .call::<chrono::DateTime<chrono::Utc>>(
                members::NEXT_RUN,
                &(json!({"schedule":{"kind":"at","at":at},"from":"2026-01-01T00:00:00Z"}),),
            )
            .await;
        assert!(elapsed.is_err(), "{at} is not after from");
    }
    let future: chrono::DateTime<chrono::Utc> = proxy
        .call(
            members::NEXT_RUN,
            &(json!({"schedule":{"kind":"at","at":"2026-01-02T00:00:00Z"},"from":"2026-01-01T00:00:00Z"}),),
        )
        .await
        .unwrap();
    assert_eq!(future.to_rfc3339(), "2026-01-02T00:00:00+00:00");

    let bad: std::result::Result<chrono::DateTime<chrono::Utc>, _> = proxy
        .call(
            members::NEXT_RUN,
            &(json!({"schedule":{"kind":"cron","expr":"not a cron"},"from":"2026-01-01T00:00:00Z"}),),
        )
        .await;
    assert!(bad.is_err());

    let normalized: String = proxy
        .call(members::NORMALIZE_EXPRESSION, &("0 9 * * 1",))
        .await
        .unwrap();
    assert_eq!(
        normalized,
        maths::normalize_expression("0 9 * * 1").unwrap()
    );
    assert!(
        proxy
            .call::<String>(members::NORMALIZE_EXPRESSION, &("nope",))
            .await
            .is_err()
    );

    let expr: Option<String> = proxy
        .call(
            members::SCHEDULE_CRON_EXPRESSION,
            &(json!({"kind":"cron","expr":"0 9 * * *"}),),
        )
        .await
        .unwrap();
    assert_eq!(expr.as_deref(), Some("0 9 * * *"));
    let none: Option<String> = proxy
        .call(members::SCHEDULE_CRON_EXPRESSION, &(every,))
        .await
        .unwrap();
    assert_eq!(none, None);
}

#[tokio::test]
async fn validation_members_enforce_the_cadence_floor_only_for_agents() {
    let (proxy, _host, _server) = proxy().await;
    let request = |agent: bool| json!({"schedule":{"kind":"every","every_ms":60000},"now":"2026-01-01T00:00:00Z","agent":agent});
    proxy
        .call::<()>(members::VALIDATE_SCHEDULE, &(request(false),))
        .await
        .unwrap();
    let error = proxy
        .call::<()>(members::VALIDATE_SCHEDULE, &(request(true),))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("minute"), "{error}");

    let closest: Option<TooFrequent> = proxy
        .call(
            members::RUNS_CLOSER_THAN,
            &(json!({"schedule":{"kind":"cron","expr":"*/7 * * * *"},"from":"2026-01-01T00:00:00Z","min_gap_ms":300000}),),
        )
        .await
        .unwrap();
    assert_eq!(closest.unwrap().gap(), chrono::Duration::minutes(4));
    let fixed: Option<TooFrequent> = proxy
        .call(
            members::RUNS_CLOSER_THAN,
            &(json!({"schedule":{"kind":"every","every_ms":1000},"from":"2026-01-01T00:00:00Z","min_gap_ms":300000}),),
        )
        .await
        .unwrap();
    assert_eq!(
        fixed,
        Some(TooFrequent::FixedInterval(chrono::Duration::seconds(1)))
    );
    let fine: Option<TooFrequent> = proxy
        .call(
            members::RUNS_CLOSER_THAN,
            &(json!({"schedule":{"kind":"every","every_ms":3600000},"from":"2026-01-01T00:00:00Z","min_gap_ms":300000}),),
        )
        .await
        .unwrap();
    assert_eq!(fine, None);
}

#[tokio::test]
async fn delivery_and_output_members_apply_host_independent_policy() {
    let (proxy, _host, _server) = proxy().await;
    let origin = json!({"kind":"web","thread_id":"t1"});
    let with_origin: DeliveryConfig = proxy
        .call(members::DEFAULT_DELIVERY, &(Some(origin.clone()),))
        .await
        .unwrap();
    assert_eq!(with_origin.mode, "origin");
    let without: DeliveryConfig = proxy
        .call(members::DEFAULT_DELIVERY, &(None::<Value>,))
        .await
        .unwrap();
    assert_eq!(without.mode, "proactive");

    let needs_origin = json!({"session_target":"current","delivery":{"mode":"none"}});
    assert!(
        proxy
            .call::<()>(members::CHECK_ORIGIN_REQUIREMENTS, &(needs_origin,))
            .await
            .is_err()
    );
    proxy
        .call::<()>(
            members::CHECK_ORIGIN_REQUIREMENTS,
            &(json!({"session_target":"current","delivery":{"mode":"origin"},"origin":origin}),),
        )
        .await
        .unwrap();

    let long = "x".repeat(schedule::MAX_CRON_OUTPUT_BYTES + 10);
    let cut: String = proxy
        .call(members::TRUNCATE_CRON_OUTPUT, &(long,))
        .await
        .unwrap();
    assert_eq!(cut.len(), schedule::MAX_CRON_OUTPUT_BYTES);
    assert!(cut.ends_with(schedule::TRUNCATED_OUTPUT_MARKER));
    let short: String = proxy
        .call(members::TRUNCATE_CRON_OUTPUT, &("ok",))
        .await
        .unwrap();
    assert_eq!(short, "ok");
}

#[tokio::test]
async fn check_patch_refuses_retargeting_a_flow_schedule_job() {
    let (proxy, _host, _server) = proxy().await;
    let job = json!({"id":"flow:f","expression":"","schedule":{"kind":"every","every_ms":60000},"command":"f","prompt":null,"name":null,"job_type":"flow","session_target":"isolated","model":null,"agent_id":null,"enabled":true,"delivery":{"mode":"none"},"delete_after_run":false,"created_at":"2026-01-01T00:00:00Z","next_run":"2026-01-01T00:00:00Z","last_run":null,"last_status":null,"last_output":null});
    proxy
        .call::<()>(
            members::CHECK_PATCH,
            &(json!({"job":job,"patch":{"command":"f","enabled":false}}),),
        )
        .await
        .unwrap();
    let error = proxy
        .call::<()>(
            members::CHECK_PATCH,
            &(json!({"job":job,"patch":{"command":"other"}}),),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("cannot be changed"), "{error}");
}

#[tokio::test]
async fn store_admission_cap_and_import_operations_are_served() {
    let (proxy, host, _server) = proxy().await;
    host.serve_at(
        "/test/Documents".try_into().unwrap(),
        document_callbacks::Documents {
            docs: {
                use tinyflows_storage::StorageBackend;
                tinystoragedrivers_testkit::MemoryBackend::new()
                    .for_scope(&tinyflows_storage::Scope::new("local").expect("valid scope"))
                    .unwrap()
                    .documents()
                    .clone()
            },
            conflict_once: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            atomic_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        },
    )
    .await
    .unwrap();
    let storage = json!({"backend":{"kind":"documents","callback":{"destination":"test.ScheduleHost","object_path":"/test/Documents"}}});
    let call = |operation: schedule::StoreOperation| json!({"storage":storage,"operation":serde_json::to_value(operation).unwrap()});
    let job: CronJob = serde_json::from_value(json!({"id":"imp-1","expression":"* * * * *","schedule":{"kind":"every","every_ms":3600000},"command":"echo hi","prompt":null,"name":"imported","job_type":"shell","session_target":"isolated","model":null,"agent_id":null,"enabled":true,"delivery":{"mode":"none"},"delete_after_run":false,"created_at":"2026-01-01T00:00:00Z","next_run":"2026-01-01T01:00:00Z","last_run":null,"last_status":null,"last_output":null})).unwrap();
    let imported: schedule::ImportedJob = serde_json::from_value(
        proxy
            .call::<Value>(
                members::SCHEDULE_STORE,
                &(call(schedule::StoreOperation::ImportJob {
                    job: job.clone(),
                }),),
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        imported,
        schedule::ImportedJob {
            id: "imp-1".into(),
            written: true
        }
    );
    let again: schedule::ImportedJob = serde_json::from_value(
        proxy
            .call::<Value>(
                members::SCHEDULE_STORE,
                &(call(schedule::StoreOperation::ImportJob { job }),),
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(!again.written);

    let run: schedule::CronRun = serde_json::from_value(json!({"id":1,"job_id":"imp-1","started_at":"2026-01-01T00:00:00Z","finished_at":"2026-01-01T00:00:02Z","status":"ok","output":"hi","duration_ms":2000})).unwrap();
    let written: bool = serde_json::from_value(
        proxy
            .call::<Value>(
                members::SCHEDULE_STORE,
                &(call(schedule::StoreOperation::ImportRun { run }),),
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(written);

    // Admission cap: the memory backend supports transactions, so a cap of
    // one is accepted and the existing job counts against it.
    let _: Value = proxy
        .call(
            members::SCHEDULE_STORE,
            &(call(schedule::StoreOperation::SetMaxActiveJobs {
                limit: Some(1),
            }),),
        )
        .await
        .unwrap();
    let refused = proxy
        .call::<Value>(
            members::SCHEDULE_STORE,
            &(call(schedule::StoreOperation::AddShell {
                name: None,
                schedule: serde_json::from_value(json!({"kind":"every","every_ms":3600000}))
                    .unwrap(),
                command: "echo two".into(),
            }),),
        )
        .await;
    assert!(refused.is_err(), "second enabled job must exceed the cap");
    let _: Value = proxy
        .call(
            members::SCHEDULE_STORE,
            &(call(schedule::StoreOperation::SetMaxActiveJobs {
                limit: None,
            }),),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn the_served_surface_is_exactly_the_advertised_member_set() {
    let (proxy, _host, _server) = proxy().await;
    // Every advertised member must at least be dispatched (not "unknown member").
    let unknown = proxy.call::<Value>("Bogus", &(json!({}),)).await;
    assert!(unknown.unwrap_err().to_string().contains("UnknownMethod"));
    for name in schedule::METHODS {
        let result = proxy.call::<Value>(name, &(json!({}),)).await;
        if let Err(error) = result {
            assert!(
                !error.to_string().contains("UnknownMethod"),
                "{name}: {error}"
            );
        }
    }
}

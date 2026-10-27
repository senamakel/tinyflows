use super::*;

#[test]
fn shared_catalog_records_are_key_fenced_and_never_scanned() {
    assert!(
        Documents::validate_record("tsd.migrations", "flows/flows.definitions/definitions_v2")
            .is_ok()
    );
    assert!(Documents::validate_record("tsd.migrations", "cron/cron.jobs/jobs_v2").is_err());
    assert!(Documents::validate_record("tsd.migrations", "flows/other/step").is_err());
    assert!(Documents::validate_record("tsd.kv", "flows/kv:state/key").is_ok());
    assert!(Documents::validate_record("tsd.kv", "cron/state/key").is_err());
    assert!(Documents::validate_record("tsd.leases", "cron/job/occurrence").is_err());
    assert!(Documents::validate_set("tsd.migrations").is_err());
    assert!(Documents::validate_set("tsd.kv").is_err());
    assert!(Documents::validate_set("tsd.leases").is_err());
    assert!(Documents::validate_collection("flows_revisions").is_ok());
    assert!(Documents::validate_collection("unrelated").is_err());
}

#[tokio::test]
async fn catalog_document_bridge_rejects_host_only_creation_metadata() {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let documents = Documents::new(
        connection,
        CallbackAddress {
            destination: "missing.Host".into(),
            object_path: "/missing/Host".into(),
        },
    )
    .await
    .unwrap();

    let error = documents
        .put_with(
            "flows.revisions",
            "revision",
            serde_json::json!({"created_ms": 1}),
            Precondition::Absent,
            WriteOptions::new().created_ms(1),
        )
        .await
        .expect_err("module ports must not forward host-only creation metadata");
    assert_eq!(error.kind(), tinyflows_storage::ErrorKind::InvalidInput);
}

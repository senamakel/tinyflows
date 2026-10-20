use serde_json::{Value, json};
use std::sync::{Arc, atomic::AtomicBool};
use tinybus::{Connection, broker::Broker, module::ModuleHost, transport::memory::MemoryTransport};
use tinyflows_bus::catalog::{
    CallbackAddress, CatalogStorage, ListRevisionsRequest, ListWorkflowsResult, Workflow,
    WorkflowRevision,
};
use tinyflows_drivers::schedule::CronDocuments;
use tinyflows_storage::{
    CollectionSpec, DocumentStore, Precondition, Scope, StorageBackend, WriteOptions,
};
use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;

pub(super) struct CatalogCallback(pub(super) Arc<dyn DocumentStore>);

fn catalog_reply<T: serde::Serialize>(result: tinyflows_storage::Result<T>) -> Value {
    match result {
        Ok(value) => json!({"value":serde_json::to_value(value).unwrap()}),
        Err(error) => {
            json!({"error":{"kind":error.kind().to_string().replace(' ', "_"),"message":error.to_string()}})
        }
    }
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CatalogStorage")]
impl CatalogCallback {
    async fn doc_capabilities(&self) -> tinybus::Result<Value> {
        Ok(json!({"value":{"record_metadata":true}}))
    }

    async fn doc_ensure(&self, request: Value) -> tinybus::Result<Value> {
        Ok(catalog_reply(
            self.0
                .ensure_collection(&serde_json::from_value(request["spec"].clone()).unwrap())
                .await,
        ))
    }

    async fn doc_get(&self, request: Value) -> tinybus::Result<Value> {
        Ok(catalog_reply(
            self.0
                .get(
                    request["collection"].as_str().unwrap(),
                    request["id"].as_str().unwrap(),
                )
                .await,
        ))
    }

    async fn doc_put(&self, request: Value) -> tinybus::Result<Value> {
        if request["created_ms"].as_i64().is_some() {
            return Ok(catalog_reply(Err::<(), _>(
                tinyflows_storage::StorageError::invalid_input(
                    "a module cannot set created_ms: it is a host-only migration option",
                ),
            )));
        }
        let mut options = WriteOptions::new();
        if let Some(schema) = request["schema"].as_u64() {
            options = options.schema(u32::try_from(schema).unwrap_or(0));
        }
        Ok(catalog_reply(
            self.0
                .put_with(
                    request["collection"].as_str().unwrap(),
                    request["id"].as_str().unwrap(),
                    request["doc"].clone(),
                    serde_json::from_value(request["precondition"].clone()).unwrap(),
                    options,
                )
                .await,
        ))
    }

    async fn doc_delete(&self, request: Value) -> tinybus::Result<Value> {
        Ok(catalog_reply(
            self.0
                .delete(
                    request["collection"].as_str().unwrap(),
                    request["id"].as_str().unwrap(),
                    serde_json::from_value(request["precondition"].clone()).unwrap(),
                )
                .await,
        ))
    }

    async fn doc_query(&self, request: Value) -> tinybus::Result<Value> {
        Ok(catalog_reply(
            self.0
                .query(
                    request["collection"].as_str().unwrap(),
                    &serde_json::from_value(request["query"].clone()).unwrap(),
                )
                .await,
        ))
    }

    async fn doc_delete_where(&self, request: Value) -> tinybus::Result<Value> {
        Ok(catalog_reply(
            self.0
                .delete_where(
                    request["collection"].as_str().unwrap(),
                    &serde_json::from_value(request["filter"].clone()).unwrap(),
                )
                .await,
        ))
    }
}

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn published_module_migrates_legacy_schedule_records_through_owned_bus_call() {
    let artifact = std::env::var_os("TINYFLOWS_MODULE_ARTIFACT")
        .expect("set TINYFLOWS_MODULE_ARTIFACT to a compiled cdylib");
    let broker = Broker::new();
    let host = ModuleHost::new(broker.clone());
    host.load_file(std::path::PathBuf::from(artifact)).unwrap();
    let (client, transport) = MemoryTransport::pair();
    broker.attach(Arc::new(transport));
    let connection = Connection::connect(Box::new(client)).await.unwrap();

    let scoped = MemoryStorage::new()
        .for_scope(&Scope::new("local").expect("valid scope"))
        .unwrap();
    let docs = scoped.documents().clone();
    docs.ensure_collection(&tinyflows_storage::CollectionSpec::new("cron_jobs"))
        .await
        .unwrap();
    let legacy: Value =
        serde_json::from_str(include_str!("../fixtures/storage/cron-job-v0.8.8.json")).unwrap();
    let legacy_id = legacy["id"].as_str().unwrap();
    docs.put(
        "cron_jobs",
        legacy_id,
        legacy["doc"].clone(),
        Precondition::None,
    )
    .await
    .unwrap();

    connection
        .serve_at(
            "/test/ScheduleDocuments".try_into().unwrap(),
            super::document_callbacks::Documents {
                docs: docs.clone(),
                conflict_once: Arc::new(AtomicBool::new(false)),
                atomic_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            },
        )
        .await
        .unwrap();
    connection
        .request_name("test.ScheduleDocuments")
        .await
        .unwrap();
    let schedule = connection
        .proxy(
            tinyflows_bus::schedule::BUS_NAME,
            tinyflows_bus::schedule::OBJECT_PATH,
            tinyflows_bus::schedule::INTERFACE,
        )
        .unwrap();
    let request = json!({
        "storage": {
            "backend": {"kind":"documents", "callback": {
                "destination":"test.ScheduleDocuments",
                "object_path":"/test/ScheduleDocuments"
            }}
        },
        "operation": {"op":"migrate"}
    });
    let schedule_args_a = (request.clone(),);
    let schedule_args_b = (request.clone(),);
    let (first, concurrent) = tokio::join!(
        schedule.call::<Value>("ScheduleStore", &schedule_args_a),
        schedule.call::<Value>("ScheduleStore", &schedule_args_b),
    );
    first.unwrap();
    concurrent.unwrap();
    let _: Value = schedule.call("ScheduleStore", &(request,)).await.unwrap();

    let migrated = docs.get("cron.jobs", legacy_id).await.unwrap().unwrap();
    let legacy_created_ms =
        chrono::DateTime::parse_from_rfc3339(legacy["doc"]["created_at"].as_str().unwrap())
            .unwrap()
            .timestamp_millis();
    assert_eq!(migrated.meta.created_ms, legacy_created_ms);
    let jobs = CronDocuments::new(docs).list_jobs().await.unwrap();
    assert_eq!(jobs.len(), 1, "the legacy job is visible after migration");
    assert_eq!(jobs[0].id, legacy_id);
    assert_eq!(jobs[0].command, "legacy-fixture-flow");
}

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn published_module_reads_previous_release_schedule_records_on_open() {
    let artifact = std::env::var_os("TINYFLOWS_MODULE_ARTIFACT")
        .expect("set TINYFLOWS_MODULE_ARTIFACT to a compiled cdylib");
    let broker = Broker::new();
    let host = ModuleHost::new(broker.clone());
    host.load_file(std::path::PathBuf::from(artifact)).unwrap();
    let (client, transport) = MemoryTransport::pair();
    broker.attach(Arc::new(transport));
    let connection = Connection::connect(Box::new(client)).await.unwrap();

    let scoped = MemoryStorage::new()
        .for_scope(&Scope::new("local").expect("valid scope"))
        .unwrap();
    let docs = scoped.documents().clone();
    docs.ensure_collection(&tinyflows_storage::CollectionSpec::new("cron.jobs"))
        .await
        .unwrap();
    let previous: Value =
        serde_json::from_str(include_str!("../fixtures/storage/cron-job-v0.8.28.json")).unwrap();
    let previous_meta = &previous["meta"];
    let created_ms = previous_meta["created_ms"].as_i64().unwrap();
    let schema = u32::try_from(previous_meta["schema"].as_u64().unwrap()).unwrap();
    let previous_id = previous["id"].as_str().unwrap();
    let original = docs
        .put_with(
            "cron.jobs",
            previous_id,
            previous["doc"].clone(),
            Precondition::None,
            WriteOptions::new().created_ms(created_ms).schema(schema),
        )
        .await
        .unwrap();

    connection
        .serve_at(
            "/test/ScheduleDocuments".try_into().unwrap(),
            super::document_callbacks::Documents {
                docs: docs.clone(),
                conflict_once: Arc::new(AtomicBool::new(false)),
                atomic_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            },
        )
        .await
        .unwrap();
    connection
        .request_name("test.ScheduleDocuments")
        .await
        .unwrap();
    let schedule = connection
        .proxy(
            tinyflows_bus::schedule::BUS_NAME,
            tinyflows_bus::schedule::OBJECT_PATH,
            tinyflows_bus::schedule::INTERFACE,
        )
        .unwrap();
    let request = json!({
        "storage": {
            "backend": {"kind":"documents", "callback": {
                "destination":"test.ScheduleDocuments",
                "object_path":"/test/ScheduleDocuments"
            }}
        },
        "operation": {"op":"migrate"}
    });
    let _: Value = schedule.call("ScheduleStore", &(request,)).await.unwrap();

    let reopened = docs
        .get("cron.jobs", previous_id)
        .await
        .unwrap()
        .expect("the previous release's schedule remains readable");
    assert_eq!(reopened.doc, previous["doc"]);
    assert_eq!(reopened.version, original);
    assert_eq!(reopened.meta.created_ms, created_ms);
    let jobs = CronDocuments::new(docs).list_jobs().await.unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].id, previous_id);
    assert_eq!(jobs[0].command, "base-fixture-flow");
}

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn published_module_migrates_legacy_catalog_records_through_owned_bus_call() {
    let artifact = std::env::var_os("TINYFLOWS_MODULE_ARTIFACT")
        .expect("set TINYFLOWS_MODULE_ARTIFACT to a compiled cdylib");
    let broker = Broker::new();
    let host = ModuleHost::new(broker.clone());
    host.load_file(std::path::PathBuf::from(artifact)).unwrap();
    let (client, transport) = MemoryTransport::pair();
    broker.attach(Arc::new(transport));
    let connection = Connection::connect(Box::new(client)).await.unwrap();
    let scoped = MemoryStorage::new()
        .for_scope(&Scope::new("local").expect("valid scope"))
        .unwrap();
    let docs = scoped.documents().clone();
    for collection in [
        "flows_definitions",
        "flows_revisions",
        "flows_kv",
        "flows_state",
    ] {
        docs.ensure_collection(&CollectionSpec::new(collection))
            .await
            .unwrap();
    }
    docs.put(
        "flows_definitions",
        "legacy-flow",
        json!({"name":"Legacy flow","graph_json":"{\"schema_version\":1,\"nodes\":[],\"edges\":[]}",
            "enabled":true,"created_at":"2023-11-14T22:15:23.456+00:00",
            "updated_at":"2023-11-14T22:15:23.456+00:00","require_approval":false,
            "incarnation":"legacy-inc"}),
        Precondition::None,
    )
    .await
    .unwrap();
    let revision_created_ms = 1_700_000_123_456_i64;
    docs.put_with(
        "flows_revisions",
        "legacy-revision",
        json!({"flow_id":"legacy-flow","name":"Legacy flow","graph_json":"{\"schema_version\":1,\"nodes\":[],\"edges\":[]}",
            "require_approval":false,"flow_incarnation":"legacy-inc"}),
        Precondition::None,
        WriteOptions::new().created_ms(revision_created_ms),
    )
    .await
    .unwrap();
    docs.put(
        "flows_kv",
        "legacy-kv",
        json!({"namespace":"legacy-flow","key":"k","value_json":"{\"n\":7}"}),
        Precondition::None,
    )
    .await
    .unwrap();
    docs.put(
        "flows_state",
        "legacy-state",
        json!({"key":"state-key","value":"state-value"}),
        Precondition::None,
    )
    .await
    .unwrap();

    let path = "/test/CatalogStorage";
    connection
        .serve_at(path.try_into().unwrap(), CatalogCallback(docs.clone()))
        .await
        .unwrap();
    connection
        .request_name("test.CatalogStorage")
        .await
        .unwrap();
    let catalog = connection
        .proxy(
            tinyflows_bus::BUS_NAME,
            tinyflows_bus::catalog::OBJECT_PATH,
            tinyflows_bus::catalog::INTERFACE,
        )
        .unwrap();
    let storage = CatalogStorage {
        callback: CallbackAddress {
            destination: "test.CatalogStorage".into(),
            object_path: path.into(),
        },
    };
    let catalog_args_a = (storage.clone(),);
    let catalog_args_b = (storage.clone(),);
    let (first, concurrent) = tokio::join!(
        catalog.call::<()>("MigrateStorage", &catalog_args_a),
        catalog.call::<()>("MigrateStorage", &catalog_args_b),
    );
    first.unwrap();
    concurrent.unwrap();
    catalog
        .call::<()>("MigrateStorage", &(storage.clone(),))
        .await
        .unwrap();

    let workflow: Option<Workflow> = catalog
        .call("GetWorkflow", &(storage.clone(), "legacy-flow".to_owned()))
        .await
        .unwrap();
    assert_eq!(workflow.unwrap().name, "Legacy flow");
    let workflows: ListWorkflowsResult = catalog
        .call("ListWorkflows", &(storage.clone(),))
        .await
        .unwrap();
    assert_eq!(workflows.workflows.len(), 1);
    let revisions: Vec<WorkflowRevision> = catalog
        .call(
            "ListRevisions",
            &(ListRevisionsRequest {
                storage: storage.clone(),
                flow_id: "legacy-flow".into(),
                limit: 10,
            },),
        )
        .await
        .unwrap();
    assert_eq!(revisions.len(), 1);
    let migrated_revision = docs
        .get("flows.revisions", "legacy-revision")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(migrated_revision.doc["created_ms"], revision_created_ms);
    assert_eq!(migrated_revision.meta.created_ms, revision_created_ms);
    let catalog_state = tinyflows_storage::Key::of(["flows", "kv:legacy-flow", "k"]).unwrap();
    let state = tinyflows_storage::Key::of(["flows", "state", "state-key"]).unwrap();
    assert_eq!(
        docs.get("tsd.kv", catalog_state.as_str())
            .await
            .unwrap()
            .unwrap()
            .doc["value"],
        json!({"n":7})
    );
    assert_eq!(
        docs.get("tsd.kv", state.as_str())
            .await
            .unwrap()
            .unwrap()
            .doc["value"],
        "state-value"
    );
}

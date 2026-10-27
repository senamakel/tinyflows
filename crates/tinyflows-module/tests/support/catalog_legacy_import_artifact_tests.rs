use serde_json::{Value, json};
use std::sync::Arc;
use tinybus::{Connection, broker::Broker, module::ModuleHost, transport::memory::MemoryTransport};
use tinyflows::graph::{CheckpointConfig, Checkpointer};
use tinyflows_bus::catalog::{
    CallbackAddress, CatalogStorage, ImportLegacyBatchRequest, ImportLegacyBatchResponse,
    LegacyDraftRecord, LegacyFlowRecord, LegacyImportKind, LegacyImportRecord,
    LegacyRevisionRecord, LegacyRunRecord, LegacyStateRecord, LegacySuggestionRecord,
    PutWorkflowRequest, PutWorkflowResponse, Workflow,
};
use tinyflows_bus::{BUS_NAME, catalog};
use tinyflows_drivers::DriverCheckpointer;
use tinyflows_storage::{DocumentStore, Scope, StorageBackend};
use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;

use super::storage_migration_artifact_tests::CatalogCallback;

fn graph(trigger: &str) -> Value {
    json!({
        "nodes": [
            {"id":"trigger","name":"Trigger","kind":"trigger","config":{"trigger_kind":trigger}},
            {"id":"agent","name":"Agent","kind":"agent","config":{"prompt":"Do the work"}}
        ],
        "edges": [{"from_node":"trigger","from_port":"main","to_node":"agent","to_port":"main"}]
    })
}

async fn catalog_module() -> (ModuleHost, Connection) {
    let artifact = std::env::var_os("TINYFLOWS_MODULE_ARTIFACT")
        .expect("set TINYFLOWS_MODULE_ARTIFACT to a compiled cdylib");
    let broker = Broker::new();
    let host = ModuleHost::new(broker.clone());
    host.load_file(std::path::PathBuf::from(artifact)).unwrap();
    let (client, transport) = MemoryTransport::pair();
    broker.attach(Arc::new(transport));
    let connection = Connection::connect(Box::new(client)).await.unwrap();
    (host, connection)
}

async fn scoped_storage(
    connection: &Connection,
    backend: &MemoryStorage,
    scope_name: &str,
) -> (CatalogStorage, Arc<dyn DocumentStore>) {
    let docs = Arc::clone(
        backend
            .for_scope(&Scope::new(scope_name).unwrap())
            .unwrap()
            .documents(),
    );
    let path = format!("/test/CatalogStorage/{scope_name}");
    connection
        .serve_at(path.parse().unwrap(), CatalogCallback(docs.clone()))
        .await
        .unwrap();
    connection
        .request_name("test.CatalogStorage")
        .await
        .unwrap();
    (
        CatalogStorage {
            callback: CallbackAddress {
                destination: "test.CatalogStorage".into(),
                object_path: path,
            },
        },
        docs,
    )
}

fn flow_record(id: &str, name: &str, graph_value: &Value) -> LegacyImportRecord {
    LegacyImportRecord::Flow(LegacyFlowRecord {
        id: id.into(),
        name: name.into(),
        graph_json: serde_json::to_string(graph_value).unwrap(),
        enabled: true,
        created_at: "2025-01-02T03:04:05Z".into(),
        updated_at: "2025-01-03T03:04:05Z".into(),
        last_run_at: None,
        last_status: None,
        require_approval: false,
    })
}

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn published_module_imports_all_legacy_catalog_kinds_with_scoped_idempotency() {
    let (_host, connection) = catalog_module().await;
    let catalog_proxy = connection
        .proxy(BUS_NAME, catalog::OBJECT_PATH, catalog::INTERFACE)
        .unwrap();
    let backend = MemoryStorage::new();
    let (first_storage, first_docs) = scoped_storage(&connection, &backend, "legacy_first").await;
    let (second_storage, second_docs) =
        scoped_storage(&connection, &backend, "legacy_second").await;

    let live: PutWorkflowResponse = catalog_proxy
        .call(
            "PutWorkflow",
            &(PutWorkflowRequest::Create {
                storage: first_storage.clone(),
                name: "live value".into(),
                graph: graph("manual"),
                require_approval: false,
            },),
        )
        .await
        .unwrap();
    let PutWorkflowResponse::Ok(live) = live else {
        panic!("preseed workflow failed: {live:?}")
    };

    let graph_value = graph("schedule");
    let mut records = vec![
        flow_record("legacy-flow", "Historic scheduled flow", &graph_value),
        flow_record(&live.workflow.id, "legacy overwrite attempt", &graph_value),
        LegacyImportRecord::Revision(LegacyRevisionRecord {
            id: "legacy-revision".into(),
            flow_id: "legacy-flow".into(),
            graph_json: serde_json::to_string(&graph_value).unwrap(),
            name: "Historic scheduled flow".into(),
            require_approval: false,
            created_at: "2025-01-03T03:04:05Z".into(),
        }),
        LegacyImportRecord::Run(LegacyRunRecord {
            id: "legacy-run-id".into(),
            flow_id: "legacy-flow".into(),
            thread_id: "legacy-checkpoint-thread".into(),
            status: "pending_approval".into(),
            started_at: "2025-01-04T03:04:05Z".into(),
            finished_at: None,
            steps_json: "[]".into(),
            pending_approvals_json: "[\"gate\"]".into(),
            error: None,
            graph_hash: Some("historic-hash".into()),
        }),
        LegacyImportRecord::State(LegacyStateRecord {
            namespace: "legacy-flow".into(),
            key: "state-key".into(),
            value_json: "{\"saved\":true}".into(),
        }),
        LegacyImportRecord::Suggestion(LegacySuggestionRecord {
            id: "legacy-suggestion".into(),
            title: "A suggestion".into(),
            one_liner: "One line".into(),
            rationale: "Historic rationale".into(),
            trigger_hint: None,
            steps_json: "[\"step\"]".into(),
            connections_json: "[]".into(),
            slugs_json: "[]".into(),
            build_prompt: "Build it".into(),
            confidence: 0.8,
            status: "new".into(),
            created_at: "2025-01-04T03:04:05Z".into(),
            source_run_id: Some("legacy-run-id".into()),
        }),
        LegacyImportRecord::Draft(LegacyDraftRecord {
            file_name: "bad.json".into(),
            bytes: b"not json".to_vec(),
        }),
        LegacyImportRecord::Draft(LegacyDraftRecord {
            file_name: "good-draft.json".into(),
            bytes: serde_json::to_vec(&json!({
                "id":"good-draft","flow_id":null,"name":"Draft",
                "graph":{},"origin":"canvas","created_at":"2025-01-04T03:04:05Z",
                "updated_at":"2025-01-04T03:04:05Z"
            }))
            .unwrap(),
        }),
        LegacyImportRecord::Checkpoint {
            raw_json: serde_json::to_string(&json!({
                "thread_id":"legacy-checkpoint-thread","checkpoint_id":"legacy-cp-1",
                "run_id":"legacy-run-id","parent_checkpoint_id":null,"namespace":[],
                "state":{"restored":true},"next_nodes":[],"completed_tasks":[],
                "pending_writes":[],"interrupts":[],"pending_activations":null,
                "barrier_arrivals":[],"metadata":{"from":"legacy"}
            }))
            .unwrap(),
        },
        LegacyImportRecord::FileCheckpointWrite {
            thread_id: "legacy-checkpoint-thread".into(),
            raw_json: serde_json::to_string(&json!({
                "namespace":[],"checkpoint_id":"legacy-cp-1",
                "write":{"node":"agent","task_id":"legacy-task","idx":0,
                    "channel":"result","payload":{"ok":true}}
            }))
            .unwrap(),
        },
    ];
    let request = ImportLegacyBatchRequest {
        storage: first_storage.clone(),
        records: records.clone(),
    };
    let first: ImportLegacyBatchResponse = catalog_proxy
        .call(catalog::members::IMPORT_LEGACY_BATCH, &(request.clone(),))
        .await
        .unwrap();
    let tally = |kind| {
        first
            .tallies
            .iter()
            .find(|tally| tally.kind == kind)
            .unwrap()
    };
    assert_eq!(tally(LegacyImportKind::Flow).written, 1);
    assert_eq!(tally(LegacyImportKind::Flow).existing, 1);
    assert_eq!(tally(LegacyImportKind::Revision).written, 1);
    assert_eq!(tally(LegacyImportKind::Run).written, 1);
    assert_eq!(tally(LegacyImportKind::State).written, 1);
    assert_eq!(tally(LegacyImportKind::Suggestion).written, 1);
    assert_eq!(tally(LegacyImportKind::Draft).invalid, 1);
    assert_eq!(tally(LegacyImportKind::Draft).written, 1);
    assert_eq!(tally(LegacyImportKind::Checkpoint).written, 1);
    assert_eq!(tally(LegacyImportKind::Write).written, 1);
    assert_eq!(first.problems.len(), 1);

    let imported: Option<Workflow> = catalog_proxy
        .call("GetWorkflow", &(first_storage.clone(), "legacy-flow"))
        .await
        .unwrap();
    let imported = imported.unwrap();
    assert!(imported.enabled, "historic enablement is preserved");
    assert!(
        !imported.require_approval,
        "historic approval state is preserved"
    );
    let live_after: Option<Workflow> = catalog_proxy
        .call(
            "GetWorkflow",
            &(first_storage.clone(), live.workflow.id.clone()),
        )
        .await
        .unwrap();
    assert_eq!(live_after.unwrap().name, "live value");
    let run: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            &(catalog::RunIdRequest {
                storage: first_storage.clone(),
                id: "legacy-run-id".into(),
            },),
        )
        .await
        .unwrap();
    assert_eq!(run.unwrap().thread_id, "legacy-checkpoint-thread");

    let checkpointer = DriverCheckpointer::<Value>::new(first_docs.clone());
    let checkpoint = checkpointer
        .get("legacy-checkpoint-thread", Some("legacy-cp-1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.state, json!({"restored":true}));
    let writes = checkpointer
        .get_writes(&CheckpointConfig {
            thread_id: "legacy-checkpoint-thread".into(),
            checkpoint_id: Some("legacy-cp-1".into()),
            namespace: vec![],
        })
        .await
        .unwrap();
    assert_eq!(writes[0].payload, json!({"ok":true}));

    let replay: ImportLegacyBatchResponse = catalog_proxy
        .call(catalog::members::IMPORT_LEGACY_BATCH, &(request,))
        .await
        .unwrap();
    let tally = |kind| {
        replay
            .tallies
            .iter()
            .find(|tally| tally.kind == kind)
            .unwrap()
    };
    assert_eq!(tally(LegacyImportKind::Flow).existing, 2);
    assert_eq!(tally(LegacyImportKind::Checkpoint).existing, 1);
    assert_eq!(tally(LegacyImportKind::Write).existing, 1);

    let second = ImportLegacyBatchRequest {
        storage: second_storage.clone(),
        records: vec![flow_record("legacy-flow", "Second scope", &graph("manual"))],
    };
    catalog_proxy
        .call::<ImportLegacyBatchResponse>(catalog::members::IMPORT_LEGACY_BATCH, &(second,))
        .await
        .unwrap();
    let second_flow: Option<Workflow> = catalog_proxy
        .call("GetWorkflow", &(second_storage, "legacy-flow"))
        .await
        .unwrap();
    assert_eq!(second_flow.unwrap().name, "Second scope");
    assert!(
        first_docs
            .get("flows.definitions", "legacy-flow")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        second_docs
            .get("flows.definitions", "legacy-flow")
            .await
            .unwrap()
            .is_some()
    );

    let count_limited = ImportLegacyBatchRequest {
        storage: first_storage.clone(),
        records: (0..=catalog::MAX_IMPORT_BATCH_RECORDS)
            .map(|index| LegacyImportRecord::Invalid {
                record_kind: LegacyImportKind::Flow,
                identity: Some(index.to_string()),
                message: "count probe".into(),
            })
            .collect(),
    };
    assert!(
        catalog_proxy
            .call::<ImportLegacyBatchResponse>(
                catalog::members::IMPORT_LEGACY_BATCH,
                &(count_limited,)
            )
            .await
            .is_err()
    );

    records.clear();
    let byte_limited = ImportLegacyBatchRequest {
        storage: first_storage,
        records: vec![LegacyImportRecord::Invalid {
            record_kind: LegacyImportKind::Flow,
            identity: None,
            message: "x".repeat(catalog::MAX_IMPORT_BATCH_BYTES),
        }],
    };
    assert!(
        catalog_proxy
            .call::<ImportLegacyBatchResponse>(
                catalog::members::IMPORT_LEGACY_BATCH,
                &(byte_limited,)
            )
            .await
            .is_err()
    );
}

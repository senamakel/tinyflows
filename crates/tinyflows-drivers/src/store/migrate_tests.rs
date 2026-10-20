use std::sync::Arc;
use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;

use serde_json::{Value, json};
use tinyflows::store::WorkflowStore;
use tinyflows_storage::{
    DocumentStore, DocumentStoreExt, MigrateOptions, Precondition, Query, Scope, ScopeClass,
    Storage, StorageBackend,
};

use super::*;

fn scoped() -> Storage {
    MemoryStorage::new()
        .for_scope(&Scope::new("local").expect("valid scope"))
        .unwrap()
}

fn docs(storage: &Storage) -> Arc<dyn DocumentStore> {
    Arc::clone(storage.documents())
}

async fn seed(storage: &Storage, collection: &str, id: &str, body: Value) {
    storage
        .documents()
        .put(collection, id, body, Precondition::None)
        .await
        .unwrap();
}

async fn migrate(storage: &Storage) {
    crate::storage_schema()
        .migrate(storage.documents(), &MigrateOptions::new())
        .await
        .unwrap();
}

/// A v1 record document: the whole record as a JSON string.
fn v1_record(id: &str, description: &str) -> String {
    let record = test_support::record(id, description);
    serde_json::to_string(&record).unwrap()
}

#[test]
fn the_schema_names_every_workflow_store_collection() {
    let schema = crate::storage_schema();
    assert_eq!(schema.owner.as_str(), "flows");
    let names: Vec<String> = schema
        .items
        .iter()
        .map(|(item, _, _)| item.name.clone())
        .collect();
    for name in [
        "flows.workflows",
        "flows.workflow_revisions",
        "flows.workflow_runs",
        "flows.workflow_notes",
        "flows.workflow_proposals",
        "flows.workflow_quarantine",
    ] {
        assert!(
            names.iter().any(|actual| actual == name),
            "{name} missing from {names:?}"
        );
    }
    assert!(
        schema
            .items
            .iter()
            .all(|(_, _, scope)| *scope == ScopeClass::Tenant)
    );
    assert!(
        schema
            .collections()
            .iter()
            .any(|item| item.name == "tsd.migrations")
    );
}

#[tokio::test]
async fn a_v1_definition_becomes_a_bare_object_and_reads_back() {
    let storage = scoped();
    seed(
        &storage,
        "wf_definitions",
        "sweep",
        json!({"id": "sweep", "name": "Greet", "record_json": v1_record("sweep", "hello")}),
    )
    .await;

    migrate(&storage).await;

    let stored = docs(&storage)
        .get("flows.workflows", "sweep")
        .await
        .unwrap()
        .expect("migrated under Key::of");
    assert_eq!(stored.doc["id"], "sweep");
    assert_eq!(stored.doc["description"], "hello");
    assert!(stored.doc["graph"].is_object(), "the graph is not a string");
    assert!(stored.doc.get("record_json").is_none());
    assert!(
        docs(&storage)
            .get("wf_definitions", "sweep")
            .await
            .unwrap()
            .is_none(),
        "the v1 source is removed"
    );
    let store = DriverWorkflowStore::new(docs(&storage));
    let record = tokio::task::spawn_blocking(move || store.get("sweep"))
        .await
        .unwrap()
        .unwrap()
        .expect("readable");
    assert_eq!(record.description, "hello");
}

#[tokio::test]
async fn a_hashed_v1_id_is_re_keyed_from_the_record_id() {
    let storage = scoped();
    let long = "w".repeat(450);
    seed(
        &storage,
        "wf_definitions",
        "h:0123456789abcdef",
        json!({"id": long, "name": "Greet", "record_json": v1_record(&long, "d")}),
    )
    .await;

    migrate(&storage).await;

    let key = tinyflows_storage::Key::of([long.as_str()]).unwrap();
    assert!(
        docs(&storage)
            .get("flows.workflows", key.as_str())
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn a_v1_revision_keeps_its_id_and_time() {
    let storage = scoped();
    seed(
        &storage,
        "wf_revisions",
        "0000000000007-000000000000-aa",
        json!({"workflow_id": "sweep", "superseded_at": 7, "record_json": v1_record("sweep", "old")}),
    )
    .await;

    migrate(&storage).await;

    let stored = docs(&storage)
        .get("flows.workflow_revisions", "0000000000007-000000000000-aa")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.doc["workflow_id"], "sweep");
    assert_eq!(stored.doc["superseded_ms"], 7);
    assert_eq!(stored.doc["description"], "old");
    assert!(stored.doc.get("id").is_none() && stored.doc.get("superseded_at").is_none());
    assert_eq!(stored.meta.created_ms, 7, "history keeps its time");
    let store = DriverWorkflowStore::new(docs(&storage));
    let history = tokio::task::spawn_blocking(move || store.list_revisions("sweep"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].superseded_at, 7);
    assert_eq!(history[0].record.description, "old");
}

#[tokio::test]
async fn a_v1_run_becomes_snake_case_with_ms_times() {
    let storage = scoped();
    let run = json!({
        "id": "run-1", "workflowId": "sweep", "status": "succeeded",
        "startedAt": 1_700_000_000_123_u64, "finishedAt": 1_700_000_000_456_u64,
        "executor": {"host": "box", "pid": 4, "startedAtSecs": 1_700_000_000_u64},
        "steps": [{"nodeId": "a", "status": "success", "durationMs": 3,
                   "output": [{"keepMe": 1}]}],
        "pendingApprovals": []
    });
    seed(
        &storage,
        "wf_runs",
        "run-1",
        json!({"workflow_id": "sweep", "started_at": 1_700_000_000_123_u64,
               "settled": true, "run_json": run.to_string()}),
    )
    .await;

    migrate(&storage).await;

    let stored = docs(&storage)
        .get("flows.workflow_runs", "run-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.doc["workflow_id"], "sweep");
    assert_eq!(stored.doc["started_ms"], 1_700_000_000_123_u64);
    assert_eq!(stored.doc["finished_ms"], 1_700_000_000_456_u64);
    assert_eq!(
        stored.doc["executor"]["process_started_ms"],
        1_700_000_000_000_u64
    );
    assert_eq!(stored.doc["steps"][0]["node_id"], "a");
    assert_eq!(
        stored.doc["steps"][0]["output"][0]["keepMe"], 1,
        "free-form output is verbatim"
    );
    assert_eq!(stored.doc["settled"], true);
    assert_eq!(stored.meta.created_ms, 1_700_000_000_123_i64);
    let store = DriverWorkflowStore::new(docs(&storage));
    let read = tokio::task::spawn_blocking(move || store.get_run("run-1"))
        .await
        .unwrap()
        .unwrap()
        .expect("readable");
    assert_eq!(read.started_at, 1_700_000_000_123);
    assert_eq!(read.executor.unwrap().started_at_secs, Some(1_700_000_000));
}

#[tokio::test]
async fn a_v1_journal_string_becomes_a_list_of_notes() {
    let storage = scoped();
    let notes = json!([{
        "id": "n1", "workflowId": "sweep", "kind": "rejection", "text": "no",
        "recordedAt": 11, "source": {"kind": "operator"}, "runIds": ["r1"], "pinned": true
    }]);
    seed(
        &storage,
        "wf_notes",
        "sweep",
        json!({"workflow_id": "sweep", "notes_json": notes.to_string()}),
    )
    .await;

    migrate(&storage).await;

    let stored = docs(&storage)
        .get("flows.workflow_notes", "sweep")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.doc["notes"][0]["recorded_ms"], 11);
    assert_eq!(stored.doc["notes"][0]["run_ids"], json!(["r1"]));
    let store = DriverWorkflowStore::new(docs(&storage));
    let listed = tokio::task::spawn_blocking(move || store.list_notes("sweep"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].recorded_at, 11);
    assert!(listed[0].pinned);
}

#[tokio::test]
async fn a_v1_proposal_becomes_a_bare_object_and_keeps_its_ops() {
    let storage = scoped();
    let proposal = json!({
        "id": "p1", "workflowId": "sweep", "createdAt": 5, "rationale": "because",
        "ops": [{"opKind": "set"}], "baseFingerprint": "abc", "status": "pending"
    });
    seed(
        &storage,
        "wf_proposals",
        "p1",
        json!({"workflow_id": "sweep", "created_at": 5, "proposal_json": proposal.to_string()}),
    )
    .await;

    migrate(&storage).await;

    let stored = docs(&storage)
        .get("flows.workflow_proposals", "p1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.doc["created_ms"], 5);
    assert_eq!(stored.doc["base_fingerprint"], "abc");
    assert_eq!(stored.doc["ops"][0]["opKind"], "set");
    let store = DriverWorkflowStore::new(docs(&storage));
    let read = tokio::task::spawn_blocking(move || store.get_proposal("p1"))
        .await
        .unwrap()
        .unwrap()
        .expect("readable");
    assert_eq!(read.created_at, 5);
}

#[tokio::test]
async fn an_unparseable_v1_record_is_carried_not_lost() {
    let storage = scoped();
    seed(
        &storage,
        "wf_definitions",
        "junk",
        json!({"id": "junk", "record_json": "{not json"}),
    )
    .await;
    seed(
        &storage,
        "wf_quarantine",
        "token-1",
        json!({"workflow_id": "w", "journal": {"notes_json": "{ nope"}}),
    )
    .await;

    migrate(&storage).await;

    let junk = docs(&storage)
        .get("flows.workflows", "junk")
        .await
        .unwrap()
        .expect("kept");
    assert_eq!(junk.doc["record_json"], "{not json");
    let kept = docs(&storage)
        .get("flows.workflow_quarantine", "token-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(kept.doc["journal"]["notes_json"], "{ nope");
    let all = docs(&storage)
        .query_all("flows.workflows", &Query::all())
        .await
        .unwrap();
    assert_eq!(all.len(), 1);
}

#[tokio::test]
async fn migrating_twice_changes_nothing() {
    let storage = scoped();
    seed(
        &storage,
        "wf_definitions",
        "sweep",
        json!({"id": "sweep", "record_json": v1_record("sweep", "d")}),
    )
    .await;
    migrate(&storage).await;
    let first = docs(&storage)
        .get("flows.workflows", "sweep")
        .await
        .unwrap();
    migrate(&storage).await;
    let second = docs(&storage)
        .get("flows.workflows", "sweep")
        .await
        .unwrap();
    assert_eq!(first.unwrap().doc, second.unwrap().doc);
}

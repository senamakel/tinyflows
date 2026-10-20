use std::sync::Arc;
use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;

use serde_json::{Value, json};
use tinyflows_storage::{
    Counters, DocumentStore, DocumentStoreExt, Key, MigrateOptions, Precondition, Scope,
    ScopeClass, Storage, StorageBackend,
};

use super::*;
use crate::drivers::{DriverLedger, DriverVault};
use crate::ledger::{Ledger, conformance};
use crate::workflows::Vault;

fn scoped() -> Storage {
    MemoryStorage::new()
        .for_scope(&Scope::new("local").expect("valid scope"))
        .unwrap()
}

fn docs(storage: &Storage) -> Arc<dyn DocumentStore> {
    Arc::clone(storage.documents())
}

fn schema() -> tinyflows_storage::Schema {
    storage_schema()
}

async fn seed(storage: &Storage, collection: &str, id: &str, body: Value) {
    storage
        .documents()
        .put(collection, id, body, Precondition::None)
        .await
        .unwrap();
}

async fn migrate(storage: &Storage) {
    schema()
        .migrate(storage.documents(), &MigrateOptions::new())
        .await
        .unwrap();
}

#[test]
fn the_schema_declares_the_eight_adaptive_collections_and_the_vault() {
    let schema = schema();
    let names: Vec<String> = schema
        .items
        .iter()
        .map(|(item, _, _)| item.name.clone())
        .collect();
    for name in [
        "flows.adaptive_rows",
        "flows.adaptive_lessons",
        "flows.adaptive_evidence",
        "flows.adaptive_scores",
        "flows.adaptive_variants",
        "flows.adaptive_episodes",
        "flows.adaptive_steps",
        "flows.adaptive_workflows",
    ] {
        assert!(names.contains(&name), "{name} missing from {names:?}");
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
async fn a_v1_row_migrates_to_a_flat_body_with_an_ms_time() {
    let storage = scoped();
    let row = json!({"id": "", "episode": "ep", "attempt": 2, "approach_sig": "sig",
        "approach_desc": "d", "workflow_id": "wf", "outcome": "ok", "cause": "",
        "cost_usd": 0.5, "at": "2026-01-01T00:00:02Z", "satisfied": true, "advanced": false});
    seed(
        &storage,
        "adaptive_rows",
        "ldg_00000007",
        json!({"episode": "ep", "scope_key": "t", "seq": 7, "record": row.to_string()}),
    )
    .await;

    migrate(&storage).await;

    let stored = docs(&storage)
        .get("flows.adaptive_rows", "ldg_00000007")
        .await
        .unwrap()
        .expect("same id");
    assert_eq!(stored.doc["scope_key"], "t");
    assert_eq!(stored.doc["seq"], 7);
    assert_eq!(stored.doc["episode"], "ep");
    assert_eq!(stored.doc["at_ms"], 1_767_225_602_000_i64);
    assert_eq!(stored.doc["satisfied"], true);
    assert!(stored.doc.get("record").is_none() && stored.doc.get("at").is_none());
    assert_eq!(stored.meta.created_ms, 1_767_225_602_000_i64);
    let ledger = DriverLedger::new(docs(&storage)).for_tenant("t");
    let rows = ledger.rows("ep").await.unwrap();
    assert_eq!(rows[0].id, "ldg_00000007");
    assert_eq!(rows[0].at, "2026-01-01T00:00:02Z");
}

#[tokio::test]
async fn a_v1_lesson_takes_its_counters_from_the_document() {
    let storage = scoped();
    let lesson = json!({"id": "", "kind": "constraint", "trigger": "t", "mechanism": "m",
        "claim": "c", "applied": 0, "helped": 0, "scope_key": null});
    seed(
        &storage,
        "adaptive_lessons",
        "les_00000001",
        json!({"kind": "constraint", "scope_key": "", "seq": 1, "applied": 5, "helped": 3,
               "record": lesson.to_string()}),
    )
    .await;
    seed(
        &storage,
        "adaptive_evidence",
        "1:a/2:b",
        json!({"lesson_id": "les_00000001", "row_id": "ldg_00000002"}),
    )
    .await;

    migrate(&storage).await;

    let ledger = DriverLedger::new(docs(&storage));
    let lessons = ledger.lessons(None).await.unwrap();
    assert_eq!((lessons[0].applied, lessons[0].helped), (5, 3));
    assert_eq!(lessons[0].id, "les_00000001");
    let evidence_id = Key::of(["les_00000001", "ldg_00000002"]).unwrap();
    assert!(
        docs(&storage)
            .get("flows.adaptive_evidence", evidence_id.as_str())
            .await
            .unwrap()
            .is_some(),
        "re-keyed with Key::of"
    );
}

#[tokio::test]
async fn scores_variants_episodes_steps_and_workflows_are_re_keyed_with_the_scope() {
    let storage = scoped();
    seed(
        &storage,
        "adaptive_scores",
        "1:t/2:wf",
        json!({"scope_key": "t", "workflow_id": "wf", "applied": 4, "helped": 1}),
    )
    .await;
    seed(
        &storage,
        "adaptive_variants",
        "1:t/1:v",
        json!({"scope_key": "t", "variant": "v", "parent": "p"}),
    )
    .await;
    let episode = json!({"id": "ep1", "goal": {"text": "g", "success_criteria": ""},
        "scope_key": "t", "status": {"state": "stood_down", "reason": "stalled"},
        "attempt": 3, "stalled": 2, "started_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:05Z"});
    seed(
        &storage,
        "adaptive_episodes",
        "1:t/3:ep1",
        json!({"scope_key": "t", "episode_id": "ep1", "updated_at": "2026-01-01T00:00:05Z",
               "running": false, "record": episode.to_string()}),
    )
    .await;
    let step = json!({"nodeId": "n", "status": "success", "output": {"file.name": "x"},
        "durationMs": 9, "nullBindings": [{"location": "a", "expression": "=b"}],
        "transcript": [{"atMs": 4, "kind": "tool_call", "text": "t"}]});
    seed(
        &storage,
        "adaptive_steps",
        "1:t/8:ldg_0001/8:00000000",
        json!({"scope_key": "t", "row_id": "ldg_0001", "seq": 0, "record": step.to_string()}),
    )
    .await;
    let wf = serde_json::to_value(crate::workflows::conformance::record("wf")).unwrap();
    seed(
        &storage,
        "adaptive_workflows",
        "1:t/2:wf",
        json!({"scope_key": "t", "workflow_id": "wf", "record": wf.to_string()}),
    )
    .await;

    migrate(&storage).await;

    let ledger = DriverLedger::new(docs(&storage)).for_tenant("t");
    assert_eq!(ledger.workflow_score("wf").await.unwrap().applied, 4);
    assert_eq!(ledger.parent_of("v").await.unwrap().as_deref(), Some("p"));
    let ep = ledger.episode("ep1").await.unwrap().expect("episode");
    assert_eq!(ep.started_at, "2026-01-01T00:00:00Z");
    assert_eq!(ep.updated_at, "2026-01-01T00:00:05Z");
    assert_eq!(ep.scope_key.as_deref(), Some("t"));
    assert_eq!(ep.attempt, 3);
    let steps = ledger.steps("ldg_0001").await.unwrap();
    assert_eq!(steps[0].node_id, "n");
    assert_eq!(steps[0].output["file.name"], "x");
    assert_eq!(steps[0].transcript[0].at_ms, 4);
    let stored = docs(&storage)
        .get(
            "flows.adaptive_steps",
            Key::of(["t", "ldg_0001", "00000000"]).unwrap().as_str(),
        )
        .await
        .unwrap()
        .expect("scope_key is a key part");
    assert_eq!(stored.doc["duration_ms"], 9);
    assert_eq!(stored.doc["transcript"][0]["at_ms"], 4);
    let vault = DriverVault::new(docs(&storage)).for_tenant("t");
    assert_eq!(vault.load().await.unwrap()[0].id, "wf");
}

#[tokio::test]
async fn migrating_twice_changes_nothing() {
    let storage = scoped();
    seed(
        &storage,
        "adaptive_variants",
        "1:t/1:v",
        json!({"scope_key": "t", "variant": "v", "parent": "p"}),
    )
    .await;
    migrate(&storage).await;
    migrate(&storage).await;
    let all = docs(&storage)
        .query_all("flows.adaptive_variants", &tinyflows_storage::Query::all())
        .await
        .unwrap();
    assert_eq!(all.len(), 1);
}

#[tokio::test]
async fn the_v1_sequences_carry_into_tsd_counters() {
    let storage = scoped();
    seed(
        &storage,
        "adaptive_counters",
        "adaptive_rows",
        json!({"seq": 41}),
    )
    .await;
    seed(
        &storage,
        "adaptive_counters",
        "adaptive_lessons",
        json!({"seq": 6}),
    )
    .await;

    assert_eq!(import_legacy_adaptive_counters(&storage).await.unwrap(), 2);

    let counters = Counters::new(&docs(&storage));
    assert_eq!(counters.peek("flows", "adaptive_rows").await.unwrap(), 41);
    assert_eq!(counters.peek("flows", "adaptive_lessons").await.unwrap(), 6);
    assert!(
        docs(&storage)
            .get("adaptive_counters", "adaptive_rows")
            .await
            .unwrap()
            .is_none(),
        "the v1 collection is emptied"
    );
    // New ids continue after the imported ones.
    let ledger = DriverLedger::new(docs(&storage));
    let id = ledger
        .append(&conformance::row("ep", 1, "sig"))
        .await
        .unwrap();
    assert_eq!(id, "ldg_00000042");
    // A second run is a no-op.
    assert_eq!(import_legacy_adaptive_counters(&storage).await.unwrap(), 0);
}

#[tokio::test]
async fn importing_with_no_v1_counters_only_records_completion() {
    let storage = scoped();
    assert_eq!(import_legacy_adaptive_counters(&storage).await.unwrap(), 0);
}

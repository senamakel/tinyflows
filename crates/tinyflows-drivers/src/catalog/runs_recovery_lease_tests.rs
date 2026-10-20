use super::*;
use tinyflows_catalog::FlowRunStep;
use tinyflows_storage::{MemoryStorage, StorageBackend};

async fn running_with_lease(lease: &str) -> (FlowCatalogDocuments, String) {
    let storage = MemoryStorage::new();
    let scoped = storage
        .for_scope(&tinyflows_storage::Scope::local())
        .unwrap();
    let catalog = FlowCatalogDocuments::new(scoped.documents().clone());
    let graph = tinyflows::migrate::deserialize_graph(serde_json::json!({
        "nodes":[{"id":"trigger","name":"Trigger","kind":"trigger","config":{}}],
        "edges":[]
    }))
    .unwrap();
    let flow = catalog
        .create_flow("actor recovery".into(), graph, false, true)
        .await
        .unwrap();
    let run_id = uuid::Uuid::new_v4().to_string();
    catalog
        .insert_flow_run(&run_id, &flow.id, &run_id, "2026-01-01T00:00:00Z")
        .await
        .unwrap();
    catalog
        .set_flow_run_snapshot(&run_id, serde_json::json!({"lease":lease}))
        .await
        .unwrap();
    (catalog, run_id)
}

async fn legacy_parked() -> (FlowCatalogDocuments, String) {
    let storage = MemoryStorage::new();
    let scoped = storage
        .for_scope(&tinyflows_storage::Scope::local())
        .unwrap();
    let catalog = FlowCatalogDocuments::new(scoped.documents().clone());
    let graph = tinyflows::migrate::deserialize_graph(serde_json::json!({
        "nodes":[{"id":"trigger","name":"Trigger","kind":"trigger","config":{}}],
        "edges":[]
    }))
    .unwrap();
    let flow = catalog
        .create_flow("legacy parked".into(), graph, false, true)
        .await
        .unwrap();
    let run_id = uuid::Uuid::new_v4().to_string();
    catalog
        .insert_flow_run(
            &run_id,
            &flow.id,
            &format!("thread-{run_id}"),
            "2026-01-01T00:00:00Z",
        )
        .await
        .unwrap();
    catalog
        .finish_flow_run(
            &run_id,
            "pending_approval",
            "2026-01-01T00:00:01Z",
            &[],
            &["gate".into()],
            None,
            None,
        )
        .await
        .unwrap();
    (catalog, run_id)
}

#[tokio::test]
async fn actor_recovery_interrupts_only_the_running_row_with_its_lease() {
    let (catalog, run_id) = running_with_lease("actor-a").await;
    assert!(
        catalog
            .interrupt_running_flow_run_for_lease(
                &run_id,
                "actor-a",
                "2026-01-01T00:00:05Z",
                "actor stopped",
            )
            .await
            .unwrap()
    );
    let run = catalog.get_flow_run(&run_id).await.unwrap().unwrap();
    assert_eq!(run.status, "interrupted");
    assert_eq!(run.error.as_deref(), Some("actor stopped"));
    assert!(run.finished_at.is_some());
    assert!(run.pending_approvals.is_empty());
    assert!(run.graph_hash.is_none());
}

#[tokio::test]
async fn actor_recovery_cannot_interrupt_a_replacement_resume_lease() {
    let (catalog, run_id) = running_with_lease("old-actor").await;
    assert!(
        catalog
            .finish_flow_run(
                &run_id,
                "pending_approval",
                "2026-01-01T00:00:01Z",
                &[],
                &["gate".into()],
                None,
                Some("graph-pin"),
            )
            .await
            .unwrap()
    );
    assert!(
        catalog
            .claim_parked_flow_run_for_resume(&run_id, "new-actor")
            .await
            .unwrap()
    );

    assert!(
        !catalog
            .interrupt_running_flow_run_for_lease(
                &run_id,
                "old-actor",
                "2026-01-01T00:00:05Z",
                "old actor stopped",
            )
            .await
            .unwrap()
    );
    let run = catalog.get_flow_run(&run_id).await.unwrap().unwrap();
    assert_eq!(run.status, "running");
    assert_eq!(
        catalog.flow_run_snapshot(&run_id).await.unwrap().unwrap()["lease"],
        "new-actor"
    );
}

#[tokio::test]
async fn actor_recovery_does_not_interrupt_a_parked_or_terminal_row() {
    let (catalog, parked_id) = running_with_lease("parked-actor").await;
    assert!(
        catalog
            .finish_flow_run(
                &parked_id,
                "pending_approval",
                "2026-01-01T00:00:01Z",
                &[],
                &["gate".into()],
                None,
                Some("graph-pin"),
            )
            .await
            .unwrap()
    );
    assert!(
        !catalog
            .interrupt_running_flow_run_for_lease(
                &parked_id,
                "parked-actor",
                "2026-01-01T00:00:05Z",
                "actor stopped",
            )
            .await
            .unwrap()
    );
    assert_eq!(
        catalog
            .get_flow_run(&parked_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "pending_approval"
    );

    let (catalog, terminal_id) = running_with_lease("terminal-actor").await;
    assert!(
        catalog
            .finish_flow_run(
                &terminal_id,
                "completed",
                "2026-01-01T00:00:01Z",
                &[FlowRunStep {
                    node_id: "finished".into(),
                    ..Default::default()
                }],
                &[],
                None,
                None,
            )
            .await
            .unwrap()
    );
    assert!(
        !catalog
            .interrupt_running_flow_run_for_lease(
                &terminal_id,
                "terminal-actor",
                "2026-01-01T00:00:05Z",
                "actor stopped",
            )
            .await
            .unwrap()
    );
    let run = catalog.get_flow_run(&terminal_id).await.unwrap().unwrap();
    assert_eq!(run.status, "completed");
    assert_eq!(run.steps[0].node_id, "finished");
}

#[tokio::test]
async fn legacy_resume_claim_uses_the_exact_versioned_parked_row() {
    let (catalog, run_id) = legacy_parked().await;
    let (run, observed) = catalog
        .get_flow_run_versioned(&run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.status, "pending_approval");
    assert!(observed.doc.get("execution_snapshot").is_none());

    assert!(
        catalog
            .claim_legacy_parked_flow_run_for_resume(
                &observed,
                "legacy-actor",
                serde_json::json!({"flow_id": run.flow_id, "thread_id": run.thread_id}),
            )
            .await
            .unwrap()
    );
    let (claimed, version) = catalog
        .get_flow_run_versioned(&run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.status, "running");
    assert_eq!(version.doc["execution_snapshot"]["lease"], "legacy-actor");
    assert_eq!(version.doc["execution_snapshot"]["flow_id"], run.flow_id);
    assert_eq!(version.doc["execution_snapshot"]["legacy_resume"], true);
}

#[tokio::test]
async fn legacy_resume_claim_does_not_claim_a_replacement_parked_row() {
    let (catalog, run_id) = legacy_parked().await;
    let (_run, observed) = catalog
        .get_flow_run_versioned(&run_id)
        .await
        .unwrap()
        .unwrap();
    let docs = catalog.docs().await.unwrap();
    let mut replacement = observed.doc.clone();
    replacement["pending_approvals"] = serde_json::json!(["replacement-gate"]);
    docs.put(
        super::super::RUNS,
        &run_id,
        replacement,
        observed.unchanged(),
    )
    .await
    .unwrap();

    assert!(
        !catalog
            .claim_legacy_parked_flow_run_for_resume(
                &observed,
                "late-actor",
                serde_json::json!({"flow_id":"replacement"}),
            )
            .await
            .unwrap()
    );
    let (still_parked, version) = catalog
        .get_flow_run_versioned(&run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(still_parked.status, "pending_approval");
    assert_eq!(
        version.doc["pending_approvals"],
        serde_json::json!(["replacement-gate"])
    );
    assert!(version.doc.get("execution_snapshot").is_none());
}

#[tokio::test]
async fn missing_legacy_checkpoint_failure_does_not_settle_a_replacement_row() {
    let (catalog, run_id) = legacy_parked().await;
    let (_run, observed) = catalog
        .get_flow_run_versioned(&run_id)
        .await
        .unwrap()
        .unwrap();
    let docs = catalog.docs().await.unwrap();
    let mut replacement = observed.doc.clone();
    replacement["pending_approvals"] = serde_json::json!(["replacement-gate"]);
    docs.put(
        super::super::RUNS,
        &run_id,
        replacement,
        observed.unchanged(),
    )
    .await
    .unwrap();

    assert!(
        !catalog
            .fail_legacy_parked_flow_run_for_resume(
                &observed,
                "2026-01-01T00:00:05Z",
                &[],
                "legacy parked run has no durable checkpoint",
            )
            .await
            .unwrap()
    );
    let (still_parked, version) = catalog
        .get_flow_run_versioned(&run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(still_parked.status, "pending_approval");
    assert_eq!(
        version.doc["pending_approvals"],
        serde_json::json!(["replacement-gate"])
    );
    assert!(still_parked.error.is_none());
}

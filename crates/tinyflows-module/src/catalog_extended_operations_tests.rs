use super::tests::{connections, graph, put_proxy, scope, scope_with_documents};
use serde_json::json;
use std::sync::Arc;
use tinyflows_bus::catalog::{
    CreateDraftRequest, DraftIdRequest, DraftOrigin, FlowDraft, FlowRun, FlowSuggestion,
    ListAllRunsRequest, ListRunsRequest, ListSuggestionsRequest, PruneRunsRequest,
    PutWorkflowRequest, PutWorkflowResponse, RunIdRequest, SetSuggestionStatusRequest,
    SuggestionStatus, UpdateDraftRequest, UpsertSuggestionsRequest,
};
use tinyflows_drivers::catalog::FlowCatalogDocuments;
use tinyflows_storage::DocumentStore;

fn suggestion(id: &str) -> FlowSuggestion {
    FlowSuggestion {
        id: id.into(),
        title: "Suggestion".into(),
        one_liner: "One line".into(),
        rationale: "Useful".into(),
        trigger_hint: Some("manual".into()),
        steps_outline: vec!["step".into()],
        suggested_connections: vec!["http_cred:test".into()],
        suggested_slugs: vec!["provider:send".into()],
        build_prompt: "Build it".into(),
        confidence: 0.8,
        status: SuggestionStatus::New,
        created_at: "2026-01-01T00:00:00Z".into(),
        source_run_id: Some("source-run".into()),
    }
}

#[tokio::test]
async fn catalog_v3_operations_delegate_to_drivers_and_keep_agent_scopes() {
    let (_server, host) = connections().await;
    let (alice, alice_docs) = scope_with_documents(&host, "extended_alice").await;
    let bob = scope(&host, "extended_bob").await;
    let proxy = put_proxy(&host).unwrap();

    let draft: FlowDraft = proxy
        .call(
            "CreateDraft",
            &(CreateDraftRequest {
                storage: alice.clone(),
                flow_id: Some("old-flow".into()),
                name: "Draft".into(),
                graph: json!({"incomplete": true}),
                origin: DraftOrigin::Canvas,
            },),
        )
        .await
        .unwrap();
    let other_draft: FlowDraft = proxy
        .call(
            "CreateDraft",
            &(CreateDraftRequest {
                storage: alice.clone(),
                flow_id: None,
                name: "Other".into(),
                graph: json!({}),
                origin: DraftOrigin::Chat,
            },),
        )
        .await
        .unwrap();
    let bob_drafts: Vec<FlowDraft> = proxy.call("ListDrafts", &(bob.clone(),)).await.unwrap();
    assert!(bob_drafts.is_empty());
    let fetched: Option<FlowDraft> = proxy
        .call(
            "GetDraft",
            &(DraftIdRequest {
                storage: alice.clone(),
                id: draft.id.clone(),
            },),
        )
        .await
        .unwrap();
    assert_eq!(fetched, Some(draft.clone()));
    let updated: FlowDraft = proxy
        .call(
            "UpdateDraft",
            &(UpdateDraftRequest {
                storage: alice.clone(),
                id: draft.id.clone(),
                name: Some("Edited".into()),
                graph: None,
                flow_id: Some(None),
            },),
        )
        .await
        .unwrap();
    assert_eq!(updated.name, "Edited");
    assert_eq!(updated.flow_id, None);
    assert_eq!(updated.graph, json!({"incomplete": true}));
    let cleared_graph: FlowDraft = proxy
        .call(
            "UpdateDraft",
            &(UpdateDraftRequest {
                storage: alice.clone(),
                id: draft.id.clone(),
                name: None,
                graph: Some(serde_json::Value::Null),
                flow_id: None,
            },),
        )
        .await
        .unwrap();
    assert_eq!(cleared_graph.graph, serde_json::Value::Null);
    let listed_drafts: Vec<FlowDraft> = proxy.call("ListDrafts", &(alice.clone(),)).await.unwrap();
    assert_eq!(listed_drafts.len(), 2);
    assert!(
        proxy
            .call::<bool>(
                "DeleteDraft",
                &(DraftIdRequest {
                    storage: alice.clone(),
                    id: other_draft.id,
                },),
            )
            .await
            .unwrap()
    );

    assert_eq!(
        proxy
            .call::<usize>(
                "UpsertSuggestions",
                &(UpsertSuggestionsRequest {
                    storage: alice.clone(),
                    suggestions: vec![suggestion("suggestion-a")],
                },),
            )
            .await
            .unwrap(),
        1
    );
    let suggestions: Vec<FlowSuggestion> = proxy
        .call(
            "ListSuggestions",
            &(ListSuggestionsRequest {
                storage: alice.clone(),
                status: None,
                limit: 20,
            },),
        )
        .await
        .unwrap();
    assert_eq!(suggestions.len(), 1);
    assert!(
        proxy
            .call::<bool>(
                "SetSuggestionStatus",
                &(SetSuggestionStatusRequest {
                    storage: alice.clone(),
                    id: "suggestion-a".into(),
                    status: SuggestionStatus::Dismissed,
                },),
            )
            .await
            .unwrap()
    );
    assert!(
        !proxy
            .call::<bool>(
                "SetSuggestionStatus",
                &(SetSuggestionStatusRequest {
                    storage: alice.clone(),
                    id: "missing".into(),
                    status: SuggestionStatus::Built,
                },),
            )
            .await
            .unwrap()
    );

    let created: PutWorkflowResponse = proxy
        .call(
            "PutWorkflow",
            &(PutWorkflowRequest::Create {
                storage: alice.clone(),
                name: "Run owner".into(),
                graph: graph("manual", false),
                require_approval: false,
            },),
        )
        .await
        .unwrap();
    let PutWorkflowResponse::Ok(created) = created else {
        panic!("workflow save failed")
    };
    let driver = FlowCatalogDocuments::new(alice_docs as Arc<dyn DocumentStore>);
    for (id, started) in [
        ("run-old", "2026-01-01T00:00:00Z"),
        ("run-new", "2026-01-02T00:00:00Z"),
    ] {
        driver
            .insert_flow_run(id, &created.workflow.id, id, started)
            .await
            .unwrap();
        driver
            .finish_flow_run(id, "completed", started, &[], &[], None, None)
            .await
            .unwrap();
    }
    let newest: Option<FlowRun> = proxy
        .call(
            "GetRun",
            &(RunIdRequest {
                storage: alice.clone(),
                id: "run-new".into(),
            },),
        )
        .await
        .unwrap();
    assert_eq!(newest.unwrap().id, "run-new");
    let other_scope: Option<FlowRun> = proxy
        .call(
            "GetRun",
            &(RunIdRequest {
                storage: bob.clone(),
                id: "run-new".into(),
            },),
        )
        .await
        .unwrap();
    assert!(other_scope.is_none());
    let per_flow: Vec<FlowRun> = proxy
        .call(
            "ListRuns",
            &(ListRunsRequest {
                storage: alice.clone(),
                flow_id: created.workflow.id.clone(),
                limit: 10,
            },),
        )
        .await
        .unwrap();
    assert_eq!(per_flow.len(), 2);
    let all: Vec<FlowRun> = proxy
        .call(
            "ListAllRuns",
            &(ListAllRunsRequest {
                storage: alice.clone(),
                limit: 10,
            },),
        )
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(
        proxy
            .call::<usize>(
                "PruneRuns",
                &(PruneRunsRequest {
                    storage: alice.clone(),
                    flow_id: created.workflow.id.clone(),
                    keep: 1,
                },),
            )
            .await
            .unwrap(),
        1
    );
    let remaining: Vec<FlowRun> = proxy
        .call(
            "ListRuns",
            &(ListRunsRequest {
                storage: alice,
                flow_id: created.workflow.id,
                limit: 10,
            },),
        )
        .await
        .unwrap();
    assert_eq!(remaining.len(), 1);
}

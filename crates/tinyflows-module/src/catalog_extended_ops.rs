//! Conversion and orchestration for the Catalog v3 operations.

use anyhow::{Context, Result};
use std::sync::Arc;
use tinybus::Connection;
use tinyflows_bus::catalog::{
    self as wire, CreateDraftRequest, DraftIdRequest, DraftPromotionFailure,
    DraftPromotionOperation, FlowDraft, FlowRun, FlowRunStep, FlowSuggestion, ListAllRunsRequest,
    ListRunsRequest, ListSuggestionsRequest, PromoteDraftRequest, PromoteDraftResponse,
    PromoteDraftResult, PruneRunsRequest, RunIdRequest, SavePromotedDraftRequest,
    SavePromotedDraftResponse, SetSuggestionStatusRequest, SuggestionStatus, UpdateDraftRequest,
    UpsertSuggestionsRequest,
};
use tinyflows_catalog::{
    DraftOrigin as StoredDraftOrigin, FlowDraft as StoredDraft, FlowRun as StoredRun,
    FlowRunStep as StoredRunStep, FlowSuggestion as StoredSuggestion,
    SuggestionStatus as StoredSuggestionStatus,
};
use tinyflows_drivers::catalog::FlowCatalogDocuments;
use tinyflows_storage::DocumentStore;

async fn store(
    connection: &Connection,
    storage: wire::CatalogStorage,
) -> Result<FlowCatalogDocuments> {
    let docs =
        super::catalog_documents::Documents::new(connection.clone(), storage.callback).await?;
    Ok(FlowCatalogDocuments::new(
        Arc::new(docs) as Arc<dyn DocumentStore>
    ))
}

fn origin(value: wire::DraftOrigin) -> StoredDraftOrigin {
    match value {
        wire::DraftOrigin::Chat => StoredDraftOrigin::Chat,
        wire::DraftOrigin::Canvas => StoredDraftOrigin::Canvas,
        wire::DraftOrigin::Import => StoredDraftOrigin::Import,
    }
}

fn draft(value: StoredDraft) -> FlowDraft {
    FlowDraft {
        id: value.id,
        flow_id: value.flow_id,
        name: value.name,
        graph: value.graph,
        origin: match value.origin {
            StoredDraftOrigin::Chat => wire::DraftOrigin::Chat,
            StoredDraftOrigin::Canvas => wire::DraftOrigin::Canvas,
            StoredDraftOrigin::Import => wire::DraftOrigin::Import,
        },
        created_at: value.created_at,
        updated_at: value.updated_at,
    }
}

fn suggestion_status(value: SuggestionStatus) -> StoredSuggestionStatus {
    match value {
        SuggestionStatus::New => StoredSuggestionStatus::New,
        SuggestionStatus::Dismissed => StoredSuggestionStatus::Dismissed,
        SuggestionStatus::Built => StoredSuggestionStatus::Built,
    }
}

fn suggestion(value: StoredSuggestion) -> FlowSuggestion {
    FlowSuggestion {
        id: value.id,
        title: value.title,
        one_liner: value.one_liner,
        rationale: value.rationale,
        trigger_hint: value.trigger_hint,
        steps_outline: value.steps_outline,
        suggested_connections: value.suggested_connections,
        suggested_slugs: value.suggested_slugs,
        build_prompt: value.build_prompt,
        confidence: value.confidence,
        status: match value.status {
            StoredSuggestionStatus::New => SuggestionStatus::New,
            StoredSuggestionStatus::Dismissed => SuggestionStatus::Dismissed,
            StoredSuggestionStatus::Built => SuggestionStatus::Built,
        },
        created_at: value.created_at,
        source_run_id: value.source_run_id,
    }
}

fn stored_suggestion(value: FlowSuggestion) -> StoredSuggestion {
    StoredSuggestion {
        id: value.id,
        title: value.title,
        one_liner: value.one_liner,
        rationale: value.rationale,
        trigger_hint: value.trigger_hint,
        steps_outline: value.steps_outline,
        suggested_connections: value.suggested_connections,
        suggested_slugs: value.suggested_slugs,
        build_prompt: value.build_prompt,
        confidence: value.confidence,
        status: suggestion_status(value.status),
        created_at: value.created_at,
        source_run_id: value.source_run_id,
    }
}

fn run_step(value: StoredRunStep) -> FlowRunStep {
    FlowRunStep {
        node_id: value.node_id,
        output: value.output,
        port: value.port,
        status: value.status,
        duration_ms: value.duration_ms,
        diagnostics: value.diagnostics,
    }
}

fn run(value: StoredRun) -> FlowRun {
    FlowRun {
        id: value.id,
        flow_id: value.flow_id,
        thread_id: value.thread_id,
        status: value.status,
        started_at: value.started_at,
        finished_at: value.finished_at,
        steps: value.steps.into_iter().map(run_step).collect(),
        pending_approvals: value.pending_approvals,
        error: value.error,
        graph_hash: value.graph_hash,
    }
}

pub(super) async fn create_draft(
    connection: &Connection,
    request: CreateDraftRequest,
) -> Result<FlowDraft> {
    store(connection, request.storage)
        .await?
        .create_draft(
            request.flow_id,
            request.name,
            request.graph,
            origin(request.origin),
        )
        .await
        .map(draft)
        .context("creating flow draft")
}

pub(super) async fn get_draft(
    connection: &Connection,
    request: DraftIdRequest,
) -> Result<Option<FlowDraft>> {
    store(connection, request.storage)
        .await?
        .get_draft(&request.id)
        .await
        .map(|value| value.map(draft))
}

pub(super) async fn update_draft(
    connection: &Connection,
    request: UpdateDraftRequest,
) -> Result<FlowDraft> {
    store(connection, request.storage)
        .await?
        .update_draft(&request.id, request.name, request.graph, request.flow_id)
        .await
        .map(draft)
}

pub(super) async fn list_drafts(
    connection: &Connection,
    storage: wire::CatalogStorage,
) -> Result<Vec<FlowDraft>> {
    store(connection, storage)
        .await?
        .list_drafts()
        .await
        .map(|items| items.into_iter().map(draft).collect())
}

pub(super) async fn delete_draft(connection: &Connection, request: DraftIdRequest) -> Result<bool> {
    store(connection, request.storage)
        .await?
        .delete_draft(&request.id)
        .await
}

pub(super) async fn promote_draft(
    connection: &Connection,
    request: PromoteDraftRequest,
) -> Result<PromoteDraftResponse> {
    let PromoteDraftRequest {
        storage,
        host,
        id,
        require_approval,
    } = request;
    let catalog = store(connection, storage.clone()).await?;
    let draft = catalog
        .get_draft(&id)
        .await?
        .with_context(|| format!("draft '{id}' not found"))?;
    let operation = match draft.flow_id.clone() {
        Some(id) => DraftPromotionOperation::Update {
            id,
            name: draft.name,
            graph: draft.graph,
            require_approval,
            expected_version: None,
        },
        None => DraftPromotionOperation::Create {
            name: draft.name,
            graph: draft.graph,
            require_approval: require_approval.unwrap_or(false),
        },
    };
    let proxy = connection.proxy(
        &host.destination,
        &host.object_path,
        wire::DRAFT_PROMOTION_HOST_INTERFACE,
    )?;
    let saved: SavePromotedDraftResponse = proxy
        .call(
            wire::draft_promotion_host::SAVE_PROMOTED_DRAFT,
            &(SavePromotedDraftRequest { operation }),
        )
        .await
        .map_err(|error| anyhow::anyhow!("saving promoted workflow through host: {error:#}"))?;
    let saved = match saved {
        SavePromotedDraftResponse::Ok(saved) => saved,
        SavePromotedDraftResponse::Error(error) => {
            return Ok(PromoteDraftResponse::Error(DraftPromotionFailure {
                code: error.code,
                message: error.message,
                details: error.details,
            }));
        }
    };
    let (draft_deleted, cleanup_warning) = match catalog.delete_draft(&id).await {
        Ok(removed) => (removed, None),
        Err(error) => {
            tracing::warn!(target: "flows", draft_id = %id, %error, "saved workflow but could not remove promoted draft");
            (false, Some(error.to_string()))
        }
    };
    Ok(PromoteDraftResponse::Ok(PromoteDraftResult {
        workflow: saved.workflow,
        metadata: saved.metadata,
        draft_deleted,
        cleanup_warning,
    }))
}

pub(super) async fn upsert_suggestions(
    connection: &Connection,
    request: UpsertSuggestionsRequest,
) -> Result<usize> {
    let suggestions: Vec<_> = request
        .suggestions
        .into_iter()
        .map(stored_suggestion)
        .collect();
    store(connection, request.storage)
        .await?
        .upsert_suggestions(&suggestions)
        .await
}

pub(super) async fn list_suggestions(
    connection: &Connection,
    request: ListSuggestionsRequest,
) -> Result<Vec<FlowSuggestion>> {
    store(connection, request.storage)
        .await?
        .list_suggestions(request.status.map(suggestion_status), request.limit)
        .await
        .map(|items| items.into_iter().map(suggestion).collect())
}

pub(super) async fn set_suggestion_status(
    connection: &Connection,
    request: SetSuggestionStatusRequest,
) -> Result<bool> {
    store(connection, request.storage)
        .await?
        .set_suggestion_status(&request.id, suggestion_status(request.status))
        .await
}

pub(super) async fn get_run(
    connection: &Connection,
    request: RunIdRequest,
) -> Result<Option<FlowRun>> {
    store(connection, request.storage)
        .await?
        .get_flow_run(&request.id)
        .await
        .map(|value| value.map(run))
}

pub(super) async fn list_runs(
    connection: &Connection,
    request: ListRunsRequest,
) -> Result<Vec<FlowRun>> {
    store(connection, request.storage)
        .await?
        .list_flow_runs(&request.flow_id, request.limit)
        .await
        .map(|items| items.into_iter().map(run).collect())
}

pub(super) async fn list_all_runs(
    connection: &Connection,
    request: ListAllRunsRequest,
) -> Result<Vec<FlowRun>> {
    store(connection, request.storage)
        .await?
        .list_all_flow_runs(request.limit)
        .await
        .map(|items| items.into_iter().map(run).collect())
}

pub(super) async fn prune_runs(
    connection: &Connection,
    request: PruneRunsRequest,
) -> Result<usize> {
    store(connection, request.storage)
        .await?
        .prune_flow_runs(&request.flow_id, request.keep)
        .await
}

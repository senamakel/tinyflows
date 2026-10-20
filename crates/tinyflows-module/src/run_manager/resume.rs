//! Admission of an owner-scoped parked run for checkpoint continuation.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};
use tinyflows::compiler;
use tinyflows::engine::Checkpointer as _;
use tinyflows_bus::runs::{FlowRunTrigger, ResumeRun, ResumeRunResponse, RunContext, RunEvent};
use tinyflows_drivers::{DriverCheckpointer, catalog::FlowCatalogDocuments};
use tinyflows_storage::DocumentStore;

use super::actor::ActorRunRequest;
use super::preparation::Prepared;
use super::{PARKED, RUNNING, active, key, reserve_resume, unregister};
use crate::catalog_documents::Documents;

const GRAPH_CHANGED: &str = "workflow graph changed after the run parked; refusing resume";

pub(super) async fn resume(
    connection: &tinybus::Connection,
    request: ResumeRun,
) -> Result<ResumeRunResponse> {
    validate(&request)?;
    let catalog = super::preparation::catalog(connection, request.storage.clone()).await?;
    super::verify_live_owner(&catalog, &request.storage, &request.run_id).await?;
    let (row, observed) = catalog
        .get_flow_run_versioned(&request.run_id)
        .await?
        .context("flow run does not exist in this owner scope")?;
    if row.flow_id != request.flow_id {
        bail!("flow run belongs to a different workflow")
    }
    if row.status != "pending_approval" {
        bail!(
            "flow run is not parked for approval (status: {})",
            row.status
        )
    }
    if let Some(gate) = request
        .approvals
        .iter()
        .find(|gate| request.rejections.contains(gate))
    {
        bail!("gate '{gate}' cannot be both approved and rejected")
    }
    if !request
        .approvals
        .iter()
        .chain(&request.rejections)
        .any(|gate| row.pending_approvals.contains(gate))
    {
        bail!("resume decisions do not name a currently pending gate")
    }

    let flow = catalog
        .get_flow(&request.flow_id)
        .await?
        .context("saved workflow does not exist in this owner scope")?;
    if let Some(expected) = row.graph_hash.as_deref() {
        let current =
            tinyflows_catalog::graph_hash::compute_graph_hash(&flow.graph, flow.require_approval);
        if current.as_deref() != Some(expected) {
            refuse_stale_graph(connection, &request, &catalog, &row).await?;
            bail!(GRAPH_CHANGED)
        }
    } else {
        tracing::warn!(
            target: "flows",
            flow_id = %request.flow_id,
            run_id = %request.run_id,
            "parked run has no graph hash; allowing resume against its available workflow state"
        );
    }

    // Resume is an authoritative compatibility boundary for the graph the
    // approval was shown, including saved children resolved through this
    // owner-scoped Catalog. Older hosts rejected these graphs before calling
    // ResumeRun and settled the parked row as failed; keep that refusal and
    // lifecycle result inside the module that owns the Catalog and run row.
    if let Some(error) = compatibility_error(&catalog, &flow.graph).await {
        refuse_parked_run(connection, &request, &catalog, &row, "failed", &error).await?;
        bail!(error)
    }

    let snapshot = catalog.flow_run_snapshot(&request.run_id).await?;
    let legacy = snapshot
        .as_ref()
        .is_none_or(|snapshot| snapshot.get("legacy_resume") == Some(&Value::Bool(true)));
    let (graph, context, require_approval, thread_id, legacy_marker) = if legacy {
        if let Some(snapshot) = snapshot.as_ref() {
            let graph = snapshot
                .get("graph")
                .cloned()
                .context("legacy resume marker omitted graph")?;
            let graph = tinyflows::migrate::deserialize_graph(graph)
                .map_err(anyhow::Error::msg)
                .context("decoding legacy resume graph")?;
            let context: RunContext = serde_json::from_value(
                snapshot
                    .get("context")
                    .cloned()
                    .context("legacy resume marker omitted context")?,
            )
            .context("decoding legacy resume context")?;
            if context.flow_id != request.flow_id {
                bail!("legacy resume marker belongs to a different workflow")
            }
            let require_approval = snapshot
                .get("require_approval")
                .and_then(Value::as_bool)
                .context("legacy resume marker omitted approval policy")?;
            let thread_id = snapshot
                .get("thread_id")
                .and_then(Value::as_str)
                .context("legacy resume marker omitted checkpoint thread id")?
                .to_owned();
            if thread_id != row.thread_id {
                bail!("legacy resume marker checkpoint thread does not match the run row")
            }
            (
                graph,
                context,
                require_approval,
                thread_id,
                snapshot.clone(),
            )
        } else {
            let context = RunContext {
                flow_id: request.flow_id.clone(),
                origin: None,
            };
            let graph = flow.graph.clone();
            let marker = serde_json::json!({
                "legacy_resume": true,
                "graph": graph,
                "context": context,
                "require_approval": flow.require_approval,
                "thread_id": row.thread_id,
            });
            (
                flow.graph.clone(),
                RunContext {
                    flow_id: request.flow_id.clone(),
                    origin: None,
                },
                flow.require_approval,
                row.thread_id.clone(),
                marker,
            )
        }
    } else {
        let snapshot = snapshot.as_ref().expect("non-legacy snapshot was present");
        let context: RunContext = serde_json::from_value(
            snapshot
                .get("context")
                .cloned()
                .context("run snapshot omitted context")?,
        )
        .context("decoding run context snapshot")?;
        if context.flow_id != request.flow_id {
            bail!("run snapshot belongs to a different workflow")
        }
        let graph_json = snapshot
            .get("graph")
            .cloned()
            .context("run snapshot omitted graph")?;
        let graph = tinyflows::migrate::deserialize_graph(graph_json)
            .map_err(anyhow::Error::msg)
            .context("decoding pinned execution graph")?;
        let require_approval = snapshot
            .get("require_approval")
            .and_then(Value::as_bool)
            .context("run snapshot omitted approval policy")?;
        (
            graph,
            context,
            require_approval,
            row.thread_id.clone(),
            Value::Null,
        )
    };
    if let Some(error) = compatibility_error(&catalog, &graph).await {
        refuse_parked_run(connection, &request, &catalog, &row, "failed", &error).await?;
        bail!(error)
    }
    let compiled = compiler::compile(&graph).context("compiling pinned execution graph")?;
    let (inputs, trigger) = if legacy {
        // A legacy row has no trustworthy original trigger or declared-input
        // metadata. Resume execution consumes the durable checkpoint directly.
        (None, None)
    } else {
        let snapshot = snapshot.as_ref().expect("non-legacy snapshot was present");
        let inputs: Map<String, Value> = serde_json::from_value(
            snapshot
                .get("inputs")
                .cloned()
                .context("run snapshot omitted inputs")?,
        )
        .context("decoding run input snapshot")?;
        tinyflows::model::resolve_inputs(&compiled.graph.inputs, &inputs)
            .context("resolving pinned workflow inputs")?;
        let trigger_kind: FlowRunTrigger = serde_json::from_value(
            snapshot
                .get("trigger_kind")
                .cloned()
                .context("run snapshot omitted trigger kind")?,
        )
        .context("decoding run trigger kind")?;
        let trigger_data = snapshot
            .get("trigger_data")
            .cloned()
            .context("run snapshot omitted trigger data")?;
        (Some(inputs), Some((trigger_kind, trigger_data)))
    };
    let run = ActorRunRequest {
        run_id: request.run_id.clone(),
        context,
        caps_host: request.caps_host.clone(),
        storage: request.storage.clone(),
        timeout_ms: request.timeout_ms,
        trigger,
    };

    let documents: Arc<dyn DocumentStore> =
        Arc::new(Documents::new(connection.clone(), request.storage.callback.clone()).await?);
    let prepared = Prepared {
        catalog: catalog.clone(),
        compiled,
        inputs,
        checkpointer: DriverCheckpointer::new(documents),
        require_approval,
        checkpoint_thread_id: thread_id.clone(),
    };

    if legacy {
        let checkpoints = prepared
            .checkpointer
            .get_scoped(&thread_id, None, &[])
            .await
            .context("reading legacy parked run checkpoint")?;
        if checkpoints.is_none() {
            let error = "legacy parked run has no durable checkpoint";
            let failed = catalog
                .fail_legacy_parked_flow_run_for_resume(
                    &observed,
                    &chrono::Utc::now().to_rfc3339(),
                    &row.steps,
                    error,
                )
                .await?;
            if !failed {
                bail!("parked run changed while checking its legacy checkpoint")
            }
            settle_missing_legacy_checkpoint(
                &request,
                &catalog,
                &prepared.checkpointer,
                error,
                snapshot.as_ref(),
                &thread_id,
            )
            .await?;
            bail!(error)
        }
    }
    let run_key = key(&request.storage, &request.run_id);
    let active = reserve_resume(run_key)?;
    let claimed = match if legacy {
        catalog
            .claim_legacy_parked_flow_run_for_resume(&observed, &active.lease, legacy_marker)
            .await
    } else {
        catalog
            .claim_parked_flow_run_for_resume(&request.run_id, &active.lease)
            .await
    } {
        Ok(claimed) => claimed,
        Err(error) => {
            unregister(&active);
            return Err(error).context("claiming parked run for resume");
        }
    };
    if !claimed {
        unregister(&active);
        bail!("parked run was claimed by another lifecycle operation")
    }
    active
        .phase
        .store(RUNNING, std::sync::atomic::Ordering::Release);
    let (actor_ready, actor_admitted) = tokio::sync::oneshot::channel();
    tokio::spawn(super::actor::drive_resume(
        connection.clone(),
        run,
        prepared,
        Arc::clone(&active),
        actor_admitted,
        request,
        row.steps,
    ));
    active.admitted.notify_waiters();
    let _ = actor_ready.send(());
    Ok(ResumeRunResponse {
        run_id: active.key.run_id.clone(),
    })
}

async fn settle_missing_legacy_checkpoint(
    request: &ResumeRun,
    catalog: &FlowCatalogDocuments,
    checkpointer: &DriverCheckpointer<Value>,
    error: &str,
    snapshot: Option<&Value>,
    thread_id: &str,
) -> Result<()> {
    super::record_settled_summary(catalog, &request.run_id, "failed").await;
    let dedup_settlement = super::dedup_settlement::from_snapshot(
        &request.storage,
        catalog.clone(),
        snapshot,
        &request.run_id,
        false,
    )
    .await;
    if let Err(cleanup_error) = checkpointer.delete_thread(thread_id).await {
        tracing::warn!(%cleanup_error, run_id = %request.run_id, "missing legacy checkpoint cleanup failed");
    }
    let seq = catalog
        .flow_run_event_last_seq(&request.run_id)
        .await?
        .saturating_add(1);
    catalog
        .append_flow_run_event(
            &request.run_id,
            serde_json::to_value(RunEvent {
                seq,
                kind: "finished".into(),
                data: serde_json::to_value(super::events::finished_payload(
                    "failed",
                    Some(error.to_owned()),
                    None,
                    None,
                    false,
                ))?,
                graph_run_id: None,
            })?,
        )
        .await?;
    if !catalog
        .mark_flow_run_terminal_event(&request.run_id, seq)
        .await?
    {
        bail!("legacy resume failure terminal cursor was not committed")
    }
    drop(dedup_settlement);
    Ok(())
}

fn validate(request: &ResumeRun) -> Result<()> {
    if request.run_id.trim().is_empty() || request.flow_id.trim().is_empty() {
        bail!("run id and flow id must not be empty")
    }
    if request.timeout_ms == 0 {
        bail!("run timeout_ms must be greater than zero")
    }
    for (name, destination, object_path) in [
        (
            "CatalogStorage",
            request.storage.callback.destination.as_str(),
            request.storage.callback.object_path.as_str(),
        ),
        (
            "CapsHost",
            request.caps_host.callback.destination.as_str(),
            request.caps_host.callback.object_path.as_str(),
        ),
    ] {
        if destination.trim().is_empty() || object_path.trim().is_empty() {
            bail!("{name} callback address must not be empty")
        }
    }
    Ok(())
}

async fn refuse_stale_graph(
    connection: &tinybus::Connection,
    request: &ResumeRun,
    catalog: &FlowCatalogDocuments,
    row: &tinyflows_catalog::FlowRun,
) -> Result<()> {
    refuse_parked_run(
        connection,
        request,
        catalog,
        row,
        "cancelled",
        GRAPH_CHANGED,
    )
    .await?;
    Ok(())
}

async fn compatibility_error(
    catalog: &FlowCatalogDocuments,
    graph: &tinyflows::model::WorkflowGraph,
) -> Option<String> {
    if let Err(error) = tinyflows::compat::ensure_compatible(graph) {
        return Some(error);
    }
    let resolver = super::preparation::SavedWorkflowResolver {
        catalog: catalog.clone(),
    };
    tinyflows::compat::referenced_workflow_errors_async(graph, &resolver)
        .await
        .into_iter()
        .next()
        .map(|error| format!("incompatible_referenced_workflow: {error}"))
}

async fn refuse_parked_run(
    connection: &tinybus::Connection,
    request: &ResumeRun,
    catalog: &FlowCatalogDocuments,
    row: &tinyflows_catalog::FlowRun,
    status: &str,
    error: &str,
) -> Result<()> {
    let snapshot = match catalog.flow_run_snapshot(&row.id).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            tracing::warn!(%error, run_id = %row.id, "parked-run compatibility refusal could not read execution snapshot");
            None
        }
    };
    if !catalog
        .finish_parked_flow_run(
            &row.id,
            status,
            &chrono::Utc::now().to_rfc3339(),
            &row.steps,
            &[],
            Some(error),
            None,
        )
        .await?
    {
        return Ok(());
    }
    if let Some(active) = active(&key(&request.storage, &row.id))
        && active.phase.load(std::sync::atomic::Ordering::Acquire) == PARKED
    {
        unregister(&active);
    }
    super::record_settled_summary(catalog, &row.id, status).await;
    let dedup_settlement =
        super::sweeps::settle_snapshot_dedup(&request.storage, catalog, snapshot.as_ref(), &row.id)
            .await;
    match Documents::new(connection.clone(), request.storage.callback.clone()).await {
        Ok(documents) => {
            let checkpointer = DriverCheckpointer::<Value>::new(Arc::new(documents));
            if let Err(error) = checkpointer.delete_thread(&row.thread_id).await {
                tracing::warn!(%error, run_id = %row.id, "parked-run compatibility refusal checkpoint cleanup failed");
            }
        }
        Err(error) => {
            tracing::warn!(%error, run_id = %row.id, "opening refused-run checkpoint scope failed")
        }
    }
    let seq = catalog
        .flow_run_event_last_seq(&row.id)
        .await?
        .saturating_add(1);
    catalog
        .append_flow_run_event(
            &row.id,
            serde_json::to_value(tinyflows_bus::runs::RunEvent {
                seq,
                kind: "finished".into(),
                data: serde_json::to_value(super::events::finished_payload(
                    status,
                    Some(error.to_owned()),
                    None,
                    None,
                    false,
                ))?,
                graph_run_id: None,
            })?,
        )
        .await?;
    if !catalog.mark_flow_run_terminal_event(&row.id, seq).await? {
        bail!("parked-run refusal terminal event cursor was not committed")
    }
    drop(dedup_settlement);
    Ok(())
}

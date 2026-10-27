//! CAS-guarded parked and stale run cancellation.

use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;
use tinybus::Connection;
use tinyflows::graph::checkpoint::Checkpointer as _;
use tinyflows_bus::runs::{CancelRunResponse, RunControlRequest, RunEvent};
use tinyflows_drivers::{DriverCheckpointer, catalog::FlowCatalogDocuments};
use tinyflows_storage::DocumentStore;

use super::dedup_settlement::{DedupSettlementScope, from_snapshot};
use super::events::EventWriter;
use super::preparation::Prepared;
use super::{ActiveRun, RUNNING, unregister};
use crate::catalog_documents::Documents;

pub(super) async fn cancel_parked_or_stale(
    connection: &Connection,
    request: RunControlRequest,
    active: Option<Arc<ActiveRun>>,
) -> Result<CancelRunResponse> {
    if let Some(active) = &active
        && active.phase.load(std::sync::atomic::Ordering::Acquire) == RUNNING
    {
        active.token.cancel();
        return Ok(CancelRunResponse { signalled: true });
    }
    let docs =
        Arc::new(Documents::new(connection.clone(), request.storage.callback.clone()).await?);
    let document_port = Arc::clone(&docs) as Arc<dyn DocumentStore>;
    let catalog = FlowCatalogDocuments::new(Arc::clone(&document_port));
    let Some(run) = catalog.get_flow_run(&request.run_id).await? else {
        anyhow::bail!("flow run does not exist in this owner scope")
    };
    if let Some(lease) = catalog
        .flow_run_snapshot(&request.run_id)
        .await?
        .and_then(|snapshot| {
            snapshot
                .get("lease")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        && let Some(live) = super::active_lease(&lease)
        && !active
            .as_ref()
            .is_some_and(|candidate| Arc::ptr_eq(candidate, &live))
    {
        return Ok(CancelRunResponse { signalled: false });
    }
    if !matches!(run.status.as_str(), "running" | "pending_approval") {
        if let Some(active) = active {
            unregister(&active);
        }
        return Ok(CancelRunResponse { signalled: false });
    }
    let checkpointer = DriverCheckpointer::<Value>::new(document_port);
    let parked = run.status == "pending_approval";
    let error = if parked {
        "parked run cancelled"
    } else {
        "orphaned running run cancelled"
    };
    let now = chrono::Utc::now().to_rfc3339();
    let changed = if parked {
        catalog
            .finish_parked_flow_run(
                &request.run_id,
                "cancelled",
                &now,
                &run.steps,
                &[],
                Some(error),
                None,
            )
            .await?
    } else {
        catalog
            .finish_flow_run(
                &request.run_id,
                "cancelled",
                &now,
                &run.steps,
                &[],
                Some(error),
                None,
            )
            .await?
    };
    if !changed {
        return Ok(CancelRunResponse { signalled: false });
    }
    super::record_settled_summary(&catalog, &request.run_id, "cancelled").await;
    let dedup_settlement = cancel_snapshot(&catalog, &request.storage, &request.run_id).await;
    if let Err(error) = checkpointer.delete_thread(&run.thread_id).await {
        tracing::warn!(%error, run_id = %request.run_id, "parked/stale cancellation checkpoint cleanup failed");
    }
    append_cancelled_event(&catalog, &request.run_id, error).await?;
    drop(dedup_settlement);
    if let Some(active) = active {
        unregister(&active);
    }
    Ok(CancelRunResponse { signalled: true })
}

pub(super) async fn cancel_parked(
    prepared: &Prepared,
    storage: &tinyflows_bus::catalog::CatalogStorage,
    run_id: &str,
    writer: &EventWriter,
) -> Result<bool> {
    let Some(run) = prepared.catalog.get_flow_run(run_id).await? else {
        return Ok(false);
    };
    if run.status != "pending_approval" {
        return Ok(false);
    }
    let now = chrono::Utc::now().to_rfc3339();
    let changed = prepared
        .catalog
        .finish_parked_flow_run(
            run_id,
            "cancelled",
            &now,
            &run.steps,
            &[],
            Some("parked run cancelled"),
            None,
        )
        .await?;
    if !changed {
        return Ok(false);
    }
    super::record_settled_summary(&prepared.catalog, run_id, "cancelled").await;
    let dedup_settlement = cancel_snapshot(&prepared.catalog, storage, run_id).await;
    if let Err(error) = prepared.checkpointer.delete_thread(&run.thread_id).await {
        tracing::warn!(%error, run_id, "parked cancellation checkpoint cleanup failed");
    }
    let seq = writer
        .emit_terminal(super::events::finished_payload(
            "cancelled",
            Some("parked run cancelled".into()),
            None,
            None,
            false,
        ))
        .await?;
    if !prepared
        .catalog
        .mark_flow_run_terminal_event(run_id, seq)
        .await?
    {
        anyhow::bail!("parked cancellation terminal cursor was not committed")
    }
    drop(dedup_settlement);
    Ok(true)
}

async fn cancel_snapshot(
    catalog: &FlowCatalogDocuments,
    storage: &tinyflows_bus::catalog::CatalogStorage,
    run_id: &str,
) -> Option<DedupSettlementScope> {
    let snapshot = match catalog.flow_run_snapshot(run_id).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            tracing::warn!(%error, run_id, "cancelled run snapshot could not be read for dedup settlement");
            None
        }
    };
    from_snapshot(storage, catalog.clone(), snapshot.as_ref(), run_id, false).await
}

async fn append_cancelled_event(
    catalog: &FlowCatalogDocuments,
    run_id: &str,
    reason: &str,
) -> Result<()> {
    let seq = catalog
        .flow_run_event_last_seq(run_id)
        .await?
        .saturating_add(1);
    catalog
        .append_flow_run_event(
            run_id,
            serde_json::to_value(RunEvent {
                seq,
                kind: "finished".into(),
                data: serde_json::to_value(super::events::finished_payload(
                    "cancelled",
                    Some(reason.to_owned()),
                    None,
                    None,
                    false,
                ))?,
                graph_run_id: None,
            })?,
        )
        .await?;
    if !catalog.mark_flow_run_terminal_event(run_id, seq).await? {
        anyhow::bail!("cancellation terminal cursor was not committed")
    }
    Ok(())
}

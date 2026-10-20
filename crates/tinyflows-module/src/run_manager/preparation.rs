//! Run preflight and the scoped driver objects captured for one actor.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use tinybus::Connection;
use tinyflows::{
    caps::WorkflowResolver,
    compiler::{self, CompiledWorkflow},
    error::{EngineError, Result as EngineResult},
    model::{WorkflowGraph, resolve_inputs},
};
use tinyflows_bus::{catalog::CatalogStorage, runs::StartRun};
use tinyflows_drivers::{DriverCheckpointer, catalog::FlowCatalogDocuments};
use tinyflows_storage::DocumentStore;

use super::super::catalog_documents::Documents;

pub(super) struct Prepared {
    pub catalog: FlowCatalogDocuments,
    pub compiled: CompiledWorkflow,
    pub inputs: Option<Map<String, Value>>,
    pub checkpointer: DriverCheckpointer<Value>,
    pub require_approval: bool,
    pub checkpoint_thread_id: String,
}

pub(super) async fn catalog(
    connection: &Connection,
    storage: CatalogStorage,
) -> Result<FlowCatalogDocuments> {
    let documents = Arc::new(Documents::new(connection.clone(), storage.callback).await?);
    Ok(FlowCatalogDocuments::new(
        documents as Arc<dyn DocumentStore>,
    ))
}

pub(super) async fn prepare(connection: &Connection, request: &StartRun) -> Result<Prepared> {
    validate_addresses(request)?;
    let documents =
        Arc::new(Documents::new(connection.clone(), request.storage.callback.clone()).await?);
    let document_port = Arc::clone(&documents) as Arc<dyn DocumentStore>;
    let catalog = FlowCatalogDocuments::new(Arc::clone(&document_port));
    let saved = catalog
        .get_flow(&request.context.flow_id)
        .await?
        .context("saved workflow does not exist in this owner scope")?;

    let graph = tinyflows::migrate::deserialize_graph(request.graph.clone())
        .map_err(|error| anyhow::anyhow!("migrating workflow graph: {error}"))?;
    tinyflows::compat::ensure_compatible(&graph).map_err(anyhow::Error::msg)?;
    let resolver = SavedWorkflowResolver {
        catalog: catalog.clone(),
    };
    let referenced = tinyflows::compat::referenced_workflow_errors_async(&graph, &resolver).await;
    if let Some(error) = referenced.first() {
        bail!("{error}")
    }
    let compiled = compiler::compile(&graph).context("compiling workflow graph")?;
    resolve_inputs(&compiled.graph.inputs, &request.inputs)
        .context("resolving declared workflow inputs")?;

    Ok(Prepared {
        catalog,
        compiled,
        inputs: Some(request.inputs.clone()),
        checkpointer: DriverCheckpointer::new(document_port),
        require_approval: saved.require_approval,
        checkpoint_thread_id: request.run_id.clone(),
    })
}

impl Prepared {
    pub async fn persist_snapshot(&self, request: &StartRun, lease: &str) -> Result<()> {
        let graph = serde_json::to_value(&self.compiled.graph)?;
        let snapshot = json!({
            "graph": graph,
            "trigger_kind": request.trigger_kind,
            "trigger_data": request.trigger_data,
            "inputs": self.inputs,
            "context": request.context,
            "require_approval": self.require_approval,
            "timeout_ms": request.timeout_ms,
            "lease": lease,
        });
        self.catalog
            .set_flow_run_snapshot(&request.run_id, snapshot)
            .await
    }
}

fn validate_addresses(request: &StartRun) -> Result<()> {
    if request.run_id.trim().is_empty() {
        bail!("run id must not be empty")
    }
    if request.context.flow_id.trim().is_empty() {
        bail!("flow id must not be empty")
    }
    if request.timeout_ms == 0 {
        bail!("run timeout_ms must be greater than zero")
    }
    for (name, destination, object_path) in [
        (
            "CapsHost",
            request.caps_host.callback.destination.as_str(),
            request.caps_host.callback.object_path.as_str(),
        ),
        (
            "CatalogStorage",
            request.storage.callback.destination.as_str(),
            request.storage.callback.object_path.as_str(),
        ),
    ] {
        if destination.trim().is_empty() || object_path.trim().is_empty() {
            bail!("{name} callback address must not be empty")
        }
    }
    Ok(())
}

pub(super) struct SavedWorkflowResolver {
    pub(super) catalog: FlowCatalogDocuments,
}

#[async_trait]
impl WorkflowResolver for SavedWorkflowResolver {
    async fn resolve(&self, id: &str) -> EngineResult<WorkflowGraph> {
        self.catalog
            .get_flow(id)
            .await
            .map_err(|error| EngineError::Capability(error.to_string()))?
            .map(|flow| flow.graph)
            .ok_or_else(|| EngineError::Capability(format!("missing workflow {id}")))
    }
}

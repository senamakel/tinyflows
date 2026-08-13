//! Authenticated loopback WebSocket adapter and native workflow runner.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::Router;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Json, State, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};

use crate::browser::{
    BROWSER_PROTOCOL_VERSION, BrowserCancel, BrowserCancelType, BrowserError, BrowserErrorCode,
    BrowserEvent, BrowserRelay, BrowserRequest, BrowserResponse, ChromeToolInvoker,
    RoutingToolInvoker,
};
use crate::caps::Capabilities;
use crate::compiler::compile;
use crate::engine::{CancellationToken, run_cancellable_with_observer};
use crate::model::WorkflowGraph;
use crate::observability::{ExecutionStep, RunObserver, StepStatus};

use super::{
    Authenticator, CompanionControlRequest, CompanionControlResponse, PROTOCOL_SUBPROTOCOL,
    PairingSecret, RelayPolicy, RelayState, RunEvent, SharedTab, TabId, WebSocketHandshake,
    WorkflowSummary,
};

/// Configuration for the native Chrome companion.
#[derive(Clone)]
pub struct CompanionServerConfig {
    /// Loopback/deadline policy for the relay listener.
    pub policy: RelayPolicy,
    /// Exact Chrome extension id allowed by the WebSocket origin check.
    pub extension_id: String,
    /// Host-local pairing secret required in a WebSocket subprotocol.
    pub pairing_secret: PairingSecret,
    /// Directory containing workflow JSON files exposed to the side panel.
    /// Ignored for listing/running when `run_host` is set.
    pub workflows_dir: PathBuf,
    /// Host capabilities used for every non-browser effect.
    pub capabilities: Capabilities,
    /// Optional embedding-host seam. When set, workflow listing + execution
    /// (over both the WS control channel and the HTTP native endpoints) delegate
    /// to the host instead of `workflows_dir` + [`CompanionServer::start_workflow`].
    /// `None` preserves the built-in standalone behaviour.
    pub run_host: Option<Arc<dyn super::CompanionRunHost>>,
}

/// Errors produced by companion configuration, I/O, or workflow startup.
#[derive(Debug, thiserror::Error)]
pub enum CompanionServerError {
    /// Relay or tab policy rejected an operation.
    #[error("relay policy error: {0}")]
    Relay(#[from] super::RelayError),
    /// Pairing or extension-id configuration was invalid.
    #[error("authentication configuration error: {0}")]
    Authentication(#[from] std::io::Error),
    /// The loopback listener failed.
    #[error("listener error: {0}")]
    Listener(String),
    /// A workflow file could not be read, validated, or decoded.
    #[error("workflow error: {0}")]
    Workflow(String),
}

type PendingSender = oneshot::Sender<std::result::Result<BrowserResponse, BrowserError>>;

struct ServerInner {
    bind_addr: SocketAddr,
    native_secret: String,
    authenticator: Authenticator,
    relay: Mutex<RelayState>,
    outbound: Mutex<Option<mpsc::UnboundedSender<Message>>>,
    pending: tokio::sync::Mutex<HashMap<String, PendingSender>>,
    workflows_dir: PathBuf,
    capabilities: Capabilities,
    run_host: Option<Arc<dyn super::CompanionRunHost>>,
    runs: Mutex<HashMap<String, CancellationToken>>,
    next_session: AtomicU64,
    next_run: AtomicU64,
}

/// Loopback-only native companion used by the TinyFlows Chrome extension.
#[derive(Clone)]
pub struct CompanionServer {
    inner: Arc<ServerInner>,
}

mod api;

struct CompanionObserver {
    server: CompanionServer,
    run_id: String,
    node_kinds: HashMap<String, String>,
}

impl RunObserver for CompanionObserver {
    fn on_step_start(&self, node_id: &str) {
        let node_kind = self
            .node_kinds
            .get(node_id)
            .cloned()
            .unwrap_or_else(|| "unknown".into());
        self.server.send_json(&RunEvent::StepStarted {
            protocol_version: BROWSER_PROTOCOL_VERSION,
            run_id: self.run_id.clone(),
            node_id: node_id.to_owned(),
            node_kind,
        });
    }

    fn on_step_finish(&self, step: &ExecutionStep) {
        let node_kind = self
            .node_kinds
            .get(&step.node_id)
            .cloned()
            .unwrap_or_else(|| "unknown".into());
        self.server.send_json(&RunEvent::StepCompleted {
            protocol_version: BROWSER_PROTOCOL_VERSION,
            run_id: self.run_id.clone(),
            node_id: step.node_id.clone(),
            node_kind,
            status: match step.status {
                StepStatus::Success => "success",
                StepStatus::Error => "error",
            }
            .into(),
            duration_ms: u64::try_from(step.duration_ms).unwrap_or(u64::MAX),
        });
    }
}

struct SocketRelay {
    inner: Arc<ServerInner>,
}

#[async_trait]
impl BrowserRelay for SocketRelay {
    async fn execute(
        &self,
        request: BrowserRequest,
    ) -> std::result::Result<BrowserResponse, BrowserError> {
        self.inner
            .relay
            .lock()
            .map_err(|_| browser_error(BrowserErrorCode::RelayDisconnected, "relay unavailable"))?
            .begin_action(&request, Instant::now())
            .map_err(|error| browser_error(error.browser_code(), &error.to_string()))?;
        let (sender, receiver) = oneshot::channel();
        self.inner
            .pending
            .lock()
            .await
            .insert(request.request_id.clone(), sender);
        let wire = serde_json::to_string(&request)
            .map_err(|error| browser_error(BrowserErrorCode::InvalidRequest, &error.to_string()))?;
        let outbound = self
            .inner
            .outbound
            .lock()
            .map_err(|_| browser_error(BrowserErrorCode::RelayDisconnected, "relay unavailable"))?
            .clone()
            .ok_or_else(|| {
                browser_error(
                    BrowserErrorCode::RelayDisconnected,
                    "extension is disconnected",
                )
            })?;
        if outbound.send(Message::Text(wire.into())).is_err() {
            self.inner.pending.lock().await.remove(&request.request_id);
            return Err(browser_error(
                BrowserErrorCode::RelayDisconnected,
                "extension connection closed",
            ));
        }
        match tokio::time::timeout(Duration::from_millis(request.timeout_ms), receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(browser_error(
                BrowserErrorCode::RelayDisconnected,
                "relay response channel closed",
            )),
            Err(_) => {
                self.inner.pending.lock().await.remove(&request.request_id);
                if let Ok(mut relay) = self.inner.relay.lock() {
                    let _ = relay.expire_actions(Instant::now());
                }
                send_cancel(&self.inner, &request.request_id);
                Err(browser_error(
                    BrowserErrorCode::ActionTimeout,
                    "browser action exceeded its deadline",
                ))
            }
        }
    }
}

fn send_cancel(inner: &ServerInner, request_id: &str) {
    let cancel = BrowserCancel {
        protocol_version: BROWSER_PROTOCOL_VERSION,
        message_type: BrowserCancelType::BrowserCancel,
        request_id: request_id.to_owned(),
    };
    let Ok(wire) = serde_json::to_string(&cancel) else {
        return;
    };
    if let Ok(outbound) = inner.outbound.lock()
        && let Some(sender) = outbound.as_ref()
    {
        let _ = sender.send(Message::Text(wire.into()));
    }
}

mod handlers;
use handlers::{
    browser_error, list_workflows, load_workflow, lock_error, native_run, native_tabs,
    native_workflows, upgrade,
};

#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;

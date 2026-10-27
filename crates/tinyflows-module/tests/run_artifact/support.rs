use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tinybus::Connection;
use tinyflows_bus::{
    catalog::{self, CatalogStorage},
    runs::{
        self, AgentDefinition, AgentEventsRequest, AgentEventsResponse, AgentProgressEvent,
        AgentRunHandle, AgentRunOutcome, AgentRunRequest,
    },
};
use tinyflows_storage::{DocumentStore, Scope, StorageBackend};
use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;
use tokio::sync::Notify;

#[path = "graphs.rs"]
mod graphs;
pub(super) use graphs::{dedup_graph, graph, parked_graph, request};
#[path = "flow_helpers.rs"]
mod flow_helpers;
pub(super) use flow_helpers::{
    create_flow, create_flow_with_graph, serve_agent, serve_failing_agent,
};
#[path = "document_callback.rs"]
mod document_callback;
pub(super) use document_callback::OwnerDocuments;

pub(super) struct AgentCallback {
    pub(super) started: Notify,
    pub(super) release: Notify,
    pub(super) approval_started: Notify,
    pub(super) calls: Arc<AtomicUsize>,
    pub(super) fail_start: bool,
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CapsHost")]
impl AgentCallback {
    async fn resolve_agent(&self, _agent_ref: String) -> tinybus::Result<Option<AgentDefinition>> {
        Ok(None)
    }

    async fn resolve_tools(
        &self,
        _request: runs::ResolveToolsRequest,
    ) -> tinybus::Result<Vec<runs::ToolDescriptor>> {
        Ok(Vec::new())
    }

    async fn approval_decide(
        &self,
        _request: runs::ApprovalRequest,
    ) -> tinybus::Result<runs::ApprovalOutcome> {
        self.approval_started.notify_one();
        Ok(runs::ApprovalOutcome::Pending)
    }

    async fn approval_cancel(&self, _request: runs::ApprovalCancelRequest) -> tinybus::Result<()> {
        Ok(())
    }

    async fn agent_run(&self, _request: AgentRunRequest) -> tinybus::Result<AgentRunHandle> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        if self.fail_start {
            return Err(tinybus::Error::failed("injected agent callback failure"));
        }
        let _ =
            tokio::time::timeout(std::time::Duration::from_secs(10), self.release.notified()).await;
        Ok(AgentRunHandle {
            handle: "agent-handle".into(),
        })
    }

    async fn agent_cancel(&self, _request: runs::AgentCancelRequest) -> tinybus::Result<()> {
        Ok(())
    }

    async fn agent_events(
        &self,
        _request: AgentEventsRequest,
    ) -> tinybus::Result<AgentEventsResponse> {
        Ok(AgentEventsResponse {
            events: vec![AgentProgressEvent {
                seq: 1,
                at_ms: 1,
                kind: "thinking".into(),
                text: "working".into(),
            }],
            done: true,
            outcome: Some(AgentRunOutcome {
                stop: runs::StopReason::Finished,
                text: Some("done".into()),
                json: json!({}),
                raw: json!({}),
                usage: None,
                transcript: Vec::new(),
            }),
        })
    }
}

pub(super) struct Owner {
    pub(super) storage: CatalogStorage,
    pub(super) caps: runs::CallbackAddress,
    pub(super) docs: Arc<dyn DocumentStore>,
}

pub(super) async fn owner(
    connection: &Connection,
    name: &str,
    terminal_gate: Option<(Arc<Notify>, Arc<Notify>)>,
    run_insert_gate: Option<(Arc<AtomicBool>, Arc<Notify>, Arc<Notify>)>,
    fail_snapshot: Option<Arc<AtomicBool>>,
    fail_checkpoint_delete: Option<Arc<AtomicBool>>,
) -> Owner {
    let scoped = MemoryStorage::new()
        .for_scope(&Scope::new(name).unwrap())
        .unwrap();
    let docs = scoped.documents().clone();
    let path = format!("/test/catalog/{}", name.replace('-', "_"));
    connection
        .serve_at(
            path.parse().unwrap(),
            OwnerDocuments {
                docs: docs.clone(),
                terminal_write_started: terminal_gate.as_ref().map(|(started, _)| started.clone()),
                release_terminal_write: terminal_gate.as_ref().map(|(_, release)| release.clone()),
                terminal_write_pending: terminal_gate
                    .as_ref()
                    .map(|_| Arc::new(AtomicBool::new(true))),
                fail_run_insert: run_insert_gate.as_ref().map(|(fail, _, _)| fail.clone()),
                run_insert_started: run_insert_gate
                    .as_ref()
                    .map(|(_, started, _)| started.clone()),
                release_run_insert: run_insert_gate.map(|(_, _, release)| release),
                fail_snapshot,
                fail_checkpoint_delete,
                fail_checkpoint_query: None,
                fail_parked_expiry: None,
                cleanup_started: None,
                release_cleanup: None,
                cleanup_pending: None,
            },
        )
        .await
        .unwrap();
    connection
        .request_name("test.CatalogStorage")
        .await
        .unwrap();
    Owner {
        storage: CatalogStorage {
            callback: catalog::CallbackAddress {
                destination: "test.CatalogStorage".into(),
                object_path: path,
            },
        },
        caps: runs::CallbackAddress {
            destination: "test.CapsHost".into(),
            object_path: format!("/test/caps/{}", name.replace('-', "_")),
        },
        docs,
    }
}

pub(super) async fn owner_with_checkpoint_query_failure(
    connection: &Connection,
    name: &str,
) -> (Owner, Arc<AtomicBool>) {
    let scoped = MemoryStorage::new()
        .for_scope(&Scope::new(name).unwrap())
        .unwrap();
    let docs = scoped.documents().clone();
    let path = format!("/test/catalog/{}", name.replace('-', "_"));
    let fail_checkpoint_query = Arc::new(AtomicBool::new(false));
    connection
        .serve_at(
            path.parse().unwrap(),
            OwnerDocuments {
                docs: docs.clone(),
                terminal_write_started: None,
                release_terminal_write: None,
                terminal_write_pending: None,
                fail_run_insert: None,
                run_insert_started: None,
                release_run_insert: None,
                fail_snapshot: None,
                fail_checkpoint_delete: None,
                fail_checkpoint_query: Some(fail_checkpoint_query.clone()),
                fail_parked_expiry: None,
                cleanup_started: None,
                release_cleanup: None,
                cleanup_pending: None,
            },
        )
        .await
        .unwrap();
    connection
        .request_name("test.CatalogStorage")
        .await
        .unwrap();
    (
        Owner {
            storage: CatalogStorage {
                callback: catalog::CallbackAddress {
                    destination: "test.CatalogStorage".into(),
                    object_path: path,
                },
            },
            caps: runs::CallbackAddress {
                destination: "test.CapsHost".into(),
                object_path: format!("/test/caps/{}", name.replace('-', "_")),
            },
            docs,
        },
        fail_checkpoint_query,
    )
}

pub(super) async fn owner_with_lifecycle_gates(
    connection: &Connection,
    name: &str,
    expiry_failure_id: Option<&str>,
    cleanup_gate: Option<(Arc<Notify>, Arc<Notify>)>,
) -> Owner {
    let scoped = MemoryStorage::new()
        .for_scope(&Scope::new(name).unwrap())
        .unwrap();
    let docs = scoped.documents().clone();
    let path = format!("/test/catalog/{}", name.replace('-', "_"));
    let expiry_failure =
        expiry_failure_id.map(|id| (id.to_owned(), Arc::new(AtomicBool::new(true))));
    connection
        .serve_at(
            path.parse().unwrap(),
            OwnerDocuments {
                docs: docs.clone(),
                terminal_write_started: None,
                release_terminal_write: None,
                terminal_write_pending: None,
                fail_run_insert: None,
                run_insert_started: None,
                release_run_insert: None,
                fail_snapshot: None,
                fail_checkpoint_delete: None,
                fail_checkpoint_query: None,
                fail_parked_expiry: expiry_failure,
                cleanup_started: cleanup_gate.as_ref().map(|(started, _)| started.clone()),
                release_cleanup: cleanup_gate.as_ref().map(|(_, release)| release.clone()),
                cleanup_pending: cleanup_gate
                    .as_ref()
                    .map(|_| Arc::new(AtomicBool::new(true))),
            },
        )
        .await
        .unwrap();
    connection
        .request_name("test.CatalogStorage")
        .await
        .unwrap();
    Owner {
        storage: CatalogStorage {
            callback: catalog::CallbackAddress {
                destination: "test.CatalogStorage".into(),
                object_path: path,
            },
        },
        caps: runs::CallbackAddress {
            destination: "test.CapsHost".into(),
            object_path: format!("/test/caps/{}", name.replace('-', "_")),
        },
        docs,
    }
}

pub(super) async fn alias_owner(
    connection: &Connection,
    name: &str,
    docs: Arc<dyn DocumentStore>,
) -> Owner {
    let path = format!("/test/catalog_alias/{}", name.replace('-', "_"));
    connection
        .serve_at(
            path.parse().unwrap(),
            OwnerDocuments {
                docs: docs.clone(),
                terminal_write_started: None,
                release_terminal_write: None,
                terminal_write_pending: None,
                fail_run_insert: None,
                run_insert_started: None,
                release_run_insert: None,
                fail_snapshot: None,
                fail_checkpoint_delete: None,
                fail_checkpoint_query: None,
                fail_parked_expiry: None,
                cleanup_started: None,
                release_cleanup: None,
                cleanup_pending: None,
            },
        )
        .await
        .unwrap();
    Owner {
        storage: CatalogStorage {
            callback: catalog::CallbackAddress {
                destination: "test.CatalogStorage".into(),
                object_path: path,
            },
        },
        caps: runs::CallbackAddress {
            destination: "test.CapsHost".into(),
            object_path: format!("/test/caps/{}", name.replace('-', "_")),
        },
        docs,
    }
}

pub(super) async fn wait_notify(notify: &Notify, boundary: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(10), notify.notified())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {boundary}"));
}

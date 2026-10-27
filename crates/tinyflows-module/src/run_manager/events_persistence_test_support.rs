use async_trait::async_trait;
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tinybus::Connection;
use tinyflows_bus::catalog::{self, CatalogStorage};
use tinyflows_bus::runs::RunControlRequest;
use tinyflows_drivers::catalog::FlowCatalogDocuments;
use tinyflows_storage::{
    Capabilities, CollectionSpec, DocumentStore, Filter, Page, Precondition, Query, Scope, Sort,
    StorageBackend, StorageError, Version, Versioned, WriteOptions,
};
use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;

pub(super) struct TestSetup {
    pub(super) catalog: FlowCatalogDocuments,
    pub(super) run_id: String,
    pub(super) module: Connection,
    pub(super) _host: Connection,
    pub(super) storage: CatalogStorage,
}

pub(super) async fn setup(mode: FaultMode, status: &str) -> TestSetup {
    let scoped = MemoryStorage::new()
        .for_scope(&Scope::new("local").expect("valid scope"))
        .unwrap();
    let docs = Arc::new(FaultingDocuments {
        inner: scoped.documents().clone(),
        mode,
        output_attempts: AtomicUsize::new(0),
        lose_ack_once: AtomicBool::new(true),
        late_original: Mutex::new(None),
    });
    let catalog = FlowCatalogDocuments::new(docs.clone());
    let graph = tinyflows::migrate::deserialize_graph(json!({
        "nodes":[{"id":"trigger","name":"Trigger","kind":"trigger","config":{}}],
        "edges":[]
    }))
    .unwrap();
    let flow = catalog
        .create_flow("terminal event test".into(), graph, false, true)
        .await
        .unwrap();
    let run_id = format!("terminal-{}", uuid::Uuid::new_v4());
    catalog
        .insert_flow_run(&run_id, &flow.id, &run_id, "2026-01-01T00:00:00Z")
        .await
        .unwrap();
    catalog
        .finish_flow_run(
            &run_id,
            status,
            "2026-01-01T00:00:01Z",
            &[],
            &[],
            None,
            None,
        )
        .await
        .unwrap();

    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let module = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    host.serve_at("/terminal_docs".parse().unwrap(), StorageReader(docs))
        .await
        .unwrap();
    host.request_name("test.TerminalCatalogStorage")
        .await
        .unwrap();
    TestSetup {
        catalog,
        run_id,
        module,
        _host: host,
        storage: CatalogStorage {
            callback: catalog::CallbackAddress {
                destination: "test.TerminalCatalogStorage".into(),
                object_path: "/terminal_docs".into(),
            },
        },
    }
}

pub(super) async fn poll(setup: &TestSetup) -> tinyflows_bus::runs::PollRun {
    super::super::super::poll(
        &setup.module,
        RunControlRequest {
            storage: setup.storage.clone(),
            run_id: setup.run_id.clone(),
            cursor: 0,
        },
    )
    .await
    .unwrap()
}

#[derive(Clone, Copy)]
pub(super) enum FaultMode {
    LimitOutcomeDocument(usize),
    LimitOutputThenLoseFallbackAck(usize),
    RejectOutputThenOriginalWins,
    CommitThenLoseAck,
    RejectEveryTerminalWrite,
}

struct FaultingDocuments {
    inner: Arc<dyn DocumentStore>,
    mode: FaultMode,
    output_attempts: AtomicUsize,
    lose_ack_once: AtomicBool,
    late_original: Mutex<Option<Value>>,
}

impl std::fmt::Debug for FaultingDocuments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultingDocuments").finish_non_exhaustive()
    }
}

impl FaultingDocuments {
    fn outcome_event(collection: &str, doc: &Value) -> bool {
        collection == "flows.runs"
            && doc["events"].as_array().is_some_and(|events| {
                events.last().is_some_and(|event| {
                    matches!(event["kind"].as_str(), Some("finished" | "parked"))
                })
            })
    }

    fn outcome_output_event(collection: &str, doc: &Value) -> bool {
        collection == "flows.runs"
            && doc["events"].as_array().is_some_and(|events| {
                events.last().is_some_and(|event| {
                    matches!(event["kind"].as_str(), Some("finished" | "parked"))
                        && event["data"]
                            .get("output")
                            .is_some_and(|value| !value.is_null())
                })
            })
    }

    fn terminal_output_event(collection: &str, doc: &Value) -> bool {
        collection == "flows.runs"
            && doc["events"].as_array().is_some_and(|events| {
                events.last().is_some_and(|event| {
                    event["kind"] == "finished"
                        && event["data"]
                            .get("output")
                            .is_some_and(|value| !value.is_null())
                })
            })
    }

    fn terminal_event(collection: &str, doc: &Value) -> bool {
        collection == "flows.runs"
            && doc["events"].as_array().is_some_and(|events| {
                events
                    .last()
                    .is_some_and(|event| event["kind"] == "finished")
            })
    }
}

#[async_trait]
impl DocumentStore for FaultingDocuments {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn ensure_collection(&self, spec: &CollectionSpec) -> tinyflows_storage::Result<()> {
        self.inner.ensure_collection(spec).await
    }

    async fn get(
        &self,
        collection: &str,
        id: &str,
    ) -> tinyflows_storage::Result<Option<Versioned<Value>>> {
        self.inner.get(collection, id).await
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
    ) -> tinyflows_storage::Result<Version> {
        self.put_with(collection, id, doc, precondition, WriteOptions::default())
            .await
    }

    async fn put_with(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
        options: WriteOptions,
    ) -> tinyflows_storage::Result<Version> {
        if Self::outcome_event(collection, &doc) {
            match self.mode {
                FaultMode::LimitOutcomeDocument(max_bytes)
                    if Self::outcome_output_event(collection, &doc)
                        && serde_json::to_vec(&doc).unwrap().len() > max_bytes =>
                {
                    self.output_attempts.fetch_add(1, Ordering::SeqCst);
                    return Err(StorageError::backend(
                        "injected terminal document size limit",
                    ));
                }
                FaultMode::LimitOutputThenLoseFallbackAck(max_bytes)
                    if Self::terminal_output_event(collection, &doc)
                        && serde_json::to_vec(&doc).unwrap().len() > max_bytes =>
                {
                    self.output_attempts.fetch_add(1, Ordering::SeqCst);
                    return Err(StorageError::backend(
                        "injected terminal document size limit",
                    ));
                }
                FaultMode::RejectOutputThenOriginalWins
                    if Self::terminal_output_event(collection, &doc)
                        && doc.get("terminal_event_seq").is_none() =>
                {
                    let original = doc["events"].as_array().unwrap().last().unwrap().clone();
                    *self
                        .late_original
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(original);
                    self.output_attempts.fetch_add(1, Ordering::SeqCst);
                    return Err(StorageError::backend(
                        "injected delayed original terminal write",
                    ));
                }
                FaultMode::RejectOutputThenOriginalWins
                    if Self::terminal_event(collection, &doc)
                        && doc.get("terminal_event_seq").is_none()
                        && !Self::terminal_output_event(collection, &doc) =>
                {
                    let original = self
                        .late_original
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .clone()
                        .expect("the output-bearing terminal write was rejected first");
                    let stored = self
                        .inner
                        .get(collection, id)
                        .await?
                        .expect("the settled run row exists");
                    let mut winner = stored.doc.clone();
                    winner["events"] = json!([original]);
                    let _version = self
                        .inner
                        .put_with(collection, id, winner, stored.unchanged(), options)
                        .await?;
                    return Err(StorageError::conflict(
                        "injected original terminal payload winning the fallback race",
                    ));
                }
                FaultMode::LimitOutputThenLoseFallbackAck(_)
                    if Self::terminal_event(collection, &doc)
                        && !Self::terminal_output_event(collection, &doc)
                        && self.lose_ack_once.swap(false, Ordering::SeqCst) =>
                {
                    let _version = self
                        .inner
                        .put_with(collection, id, doc, precondition, options)
                        .await?;
                    self.output_attempts.fetch_add(1, Ordering::SeqCst);
                    return Err(StorageError::unavailable(
                        "injected lost acknowledgement after fallback commit",
                    ));
                }
                FaultMode::CommitThenLoseAck
                    if Self::terminal_output_event(collection, &doc)
                        && self.lose_ack_once.swap(false, Ordering::SeqCst) =>
                {
                    let _version = self
                        .inner
                        .put_with(collection, id, doc, precondition, options)
                        .await?;
                    self.output_attempts.fetch_add(1, Ordering::SeqCst);
                    return Err(StorageError::unavailable(
                        "injected lost acknowledgement after terminal commit",
                    ));
                }
                FaultMode::RejectEveryTerminalWrite => {
                    if !Self::terminal_event(collection, &doc) {
                        return self
                            .inner
                            .put_with(collection, id, doc, precondition, options)
                            .await;
                    }
                    return Err(StorageError::unavailable("injected terminal write failure"));
                }
                _ => {}
            }
        }
        self.inner
            .put_with(collection, id, doc, precondition, options)
            .await
    }

    async fn delete(
        &self,
        collection: &str,
        id: &str,
        precondition: Precondition,
    ) -> tinyflows_storage::Result<bool> {
        self.inner.delete(collection, id, precondition).await
    }

    async fn query(
        &self,
        collection: &str,
        query: &Query,
    ) -> tinyflows_storage::Result<Page<Versioned<Value>>> {
        self.inner.query(collection, query).await
    }

    async fn count(&self, collection: &str, filter: &Filter) -> tinyflows_storage::Result<u64> {
        self.inner.count(collection, filter).await
    }

    async fn delete_where(
        &self,
        collection: &str,
        filter: &Filter,
    ) -> tinyflows_storage::Result<u64> {
        self.inner.delete_where(collection, filter).await
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &Value,
    ) -> tinyflows_storage::Result<Option<Versioned<Value>>> {
        self.inner.claim(collection, filter, sort, patch).await
    }

    async fn drop_collection(&self, collection: &str) -> tinyflows_storage::Result<()> {
        self.inner.drop_collection(collection).await
    }
}

struct StorageReader(Arc<FaultingDocuments>);

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CatalogStorage")]
impl StorageReader {
    async fn doc_ensure(&self, request: Value) -> tinybus::Result<Value> {
        let spec: CollectionSpec = serde_json::from_value(request["spec"].clone())?;
        Ok(reply(self.0.ensure_collection(&spec).await))
    }

    async fn doc_get(&self, request: Value) -> tinybus::Result<Value> {
        let result = self
            .0
            .get(
                request["collection"].as_str().unwrap(),
                request["id"].as_str().unwrap(),
            )
            .await;
        Ok(reply(result))
    }
}

fn reply<T: serde::Serialize>(result: tinyflows_storage::Result<T>) -> Value {
    match result {
        Ok(value) => json!({"value":serde_json::to_value(value).unwrap()}),
        Err(error) => json!({"error":{
            "kind":error.kind().to_string().replace(' ', "_"),
            "message":error.to_string()
        }}),
    }
}

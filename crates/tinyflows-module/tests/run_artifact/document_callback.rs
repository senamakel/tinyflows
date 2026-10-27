use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tinyflows_storage::DocumentStore;
use tokio::sync::Notify;

pub(crate) struct OwnerDocuments {
    pub(super) docs: Arc<dyn DocumentStore>,
    pub(super) terminal_write_started: Option<Arc<Notify>>,
    pub(super) release_terminal_write: Option<Arc<Notify>>,
    pub(super) terminal_write_pending: Option<Arc<AtomicBool>>,
    pub(super) fail_run_insert: Option<Arc<AtomicBool>>,
    pub(super) run_insert_started: Option<Arc<Notify>>,
    pub(super) release_run_insert: Option<Arc<Notify>>,
    pub(super) fail_snapshot: Option<Arc<AtomicBool>>,
    pub(super) fail_checkpoint_delete: Option<Arc<AtomicBool>>,
    pub(super) fail_checkpoint_query: Option<Arc<AtomicBool>>,
    pub(super) fail_parked_expiry: Option<(String, Arc<AtomicBool>)>,
    pub(super) cleanup_started: Option<Arc<Notify>>,
    pub(super) release_cleanup: Option<Arc<Notify>>,
    pub(super) cleanup_pending: Option<Arc<AtomicBool>>,
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CatalogStorage")]
impl OwnerDocuments {
    async fn doc_ensure(&self, request: Value) -> tinybus::Result<Value> {
        Ok(reply(
            self.docs
                .ensure_collection(&decode(&request["spec"])?)
                .await,
        ))
    }

    async fn doc_get(&self, request: Value) -> tinybus::Result<Value> {
        Ok(reply(
            self.docs
                .get(text(&request, "collection"), text(&request, "id"))
                .await,
        ))
    }

    async fn doc_put(&self, request: Value) -> tinybus::Result<Value> {
        if request["collection"] == "flows.runs"
            && request["doc"]["status"] == "cancelled"
            && request["doc"]["error"]
                .as_str()
                .is_some_and(|error| error.starts_with("parked run expired"))
            && self.fail_parked_expiry.as_ref().is_some_and(|(id, fail)| {
                id == text(&request, "id") && fail.swap(false, Ordering::SeqCst)
            })
        {
            return Ok(json!({
                "error": {
                    "kind": "unavailable",
                    "message": "injected parked expiry failure"
                }
            }));
        }
        if request["collection"] == "flows.runs"
            && request["doc"]["status"] == "running"
            && self
                .fail_run_insert
                .as_ref()
                .is_some_and(|fail| fail.swap(false, Ordering::SeqCst))
        {
            if let Some(started) = &self.run_insert_started {
                started.notify_one();
            }
            if let Some(release) = &self.release_run_insert {
                let _ =
                    tokio::time::timeout(std::time::Duration::from_secs(10), release.notified())
                        .await;
            }
            return Ok(json!({
                "error": {
                    "kind": "unavailable",
                    "message": "injected run-row insert failure"
                }
            }));
        }
        if request["collection"] == "flows.runs"
            && request["doc"].get("execution_snapshot").is_some()
            && self
                .fail_snapshot
                .as_ref()
                .is_some_and(|fail| fail.swap(false, Ordering::SeqCst))
        {
            return Ok(json!({
                "error": {
                    "kind": "unavailable",
                    "message": "injected execution-snapshot failure"
                }
            }));
        }
        if let Some(events) = request["doc"]["events"].as_array()
            && events
                .last()
                .is_some_and(|event| event["kind"] == "finished")
            && self
                .terminal_write_pending
                .as_ref()
                .is_some_and(|pending| pending.swap(false, Ordering::SeqCst))
            && let (Some(started), Some(release)) =
                (&self.terminal_write_started, &self.release_terminal_write)
        {
            started.notify_one();
            let _ =
                tokio::time::timeout(std::time::Duration::from_secs(10), release.notified()).await;
        }
        Ok(reply(
            self.docs
                .put(
                    text(&request, "collection"),
                    text(&request, "id"),
                    request["doc"].clone(),
                    decode(&request["precondition"])?,
                )
                .await,
        ))
    }

    async fn doc_delete(&self, request: Value) -> tinybus::Result<Value> {
        if request["collection"] == "tsd.kv"
            && self
                .cleanup_pending
                .as_ref()
                .is_some_and(|pending| pending.swap(false, Ordering::SeqCst))
            && let (Some(started), Some(release)) = (&self.cleanup_started, &self.release_cleanup)
        {
            started.notify_one();
            let _ =
                tokio::time::timeout(std::time::Duration::from_secs(10), release.notified()).await;
        }
        if request["collection"] == "graph.checkpoints"
            && self
                .fail_checkpoint_delete
                .as_ref()
                .is_some_and(|fail| fail.swap(false, Ordering::SeqCst))
        {
            return Ok(json!({
                "error": {
                    "kind": "unavailable",
                    "message": "injected checkpoint delete failure"
                }
            }));
        }
        Ok(reply(
            self.docs
                .delete(
                    text(&request, "collection"),
                    text(&request, "id"),
                    decode(&request["precondition"])?,
                )
                .await,
        ))
    }

    async fn doc_query(&self, request: Value) -> tinybus::Result<Value> {
        if request["collection"] == "graph.checkpoints"
            && self
                .fail_checkpoint_query
                .as_ref()
                .is_some_and(|fail| fail.swap(false, Ordering::SeqCst))
        {
            return Ok(json!({
                "error": {
                    "kind": "unavailable",
                    "message": "injected checkpoint lookup failure"
                }
            }));
        }
        Ok(reply(
            self.docs
                .query(text(&request, "collection"), &decode(&request["query"])?)
                .await,
        ))
    }

    async fn doc_delete_where(&self, request: Value) -> tinybus::Result<Value> {
        if request["collection"] == "graph.checkpoints"
            && self
                .fail_checkpoint_delete
                .as_ref()
                .is_some_and(|fail| fail.swap(false, Ordering::SeqCst))
        {
            return Ok(json!({
                "error": {
                    "kind": "unavailable",
                    "message": "injected checkpoint delete-where failure"
                }
            }));
        }
        Ok(reply(
            self.docs
                .delete_where(text(&request, "collection"), &decode(&request["filter"])?)
                .await,
        ))
    }
}

fn text<'a>(value: &'a Value, field: &str) -> &'a str {
    value[field]
        .as_str()
        .expect("valid document callback field")
}

fn decode<T: DeserializeOwned>(value: &Value) -> tinybus::Result<T> {
    serde_json::from_value(value.clone()).map_err(tinybus::Error::from)
}

fn reply(value: tinyflows_storage::Result<impl Serialize>) -> Value {
    match value {
        Ok(value) => json!({"value":serde_json::to_value(value).unwrap()}),
        Err(error) => json!({
            "error": {
                "kind": error.kind().to_string().replace(' ', "_"),
                "message": error.to_string()
            }
        }),
    }
}

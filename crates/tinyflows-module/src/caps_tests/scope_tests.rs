use super::*;

use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::sync::Arc;
use tinyflows_bus::catalog::{self, CatalogStorage};
use tinyflows_drivers::catalog::FlowCatalogDocuments;
use tinyflows_storage::{DocumentStore,  Scope, StorageBackend};

struct DocumentsHost(Arc<dyn DocumentStore>);

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CatalogStorage")]
impl DocumentsHost {
    async fn doc_ensure(&self, request: Value) -> tinybus::Result<Value> {
        Ok(reply(
            self.0.ensure_collection(&decode(&request["spec"])?).await,
        ))
    }
    async fn doc_get(&self, request: Value) -> tinybus::Result<Value> {
        Ok(reply(
            self.0
                .get(text(&request, "collection"), text(&request, "id"))
                .await,
        ))
    }
    async fn doc_put(&self, request: Value) -> tinybus::Result<Value> {
        Ok(reply(
            self.0
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
        Ok(reply(
            self.0
                .delete(
                    text(&request, "collection"),
                    text(&request, "id"),
                    decode(&request["precondition"])?,
                )
                .await,
        ))
    }
    async fn doc_query(&self, request: Value) -> tinybus::Result<Value> {
        Ok(reply(
            self.0
                .query(text(&request, "collection"), &decode(&request["query"])?)
                .await,
        ))
    }
    async fn doc_delete_where(&self, request: Value) -> tinybus::Result<Value> {
        Ok(reply(
            self.0
                .delete_where(text(&request, "collection"), &decode(&request["filter"])?)
                .await,
        ))
    }
}

fn text<'a>(value: &'a Value, field: &str) -> &'a str {
    value[field]
        .as_str()
        .expect("well-formed document callback")
}

fn decode<T: DeserializeOwned>(value: &Value) -> tinybus::Result<T> {
    serde_json::from_value(value.clone()).map_err(tinybus::Error::from)
}

fn reply<T: Serialize>(result: tinyflows_storage::Result<T>) -> Value {
    match result {
        Ok(value) => json!({"value": serde_json::to_value(value).unwrap()}),
        Err(error) => json!({"error":{
                "kind": error.kind().to_string().replace(' ', "_"),
                "message": error.to_string(),
        }}),
    }
}

pub(super) async fn storage_callback(
    host: &Connection,
    owner: &str,
) -> (CatalogStorage, FlowCatalogDocuments) {
    let scope = Scope::new(owner).unwrap();
    let storage = MemoryStorage::new().for_scope(&scope).unwrap();
    let docs: Arc<dyn DocumentStore> = storage.documents().clone();
    let path = format!("/documents/{owner}");
    host.serve_at(path.parse().unwrap(), DocumentsHost(Arc::clone(&docs)))
        .await
        .unwrap();
    (
        CatalogStorage {
            callback: catalog::CallbackAddress {
                destination: "test.CatalogStorage".into(),
                object_path: path,
            },
        },
        FlowCatalogDocuments::new(docs),
    )
}

#[tokio::test]
async fn module_builder_uses_scoped_flow_documents_for_state_and_catalog_resolution() {
    let (module, host) = support::memory_bus().await;
    host.request_name("test.CatalogStorage").await.unwrap();
    let (alice_storage, alice_catalog) = storage_callback(&host, "alice").await;
    let (bob_storage, bob_catalog) = storage_callback(&host, "bob").await;
    let child = alice_catalog
        .create_flow(
            "Child".into(),
            tinyflows::model::WorkflowGraph::default(),
            false,
            false,
        )
        .await
        .unwrap();
    let unsafe_child: tinyflows::model::WorkflowGraph =
        serde_json::from_value(support::unsafe_child_graph()).unwrap();
    let legacy_unsafe = alice_catalog
        .create_flow("Legacy unsafe child".into(), unsafe_child, false, false)
        .await
        .unwrap();

    let alice = CapsHostPorts::capabilities(
        module.clone(),
        support::caps_host("/caps/alice"),
        RunScope {
            flow_id: "parent-flow".into(),
            storage: alice_storage,
        },
    )
    .await
    .unwrap();
    let bob = CapsHostPorts::capabilities(
        module,
        support::caps_host("/caps/bob"),
        RunScope {
            flow_id: "parent-flow".into(),
            storage: bob_storage,
        },
    )
    .await
    .unwrap();

    alice
        .state
        .store("key", json!({"owner":"alice"}))
        .await
        .unwrap();
    assert_eq!(
        alice.state.load("key").await.unwrap(),
        Some(json!({"owner":"alice"}))
    );
    assert_eq!(
        alice_catalog
            .kv_get("flow:parent-flow", "key")
            .await
            .unwrap(),
        Some(json!({"owner":"alice"}))
    );
    assert_eq!(
        alice_catalog.kv_get("parent-flow", "key").await.unwrap(),
        None
    );
    assert_eq!(bob.state.load("key").await.unwrap(), None);

    let resolved = alice.resolver.resolve(&child.id).await.unwrap();
    assert_eq!(resolved, child.graph);
    let compatibility = alice.resolver.resolve(&legacy_unsafe.id).await.unwrap_err();
    assert!(
        compatibility
            .to_string()
            .contains("unsupported_nested_conditional_fan_in")
    );
    assert!(bob_catalog.get_flow(&child.id).await.unwrap().is_none());
    assert!(bob.resolver.resolve(&child.id).await.is_err());
}

use super::*;

use serde_json::json;
use std::sync::Arc;
use tinybus_sdk::{Connection, broker::Broker, transport::memory::MemoryBus};
use tinystoragedrivers_busproxy::{PerCallerEndpoint, StorageServer, connect_storage, serve};
use tinystoragedrivers_testkit::MemoryBackend;

struct BusDocumentsFixture {
    _broker_task: tokio::task::JoinHandle<tinybus_sdk::Result<()>>,
    _host: Connection,
    _served: tinystoragedrivers_busproxy::ServedStorage,
    backing: tinystoragedrivers_api::Storage,
    client: tinystoragedrivers_api::Storage,
}

impl BusDocumentsFixture {
    async fn new() -> Self {
        let bus = MemoryBus::new();
        let broker = Broker::new();
        let broker_task = broker.spawn(bus.clone());
        let host = Connection::connect(bus.connect().await.expect("host transport"))
            .await
            .expect("host connection");
        let backing = MemoryBackend::new()
            .for_scope(&Scope::new("schema-bus").expect("valid test scope"))
            .expect("memory storage");
        let endpoint_storage = backing.clone();
        let endpoint =
            PerCallerEndpoint::new(move |_| Some(StorageServer::new(endpoint_storage.clone())));
        let served = serve(&host, Arc::new(endpoint))
            .await
            .expect("serve TSD storage endpoint");
        let client_connection = Connection::connect(bus.connect().await.expect("client transport"))
            .await
            .expect("client connection");
        let client = connect_storage(client_connection)
            .await
            .expect("connect through TSD busproxy");
        Self {
            _broker_task: broker_task,
            _host: host,
            _served: served,
            backing,
            client,
        }
    }
}

#[tokio::test]
async fn schema_migration_uses_metadata_supported_by_the_real_tsd_busproxy() {
    let fixture = BusDocumentsFixture::new().await;
    let backing_docs = Arc::clone(fixture.backing.documents());
    backing_docs
        .ensure_collection(&CollectionSpec::new("legacy.items"))
        .await
        .expect("legacy collection");
    backing_docs
        .put_with(
            "legacy.items",
            "one",
            json!({"value":"before"}),
            Precondition::Absent,
            WriteOptions::new().schema(1).expires_ms(i64::MAX),
        )
        .await
        .expect("seed legacy record");

    let schema = Schema::new(Owner::new("flows").expect("valid owner"))
        .collection("flows.items", 2, ScopeClass::Tenant)
        .step(
            Step::new("items_v2", "flows.items", 1, 2, |rows| {
                Ok(rows
                    .into_iter()
                    .map(|row| TargetRecord::new("flows.items", row.id, json!({"value":"after"})))
                    .collect())
            })
            .source("legacy.items"),
        );
    let client_docs = Arc::clone(fixture.client.documents());
    schema
        .migrate(&client_docs, &MigrateOptions::default())
        .await
        .expect("module-compatible migration must not send host-only created_ms");

    let migrated = backing_docs
        .get("flows.items", "one")
        .await
        .expect("read migrated record")
        .expect("record migrated");
    assert_eq!(migrated.doc, json!({"value":"after"}));
    assert_eq!(migrated.meta.schema, 2);
    assert_eq!(migrated.meta.expires_ms, Some(i64::MAX));
    assert!(migrated.meta.created_ms > 0);
    assert!(
        backing_docs
            .get("legacy.items", "one")
            .await
            .expect("read old source")
            .is_none()
    );
}

async fn cross_collection_expiry_migration(
    target_expiry_ms: i64,
) -> (Arc<dyn DocumentStore>, Schema) {
    let storage = MemoryBackend::new()
        .for_scope(&Scope::new("local").expect("valid scope"))
        .expect("memory scope");
    let docs = Arc::clone(storage.documents());
    docs.ensure_collection(&CollectionSpec::new("legacy.items"))
        .await
        .expect("legacy collection");
    docs.ensure_collection(&CollectionSpec::new("flows.archive"))
        .await
        .expect("archive collection");
    docs.put_with(
        "legacy.items",
        "same-id",
        json!({"value":"before"}),
        Precondition::Absent,
        WriteOptions::new().schema(1).expires_ms(i64::MAX),
    )
    .await
    .expect("seed source with absolute expiry");
    docs.put_with(
        "flows.archive",
        "same-id",
        json!({"value":"after"}),
        Precondition::Absent,
        WriteOptions::new().schema(9).expires_ms(target_expiry_ms),
    )
    .await
    .expect("seed target as if first migration write committed");
    let schema = Schema::new(Owner::new("flows").expect("valid owner"))
        .collection("flows.items", 2, ScopeClass::Tenant)
        .collection("flows.archive", 9, ScopeClass::Tenant)
        .step(
            Step::new("archive_v2", "flows.items", 1, 2, |rows| {
                Ok(rows
                    .into_iter()
                    .map(|row| TargetRecord::new("flows.archive", row.id, json!({"value":"after"})))
                    .collect())
            })
            .source("legacy.items"),
        );
    (docs, schema)
}

#[tokio::test]
async fn cross_collection_retry_accepts_exact_body_schema_and_source_expiry() {
    let (docs, schema) = cross_collection_expiry_migration(i64::MAX).await;
    schema
        .migrate(&docs, &MigrateOptions::default())
        .await
        .expect("a lost acknowledgement can retry the identical target write");

    let target = docs
        .get("flows.archive", "same-id")
        .await
        .expect("read target")
        .expect("target remains");
    assert_eq!(target.doc, json!({"value":"after"}));
    assert_eq!(target.meta.schema, 9);
    assert_eq!(target.meta.expires_ms, Some(i64::MAX));
    assert!(
        docs.get("legacy.items", "same-id")
            .await
            .expect("read source")
            .is_none()
    );
}

#[tokio::test]
async fn cross_collection_expiry_mismatch_preserves_the_source_record() {
    let (docs, schema) = cross_collection_expiry_migration(i64::MAX - 1).await;
    let error = schema
        .migrate(&docs, &MigrateOptions::default())
        .await
        .expect_err("a target with a different expiry is not an identical retry");
    assert_eq!(error.kind(), ErrorKind::Conflict);
    let source = docs
        .get("legacy.items", "same-id")
        .await
        .expect("read source")
        .expect("source remains until target equivalence is proven");
    assert_eq!(source.meta.expires_ms, Some(i64::MAX));
}

#[tokio::test]
async fn renamed_migration_targets_keep_their_source_absolute_expiry() {
    let storage = MemoryBackend::new()
        .for_scope(&Scope::new("local").expect("valid scope"))
        .expect("memory scope");
    let docs = Arc::clone(storage.documents());
    docs.ensure_collection(&CollectionSpec::new("legacy.items"))
        .await
        .expect("legacy source collection");
    let expiry = i64::MAX - 100;
    docs.put_with(
        "legacy.items",
        "old-id",
        json!({"value":"before"}),
        Precondition::Absent,
        WriteOptions::new().schema(1).expires_ms(expiry),
    )
    .await
    .expect("seed source with finite absolute expiry");

    let schema = Schema::new(Owner::new("flows").expect("valid owner"))
        .collection("flows.items", 2, ScopeClass::Tenant)
        .step(
            Step::new("rename_items_v2", "flows.items", 1, 2, |rows| {
                Ok(rows
                    .into_iter()
                    .map(|row| {
                        TargetRecord::new("flows.items", "new-id", json!({"value":"after"}))
                            .preserve_expiry_from(&row)
                    })
                    .collect())
            })
            .source("legacy.items"),
        );
    schema
        .migrate(&docs, &MigrateOptions::default())
        .await
        .expect("renamed target migration succeeds");

    let target = docs
        .get("flows.items", "new-id")
        .await
        .expect("read renamed target")
        .expect("renamed target exists");
    assert_eq!(target.meta.expires_ms, Some(expiry));
    assert!(docs
        .get("legacy.items", "old-id")
        .await
        .expect("read source")
        .is_none());
}

#[tokio::test]
async fn owner_migration_rewrites_in_place_once_and_preserves_document_metadata() {
    let storage = MemoryBackend::new()
        .for_scope(&Scope::new("local").expect("valid scope"))
        .expect("memory scope");
    let docs = Arc::clone(storage.documents());
    docs.ensure_collection(&CollectionSpec::new("flows.items"))
        .await
        .expect("source collection");
    docs.put_with(
        "flows.items",
        "one",
        json!({"value": "before"}),
        Precondition::Absent,
        WriteOptions::new()
            .schema(1)
            .created_ms(123)
            .expires_ms(i64::MAX),
    )
    .await
    .expect("seed source");
    let before = docs
        .query("flows.items", &Query::filter(Filter::default()).limit(100))
        .await
        .expect("query before migration");
    assert_eq!(before.items.len(), 1);
    assert_eq!(before.items[0].meta.schema, 1);

    let schema = Schema::new(Owner::new("flows").expect("valid owner"))
        .collection("flows.items", 2, ScopeClass::Tenant)
        .step(Step::new("items_v2", "flows.items", 1, 2, |rows| {
            assert_eq!(rows.len(), 1, "one source row should be passed to the step");
            Ok(rows
                .into_iter()
                .map(|row| TargetRecord::new("flows.items", row.id, json!({"value": "after"})))
                .collect())
        }));

    let first = schema
        .migrate(&docs, &MigrateOptions::default())
        .await
        .expect("migration succeeds");
    let stored = docs
        .get("flows.items", "one")
        .await
        .expect("read migrated row");
    assert!(stored.is_some(), "migration report: {first:?}");
    let stored = stored.expect("row retained");
    assert_eq!(stored.doc["value"], "after");
    assert_eq!(stored.meta.schema, 2);
    assert_eq!(stored.meta.created_ms, 123);
    assert_eq!(stored.meta.expires_ms, Some(i64::MAX));
    assert_eq!(first.ran.len(), 1);

    let second = schema
        .migrate(&docs, &MigrateOptions::default())
        .await
        .expect("completed migration is resumable");
    assert_eq!(second.already_done, 1);
    assert_eq!(
        docs.get("flows.items", "one")
            .await
            .expect("read after replay")
            .expect("row retained")
            .doc["value"],
        "after"
    );
}

#[tokio::test]
async fn owner_migration_rejects_same_id_conflicts_in_a_different_declared_collection() {
    let storage = MemoryBackend::new()
        .for_scope(&Scope::new("local").expect("valid scope"))
        .expect("memory scope");
    let docs = Arc::clone(storage.documents());
    docs.ensure_collection(&CollectionSpec::new("legacy.items"))
        .await
        .expect("legacy source collection");
    docs.put_with(
        "legacy.items",
        "same-id",
        json!({"value": "legacy"}),
        Precondition::Absent,
        WriteOptions::new().schema(1),
    )
    .await
    .expect("seed source");
    docs.put_with(
        "flows.other",
        "same-id",
        json!({"value": "stale target"}),
        Precondition::Absent,
        WriteOptions::new().schema(1),
    )
    .await
    .expect("seed target with matching source version");

    let schema = Schema::new(Owner::new("flows").expect("valid owner"))
        .collection("flows.items", 2, ScopeClass::Tenant)
        .collection("flows.other", 9, ScopeClass::Tenant)
        .step(
            Step::new("items_v2", "flows.items", 1, 2, |rows| {
                Ok(rows
                    .into_iter()
                    .map(|row| {
                        TargetRecord::new("flows.other", row.id, json!({"value": "migrated"}))
                    })
                    .collect())
            })
            .source("legacy.items"),
        );

    let error = schema
        .migrate(&docs, &MigrateOptions::default())
        .await
        .expect_err("a different collection's same-id row must not be overwritten");
    assert_eq!(error.kind(), ErrorKind::Conflict);
    assert_eq!(
        docs.get("flows.other", "same-id")
            .await
            .expect("read target")
            .expect("target remains")
            .doc["value"],
        "stale target"
    );
    assert!(
        docs.get("legacy.items", "same-id")
            .await
            .expect("read source")
            .is_some()
    );
}

#[tokio::test]
async fn owner_migration_uses_the_declared_version_of_a_different_target_collection() {
    let storage = MemoryBackend::new()
        .for_scope(&Scope::new("local").expect("valid scope"))
        .expect("memory scope");
    let docs = Arc::clone(storage.documents());
    docs.ensure_collection(&CollectionSpec::new("legacy.items"))
        .await
        .expect("legacy source collection");
    docs.put_with(
        "legacy.items",
        "one",
        json!({"value": "before"}),
        Precondition::Absent,
        WriteOptions::new().schema(1),
    )
    .await
    .expect("seed source");

    let schema = Schema::new(Owner::new("flows").expect("valid owner"))
        .collection("flows.items", 2, ScopeClass::Tenant)
        .collection("flows.archive", 9, ScopeClass::Tenant)
        .step(
            Step::new("items_to_archive", "flows.items", 1, 2, |rows| {
                Ok(rows
                    .into_iter()
                    .map(|row| {
                        TargetRecord::new("flows.archive", row.id, json!({"value": "archived"}))
                            .expires_ms(i64::MAX - 10)
                    })
                    .collect())
            })
            .source("legacy.items"),
        );

    schema
        .migrate(&docs, &MigrateOptions::default())
        .await
        .expect("cross-collection migration succeeds");
    let archived = docs
        .get("flows.archive", "one")
        .await
        .expect("read archived row")
        .expect("archived row exists");
    assert_eq!(archived.meta.schema, 9);
    assert!(archived.meta.created_ms > 0);
    assert_eq!(archived.meta.expires_ms, Some(i64::MAX - 10));
    assert_eq!(archived.doc["value"], "archived");
}

#[tokio::test]
async fn owner_migration_reads_compatible_shared_progress_records() {
    let storage = MemoryBackend::new()
        .for_scope(&Scope::new("local").expect("valid scope"))
        .expect("memory scope");
    let docs = Arc::clone(storage.documents());
    let key = Key::of(["flows", "flows.items", "items_v2"]).expect("progress key");
    docs.put_as(
        "tsd.migrations",
        key.as_str(),
        &json!({
            "owner": "flows",
            "step": "items_v2",
            "from": 1,
            "to": 2,
            "cursor": null,
            "written": 0,
            "done": true
        }),
        Precondition::Absent,
    )
    .await
    .expect("seed legacy progress record");
    let schema = Schema::new(Owner::new("flows").expect("valid owner"))
        .collection("flows.items", 2, ScopeClass::Tenant)
        .step(Step::new("items_v2", "flows.items", 1, 2, |_| {
            panic!("completed progress must skip the migration")
        }));

    let report = schema
        .migrate(&docs, &MigrateOptions::default())
        .await
        .expect("legacy progress remains readable");
    assert_eq!(report.already_done, 1);
}

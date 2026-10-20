use super::*;

use serde_json::json;
use std::sync::Arc;
use tinystoragedrivers_testkit::MemoryBackend;

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
                            .created_ms(456)
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
    assert_eq!(archived.meta.created_ms, 456);
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

use super::*;
use crate::catalog::test_support::{catalog, catalog_in};
use tinystoragedrivers_core::MemoryStorage;

fn graph() -> Value {
    json!({ "nodes": [{ "id": "t", "kind": "trigger" }], "edges": [] })
}

#[tokio::test]
async fn drafts_round_trip_and_patch() {
    let store = catalog();
    let draft = store
        .create_draft(None, "draft".into(), graph(), DraftOrigin::Chat)
        .await
        .unwrap();
    assert_eq!(
        store.get_draft(&draft.id).await.unwrap(),
        Some(draft.clone())
    );
    assert!(store.get_draft("00000000-missing").await.unwrap().is_none());
    let updated = store
        .update_draft(
            &draft.id,
            Some("renamed".into()),
            None,
            Some(Some("flow-1".into())),
        )
        .await
        .unwrap();
    assert_eq!(updated.name, "renamed");
    assert_eq!(updated.flow_id.as_deref(), Some("flow-1"));
    assert_eq!(updated.graph, graph());
    assert_eq!(updated.created_at, draft.created_at);
    assert_ne!(updated.updated_at, draft.updated_at);
    let cleared = store
        .update_draft(&draft.id, None, None, Some(None))
        .await
        .unwrap();
    assert!(cleared.flow_id.is_none());
    assert!(
        store
            .update_draft("missing", None, None, None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn drafts_list_most_recently_updated_first_and_delete() {
    let store = catalog();
    let a = store
        .create_draft(None, "a".into(), graph(), DraftOrigin::Canvas)
        .await
        .unwrap();
    let b = store
        .create_draft(None, "b".into(), graph(), DraftOrigin::Import)
        .await
        .unwrap();
    store
        .update_draft(&a.id, Some("a2".into()), None, None)
        .await
        .unwrap();
    let names: Vec<String> = store
        .list_drafts()
        .await
        .unwrap()
        .into_iter()
        .map(|d| d.name)
        .collect();
    assert_eq!(names, ["a2", "b"]);
    assert!(store.delete_draft(&b.id).await.unwrap());
    assert!(!store.delete_draft(&b.id).await.unwrap());
    assert_eq!(store.list_drafts().await.unwrap().len(), 1);
}

#[tokio::test]
async fn unsafe_ids_are_refused() {
    let store = catalog();
    for id in ["", "../etc", "a/b", &"x".repeat(65)] {
        assert!(store.get_draft(id).await.is_err(), "{id:?}");
        assert!(store.delete_draft(id).await.is_err(), "{id:?}");
        assert!(
            store.update_draft(id, None, None, None).await.is_err(),
            "{id:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_patches_to_different_fields_both_land() {
    let store = catalog();
    let draft = store
        .create_draft(None, "d".into(), graph(), DraftOrigin::Chat)
        .await
        .unwrap();
    let (s1, s2, id1, id2) = (
        store.clone(),
        store.clone(),
        draft.id.clone(),
        draft.id.clone(),
    );
    let name = tokio::spawn(async move {
        s1.update_draft(&id1, Some("named".into()), None, None)
            .await
    });
    let link = tokio::spawn(async move {
        s2.update_draft(&id2, None, None, Some(Some("f".into())))
            .await
    });
    name.await.unwrap().unwrap();
    link.await.unwrap().unwrap();
    let read = store.get_draft(&draft.id).await.unwrap().unwrap();
    assert_eq!(
        (read.name.as_str(), read.flow_id.as_deref()),
        ("named", Some("f"))
    );
}

#[tokio::test]
async fn a_corrupt_draft_is_skipped_in_lists_and_an_error_alone() {
    let store = catalog();
    let good = store
        .create_draft(None, "good".into(), graph(), DraftOrigin::Chat)
        .await
        .unwrap();
    let docs = store.docs().await.unwrap();
    docs.put(
        DRAFTS,
        "bad",
        json!({ "draft_json": "{", "updated_ns": 0 }),
        Precondition::None,
    )
    .await
    .unwrap();
    let listed = store.list_drafts().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, good.id);
    assert!(store.get_draft("bad").await.is_err());
    assert!(
        store
            .update_draft("bad", Some("x".into()), None, None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn scopes_keep_drafts_apart() {
    let storage = MemoryStorage::new();
    let draft = catalog_in(&storage, "alice")
        .create_draft(None, "d".into(), graph(), DraftOrigin::Chat)
        .await
        .unwrap();
    let bob = catalog_in(&storage, "bob");
    assert!(bob.get_draft(&draft.id).await.unwrap().is_none());
    assert!(bob.list_drafts().await.unwrap().is_empty());
}

#[tokio::test]
async fn every_update_stamps_a_later_time() {
    let store = catalog();
    let draft = store
        .create_draft(None, "d".into(), graph(), DraftOrigin::Chat)
        .await
        .unwrap();
    let mut last = draft.updated_at.clone();
    for i in 0..10 {
        let updated = store
            .update_draft(&draft.id, Some(format!("n{i}")), None, None)
            .await
            .unwrap();
        assert!(crate::catalog::instant_before(&last, &updated.updated_at));
        last = updated.updated_at;
    }
}

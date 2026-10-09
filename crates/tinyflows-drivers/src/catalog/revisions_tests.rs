use super::*;
use crate::catalog::test_support::{automatic_schedule_graph, catalog, trigger_graph};

#[tokio::test]
async fn an_update_captures_the_prior_graph_and_keeps_created_at() {
    let store = catalog();
    let flow = store.create_flow("v1".into(), trigger_graph(), false, true).await.unwrap();
    let mut graph = trigger_graph();
    graph.nodes[0].name = "Renamed".into();
    let updated = store
        .update_flow_graph(&flow.id, "v2".into(), graph, true, None, false, Some(&flow.updated_at))
        .await
        .unwrap();
    assert_eq!(updated.name, "v2");
    assert!(updated.require_approval);
    assert_eq!(updated.created_at, flow.created_at);
    assert_ne!(updated.updated_at, flow.updated_at);
    let revisions = store.list_revisions(&flow.id, 10).await.unwrap();
    assert_eq!(revisions.len(), 1);
    assert_eq!(revisions[0].name, "v1");
    assert_eq!(revisions[0].graph["nodes"][0]["name"], "Trigger");
    let one = store.revision_by_id(&flow.id, &revisions[0].id).await.unwrap();
    assert_eq!(one.map(|r| r.id), Some(revisions[0].id.clone()));
    assert!(store.revision_by_id("other", &revisions[0].id).await.unwrap().is_none());
    assert!(store.list_revisions(&flow.id, 0).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_stale_expected_version_is_a_conflict_carrying_the_current_flow() {
    let store = catalog();
    let flow = store.create_flow("v1".into(), trigger_graph(), false, true).await.unwrap();
    let error = store
        .update_flow_graph(&flow.id, "v2".into(), trigger_graph(), false, None, false, Some("stale"))
        .await
        .unwrap_err();
    match error {
        FlowUpdateError::Conflict(current) => assert_eq!(current.name, "v1"),
        other => panic!("expected a conflict, got {other}"),
    }
    assert!(matches!(
        store.update_flow_graph("missing", "x".into(), trigger_graph(), false, None, false, None).await,
        Err(FlowUpdateError::NotFound)
    ));
    assert!(store.list_revisions(&flow.id, 10).await.unwrap().is_empty());
}

#[tokio::test]
async fn enabled_follows_the_override_and_the_disarm_rules() {
    let store = catalog();
    let flow = store.create_flow("m".into(), trigger_graph(), false, true).await.unwrap();
    let kept = store
        .update_flow_graph(&flow.id, "m".into(), trigger_graph(), false, None, false, None)
        .await
        .unwrap();
    assert!(kept.enabled, "no override keeps enabled");
    let forced = store
        .update_flow_graph(&flow.id, "m".into(), trigger_graph(), false, Some(false), false, None)
        .await
        .unwrap();
    assert!(!forced.enabled);
    store.set_enabled(&flow.id, true).await.unwrap();
    let disarmed = store
        .update_flow_graph(&flow.id, "m".into(), automatic_schedule_graph(), false, Some(true), false, None)
        .await
        .unwrap();
    assert!(!disarmed.enabled, "manual → automatic always disarms (R-m2)");
    store.set_enabled(&flow.id, true).await.unwrap();
    let auto_to_auto = store
        .update_flow_graph(&flow.id, "m".into(), automatic_schedule_graph(), false, None, false, None)
        .await
        .unwrap();
    assert!(auto_to_auto.enabled, "automatic → automatic does not disarm");
    let forced_disarm = store
        .update_flow_graph(&flow.id, "m".into(), automatic_schedule_graph(), false, None, true, None)
        .await
        .unwrap();
    assert!(!forced_disarm.enabled);
}

#[tokio::test]
async fn revisions_are_capped_newest_first() {
    let store = catalog();
    let flow = store.create_flow("v0".into(), trigger_graph(), false, true).await.unwrap();
    for i in 1..=(MAX_REVISIONS_PER_FLOW + 3) {
        store
            .update_flow_graph(&flow.id, format!("v{i}"), trigger_graph(), false, None, false, None)
            .await
            .unwrap();
    }
    let revisions = store.list_revisions(&flow.id, 100).await.unwrap();
    assert_eq!(revisions.len(), MAX_REVISIONS_PER_FLOW);
    assert_eq!(revisions[0].name, format!("v{}", MAX_REVISIONS_PER_FLOW + 2));
}

#[tokio::test]
async fn concurrent_updates_from_one_version_have_one_winner() {
    let store = catalog();
    let flow = store.create_flow("v1".into(), trigger_graph(), false, true).await.unwrap();
    let mut tasks = Vec::new();
    for i in 0..8 {
        let (store, id, seen) = (store.clone(), flow.id.clone(), flow.updated_at.clone());
        tasks.push(tokio::spawn(async move {
            store
                .update_flow_graph(&id, format!("w{i}"), trigger_graph(), false, None, false, Some(&seen))
                .await
                .is_ok()
        }));
    }
    let mut winners = 0;
    for task in tasks {
        winners += usize::from(task.await.unwrap());
    }
    assert_eq!(winners, 1);
    assert_eq!(
        store.list_revisions(&flow.id, 100).await.unwrap().len(),
        1,
        "losers leave no revision behind"
    );
}

#[tokio::test]
async fn a_corrupt_revision_is_an_error() {
    let store = catalog();
    let flow = store.create_flow("v1".into(), trigger_graph(), false, true).await.unwrap();
    store
        .update_flow_graph(&flow.id, "v2".into(), trigger_graph(), false, None, false, None)
        .await
        .unwrap();
    let revision = store.list_revisions(&flow.id, 1).await.unwrap().remove(0);
    let docs = store.docs().await.unwrap();
    compare_and_swap(docs, REVISIONS, &revision.id, |doc| {
        let mut next = doc.clone();
        next["graph_json"] = json!("{broken");
        Some(next)
    })
    .await
    .unwrap();
    assert!(store.list_revisions(&flow.id, 1).await.is_err());
}

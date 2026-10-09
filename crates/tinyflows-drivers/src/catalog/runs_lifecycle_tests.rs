//! A run lifecycle driven through two catalog handles on one backend.

use super::*;
use crate::catalog::test_support::trigger_graph;

fn step(node: &str) -> FlowRunStep {
    FlowRunStep {
        node_id: node.into(),
        ..Default::default()
    }
}

/// Two handles on one backend, as two processes sharing one database would
/// be: park through one, resume and settle through the other, expire through
/// the first, and read everything back through both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_handles_on_one_backend_agree_on_a_run_lifecycle() {
    use crate::catalog::FlowStateDocuments;
    use tinyflows::caps::StateStore;
    use tinyflows::nodes::control_flow::dedup_settle::DedupKv;
    use tinystoragedrivers_core::MemoryStorage;

    let storage = MemoryStorage::new();
    let a = crate::catalog::test_support::catalog_in(&storage, "tenant");
    let b = crate::catalog::test_support::catalog_in(&storage, "tenant");
    let flow = a
        .create_flow("f".into(), trigger_graph(), true, true)
        .await
        .unwrap()
        .id;

    a.insert_flow_run("r1", &flow, "r1", "2026-01-01T00:00:00Z")
        .await
        .unwrap();
    a.upsert_flow_run_step("r1", &step("t")).await.unwrap();
    a.finish_flow_run(
        "r1",
        "pending_approval",
        "2026-01-01T00:00:01Z",
        &[step("t")],
        &["send".into()],
        None,
        Some("h"),
    )
    .await
    .unwrap();
    let state = FlowStateDocuments::new(a.clone(), flow.clone());
    state.store("tentative", json!(["k1"])).await.unwrap();

    assert!(b.mark_run_resuming("r1").await.unwrap());
    assert!(
        !a.mark_run_resuming("r1").await.unwrap(),
        "the other handle sees the resume"
    );
    b.upsert_flow_run_step("r1", &step("send")).await.unwrap();
    assert!(
        b.finish_flow_run(
            "r1",
            "completed",
            "2026-01-01T00:00:05Z",
            &[step("t"), step("send")],
            &[],
            None,
            None
        )
        .await
        .unwrap()
    );
    let settle = FlowStateDocuments::new(b.clone(), flow.clone());
    let settled = tokio::task::spawn_blocking(move || {
        let tentative = DedupKv::kv_get(&settle, "tentative").unwrap();
        DedupKv::kv_set(&settle, "seen", tentative.as_ref().unwrap()).unwrap();
        DedupKv::kv_delete(&settle, "tentative").unwrap();
        tentative
    })
    .await
    .unwrap();
    assert_eq!(settled, Some(json!(["k1"])));

    a.insert_flow_run("r2", &flow, "r2", "2026-01-02T00:00:00Z")
        .await
        .unwrap();
    a.finish_flow_run(
        "r2",
        "pending_approval",
        "2026-01-02T00:00:01Z",
        &[],
        &["send".into()],
        None,
        None,
    )
    .await
    .unwrap();
    let swept = a
        .expire_parked_runs("2026-01-03T00:00:00Z", "2026-01-03T00:00:00Z", "ttl")
        .await
        .unwrap();
    assert_eq!(swept, [("r2".to_string(), flow.clone())]);

    for handle in [&a, &b] {
        let runs = handle.list_flow_runs(&flow, 10).await.unwrap();
        let summary: Vec<(&str, &str, usize)> = runs
            .iter()
            .map(|r| (r.id.as_str(), r.status.as_str(), r.steps.len()))
            .collect();
        assert_eq!(summary, [("r2", "cancelled", 0), ("r1", "completed", 2)]);
        assert_eq!(
            handle.kv_get(&flow, "seen").await.unwrap(),
            Some(json!(["k1"]))
        );
        assert!(handle.kv_get(&flow, "tentative").await.unwrap().is_none());
    }
}

use super::*;
use crate::catalog::test_support::{catalog, trigger_graph};

fn step(node: &str, status: &str) -> FlowRunStep {
    FlowRunStep {
        node_id: node.into(),
        status: Some(status.into()),
        ..Default::default()
    }
}

async fn with_run() -> FlowCatalogDocuments {
    let store = catalog();
    let flow = store
        .create_flow("f".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    store
        .insert_flow_run("r", &flow.id, "r", "2026-01-01T00:00:00Z")
        .await
        .unwrap();
    store
}

#[tokio::test]
async fn a_step_without_its_run_is_skipped() {
    let store = catalog();
    store
        .upsert_flow_run_step("ghost", &step("a", "ok"))
        .await
        .unwrap();
    assert!(store.get_flow_run("ghost").await.unwrap().is_none());
}

#[tokio::test]
async fn a_rerun_node_replaces_its_step_in_place() {
    let store = with_run().await;
    store
        .upsert_flow_run_step("r", &step("a", "error"))
        .await
        .unwrap();
    store
        .upsert_flow_run_step("r", &step("b", "ok"))
        .await
        .unwrap();
    store
        .upsert_flow_run_step("r", &step("a", "ok"))
        .await
        .unwrap();
    let steps = store.get_flow_run("r").await.unwrap().unwrap().steps;
    let seen: Vec<(&str, Option<&str>)> = steps
        .iter()
        .map(|s| (s.node_id.as_str(), s.status.as_deref()))
        .collect();
    assert_eq!(seen, [("a", Some("ok")), ("b", Some("ok"))]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_steps_of_parallel_branches_are_all_kept() {
    let store = with_run().await;
    let mut tasks = Vec::new();
    for i in 0..8 {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            store
                .upsert_flow_run_step("r", &step(&format!("n{i}"), "ok"))
                .await
                .unwrap();
            store
                .upsert_flow_run_step("r", &step("shared", &format!("w{i}")))
                .await
                .unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let steps = store.get_flow_run("r").await.unwrap().unwrap().steps;
    assert_eq!(
        steps.len(),
        9,
        "eight branch steps plus one shared node, none lost"
    );
    assert_eq!(steps.iter().filter(|s| s.node_id == "shared").count(), 1);
}

#[tokio::test]
async fn finishing_rewrites_the_list_without_emptying_it() {
    let store = with_run().await;
    store
        .upsert_flow_run_step("r", &step("gone", "ok"))
        .await
        .unwrap();
    store
        .upsert_flow_run_step("r", &step("b", "ok"))
        .await
        .unwrap();
    store
        .finish_flow_run(
            "r",
            "completed",
            "2026-01-01T00:00:01Z",
            &[step("b", "ok"), step("a", "ok")],
            &[],
            None,
            None,
        )
        .await
        .unwrap();
    let steps = store.get_flow_run("r").await.unwrap().unwrap().steps;
    assert_eq!(
        steps.iter().map(|s| s.node_id.as_str()).collect::<Vec<_>>(),
        ["b", "a"]
    );
}

#[tokio::test]
async fn a_corrupt_step_is_an_error() {
    let store = with_run().await;
    let docs = store.docs().await.unwrap();
    docs.put(
        STEPS,
        &step_id("r", "bad"),
        json!({ "run_id": "r", "node_id": "bad", "order": 0, "step_json": "{" }),
        Precondition::None,
    )
    .await
    .unwrap();
    assert!(store.get_flow_run("r").await.is_err());
}

use super::*;
use crate::catalog::test_support::{catalog, trigger_graph};

async fn with_flow() -> (FlowCatalogDocuments, String) {
    let store = catalog();
    let flow = store
        .create_flow("f".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    (store, flow.id)
}

fn step(node: &str) -> FlowRunStep {
    FlowRunStep {
        node_id: node.into(),
        output: json!([{ "json": { "node": node } }]),
        ..Default::default()
    }
}

#[tokio::test]
async fn insert_finish_get_round_trip() {
    let (store, flow) = with_flow().await;
    store
        .insert_flow_run("r", &flow, "r", "2026-01-01T00:00:00Z")
        .await
        .unwrap();
    let running = store.get_flow_run("r").await.unwrap().unwrap();
    assert_eq!(running.status, "running");
    assert!(running.finished_at.is_none() && running.steps.is_empty());
    assert!(
        store
            .insert_flow_run("r", &flow, "r", "2026-01-01T00:00:00Z")
            .await
            .is_err()
    );
    assert!(
        store
            .insert_flow_run("x", "no-flow", "x", "2026-01-01T00:00:00Z")
            .await
            .is_err()
    );

    let finished = store
        .finish_flow_run(
            "r",
            "failed",
            "2026-01-01T00:00:01Z",
            &[step("a"), step("b")],
            &["n1".into()],
            Some("boom"),
            None,
        )
        .await
        .unwrap();
    assert!(finished);
    let run = store.get_flow_run("r").await.unwrap().unwrap();
    assert_eq!(run.status, "failed");
    assert_eq!(run.finished_at.as_deref(), Some("2026-01-01T00:00:01Z"));
    assert_eq!(
        run.steps
            .iter()
            .map(|s| s.node_id.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert_eq!(run.pending_approvals, ["n1"]);
    assert_eq!(run.error.as_deref(), Some("boom"));
    assert!(
        !store
            .finish_flow_run(
                "r",
                "cancelled",
                "2026-01-01T00:00:02Z",
                &[],
                &[],
                None,
                None
            )
            .await
            .unwrap(),
        "a settled run is never overwritten"
    );
    assert_eq!(
        store.get_flow_run("r").await.unwrap().unwrap().steps.len(),
        2
    );
    assert!(store.get_flow_run("missing").await.unwrap().is_none());
}

#[tokio::test]
async fn parking_pins_the_graph_hash_and_settling_clears_it() {
    let (store, flow) = with_flow().await;
    store
        .insert_flow_run("r", &flow, "r", "2026-01-01T00:00:00Z")
        .await
        .unwrap();
    store
        .finish_flow_run(
            "r",
            "pending_approval",
            "2026-01-01T00:00:01Z",
            &[step("a")],
            &["a".into()],
            None,
            Some("hash"),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .get_flow_run("r")
            .await
            .unwrap()
            .unwrap()
            .graph_hash
            .as_deref(),
        Some("hash")
    );
    assert!(store.mark_run_resuming("r").await.unwrap());
    assert!(!store.mark_run_resuming("r").await.unwrap(), "only once");
    let resumed = store.get_flow_run("r").await.unwrap().unwrap();
    assert_eq!(resumed.status, "running");
    assert!(resumed.finished_at.is_none());
    store
        .finish_flow_run(
            "r",
            "completed",
            "2026-01-01T00:00:03Z",
            &[step("a"), step("b")],
            &[],
            None,
            None,
        )
        .await
        .unwrap();
    let done = store.get_flow_run("r").await.unwrap().unwrap();
    assert!(done.graph_hash.is_none());
    assert_eq!(done.steps.len(), 2);
}

#[tokio::test]
async fn runs_list_newest_first_scoped_to_the_flow() {
    let (store, a) = with_flow().await;
    let b = store
        .create_flow("b".into(), trigger_graph(), false, true)
        .await
        .unwrap()
        .id;
    store
        .insert_flow_run("a1", &a, "a1", "2026-01-01T00:00:00Z")
        .await
        .unwrap();
    store
        .insert_flow_run("a2", &a, "a2", "2026-01-02T00:00:00Z")
        .await
        .unwrap();
    store
        .insert_flow_run("b1", &b, "b1", "2026-01-01T12:00:00Z")
        .await
        .unwrap();
    store.upsert_flow_run_step("a1", &step("x")).await.unwrap();
    let ids = |runs: Vec<FlowRun>| runs.into_iter().map(|r| r.id).collect::<Vec<_>>();
    assert_eq!(
        ids(store.list_flow_runs(&a, 10).await.unwrap()),
        ["a2", "a1"]
    );
    assert_eq!(ids(store.list_flow_runs(&a, 0).await.unwrap()), ["a2"]);
    assert_eq!(
        ids(store.list_all_flow_runs(10).await.unwrap()),
        ["a2", "b1", "a1"]
    );
    assert_eq!(
        store.list_flow_runs(&a, 10).await.unwrap()[1].steps.len(),
        1
    );
}

async fn seed(store: &FlowCatalogDocuments, flow: &str, id: &str, day: u32, status: &str) {
    store
        .insert_flow_run(id, flow, id, &format!("2026-01-{day:02}T00:00:00Z"))
        .await
        .unwrap();
    if status != "running" {
        store
            .finish_flow_run(
                id,
                status,
                &format!("2026-01-{day:02}T00:00:05Z"),
                &[step("t")],
                &[],
                None,
                None,
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn pruning_keeps_the_newest_and_every_live_run() {
    let (store, flow) = with_flow().await;
    seed(&store, &flow, "old-parked", 1, "pending_approval").await;
    seed(&store, &flow, "old-running", 2, "running").await;
    seed(&store, &flow, "old-done", 3, "completed").await;
    seed(&store, &flow, "mid", 4, "failed").await;
    seed(&store, &flow, "new", 5, "completed").await;
    assert_eq!(store.prune_flow_runs(&flow, 2).await.unwrap(), 1);
    let mut left: Vec<String> = store
        .list_flow_runs(&flow, 10)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.id)
        .collect();
    left.sort();
    assert_eq!(left, ["mid", "new", "old-parked", "old-running"]);
    assert_eq!(
        store.prune_flow_runs(&flow, 0).await.unwrap(),
        1,
        "keep is at least 1"
    );
}

#[tokio::test]
async fn inserting_prunes_past_the_retention_cap() {
    let (store, flow) = with_flow().await;
    for i in 0..MAX_FLOW_RUNS_PER_FLOW {
        let id = format!("r{i:03}");
        store
            .insert_flow_run(
                &id,
                &flow,
                &id,
                &format!("2026-01-01T00:{:02}:{:02}Z", i / 60, i % 60),
            )
            .await
            .unwrap();
        store
            .finish_flow_run(
                &id,
                "completed",
                "2026-01-01T01:00:00Z",
                &[],
                &[],
                None,
                None,
            )
            .await
            .unwrap();
    }
    store
        .insert_flow_run("extra", &flow, "extra", "2026-01-02T00:00:00Z")
        .await
        .unwrap();
    let runs = store.list_flow_runs(&flow, 1000).await.unwrap();
    assert_eq!(runs.len(), MAX_FLOW_RUNS_PER_FLOW);
    assert!(runs.iter().all(|r| r.id != "r000"), "the oldest went");
}

#[tokio::test]
async fn orphans_are_listed_before_the_floor_and_interrupted_once() {
    let (store, flow) = with_flow().await;
    seed(&store, &flow, "old", 1, "running").await;
    seed(&store, &flow, "at", 2, "running").await;
    seed(&store, &flow, "done", 1, "completed").await;
    let running = store
        .list_running_run_ids("2026-01-02T00:00:00Z")
        .await
        .unwrap();
    assert_eq!(running, [("old".to_string(), flow.clone())]);
    assert!(
        store
            .mark_run_interrupted("old", "2026-01-03T00:00:00Z", "crash")
            .await
            .unwrap()
    );
    assert!(
        !store
            .mark_run_interrupted("old", "2026-01-03T00:00:00Z", "crash")
            .await
            .unwrap()
    );
    assert!(
        !store
            .mark_run_interrupted("done", "2026-01-03T00:00:00Z", "crash")
            .await
            .unwrap()
    );
    let old = store.get_flow_run("old").await.unwrap().unwrap();
    assert_eq!(
        (old.status.as_str(), old.error.as_deref()),
        ("interrupted", Some("crash"))
    );
}

#[tokio::test]
async fn only_runs_parked_before_the_cutoff_expire() {
    let (store, flow) = with_flow().await;
    seed(&store, &flow, "stale", 1, "pending_approval").await;
    seed(&store, &flow, "fresh", 9, "pending_approval").await;
    seed(&store, &flow, "running", 1, "running").await;
    let swept = store
        .expire_parked_runs("2026-01-05T00:00:00Z", "2026-01-10T00:00:00Z", "ttl")
        .await
        .unwrap();
    assert_eq!(swept, [("stale".to_string(), flow.clone())]);
    let stale = store.get_flow_run("stale").await.unwrap().unwrap();
    assert_eq!(
        (stale.status.as_str(), stale.error.as_deref()),
        ("cancelled", Some("ttl"))
    );
    assert!(
        store
            .expire_parked_runs("2026-01-05T00:00:00Z", "x", "ttl")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_transitions_flip_a_run_exactly_once() {
    let (store, flow) = with_flow().await;
    seed(&store, &flow, "r", 1, "pending_approval").await;
    let mut tasks = Vec::new();
    for i in 0..8 {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            if i % 2 == 0 {
                store.mark_run_resuming("r").await.unwrap()
            } else {
                !store
                    .expire_parked_runs("2026-02-01T00:00:00Z", "2026-02-01T00:00:00Z", "ttl")
                    .await
                    .unwrap()
                    .is_empty()
            }
        }));
    }
    let mut flips = 0;
    for task in tasks {
        flips += usize::from(task.await.unwrap());
    }
    assert_eq!(flips, 1);
}

#[tokio::test]
async fn the_status_fixture_bypasses_the_guard() {
    let (store, flow) = with_flow().await;
    seed(&store, &flow, "r", 1, "completed").await;
    store
        .force_run_status_for_test("r", "running", Some("staged"))
        .await
        .unwrap();
    let run = store.get_flow_run("r").await.unwrap().unwrap();
    assert_eq!(
        (run.status.as_str(), run.error.as_deref()),
        ("running", Some("staged"))
    );
}

#[tokio::test]
async fn a_corrupt_run_is_an_error() {
    let (store, flow) = with_flow().await;
    seed(&store, &flow, "r", 1, "running").await;
    let docs = store.docs().await.unwrap();
    compare_and_swap(docs, RUNS, "r", |doc| {
        let mut next = doc.clone();
        next["pending_approvals_json"] = json!("{nope");
        Some(next)
    })
    .await
    .unwrap();
    assert!(store.get_flow_run("r").await.is_err());
}

#[tokio::test]
async fn a_run_inserted_for_a_removed_flow_does_not_survive() {
    let (store, flow) = with_flow().await;
    store
        .insert_flow_run("r", &flow, "r", "2026-01-01T00:00:00Z")
        .await
        .unwrap();
    store.remove_flow(&flow).await.unwrap();
    assert!(store.get_flow_run("r").await.unwrap().is_none());
    assert!(
        store
            .insert_flow_run("late", &flow, "late", "2026-01-01T00:00:00Z")
            .await
            .is_err()
    );
    assert!(store.get_flow_run("late").await.unwrap().is_none());
}

#[tokio::test]
async fn expiry_rechecks_the_parking_time_on_the_current_run() {
    let (store, flow) = with_flow().await;
    seed(&store, &flow, "r", 1, "pending_approval").await;
    // Resumed and parked again after the cutoff, behind the sweep's back.
    assert!(store.mark_run_resuming("r").await.unwrap());
    store
        .finish_flow_run(
            "r",
            "pending_approval",
            "2026-01-20T00:00:00Z",
            &[],
            &["a".into()],
            None,
            None,
        )
        .await
        .unwrap();
    let swept = store
        .expire_parked_runs("2026-01-10T00:00:00Z", "2026-01-21T00:00:00Z", "ttl")
        .await
        .unwrap();
    assert!(swept.is_empty());
    assert_eq!(
        store.get_flow_run("r").await.unwrap().unwrap().status,
        "pending_approval"
    );
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

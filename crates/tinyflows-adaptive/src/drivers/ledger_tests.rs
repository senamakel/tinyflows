use super::*;
use crate::execute::StepRecord;
use crate::ledger::{Ledger, Score, conformance};
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};

fn memory_docs() -> Arc<dyn DocumentStore> {
    Arc::clone(
        MemoryStorage::new()
            .for_scope(&Scope::local())
            .unwrap()
            .documents(),
    )
}

#[tokio::test]
async fn passes_the_conformance_suite() {
    conformance::run_all(&DriverLedger::new(memory_docs())).await;
}

#[tokio::test]
async fn passes_the_tenant_isolation_suite() {
    let store = DriverLedger::new(memory_docs());
    conformance::run_tenants(
        &store,
        &store.for_tenant("user-a"),
        &store.for_tenant("user-b"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_scores_never_lose_an_increment() {
    let store = DriverLedger::new(memory_docs());
    let lesson = store
        .promote(&conformance::lesson("a class of failure"), &[])
        .await
        .unwrap();
    let tasks: Vec<_> = (0..16)
        .map(|n| {
            let store = store.clone();
            let lesson = lesson.clone();
            tokio::spawn(async move {
                store.score_workflow("wf", n % 2 == 0).await.unwrap();
                store.score_lesson(&lesson, true).await.unwrap();
                store
                    .append(&conformance::row("ep", n, "sig"))
                    .await
                    .unwrap()
            })
        })
        .collect();
    let mut ids = Vec::new();
    for task in tasks {
        ids.push(task.await.unwrap());
    }
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 16, "every append got its own id");
    assert_eq!(
        store.workflow_score("wf").await.unwrap(),
        Score {
            applied: 16,
            helped: 8
        }
    );
    let lessons = store.lessons(None).await.unwrap();
    assert_eq!((lessons[0].applied, lessons[0].helped), (16, 16));
}

#[tokio::test]
async fn a_tenant_cannot_score_another_tenants_lesson() {
    let store = DriverLedger::new(memory_docs());
    let alice = store.for_tenant("alice");
    let lesson = alice
        .promote(&conformance::lesson("alice's lesson"), &[])
        .await
        .unwrap();
    store
        .for_tenant("bob")
        .score_lesson(&lesson, true)
        .await
        .unwrap();
    let mine = alice.lessons(None).await.unwrap();
    assert_eq!((mine[0].applied, mine[0].helped), (0, 0));
    store.score_lesson("les_missing", true).await.unwrap();
}

#[tokio::test]
async fn an_unreadable_record_is_corrupt() {
    let docs = memory_docs();
    let store = DriverLedger::new(Arc::clone(&docs));
    let id = store
        .append(&conformance::row("ep", 1, "sig"))
        .await
        .unwrap();
    docs.put(
        ROWS,
        &id,
        json!({ "episode": "ep", "scope_key": "", "seq": 1, "record": "nope" }),
        Precondition::None,
    )
    .await
    .unwrap();
    assert!(matches!(
        store.rows("ep").await,
        Err(LedgerError::Corrupt(_))
    ));
    docs.put(
        ROWS,
        &id,
        json!({ "episode": "ep", "scope_key": "", "seq": 1 }),
        Precondition::None,
    )
    .await
    .unwrap();
    let error = store.rows("ep").await.unwrap_err();
    assert!(error.to_string().contains("no record"), "{error}");
    assert!(format!("{store:?}").contains("DriverLedger"));
    assert!(matches!(
        backend(StorageError::unavailable("x")),
        LedgerError::Backend(_)
    ));
}

#[tokio::test]
async fn steps_are_one_document_each_in_order() {
    let docs = memory_docs();
    let store = DriverLedger::new(Arc::clone(&docs));
    let step = |node: &str| StepRecord {
        node_id: node.to_string(),
        status: crate::execute::StepOutcome::Success,
        output: json!({ "file.name": node, "$weird": true }),
        duration_ms: 1,
        null_bindings: Vec::new(),
        transcript: Vec::new(),
    };
    let steps: Vec<StepRecord> = (0..12).map(|n| step(&format!("n{n:02}"))).collect();
    store.save_steps("ldg_1", &steps).await.unwrap();
    let stored = docs.query_all(STEPS, &Query::all()).await.unwrap();
    assert_eq!(
        stored.len(),
        12,
        "a step per document, never one oversized record"
    );
    assert!(stored.iter().all(|doc| doc.doc["record"].is_string()));
    let back = store.steps("ldg_1").await.unwrap();
    assert_eq!(
        back.iter().map(|s| s.node_id.as_str()).collect::<Vec<_>>(),
        steps.iter().map(|s| s.node_id.as_str()).collect::<Vec<_>>()
    );
    assert_eq!(back[3].output["file.name"], json!("n03"));
    // Saving again replaces, never appends.
    store.save_steps("ldg_1", &steps[..2]).await.unwrap();
    assert_eq!(store.steps("ldg_1").await.unwrap().len(), 2);
    assert!(
        store
            .for_tenant("x")
            .steps("ldg_1")
            .await
            .unwrap()
            .is_empty()
    );
}

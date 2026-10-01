use super::*;
use crate::workflows::conformance::record;
use crate::workflows::memory::MemoryVault;
use std::sync::Mutex;

/// A layer that is asleep.
struct Offline;

#[async_trait]
impl Vault for Offline {
    async fn load(&self) -> Result<Vec<WorkflowRecord>, WorkflowError> {
        Err(WorkflowError::Engine("device not connected".into()))
    }
    async fn put(&self, _record: &WorkflowRecord) -> Result<(), WorkflowError> {
        Err(WorkflowError::Engine("device not connected".into()))
    }
    async fn remove(&self, _id: &str) -> Result<(), WorkflowError> {
        Err(WorkflowError::Engine("device not connected".into()))
    }
}

async fn ours_with(id: &str) -> Arc<MemoryVault> {
    let vault = Arc::new(MemoryVault::new());
    vault.put(&record(id)).await.expect("put");
    vault
}

#[tokio::test]
async fn strict_is_the_default_and_an_unreadable_layer_fails_the_load() {
    // Right when every layer is a database you own: a store that will not
    // answer is a fault, not a shrug.
    let stack = Layered::new(
        vec![("db".into(), Arc::new(Offline))],
        ours_with("learned-1").await,
    );
    assert!(stack.load().await.is_err());
}

#[tokio::test]
async fn a_sleeping_device_costs_its_catalogue_and_nothing_else() {
    // The case per-episode fetching creates. Without this, one machine
    // being asleep stops every goal that tenant has, though their own
    // procedures are in another layer and perfectly readable.
    let told: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&told);

    let stack = Layered::new(
        vec![("device".into(), Arc::new(Offline))],
        ours_with("learned-1").await,
    )
    .degrading(Arc::new(move |name: &str, why: &WorkflowError| {
        sink.lock().expect("lock").push(format!("{name}: {why}"));
    }));

    let loaded = stack.load().await.expect("the episode still starts");
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].id, "learned-1", "our own catalogue survives");

    let told = told.lock().expect("lock").clone();
    assert_eq!(told.len(), 1, "and it did not happen quietly");
    assert!(told[0].contains("device"), "{}", told[0]);
    assert!(told[0].contains("not connected"), "{}", told[0]);
}

#[tokio::test]
async fn the_writable_layer_is_fatal_even_when_degrading() {
    // Our own store. A loop that cannot read the procedures it wrote should
    // stop, not quietly relearn them and file duplicates.
    let stack = Layered::new(
        vec![("device".into(), ours_with("device-1").await)],
        Arc::new(Offline),
    )
    .degrading(Arc::new(|_: &str, _: &WorkflowError| {}));
    assert!(stack.load().await.is_err());
}

#[tokio::test]
async fn one_layer_failing_does_not_hide_the_others() {
    let told: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
    let sink = Arc::clone(&told);

    let stack = Layered::new(
        vec![
            ("device-a".into(), Arc::new(Offline)),
            ("device-b".into(), ours_with("device-b-1").await),
        ],
        ours_with("learned-1").await,
    )
    .degrading(Arc::new(move |_: &str, _: &WorkflowError| {
        *sink.lock().expect("lock") += 1;
    }));

    let loaded = stack.load().await.expect("load");
    assert_eq!(loaded.len(), 2, "b and ours: {loaded:?}");
    assert_eq!(*told.lock().expect("lock"), 1, "only a was missing");
}

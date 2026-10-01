use super::*;
use crate::workflows::conformance::record;
use crate::workflows::memory::MemoryVault;
use std::sync::Mutex;

fn policy() -> Arc<dyn HostPolicy> {
    #[derive(Debug, Default)]
    struct Permissive;
    impl HostPolicy for Permissive {}
    Arc::new(Permissive)
}

#[tokio::test]
async fn reads_are_served_from_memory_after_one_load() {
    let vault = MemoryVault::new();
    vault.put(&record("weekly")).await.expect("put");

    let snapshot = Snapshot::load(&vault, policy()).await.expect("load");
    assert_eq!(snapshot.list().expect("list").len(), 1);
    assert!(snapshot.get("weekly").expect("get").is_some());
    assert!(snapshot.get("absent").expect("get").is_none());
    assert_eq!(snapshot.pending(), 0, "reading dirties nothing");
}

#[tokio::test]
async fn a_write_is_visible_at_once_and_flushed_later() {
    // The loop saves a variant mid-episode and the next attempt has to see
    // it. Buffering must not mean "invisible until flush".
    let vault = MemoryVault::new();
    let snapshot = Snapshot::load(&vault, policy()).await.expect("load");

    snapshot.save(&record("learned-abc")).expect("save");
    assert!(snapshot.get("learned-abc").expect("get").is_some());
    assert!(
        vault.load().await.expect("load").is_empty(),
        "not yet in the vault"
    );

    assert_eq!(snapshot.flush(&vault).await.expect("flush"), 1);
    assert_eq!(vault.load().await.expect("load").len(), 1);
    assert_eq!(snapshot.pending(), 0);
}

#[tokio::test]
async fn only_what_changed_is_written_back() {
    // The property that makes this safe beside a human editor: a workflow
    // the loop read and did not touch is never rewritten, so an edit made
    // elsewhere in the meantime survives.
    let vault = MemoryVault::new();
    vault.put(&record("untouched")).await.expect("put");
    let snapshot = Snapshot::load(&vault, policy()).await.expect("load");

    let _ = snapshot.list().expect("list");
    let _ = snapshot.get("untouched").expect("get");
    snapshot.save(&record("new-one")).expect("save");

    assert_eq!(
        snapshot.flush(&vault).await.expect("flush"),
        1,
        "one write, not two"
    );
}

#[tokio::test]
async fn flushing_twice_is_not_two_writes() {
    let vault = MemoryVault::new();
    let snapshot = Snapshot::load(&vault, policy()).await.expect("load");
    snapshot.save(&record("once")).expect("save");
    assert_eq!(snapshot.flush(&vault).await.expect("flush"), 1);
    assert_eq!(snapshot.flush(&vault).await.expect("flush"), 0);
}

#[tokio::test]
async fn a_delete_survives_the_flush() {
    let vault = MemoryVault::new();
    vault.put(&record("doomed")).await.expect("put");
    let snapshot = Snapshot::load(&vault, policy()).await.expect("load");

    snapshot.delete("doomed").expect("delete");
    assert!(snapshot.get("doomed").expect("get").is_none());
    snapshot.flush(&vault).await.expect("flush");
    assert!(vault.load().await.expect("load").is_empty());
}

#[tokio::test]
async fn clones_share_the_buffer_so_the_loop_and_the_flusher_agree() {
    // The loop is handed `Arc<dyn WorkflowStore>`; the caller keeps a
    // `Snapshot` to flush. Those must be the same state.
    let vault = MemoryVault::new();
    let snapshot = Snapshot::load(&vault, policy()).await.expect("load");
    let handed_to_the_loop: Arc<dyn WorkflowStore> = Arc::new(snapshot.clone());

    handed_to_the_loop
        .save(&record("via-the-loop"))
        .expect("save");
    assert_eq!(snapshot.pending(), 1, "the flusher sees the loop's write");
    snapshot.flush(&vault).await.expect("flush");
    assert_eq!(vault.load().await.expect("load").len(), 1);
}

#[tokio::test]
async fn a_save_landing_during_a_flush_is_not_dropped() {
    // The vault's put() writes back into the snapshot through a clone —
    // the shape of a second episode saving while the first one flushes.
    // Clearing the whole dirty map would silently lose that record.
    struct Reentrant {
        inner: MemoryVault,
        target: Mutex<Option<Snapshot>>,
    }
    #[async_trait]
    impl Vault for Reentrant {
        async fn load(&self) -> Result<Vec<WorkflowRecord>, WorkflowError> {
            self.inner.load().await
        }
        async fn put(&self, incoming: &WorkflowRecord) -> Result<(), WorkflowError> {
            if let Some(snapshot) = self.target.lock().expect("lock").take() {
                snapshot.save(&record("late")).expect("save mid-flush");
            }
            self.inner.put(incoming).await
        }
        async fn remove(&self, id: &str) -> Result<(), WorkflowError> {
            self.inner.remove(id).await
        }
    }

    let vault = Reentrant {
        inner: MemoryVault::new(),
        target: Mutex::new(None),
    };
    let snapshot = Snapshot::empty(policy());
    snapshot.save(&record("first")).expect("save");
    *vault.target.lock().expect("lock") = Some(snapshot.clone());

    assert_eq!(snapshot.flush(&vault).await.expect("flush"), 1);
    assert_eq!(
        snapshot.pending(),
        1,
        "the save that landed mid-flush survives to the next flush"
    );
    assert_eq!(snapshot.flush(&vault).await.expect("flush"), 1);
    assert_eq!(snapshot.pending(), 0);
}

#[tokio::test]
async fn the_authoring_surface_refuses_rather_than_pretending() {
    // A run record accepted and then lost on the next load is worse than a
    // refusal, because nothing tells the caller it vanished.
    let snapshot = Snapshot::empty(policy());
    assert!(snapshot.list_runs("any").expect("empty").is_empty());
    assert!(snapshot.get_run("any").expect("none").is_none());
    let run: tinyflows::store::types::RunRecord = serde_json::from_value(serde_json::json!({
        "id": "r1", "workflowId": "weekly", "status": "succeeded", "startedAt": 0
    }))
    .expect("a minimal run record");
    assert!(snapshot.record_run(&run).is_err());
}

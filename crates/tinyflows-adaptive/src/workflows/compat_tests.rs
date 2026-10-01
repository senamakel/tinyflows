use super::*;
use crate::workflows::conformance::record;
use crate::workflows::memory::MemoryVault;

async fn layer(ids: &[&str]) -> Arc<MemoryVault> {
    let vault = Arc::new(MemoryVault::new());
    for id in ids {
        vault.put(&record(id)).await.expect("put");
    }
    vault
}

#[tokio::test]
async fn a_plain_store_becomes_selectable_without_being_migrated() {
    let dir = std::env::temp_dir().join(format!("adaptive-compat-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("workflows")).expect("temp dir");
    let store: Arc<dyn WorkflowStore> = Arc::new(tinyflows::store::FileWorkflowStore::new(
        vec![dir.join("workflows")],
        dir.join("runs"),
    ));
    store.save(&record("theirs")).expect("save");

    let vault = StoreVault::new(store);
    let loaded = vault.load().await.expect("load");
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].description, "does the theirs thing");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn reads_are_the_union_of_every_layer() {
    let theirs = layer(&["device-a", "device-b"]).await;
    let ours = layer(&["learned-1"]).await;
    let stack = Layered::new(vec![("device".into(), theirs)], ours);

    let mut ids: Vec<String> = stack
        .load()
        .await
        .expect("load")
        .into_iter()
        .map(|r| r.id)
        .collect();
    ids.sort();
    assert_eq!(ids, ["device-a", "device-b", "learned-1"]);
}

#[tokio::test]
async fn what_we_wrote_shadows_what_we_read() {
    let theirs = Arc::new(MemoryVault::new());
    let mut original = record("shared-id");
    original.description = "the device's version".into();
    theirs.put(&original).await.expect("put");

    let ours = Arc::new(MemoryVault::new());
    let mut taken = record("shared-id");
    taken.description = "the copy we took ownership of".into();
    ours.put(&taken).await.expect("put");

    let stack = Layered::new(vec![("device".into(), theirs)], ours);
    let loaded = stack.load().await.expect("load");
    assert_eq!(loaded.len(), 1, "one id, one record");
    assert_eq!(loaded[0].description, "the copy we took ownership of");
}

#[tokio::test]
async fn a_variant_of_their_workflow_lands_in_our_layer_not_theirs() {
    // The reason this is layered rather than merged. Their catalogue is
    // evidence; the repair is ours, and their machine never changes.
    let theirs = Arc::new(MemoryVault::new());
    theirs.put(&record("device-weekly")).await.expect("put");
    let ours = Arc::new(MemoryVault::new());

    let stack = Layered::new(vec![("device".into(), theirs.clone())], ours.clone());
    stack
        .put(&record("device-weekly-fix-a1b2c3d"))
        .await
        .expect("put");

    assert_eq!(
        theirs.load().await.expect("load").len(),
        1,
        "their catalogue is untouched"
    );
    assert_eq!(ours.load().await.expect("load").len(), 1, "ours gained it");
}

#[tokio::test]
async fn a_delete_never_reaches_a_read_only_layer() {
    // Otherwise the loop could remove a workflow from a machine that never
    // asked it to.
    let theirs = Arc::new(MemoryVault::new());
    theirs.put(&record("device-weekly")).await.expect("put");
    let stack = Layered::new(
        vec![("device".into(), theirs.clone())],
        Arc::new(MemoryVault::new()),
    );

    stack.remove("device-weekly").await.expect("remove");
    assert_eq!(
        theirs.load().await.expect("load").len(),
        1,
        "still theirs, still there"
    );
}

#[tokio::test]
async fn the_scope_reported_is_the_one_writes_land_in() {
    // A device store has no tenant concept, so a read-only layer over it is
    // unscoped. Reporting that would understate who the handle belongs to.
    let unscoped_device = Arc::new(MemoryVault::new());
    let ours = Arc::new(MemoryVault::new().for_tenant("user-7"));
    let stack = Layered::new(vec![("device".into(), unscoped_device)], ours);
    assert_eq!(stack.scope(), Some("user-7"));
}

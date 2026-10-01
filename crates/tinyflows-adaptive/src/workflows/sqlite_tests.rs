use super::*;
use crate::workflows::conformance;

#[tokio::test]
async fn passes_the_conformance_suite() {
    conformance::run_all(&SqliteVault::in_memory().expect("open")).await;
}

#[tokio::test]
async fn passes_the_tenant_isolation_suite() {
    let vault = SqliteVault::in_memory().expect("open");
    conformance::run_tenants(&vault, &vault.for_tenant("a"), &vault.for_tenant("b")).await;
}

#[tokio::test]
async fn a_reopened_vault_still_has_its_workflows() {
    let dir = std::env::temp_dir().join(format!("adaptive-vault-{}", std::process::id()));
    let path = dir.join("nested").join("workflows.db");
    let _ = std::fs::remove_dir_all(&dir);

    SqliteVault::open(&path)
        .expect("open")
        .put(&conformance::record("wf-durable"))
        .await
        .expect("put");

    let again = SqliteVault::open(&path).expect("reopen");
    assert_eq!(again.load().await.expect("load").len(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn the_ledger_and_the_vault_share_one_file_without_colliding() {
    // The module note claims "one file for everything durable". Two
    // `Connection`s onto one path is only fine because the two schemas
    // share no table name — ledger_rows/lessons/episodes/… beside
    // workflows — and asserting it here is cheaper than finding out when a
    // deployment points both at the same DSN.
    use crate::ledger::Ledger;

    let dir = std::env::temp_dir().join(format!("adaptive-onefile-{}", std::process::id()));
    let path = dir.join("adaptive.db");
    let _ = std::fs::remove_dir_all(&dir);

    let ledger = crate::ledger::sqlite::SqliteLedger::open(&path).expect("ledger");
    let vault = SqliteVault::open(&path).expect("vault");

    vault
        .put(&conformance::record("weekly"))
        .await
        .expect("put");
    ledger
        .append(&crate::ledger::conformance::row("ep-1", 1, "authored"))
        .await
        .expect("append");

    assert_eq!(vault.load().await.expect("load").len(), 1);
    assert_eq!(ledger.rows("ep-1").await.expect("rows").len(), 1);

    // And both survive a reopen of the same file, which is the point of
    // putting them there.
    drop(ledger);
    drop(vault);
    let ledger = crate::ledger::sqlite::SqliteLedger::open(&path).expect("reopen ledger");
    let vault = SqliteVault::open(&path).expect("reopen vault");
    assert_eq!(vault.load().await.expect("load").len(), 1);
    assert_eq!(ledger.rows("ep-1").await.expect("rows").len(), 1);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn the_stored_document_is_the_whole_record_not_just_the_graph() {
    // `description` is what a planner reads to choose. A vault that kept
    // only the graph would file every workflow as unfindable.
    let vault = SqliteVault::in_memory().expect("open");
    vault
        .put(&conformance::record("wf-prose"))
        .await
        .expect("put");
    let back = &vault.load().await.expect("load")[0];
    assert_eq!(back.description, "does the wf-prose thing");
}

use super::*;
use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;

#[test]
fn memory_has_to_be_asked_for_by_name() {
    assert_eq!(Config::parse("memory").expect("parse"), Config::Memory);
    assert_eq!(Config::parse(":memory:").expect("parse"), Config::Memory);
}

#[test]
fn a_setting_that_names_a_database_is_refused_with_the_way_forward() {
    for (setting, names) in [
        ("/var/lib/app/adaptive.db", "a sqlite path"),
        ("sqlite:./adaptive.db", "a sqlite path"),
        ("adaptive.db", "a sqlite path"),
        (
            "mongodb://db.internal:27017/adaptive?replicaSet=rs0",
            "a mongodb:// URI",
        ),
        ("mongodb+srv://cluster.example.net", "a mongodb:// URI"),
    ] {
        let err = Config::parse(setting).expect_err(setting);
        let message = err.to_string();
        assert!(message.contains(names), "{message}");
        assert!(message.contains("Storage::from_documents"), "{message}");
        assert!(message.contains("memory"), "{message}");
    }
}

#[test]
fn an_unset_variable_errors_naming_the_variable_rather_than_defaulting() {
    // Defaulting to a path invents a location nobody named; defaulting to
    // memory is a service that runs perfectly and learns nothing.
    let err = Config::from_setting(None).expect_err("unset");
    assert!(err.to_string().contains(STORAGE_VAR), "{err}");
    let err = Config::from_setting(Some("  ")).expect_err("blank is unset");
    assert!(err.to_string().contains(STORAGE_VAR), "{err}");
}

#[test]
fn a_set_variable_goes_through_the_same_parse() {
    assert_eq!(
        Config::from_setting(Some("memory")).expect("parse"),
        Config::Memory
    );
}

#[test]
fn an_empty_setting_is_an_error_that_lists_the_choices() {
    let err = Config::parse("   ").expect_err("empty");
    assert!(err.to_string().contains("memory"), "{err}");
}

#[tokio::test]
async fn one_call_scopes_both_halves() {
    // The failure this module exists to prevent: scoping the ledger and
    // forgetting the vault, or the reverse.
    let storage = Storage::open(&Config::Memory).await.expect("open");
    let tenant = storage.for_tenant("user-7");
    assert_eq!(tenant.ledger().scope(), Some("user-7"));
    assert_eq!(tenant.vault().scope(), Some("user-7"));
    assert_eq!(storage.ledger().scope(), None, "the root stays unscoped");
}

#[cfg(feature = "storage-drivers")]
#[tokio::test]
async fn one_document_handle_holds_both_halves_and_survives_reopening() {
    use tinyflows_storage::{ Scope, StorageBackend};
    let scoped = MemoryStorage::new()
        .for_scope(&Scope::local())
        .expect("scope");
    let docs = scoped.documents().clone();

    let storage = Storage::from_documents(docs.clone());
    let tenant = storage.for_tenant("user-7");
    assert_eq!(tenant.ledger().scope(), Some("user-7"));
    assert_eq!(tenant.vault().scope(), Some("user-7"));
    tenant
        .ledger()
        .append(&crate::ledger::conformance::row("ep-1", 1, "authored"))
        .await
        .expect("append");
    tenant
        .vault()
        .put(&crate::workflows::conformance::record("weekly"))
        .await
        .expect("put");

    // A new `Storage` over the same handle sees both halves, still scoped.
    let again = Storage::from_documents(docs);
    let tenant = again.for_tenant("user-7");
    assert_eq!(tenant.ledger().rows("ep-1").await.expect("rows").len(), 1);
    assert_eq!(tenant.vault().load().await.expect("load").len(), 1);
    let other = again.for_tenant("user-8");
    assert!(other.vault().load().await.expect("load").is_empty());
}

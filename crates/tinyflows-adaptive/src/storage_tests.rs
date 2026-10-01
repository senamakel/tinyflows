use super::*;

#[test]
fn memory_has_to_be_asked_for_by_name() {
    assert_eq!(Config::parse("memory").expect("parse"), Config::Memory);
    assert_eq!(Config::parse(":memory:").expect("parse"), Config::Memory);
}

#[cfg(feature = "sqlite")]
#[test]
fn a_bare_path_reads_as_sqlite() {
    assert_eq!(
        Config::parse("/var/lib/app/adaptive.db").expect("parse"),
        Config::Sqlite(PathBuf::from("/var/lib/app/adaptive.db"))
    );
    assert_eq!(
        Config::parse("sqlite:./adaptive.db").expect("parse"),
        Config::Sqlite(PathBuf::from("./adaptive.db"))
    );
}

#[cfg(feature = "mongo")]
#[test]
fn a_mongo_uri_carries_its_database_or_gets_the_default() {
    match Config::parse("mongodb://db.internal:27017/adaptive?replicaSet=rs0").expect("parse") {
        Config::Mongo { database, .. } => assert_eq!(database, "adaptive"),
        other => panic!("{other:?}"),
    }
    match Config::parse("mongodb+srv://cluster.example.net").expect("parse") {
        Config::Mongo { database, .. } => assert_eq!(database, "tinyflows_adaptive"),
        other => panic!("{other:?}"),
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

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn one_sqlite_setting_yields_one_file_holding_both_halves() {
    let dir = std::env::temp_dir().join(format!("adaptive-storage-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let path = dir.join("adaptive.db");

    let config = Config::parse(path.to_str().expect("utf8 path")).expect("parse");
    let storage = Storage::open(&config).await.expect("open");
    let tenant = storage.for_tenant("user-7");

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

    // Reopen from the same setting: both halves are still there, scoped.
    let again = Storage::open(&config).await.expect("reopen");
    let tenant = again.for_tenant("user-7");
    assert_eq!(tenant.ledger().rows("ep-1").await.expect("rows").len(), 1);
    assert_eq!(tenant.vault().load().await.expect("load").len(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

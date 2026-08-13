use super::*;

#[test]
fn binds_only_explicitly_shared_regular_tabs() {
    let mut registry = TabRegistry::new();
    assert_eq!(
        registry.bind_run("run-1", 7).unwrap_err(),
        TabRegistryError::TabNotShared
    );
    assert_eq!(
        registry
            .share(7, 1, "chrome://settings", "Settings")
            .unwrap_err(),
        TabRegistryError::UnsupportedPage
    );
    registry
        .share(7, 1, "https://example.test", "Example")
        .unwrap();
    registry.bind_run("run-1", 7).unwrap();
    assert_eq!(registry.authorize("run-1", 7).unwrap().id, 7);
}

#[test]
fn never_falls_back_to_another_shared_tab() {
    let mut registry = TabRegistry::new();
    registry.share(7, 1, "https://one.test", "One").unwrap();
    registry.share(8, 1, "https://two.test", "Two").unwrap();
    registry.bind_run("run-1", 7).unwrap();
    assert_eq!(
        registry.authorize("run-1", 8).unwrap_err(),
        TabRegistryError::RunTabMismatch
    );
}

#[test]
fn revocation_invalidates_runs_and_new_share_gets_new_generation() {
    let mut registry = TabRegistry::new();
    let first_generation = registry
        .share(7, 1, "https://example.test", "Example")
        .unwrap()
        .generation;
    registry.bind_run("run-1", 7).unwrap();
    assert_eq!(registry.revoke(7), vec!["run-1"]);
    let second_generation = registry
        .share(7, 1, "https://example.test", "Example")
        .unwrap()
        .generation;
    assert!(second_generation > first_generation);
    assert_eq!(
        registry.authorize("run-1", 7).unwrap_err(),
        TabRegistryError::TabRevoked
    );
}

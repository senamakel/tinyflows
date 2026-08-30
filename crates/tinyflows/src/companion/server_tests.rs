use super::*;

fn test_server(workflows_dir: PathBuf) -> CompanionServer {
    CompanionServer::new(CompanionServerConfig {
        policy: RelayPolicy::loopback(0),
        extension_id: "a".repeat(32),
        pairing_secret: PairingSecret::parse("a".repeat(32)).unwrap(),
        workflows_dir,
        capabilities: crate::caps::mock::mock_capabilities(),
        run_host: None,
    })
    .unwrap()
}

#[test]
fn workflow_ids_cannot_escape_the_configured_directory() {
    let error = load_workflow(Path::new("/tmp"), "../secret").unwrap_err();
    assert!(error.to_string().contains("invalid workflow id"));
}

#[tokio::test]
async fn external_run_cancellation_clears_its_relay_binding() {
    let server = test_server(PathBuf::from("."));
    server
        .inner
        .relay
        .lock()
        .unwrap()
        .tabs_mut()
        .share(7, 1, "https://example.com", "Example")
        .unwrap();
    server.bind_run("external-1", 7).unwrap();

    assert!(!server.cancel_workflow("external-1").await);
    assert!(server.cancel_bound_run("external-1").await);
    assert!(
        server
            .inner
            .relay
            .lock()
            .unwrap()
            .tabs()
            .binding("external-1")
            .is_none()
    );
    assert!(!server.cancel_bound_run("external-1").await);
}

use super::*;
use crate::browser::BrowserAction;

fn request(id: &str, run_id: &str, tab_id: TabId, timeout_ms: u64) -> BrowserRequest {
    BrowserRequest {
        protocol_version: BROWSER_PROTOCOL_VERSION,
        request_id: id.into(),
        run_id: run_id.into(),
        tab_id,
        timeout_ms,
        action: BrowserAction::GetTitle,
    }
}

fn connected_state(now: Instant) -> RelayState {
    let mut state = RelayState::new(RelayPolicy::loopback(3210)).unwrap();
    state
        .tabs_mut()
        .share(7, 1, "https://example.test", "Example")
        .unwrap();
    state.tabs_mut().bind_run("run-1", 7).unwrap();
    state.connect("session-1".into(), now).unwrap();
    state
}

#[test]
fn policy_rejects_non_loopback_listener() {
    let mut policy = RelayPolicy::loopback(3210);
    policy.bind_addr = "0.0.0.0:3210".parse().unwrap();
    assert_eq!(policy.validate(), Err(RelayError::NonLoopbackBind));
}

#[test]
fn action_requires_connection_binding_and_bounded_timeout() {
    let now = Instant::now();
    let mut disconnected = RelayState::new(RelayPolicy::loopback(3210)).unwrap();
    assert_eq!(
        disconnected
            .begin_action(&request("r0", "run-1", 7, 1000), now)
            .unwrap_err(),
        RelayError::RelayDisconnected
    );

    let mut state = connected_state(now);
    assert!(
        state
            .begin_action(&request("r1", "run-1", 7, 1000), now)
            .is_ok()
    );
    assert_eq!(
        state
            .begin_action(&request("r2", "run-1", 8, 1000), now)
            .unwrap_err(),
        RelayError::Tab(TabRegistryError::RunTabMismatch)
    );
    assert_eq!(
        state
            .begin_action(&request("r3", "run-1", 7, 60_001), now)
            .unwrap_err(),
        RelayError::InvalidTimeout
    );
}

#[test]
fn timeout_disconnect_revocation_and_cancel_all_fail_closed() {
    let now = Instant::now();
    let mut state = connected_state(now);
    state
        .begin_action(&request("timeout", "run-1", 7, 100), now)
        .unwrap();
    let expired = state.expire_actions(now + Duration::from_millis(100));
    assert!(matches!(
        &expired[0],
        BrowserResponse::Error { error, .. }
            if error.code == BrowserErrorCode::ActionTimeout
    ));

    state
        .begin_action(&request("revoked", "run-1", 7, 1000), now)
        .unwrap();
    let (_, revoked) = state.revoke_tab(7);
    assert!(matches!(
        &revoked[0],
        BrowserResponse::Error { error, .. }
            if error.code == BrowserErrorCode::TabRevoked
    ));

    state
        .tabs_mut()
        .share(7, 1, "https://example.test", "Example")
        .unwrap();
    state.tabs_mut().bind_run("run-2", 7).unwrap();
    state
        .begin_action(&request("cancelled", "run-2", 7, 1000), now)
        .unwrap();
    let cancelled = state.cancel_run("run-2");
    assert!(matches!(
        &cancelled[0],
        BrowserResponse::Error { error, .. }
            if error.code == BrowserErrorCode::Cancelled
    ));

    state.tabs_mut().bind_run("run-3", 7).unwrap();
    state
        .begin_action(&request("disconnect", "run-3", 7, 1000), now)
        .unwrap();
    let disconnected = state.disconnect("session-1");
    assert!(matches!(
        &disconnected.responses[0],
        BrowserResponse::Error { error, .. }
            if error.code == BrowserErrorCode::RelayDisconnected
    ));
    assert!(!state.is_connected());
}

#[test]
fn response_must_match_protocol_request_and_session() {
    let now = Instant::now();
    let mut state = connected_state(now);
    state
        .begin_action(&request("r1", "run-1", 7, 1000), now)
        .unwrap();
    let response = error_response("r1".into(), BrowserErrorCode::ElementNotFound, "not found");
    assert_eq!(
        state.complete_action("session-2", &response).unwrap_err(),
        RelayError::SessionMismatch
    );
    assert_eq!(
        state
            .complete_action("session-1", &response)
            .unwrap()
            .request_id,
        "r1"
    );
}

#[test]
fn heartbeat_refreshes_liveness_and_stale_session_fails_actions() {
    let now = Instant::now();
    let mut state = connected_state(now);
    state
        .begin_action(&request("pending", "run-1", 7, 60_000), now)
        .unwrap();

    let refreshed = now + Duration::from_secs(20);
    state.heartbeat("session-1", refreshed).unwrap();
    assert!(
        state
            .disconnect_if_stale(now + Duration::from_secs(40))
            .is_none()
    );

    let outcome = state
        .disconnect_if_stale(now + Duration::from_secs(50))
        .expect("thirty seconds without a heartbeat is stale");
    assert!(matches!(
        &outcome.responses[0],
        BrowserResponse::Error { error, .. }
            if error.code == BrowserErrorCode::RelayDisconnected
    ));
    assert!(!state.is_connected());
}

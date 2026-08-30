use super::*;
use serde_json::json;

#[test]
fn every_action_has_the_stable_snake_case_wire_name() {
    let cases = [
        (json!({"action":"open","url":"https://example.com"}), "open"),
        (json!({"action":"snapshot"}), "snapshot"),
        (json!({"action":"click","selector":"#go"}), "click"),
        (
            json!({"action":"fill","selector":"#q","value":"hi"}),
            "fill",
        ),
        (json!({"action":"type","text":"hi"}), "type"),
        (json!({"action":"get_text"}), "get_text"),
        (json!({"action":"get_title"}), "get_title"),
        (json!({"action":"get_url"}), "get_url"),
        (json!({"action":"screenshot"}), "screenshot"),
        (json!({"action":"wait","duration_ms":1}), "wait"),
        (json!({"action":"press","key":"Enter"}), "press"),
        (json!({"action":"hover","selector":"a"}), "hover"),
        (json!({"action":"scroll","y":100}), "scroll"),
        (
            json!({"action":"is_visible","selector":"main"}),
            "is_visible",
        ),
        (json!({"action":"close"}), "close"),
        (json!({"action":"find","query":"checkout"}), "find"),
    ];

    for (wire, name) in cases {
        let action: BrowserAction = serde_json::from_value(wire).expect(name);
        assert_eq!(serde_json::to_value(action).unwrap()["action"], name);
    }
}

#[test]
fn action_schema_rejects_missing_action_unknown_actions_and_extra_fields() {
    assert!(serde_json::from_value::<BrowserAction>(json!({"selector":"x"})).is_err());
    assert!(serde_json::from_value::<BrowserAction>(json!({"action":"tap"})).is_err());
    assert!(
        serde_json::from_value::<BrowserAction>(
            json!({"action":"click","selector":"x","secret":"nope"})
        )
        .is_err()
    );
    for action in ["snapshot", "get_title", "get_url", "close"] {
        assert!(
            serde_json::from_value::<BrowserAction>(json!({"action":action,"extra":true})).is_err(),
            "{action} must reject unknown fields"
        );
    }
}

#[test]
fn request_schema_is_versioned_and_strict() {
    let request = BrowserRequest {
        protocol_version: BROWSER_PROTOCOL_VERSION,
        request_id: "run-1:1".into(),
        run_id: "run-1".into(),
        tab_id: 42,
        timeout_ms: 30_000,
        action: BrowserAction::GetTitle,
    };
    let mut wire = serde_json::to_value(&request).unwrap();
    assert_eq!(
        serde_json::from_value::<BrowserRequest>(wire.clone()).unwrap(),
        request
    );
    wire["unexpected"] = json!(true);
    assert!(serde_json::from_value::<BrowserRequest>(wire).is_err());
}

#[test]
fn error_codes_are_stable_and_reject_unknown_values() {
    for (code, wire) in [
        (BrowserErrorCode::TabNotShared, "tab_not_shared"),
        (BrowserErrorCode::TabRevoked, "tab_revoked"),
        (BrowserErrorCode::RelayDisconnected, "relay_disconnected"),
        (BrowserErrorCode::UnsupportedPage, "unsupported_page"),
        (BrowserErrorCode::ActionTimeout, "action_timeout"),
        (BrowserErrorCode::ElementNotFound, "element_not_found"),
    ] {
        assert_eq!(code.as_str(), wire);
        assert_eq!(serde_json::to_value(code).unwrap(), wire);
    }
    assert!(serde_json::from_str::<BrowserErrorCode>("\"mystery\"").is_err());
}

#[test]
fn responses_are_correlated_and_strict() {
    let response = BrowserResponse::Ok {
        protocol_version: BROWSER_PROTOCOL_VERSION,
        request_id: "r:7".into(),
        result: BrowserResult {
            data: json!({"title":"TinyFlows"}),
        },
    };
    assert_eq!(response.protocol_version(), 1);
    assert_eq!(response.request_id(), "r:7");
    let wire = serde_json::to_value(response).unwrap();
    assert_eq!(wire["status"], "ok");
    assert!(
        serde_json::from_value::<BrowserResponse>(json!({
            "status":"ok", "protocol_version":1, "request_id":"r:7",
            "result":{"data":null}, "extra":true
        }))
        .is_err()
    );
}

#[test]
fn canonical_repository_fixtures_decode_as_rust_contracts() {
    let request: BrowserRequest = serde_json::from_str(include_str!(
        "../../protocol/fixtures/browser-request.v1.json"
    ))
    .unwrap();
    let response: BrowserResponse = serde_json::from_str(include_str!(
        "../../protocol/fixtures/browser-response.v1.json"
    ))
    .unwrap();
    let cancel: BrowserCancel = serde_json::from_str(include_str!(
        "../../protocol/fixtures/browser-cancel.v1.json"
    ))
    .unwrap();
    assert_eq!(request.request_id, response.request_id());
    assert_eq!(request.request_id, cancel.request_id);
    assert_eq!(
        request.action,
        BrowserAction::Fill {
            selector: "#email".into(),
            value: "person@example.com".into(),
        }
    );
}

#[test]
fn cancellation_message_is_strict_and_versioned() {
    let cancel = BrowserCancel {
        protocol_version: BROWSER_PROTOCOL_VERSION,
        message_type: BrowserCancelType::BrowserCancel,
        request_id: "run:1".into(),
    };
    assert_eq!(
        serde_json::to_value(&cancel).unwrap()["type"],
        "browser.cancel"
    );
    assert!(
        serde_json::from_value::<BrowserCancel>(json!({
            "protocol_version":1,"type":"browser.cancel","request_id":"r","extra":true
        }))
        .is_err()
    );
}

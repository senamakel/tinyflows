use super::*;
use serde_json::json;

#[test]
fn methods_use_dotted_names_and_reject_unknown_fields() {
    let request: CompanionControlRequest = serde_json::from_value(json!({
        "method": "workflow.start",
        "protocol_version": 1,
        "request_id": "control-1",
        "workflow_id": "login",
        "tab_id": 42,
        "input": {"query":"shoes"}
    }))
    .unwrap();
    assert_eq!(request.protocol_version(), 1);
    assert_eq!(request.request_id(), "control-1");

    assert!(
        serde_json::from_value::<CompanionControlRequest>(json!({
            "method": "tab.list", "protocol_version": 1,
            "request_id": "control-2", "include_unshared": true
        }))
        .is_err()
    );
}

#[test]
fn completed_event_contains_no_host_owned_output() {
    let event = RunEvent::Completed {
        protocol_version: 1,
        run_id: "run-1".into(),
        status: "success".into(),
    };
    assert_eq!(
        serde_json::to_value(event).unwrap(),
        json!({
            "event": "completed",
            "protocol_version": 1,
            "run_id": "run-1",
            "status": "success"
        })
    );
}

// SPDX-License-Identifier: GPL-3.0-or-later
use crate::FlowRunStep;
use crate::run_summary::*;
use serde_json::json;

fn step(node_id: &str) -> FlowRunStep {
    FlowRunStep {
        node_id: node_id.to_string(),
        output: json!([]),
        ..Default::default()
    }
}

#[test]
fn reconstruct_steps_reads_items_and_port() {
    let steps = reconstruct_steps(&json!({
        "nodes": {"switch": {"items": [{"json": {"ok": true}}], "port": "true"}}
    }));
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].node_id, "switch");
    assert_eq!(steps[0].port.as_deref(), Some("true"));
    assert_eq!(steps[0].output, json!([{"json": {"ok": true}}]));
}

#[test]
fn settle_steps_keeps_observed_data_and_fills_routing_port() {
    let mut observed = step("switch");
    observed.status = Some("success".to_string());
    let settled = settle_steps(
        vec![observed],
        &json!({"nodes": {"switch": {"items": [], "port": "false"}, "done": {"items": []}}}),
    );
    assert_eq!(settled.len(), 2);
    assert_eq!(settled[0].status.as_deref(), Some("success"));
    assert_eq!(settled[0].port.as_deref(), Some("false"));
    assert_eq!(settled[1].node_id, "done");
}

#[test]
fn terminal_status_prioritizes_approval_then_failure_then_warnings() {
    assert_eq!(
        terminal_status(&[], &["approval".to_string()]).status,
        "pending_approval"
    );
    let mut failed = step("broken");
    failed.status = Some("error".to_string());
    assert_eq!(terminal_status(&[failed], &[]).status, "failed");
    let mut warning = step("warn");
    warning.diagnostics.push(json!({"location": "args.x"}));
    assert_eq!(
        terminal_status(&[warning], &[]).status,
        "completed_with_warnings"
    );
    assert_eq!(terminal_status(&[], &[]).status, "completed");
}

#[test]
fn an_outstanding_approval_wins_over_a_settled_step_error() {
    let mut errored = step("a");
    errored.status = Some("error".to_string());
    let summary = terminal_status(&[errored], &["gate".to_string()]);
    assert_eq!(summary.status, "pending_approval");
    assert_eq!(summary.error, None);
}

#[test]
fn a_degraded_failure_names_the_failed_step_and_a_clean_run_has_no_error() {
    let mut errored = step("x");
    errored.status = Some("error".to_string());
    let summary = terminal_status(&[errored], &[]);
    assert_eq!(summary.status, "failed");
    assert!(summary.error.unwrap().contains('x'));

    let clean = terminal_status(&[step("a")], &[]);
    assert_eq!(clean.status, "completed");
    assert_eq!(clean.error, None);
}

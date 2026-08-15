use super::*;
use serde_json::json;

use crate::caps::mock::{MockApprovals, mock_capabilities, mock_capabilities_with_approvals};
use crate::compiler::compile;
use crate::engine::{RunInput, resume, run};
use crate::model::{Edge, Node, NodeKind, WorkflowGraph};

/// A trigger wired into one `approval` node, with `config` on the approval.
///
/// Fills in a stable `request_id` when the caller's config does not already
/// name one, since without a run-scoped identity `build_request` now refuses
/// to guess one (see `a_missing_request_id_and_run_id_is_a_configuration_error`
/// below for the case that tests the refusal itself).
fn wf(mut config: Value) -> WorkflowGraph {
    if let Some(obj) = config.as_object_mut() {
        obj.entry("request_id".to_string())
            .or_insert_with(|| json!("review-request"));
    }
    wf_raw(config)
}

/// [`wf`] without the `request_id` auto-fill, for tests that need to control
/// exactly what identity information the node config and run carry.
fn wf_raw(config: Value) -> WorkflowGraph {
    WorkflowGraph {
        nodes: vec![
            Node {
                id: "t".into(),
                kind: NodeKind::Trigger,
                type_version: 1,
                name: "t".into(),
                config: Value::Null,
                ports: vec![],
                position: None,
            },
            Node {
                id: "review".into(),
                kind: NodeKind::Approval,
                type_version: 1,
                name: "review".into(),
                config,
                ports: vec![],
                position: None,
            },
        ],
        edges: vec![Edge {
            from_node: "t".into(),
            from_port: "main".into(),
            to_node: "review".into(),
            to_port: "main".into(),
        }],
        ..Default::default()
    }
}

fn request(id: &str) -> ApprovalRequest {
    ApprovalRequest {
        request_id: id.to_string(),
        node_id: "review".to_string(),
        run_id: None,
        title: None,
        prompt: None,
        subject: ApprovalSubject {
            kind: "url".to_string(),
            value: json!("https://example.com/post/1"),
        },
        assignees: vec![],
        metadata: Value::Null,
    }
}

/// Opposite default to `gate`, and deliberately so: a human review is a
/// minutes-to-days wait, and polling one burns super-steps for nothing.
#[test]
fn wait_mode_defaults_to_suspend() {
    assert_eq!(WaitMode::from_config(&json!({})), WaitMode::Suspend);
    assert_eq!(
        WaitMode::from_config(&json!({ "wait_mode": "poll" })),
        WaitMode::Poll
    );
    assert_eq!(
        WaitMode::from_config(&json!({ "wait_mode": "nonsense" })),
        WaitMode::Suspend
    );
}

/// Failing safe: an unrecognised policy must not drop a rejection on the floor
/// or fail a run that had a perfectly good recovery branch.
#[test]
fn reject_and_timeout_policies_default_conservatively() {
    assert_eq!(OnReject::from_config(&json!({})), OnReject::Route);
    assert_eq!(
        OnReject::from_config(&json!({ "on_reject": "whatever" })),
        OnReject::Route
    );
    assert_eq!(OnTimeout::from_config(&json!({})), OnTimeout::Error);
    assert_eq!(
        OnTimeout::from_config(&json!({ "on_timeout": "eventually" })),
        OnTimeout::Error
    );
}

/// The engine's own denial shape wins over an approval delivered in the same
/// resume value — the same precedence a `requires_approval` gate applies.
#[test]
fn resume_denial_beats_an_approval_in_the_same_value() {
    let req = request("run-1:review");
    let decision = decision_from_resume(
        &json!({ "rejected": ["review"], "approved": ["review"] }),
        &req,
    )
    .expect("a decision");
    assert!(!decision.approved);
}

#[test]
fn resume_reads_a_verdict_inline_or_nested() {
    let req = request("run-1:review");

    let inline = decision_from_resume(
        &json!({ "approved": true, "decided_by": "ada", "comment": "ship it" }),
        &req,
    )
    .expect("a decision");
    assert!(inline.approved);
    assert_eq!(inline.decided_by.as_deref(), Some("ada"));
    assert_eq!(inline.comment.as_deref(), Some("ship it"));

    let nested = decision_from_resume(
        &json!({ "decision": { "approved": false, "comment": "wrong link" } }),
        &req,
    )
    .expect("a decision");
    assert!(!nested.approved);
    assert_eq!(nested.comment.as_deref(), Some("wrong link"));

    assert!(
        decision_from_resume(&json!({ "unrelated": true }), &req).is_none(),
        "a resume that says nothing about this review leaves it pending"
    );
}

/// A review can be addressed by node id or by its own request id — a host that
/// tracks reviews by request id must be able to resume with that.
#[test]
fn a_review_answers_to_its_node_id_and_its_request_id() {
    let req = request("run-1:review");
    assert!(names(Some(&json!(["review"])), &req));
    assert!(names(Some(&json!(["run-1:review"])), &req));
    assert!(!names(Some(&json!(["other"])), &req));
    assert!(!names(None, &req));
}

#[test]
fn positive_config_fields_fall_back_on_zero_or_garbage() {
    assert_eq!(positive_u64(&json!({}), "max_polls", 7), 7);
    assert_eq!(positive_u64(&json!({ "max_polls": 0 }), "max_polls", 7), 7);
    assert_eq!(positive_u64(&json!({ "max_polls": 3 }), "max_polls", 7), 3);
}

#[tokio::test]
async fn an_approved_review_emits_the_subject_on_the_approved_port() {
    let graph = wf(json!({
        "title": "Publish this?",
        "subject_kind": "url",
        "subject": "=item.url",
    }));
    let compiled = compile(&graph).expect("compile");
    let out = run(
        &compiled,
        json!({ "url": "https://example.com/post/1" }),
        &mock_capabilities(),
    )
    .await
    .expect("run");

    let slot = &out.output["nodes"]["review"];
    assert_eq!(slot["port"], "approved");
    let item = &slot["items"][0]["json"];
    assert_eq!(item["approved"], json!(true));
    assert_eq!(item["subject"], "https://example.com/post/1");
    assert_eq!(item["subject_kind"], "url");
    assert_eq!(item["decided_by"], "mock-reviewer");
    assert_eq!(
        slot["decision"]["approved"],
        json!(true),
        "the verdict is addressable as =nodes.review.decision.approved"
    );
}

#[tokio::test]
async fn the_subject_defaults_to_the_item_that_arrived() {
    let graph = wf(json!({ "title": "Look at this" }));
    let compiled = compile(&graph).expect("compile");
    let out = run(
        &compiled,
        json!({ "draft": "hello world" }),
        &mock_capabilities(),
    )
    .await
    .expect("run");

    assert_eq!(
        out.output["nodes"]["review"]["items"][0]["json"]["subject"],
        json!({ "draft": "hello world" })
    );
}

#[tokio::test]
async fn a_rejection_routes_to_the_rejected_port_with_its_reason() {
    let graph = wf(json!({ "title": "Publish this?" }));
    let compiled = compile(&graph).expect("compile");
    let out = run(
        &compiled,
        json!({ "url": "https://example.com" }),
        &mock_capabilities_with_approvals(MockApprovals::rejecting()),
    )
    .await
    .expect("run");

    let slot = &out.output["nodes"]["review"];
    assert_eq!(slot["port"], "rejected");
    assert_eq!(slot["items"][0]["json"]["approved"], json!(false));
    assert_eq!(slot["items"][0]["json"]["comment"], "mock rejection");
}

#[tokio::test]
async fn on_reject_error_fails_the_node_with_the_reviewer_and_reason() {
    let graph = wf(json!({ "on_reject": "error" }));
    let compiled = compile(&graph).expect("compile");
    let err = run(
        &compiled,
        Value::Null,
        &mock_capabilities_with_approvals(MockApprovals::rejecting()),
    )
    .await
    .expect_err("a rejection with on_reject: error fails the run");

    let message = err.to_string();
    assert!(message.contains("mock-reviewer"), "got {message}");
    assert!(message.contains("mock rejection"), "got {message}");
}

#[tokio::test]
async fn a_pending_review_pauses_the_run_and_names_itself() {
    let graph = wf(json!({ "title": "Publish this?" }));
    let compiled = compile(&graph).expect("compile");
    let out = run(
        &compiled,
        Value::Null,
        &mock_capabilities_with_approvals(MockApprovals::pending()),
    )
    .await
    .expect("run");

    assert_eq!(out.pending_approvals, vec!["review".to_string()]);
}

/// The zero-capability path: a host that wired no provider still gets a
/// working node, because waiting *is* the pause it already knows how to resume.
#[tokio::test]
async fn with_no_provider_the_node_reduces_to_a_pause_the_host_resumes() {
    let caps = crate::caps::Capabilities {
        approvals: None,
        ..mock_capabilities()
    };
    let graph = wf(json!({ "title": "Publish this?" }));
    let compiled = compile(&graph).expect("compile");

    let paused = run(&compiled, Value::Null, &caps).await.expect("run");
    assert_eq!(paused.pending_approvals, vec!["review".to_string()]);

    let resumed = resume(&compiled, Value::Null, vec!["review".to_string()], &caps)
        .await
        .expect("resume");
    assert!(resumed.pending_approvals.is_empty());
    assert_eq!(resumed.output["nodes"]["review"]["port"], "approved");
}

/// An approval already on the run input settles the review without the host
/// ever being asked — otherwise a resume would re-open a decided review.
#[tokio::test]
async fn a_listed_approval_settles_the_review_without_asking_the_host() {
    let graph = wf(json!({ "title": "Publish this?" }));
    let compiled = compile(&graph).expect("compile");
    let provider = std::sync::Arc::new(MockApprovals::pending());
    let caps = crate::caps::Capabilities {
        approvals: Some(provider.clone()),
        ..mock_capabilities()
    };

    let out = run(
        &compiled,
        RunInput::new(Value::Null).with_approvals(vec!["review".to_string()]),
        &caps,
    )
    .await
    .expect("run");

    assert_eq!(out.output["nodes"]["review"]["port"], "approved");
    assert!(
        provider.requested().is_empty(),
        "a settled review must not be handed to the provider again"
    );
}

#[tokio::test]
async fn polling_spends_a_bounded_budget_then_follows_on_timeout() {
    let graph = wf(json!({
        "wait_mode": "poll",
        "poll_interval_ms": 1,
        "max_polls": 2,
        "on_timeout": "route",
    }));
    let compiled = compile(&graph).expect("compile");
    let provider = std::sync::Arc::new(MockApprovals::pending());
    let caps = crate::caps::Capabilities {
        approvals: Some(provider.clone()),
        ..mock_capabilities()
    };

    let out = run(&compiled, Value::Null, &caps).await.expect("run");

    let slot = &out.output["nodes"]["review"];
    assert_eq!(slot["port"], "timeout");
    assert_eq!(slot["items"][0]["json"]["timed_out"], json!(true));

    // Every activation asked about the SAME review: the create-or-fetch
    // contract is what stops a poll loop notifying a human once per poll.
    let ids = provider.requested();
    assert!(ids.len() > 1, "expected repeated polls, got {ids:?}");
    assert!(
        ids.iter().all(|id| id == &ids[0]),
        "every poll must reuse one request id, got {ids:?}"
    );
    assert_eq!(
        provider.cancelled(),
        ids[..1].to_vec(),
        "a timed-out review is withdrawn rather than left in a queue"
    );
}

#[tokio::test]
async fn on_timeout_error_is_the_default_and_names_the_node() {
    let graph = wf(json!({ "wait_mode": "poll", "poll_interval_ms": 1, "max_polls": 1 }));
    let compiled = compile(&graph).expect("compile");
    let err = run(
        &compiled,
        Value::Null,
        &mock_capabilities_with_approvals(MockApprovals::pending()),
    )
    .await
    .expect_err("an unanswered review fails by default");
    assert!(err.to_string().contains("review"), "got {err}");
}

/// Without an explicit `request_id` and without a run-scoped identity the node
/// must refuse to guess: falling back to the bare node id would let a later
/// run of the same graph reuse an earlier run's decision through the
/// provider's create-or-fetch contract, and route an unreviewed subject
/// straight through `approved`.
#[tokio::test]
async fn a_missing_request_id_and_run_id_is_a_configuration_error() {
    let graph = wf_raw(json!({ "title": "Publish this?" }));
    let compiled = compile(&graph).expect("compile");
    let err = run(&compiled, Value::Null, &mock_capabilities())
        .await
        .expect_err("no request_id and no run id must be refused");
    let message = err.to_string();
    assert!(message.contains("request_id"), "got {message}");
}

/// With a run-scoped id available (`trigger.run_id`, in the shape
/// `run.trigger.run_id` a host's trigger payload takes), the node derives a
/// stable `"<run id>:<node id>"` request id without needing an explicit
/// `config.request_id`.
#[tokio::test]
async fn a_run_id_in_the_trigger_derives_a_stable_request_id() {
    let graph = wf_raw(json!({ "title": "Publish this?" }));
    let compiled = compile(&graph).expect("compile");
    let out = run(
        &compiled,
        json!({ "run_id": "run-42" }),
        &mock_capabilities(),
    )
    .await
    .expect("a run id makes the request_id derivable");

    assert_eq!(
        out.output["nodes"]["review"]["items"][0]["json"]["request_id"],
        "run-42:review"
    );
}

/// `on_reject: "drop"` emits nothing — a regression that accidentally emitted
/// an item here would go unnoticed by every other rejection test, which all
/// use `route`.
#[tokio::test]
async fn on_reject_drop_emits_nothing_but_still_records_the_decision() {
    let graph = wf(json!({ "on_reject": "drop" }));
    let compiled = compile(&graph).expect("compile");
    let out = run(
        &compiled,
        Value::Null,
        &mock_capabilities_with_approvals(MockApprovals::rejecting()),
    )
    .await
    .expect("run");

    let slot = &out.output["nodes"]["review"];
    assert_eq!(
        slot["items"], json!([]),
        "on_reject: drop must not emit an item"
    );
    assert_eq!(
        slot["decision"]["approved"],
        json!(false),
        "the verdict stays addressable as =nodes.review.decision.approved even when dropped"
    );
}

/// `on_timeout: "reject"` hands the timed-out review to the `on_reject`
/// policy. Covers all three `on_reject` sub-paths so a regression in any one
/// of them is caught here rather than by a host.
#[tokio::test]
async fn on_timeout_reject_follows_the_on_reject_policy() {
    // route: timed_out item lands on `rejected`, with the fields a settled
    // review always carries (not just `approved`/`timed_out`).
    let graph = wf(json!({
        "wait_mode": "poll",
        "poll_interval_ms": 1,
        "max_polls": 1,
        "on_timeout": "reject",
    }));
    let compiled = compile(&graph).expect("compile");
    let out = run(
        &compiled,
        Value::Null,
        &mock_capabilities_with_approvals(MockApprovals::pending()),
    )
    .await
    .expect("run");
    let slot = &out.output["nodes"]["review"];
    assert_eq!(slot["port"], "rejected");
    let item = &slot["items"][0]["json"];
    assert_eq!(item["approved"], json!(false));
    assert_eq!(item["timed_out"], json!(true));
    assert_eq!(item["edited"], json!(false));
    assert_eq!(item["decided_by"], Value::Null);
    assert!(
        item["comment"].as_str().is_some_and(|c| !c.is_empty()),
        "a timed-out rejection still carries a comment explaining why, got {item:?}"
    );
    assert_eq!(
        slot["decision"]["approved"],
        json!(false),
        "=nodes.review.decision.approved must resolve to false after a timeout, not be absent"
    );

    // drop: nothing emitted.
    let graph = wf(json!({
        "wait_mode": "poll",
        "poll_interval_ms": 1,
        "max_polls": 1,
        "on_timeout": "reject",
        "on_reject": "drop",
    }));
    let compiled = compile(&graph).expect("compile");
    let out = run(
        &compiled,
        Value::Null,
        &mock_capabilities_with_approvals(MockApprovals::pending()),
    )
    .await
    .expect("run");
    assert_eq!(out.output["nodes"]["review"]["items"], json!([]));

    // error: the node fails rather than silently continuing.
    let graph = wf(json!({
        "wait_mode": "poll",
        "poll_interval_ms": 1,
        "max_polls": 1,
        "on_timeout": "reject",
        "on_reject": "error",
    }));
    let compiled = compile(&graph).expect("compile");
    let err = run(
        &compiled,
        Value::Null,
        &mock_capabilities_with_approvals(MockApprovals::pending()),
    )
    .await
    .expect_err("on_timeout: reject with on_reject: error must fail the node");
    assert!(err.to_string().contains("on_reject"), "got {err}");
}

/// A decision that carries the reviewer's own edit (`payload`) drives
/// `subject` to that edit and marks `edited: true` — the feature this node
/// exists for, exercised through the resume channel here since `MockApprovals`
/// has no payload-bearing outcome.
#[tokio::test]
async fn a_decision_with_a_payload_edits_the_subject() {
    let graph = wf(json!({ "subject": "=item.url" }));
    let compiled = compile(&graph).expect("compile");
    let caps = mock_capabilities_with_approvals(MockApprovals::pending());

    let paused = run(
        &compiled,
        json!({ "url": "https://example.com/original" }),
        &caps,
    )
    .await
    .expect("run pauses on the poll budget or the pause path");
    assert_eq!(paused.pending_approvals, vec!["review".to_string()]);

    let resumed = crate::engine::resume_with(
        &compiled,
        Value::Null,
        json!({
            "decision": {
                "approved": true,
                "decided_by": "ada",
                "payload": "https://example.com/edited",
            }
        }),
        &caps,
    )
    .await
    .expect("resume with an edited payload");

    let item = &resumed.output["nodes"]["review"]["items"][0]["json"];
    assert_eq!(item["approved"], json!(true));
    assert_eq!(item["subject"], "https://example.com/edited");
    assert_eq!(item["edited"], json!(true));
    assert_eq!(item["decided_by"], "ada");
}

/// When a resume (or a listed approval) settles the review, any provider card
/// opened by an earlier `decide` call must be withdrawn — otherwise the
/// provider's queue keeps a stale entry for a review the run already closed.
#[tokio::test]
async fn a_resume_decision_withdraws_the_provider_card() {
    let graph = wf(json!({ "wait_mode": "poll", "poll_interval_ms": 1, "max_polls": 5 }));
    let compiled = compile(&graph).expect("compile");
    let provider = std::sync::Arc::new(MockApprovals::pending());
    let caps = crate::caps::Capabilities {
        approvals: Some(provider.clone()),
        ..mock_capabilities()
    };

    // First activation asks the provider and gets Pending, opening a card.
    let paused = run(&compiled, Value::Null, &caps).await.expect("run");
    assert_eq!(paused.pending_approvals, vec!["review".to_string()]);
    assert!(
        !provider.requested().is_empty(),
        "the provider must have been asked at least once"
    );
    assert!(provider.cancelled().is_empty(), "no reason to cancel yet");

    // A resume delivers the decision directly (bypassing the provider), so the
    // node must withdraw the provider's now-stale card rather than leave it.
    let resumed = resume(&compiled, Value::Null, vec!["review".to_string()], &caps)
        .await
        .expect("resume");
    assert_eq!(resumed.output["nodes"]["review"]["port"], "approved");
    assert!(
        !provider.cancelled().is_empty(),
        "the provider's card must be withdrawn once a resume settles the review"
    );
}

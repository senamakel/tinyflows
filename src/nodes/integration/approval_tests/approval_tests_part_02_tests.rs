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

/// `validate::validate_all` only sees the config as authored — a
/// one-element `assignees` array of `=`-bindings passes it. If every binding
/// resolves to something that is not a string (here: a field the trigger
/// input never set, so the binding reads `null`), the array a real review
/// would be routed to is empty at execution time, and that is refused too.
#[tokio::test]
async fn assignees_resolving_to_no_strings_at_runtime_is_a_configuration_error() {
    let graph = wf(json!({ "assignees": ["=item.missing_reviewer"] }));
    let compiled = compile(&graph).expect("compile");
    let err = run(&compiled, json!({}), &mock_capabilities())
        .await
        .expect_err("assignees resolving to zero strings must be refused");
    let message = err.to_string();
    assert!(message.contains("assignees"), "got {message}");
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
        slot["items"],
        json!([]),
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
/// exists for. `decision_from_resume` and `decided_item` are exercised
/// separately elsewhere; this checks the two compose correctly, matching what
/// a `{"decision": {..., "payload": ...}}` resume value produces end to end.
#[test]
fn a_decision_with_a_payload_edits_the_subject() {
    let req = request("run-1:review");
    let resume_value = json!({
        "decision": {
            "approved": true,
            "decided_by": "ada",
            "payload": "https://example.com/edited",
        }
    });
    let decision = decision_from_resume(&resume_value, &req).expect("a decision");
    assert!(decision.approved);
    assert_eq!(decision.payload, Some(json!("https://example.com/edited")));

    let item = decided_item(
        &req,
        &decision,
        json!({ "url": "https://example.com/original" }),
    );
    let json = item.json;
    assert_eq!(json["approved"], json!(true));
    assert_eq!(json["subject"], "https://example.com/edited");
    assert_eq!(json["edited"], json!(true));
    assert_eq!(json["decided_by"], "ada");
    assert_eq!(
        json["input"],
        json!({ "url": "https://example.com/original" })
    );
}

/// When a resume (or a listed approval) settles the review, any provider card
/// opened by an earlier `decide` call must be withdrawn — otherwise the
/// provider's queue keeps a stale entry for a review the run already closed.
#[tokio::test]
async fn a_resume_decision_withdraws_the_provider_card() {
    let graph = wf(json!({ "title": "Publish this?" }));
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
